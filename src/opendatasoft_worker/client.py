"""HTTP plumbing for the Opendatasoft (Huwise) Explore API v2.1.

* ``normalize_portal`` turns ``opendata.paris.fr``, ``https://opendata.paris.fr/explore/...``
  or a self-hosted/fixture URL into a canonical base URL without a trailing slash.
* ``OdsClient.get_json`` performs one GET with timeouts and retries 429/5xx with
  backoff that honours ``Retry-After``.
* ``OdsClient.open_stream`` opens an export (``jsonl`` or a ``json`` array) and
  yields one decoded object at a time, so a 500,000-row export never sits in memory.

The API key (if any) is sent as ``Authorization: Apikey <key>``, never in the URL,
so it cannot leak into logs or error messages.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import IO, Any, cast

log = logging.getLogger(__name__)

API_PATH = "/api/explore/v2.1"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 5
MAX_RETRY_DELAY = 30.0
USER_AGENT = "vgi-opendatasoft-worker/0.1 (+https://query.farm)"

_HOST_RE = re.compile(r"^[A-Za-z0-9.-]+(:\d+)?(/.*)?$")


class OdsError(RuntimeError):
    """The portal returned an error, or a request could not be made."""


def normalize_portal(value: str) -> str:
    """Return the canonical base URL of a portal.

    ``opendata.paris.fr`` becomes ``https://opendata.paris.fr``. A pasted page or API
    URL is trimmed at ``/explore`` or ``/api``. Any other path is kept, so a portal
    served under a prefix (or a local fixture server) works.

    Args:
        value: A domain, or an http(s) URL.

    Returns:
        The base URL with no trailing slash.

    Raises:
        OdsError: The value is empty or not a domain/URL.

    """
    text = (value or "").strip()
    if not text:
        raise OdsError("portal is empty: pass a domain such as 'opendata.paris.fr' or an https:// URL")
    if "://" not in text:
        if not _HOST_RE.match(text):
            raise OdsError(f"portal {value!r} is not a domain or URL, e.g. 'opendata.paris.fr'")
        text = "https://" + text
    parsed = urllib.parse.urlsplit(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise OdsError(f"portal {value!r} must be an http(s) URL or a domain")
    path = parsed.path
    for marker in ("/api/", "/explore/", "/explore"):
        idx = path.find(marker)
        if idx >= 0:
            path = path[:idx]
            break
    if path.endswith("/api"):
        path = path[: -len("/api")]
    return f"{parsed.scheme}://{parsed.netloc.lower()}{path.rstrip('/')}"


def portal_label(base_url: str) -> str:
    """Return the short portal name used in result columns: the host for https portals, else the URL."""
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme == "https" and not parsed.path:
        return parsed.netloc
    return base_url


def dataset_page_url(base_url: str, dataset_id: str) -> str:
    """Return the portal's human-facing page for a dataset."""
    return f"{base_url}/explore/dataset/{urllib.parse.quote(dataset_id, safe='@')}/"


@dataclass
class OdsClient:
    """A small client bound to one portal and (optionally) one API key."""

    base_url: str
    apikey: str | None = None

    def url(self, path: str, params: dict[str, Any] | None = None) -> str:
        """Build ``<base>/api/explore/v2.1<path>?<params>`` (None values dropped, keys sorted)."""
        pairs: list[tuple[str, str]] = []
        for k, v in (params or {}).items():
            for item in v if isinstance(v, (list, tuple)) else [v]:
                if item is not None and item != "":
                    pairs.append((k, str(item)))
        query = urllib.parse.urlencode(sorted(pairs))
        return f"{self.base_url}{API_PATH}{path}" + (f"?{query}" if query else "")

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.apikey:
            headers["Authorization"] = f"Apikey {self.apikey}"
        return headers

    def _open(self, url: str) -> IO[bytes]:
        """Open ``url``, retrying 429/5xx and connection errors with backoff."""
        for attempt in range(1, MAX_ATTEMPTS + 1):
            request = urllib.request.Request(url, headers=self._headers())
            try:
                response: IO[bytes] = urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS)
                return _decoded(response)
            except urllib.error.HTTPError as err:
                retryable = err.code == 429 or err.code >= 500
                if not retryable or attempt == MAX_ATTEMPTS:
                    raise OdsError(_describe_http_error(url, err)) from err
                retry_after = err.headers.get("Retry-After") if err.headers else None
                delay = _retry_delay(retry_after, attempt)
                log.info("HTTP %s from %s; retrying in %.1fs", err.code, _strip_query(url), delay)
                time.sleep(delay)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise OdsError(f"GET {_strip_query(url)} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_RETRY_DELAY))
        raise AssertionError("unreachable")

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET a JSON document from the Explore API."""
        url = self.url(path, params)
        log.debug("GET %s", url)
        with self._open(url) as response:
            return json.load(response)

    def open_stream(self, path: str, params: dict[str, Any] | None, *, fmt: str) -> ExportStream:
        """Open an export and return a stream of decoded rows.

        Args:
            path: API path of the export, e.g. ``/catalog/datasets/x/exports/jsonl``.
            params: Query parameters (ODSQL clauses).
            fmt: ``jsonl`` (one object per line) or ``json`` (one array).

        Returns:
            An open :class:`ExportStream`.

        """
        url = self.url(path, params)
        log.debug("EXPORT %s", url)
        response = self._open(url)
        rows = _iter_jsonl(response) if fmt == "jsonl" else _iter_json_array(response)
        return ExportStream(response=response, rows=rows)


class _GzipResponse(gzip.GzipFile):
    """A gzip-encoded HTTP response, decompressed as it is read; closing it closes the socket."""

    def __init__(self, raw: IO[bytes]) -> None:
        super().__init__(fileobj=raw, mode="rb")
        self._raw = raw

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._raw.close()


def _decoded(response: IO[bytes]) -> IO[bytes]:
    """Undo ``Content-Encoding: gzip``.

    urllib never asks for compression, but some portals (opendata.paris.fr) gzip
    every response anyway, and urllib hands the compressed bytes through as-is.
    """
    headers = getattr(response, "headers", None)
    encoding = (headers.get("Content-Encoding") if headers else None) or ""
    if encoding.strip().lower() in ("gzip", "x-gzip"):
        return cast("IO[bytes]", _GzipResponse(response))
    return response


def _strip_query(url: str) -> str:
    return url.split("?", 1)[0]


def _retry_delay(retry_after: str | None, attempt: int) -> float:
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_RETRY_DELAY)
        except ValueError:
            pass
    return float(min(0.5 * 2**attempt, MAX_RETRY_DELAY))


def _describe_http_error(url: str, err: urllib.error.HTTPError) -> str:
    """Return a readable error: HTTP status plus the portal's ``error_code``/``message`` when present."""
    body = b""
    with contextlib.suppress(Exception):
        body = err.read()
        if (err.headers.get("Content-Encoding") or "").strip().lower() in ("gzip", "x-gzip"):
            body = gzip.decompress(body)
        body = body[:2000]
    detail = body.decode(errors="replace").strip()
    try:
        parsed = json.loads(detail)
        if isinstance(parsed, dict) and parsed.get("message"):
            detail = f"{parsed.get('error_code', 'error')}: {parsed['message']}"
    except ValueError:
        pass
    hint = ""
    if err.code in (401, 403):
        hint = " (private dataset? CREATE SECRET (TYPE opendatasoft, APIKEY '...', SCOPE '<portal url>'))"
    elif err.code == 404:
        hint = " (unknown portal or dataset_id, or the dataset is private)"
    return f"GET {_strip_query(url)} failed with HTTP {err.code}: {detail}{hint}"


def _iter_jsonl(response: IO[bytes]) -> Iterator[dict[str, Any]]:
    for line in response:
        line = line.strip()
        if line:
            yield json.loads(line)


def _iter_json_array(response: IO[bytes], chunk_size: int = 1 << 16) -> Iterator[dict[str, Any]]:
    """Decode the elements of a top-level JSON array without reading it all into memory."""
    decoder = json.JSONDecoder()
    buf = ""
    started = False
    eof = False
    while True:
        # Skip separators.
        i = 0
        while i < len(buf) and (buf[i].isspace() or buf[i] == "," or (not started and buf[i] == "[")):
            if buf[i] == "[":
                started = True
            i += 1
        buf = buf[i:]
        if buf.startswith("]"):
            return
        if buf:
            try:
                obj, end = decoder.raw_decode(buf)
            except json.JSONDecodeError:
                if eof:
                    raise OdsError("export ended in the middle of a JSON value") from None
            else:
                yield obj
                buf = buf[end:]
                continue
        if eof:
            return
        chunk = response.read(chunk_size)
        if not chunk:
            eof = True
        else:
            buf += chunk.decode("utf-8")


@dataclass
class ExportStream:
    """An open export: the HTTP response plus an iterator of decoded rows."""

    response: IO[bytes]
    rows: Iterator[dict[str, Any]]
    position: int = 0
    last_used: float = field(default_factory=time.monotonic)

    def take(self, n: int) -> list[dict[str, Any]]:
        """Read up to ``n`` more rows."""
        out: list[dict[str, Any]] = []
        for row in self.rows:
            out.append(row)
            if len(out) >= n:
                break
        self.position += len(out)
        self.last_used = time.monotonic()
        return out

    def skip(self, n: int) -> None:
        """Discard ``n`` rows (used when resuming in a process that did not open the stream)."""
        for _ in range(n):
            if next(self.rows, None) is None:
                break
            self.position += 1

    def close(self) -> None:
        """Close the HTTP response."""
        with contextlib.suppress(Exception):
            self.response.close()


class StreamCache:
    """Open export streams kept per process, keyed by a token stored in scan state.

    A scan's state carries only the token and the number of rows already
    emitted. If the next ``process`` call lands in the same process the stream
    simply continues; otherwise (HTTP transport behind a load balancer) the
    export is reopened and the already-emitted rows are skipped.
    """

    def __init__(self, max_streams: int = 8, idle_seconds: float = 600.0) -> None:
        """Create an empty cache."""
        self._streams: OrderedDict[str, ExportStream] = OrderedDict()
        self._lock = threading.Lock()
        self._max = max_streams
        self._idle = idle_seconds

    @staticmethod
    def new_token() -> str:
        """Return a fresh random token for a new scan."""
        return uuid.uuid4().hex

    def get(self, token: str, position: int) -> ExportStream | None:
        """Return the cached stream for ``token`` if it is exactly at ``position``."""
        with self._lock:
            self._evict_idle()
            stream = self._streams.get(token)
            if stream is None:
                return None
            if stream.position != position:
                self._streams.pop(token, None)
                stream.close()
                return None
            self._streams.move_to_end(token)
            return stream

    def put(self, token: str, stream: ExportStream) -> None:
        """Remember an open stream, closing the least recently used one if full."""
        with self._lock:
            self._streams[token] = stream
            self._streams.move_to_end(token)
            while len(self._streams) > self._max:
                _, old = self._streams.popitem(last=False)
                old.close()

    def drop(self, token: str) -> None:
        """Close and forget a stream."""
        with self._lock:
            stream = self._streams.pop(token, None)
        if stream is not None:
            stream.close()

    def _evict_idle(self) -> None:
        now = time.monotonic()
        for token in [t for t, s in self._streams.items() if now - s.last_used > self._idle]:
            self._streams.pop(token).close()


STREAMS = StreamCache()


class TtlCache:
    """A tiny per-process cache for bind-time metadata lookups (binds can repeat)."""

    def __init__(self, ttl: float = 120.0, max_items: int = 64) -> None:
        """Create an empty cache."""
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._ttl = ttl
        self._max = max_items
        self._lock = threading.Lock()

    def get(self, key: str) -> Any:
        """Return the cached value, or None if absent or expired."""
        with self._lock:
            hit = self._items.get(key)
            if hit is None or time.monotonic() - hit[0] > self._ttl:
                return None
            return hit[1]

    def put(self, key: str, value: Any) -> None:
        """Cache ``value`` under ``key``."""
        with self._lock:
            self._items[key] = (time.monotonic(), value)
            self._items.move_to_end(key)
            while len(self._items) > self._max:
                self._items.popitem(last=False)
