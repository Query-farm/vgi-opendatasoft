#!/usr/bin/env python3
"""Strict replay server for recorded Opendatasoft Explore API v2.1 responses.

Each portal is served under a path prefix (its *alias*)::

    http://127.0.0.1:<port>/paris/api/explore/v2.1/...   -> https://opendata.paris.fr
    http://127.0.0.1:<port>/public/api/explore/v2.1/...  -> https://public.opendatasoft.com
    http://127.0.0.1:<port>/hub/api/explore/v2.1/...     -> https://data.opendatasoft.com
    http://127.0.0.1:<port>/private/api/explore/v2.1/... -> hand-written, needs an API key

so the worker is pointed at a fixture portal with ``portal := 'http://127.0.0.1:<port>/paris'``.

**Strict.** A request is answered only if its path *and its exact query string*
(``select``, ``where``, ``order_by``, ``limit``, ``offset``, ``refine`` ... after
sorting) were recorded. Anything else gets ``404 UnrecordedRequest``, which fails
the SQL test. That is how the tests assert pushdown: if the worker stops pushing a
filter into ODSQL (or quotes a literal differently), its request no longer matches.

Switches (environment):

* ``MOCK_RECORD=1``  record misses from the live portal once (exports are truncated
  to ``MOCK_EXPORT_ROWS`` rows, default 40). Never set in CI.
* ``MOCK_FLAKY=1``   answer the first request for each URL with 429 + ``Retry-After: 0``.
* ``MOCK_MISS_LOG``  append every unrecorded request to this file.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
UPSTREAM = {
    "paris": "https://opendata.paris.fr",
    "public": "https://public.opendatasoft.com",
    "hub": "https://data.opendatasoft.com",
}
PRIVATE_KEY = "test-key"
RECORD = os.environ.get("MOCK_RECORD") == "1"
FLAKY = os.environ.get("MOCK_FLAKY") == "1"
EXPORT_ROWS = int(os.environ.get("MOCK_EXPORT_ROWS", "40"))
MISS_LOG = os.environ.get("MOCK_MISS_LOG")

_seen: set[str] = set()
_lock = threading.Lock()


def canonical(path: str, query: str) -> str:
    """Path plus the query parameters sorted, so parameter order does not matter."""
    pairs = sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
    return path + ("?" + urllib.parse.urlencode(pairs) if pairs else "")


def fixture_file(alias: str, key: str) -> Path:
    """Where the response for ``key`` is stored."""
    digest = hashlib.sha1(key.encode()).hexdigest()[:16]
    return FIXTURES / alias / f"{digest}.json"


def load(alias: str, key: str) -> dict[str, Any] | None:
    """The recorded response, or None."""
    f = fixture_file(alias, key)
    if not f.exists():
        return None
    data: dict[str, Any] = json.loads(f.read_text())
    if data.get("request") != key:  # hash collision guard
        return None
    return data


def record(alias: str, key: str) -> dict[str, Any]:
    """Fetch ``key`` from the live portal once and store it."""
    url = UPSTREAM[alias] + key
    req = urllib.request.Request(url, headers={"User-Agent": "vgi-opendatasoft-fixture-recorder", "Accept": "*/*"})
    truncated = False
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = resp.status
            ctype = resp.headers.get("Content-Type", "application/json")
            if "/exports/jsonl" in key:
                lines = []
                for line in resp:
                    if len(lines) >= EXPORT_ROWS:
                        truncated = True
                        break
                    lines.append(line.decode())
                body = "".join(lines)
            elif "/exports/json" in key:
                decoder = json.JSONDecoder()
                text = ""
                items: list[Any] = []
                chunk = resp.read(1 << 16)
                while chunk and len(items) < EXPORT_ROWS:
                    text += chunk.decode()
                    text = text.lstrip()
                    if text.startswith("["):
                        text = text[1:]
                    while len(items) < EXPORT_ROWS:
                        text = text.lstrip().lstrip(",").lstrip()
                        try:
                            obj, end = decoder.raw_decode(text)
                        except ValueError:
                            break
                        items.append(obj)
                        text = text[end:]
                    chunk = resp.read(1 << 16)
                truncated = bool(chunk) or bool(text.strip(" ]\n"))
                body = json.dumps(items)
            else:
                body = resp.read().decode()
    except urllib.error.HTTPError as err:
        status, ctype, body = err.code, "application/json", err.read().decode()
    data = {"request": key, "status": status, "content_type": ctype, "truncated": truncated, "body": body}
    f = fixture_file(alias, key)
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n")
    print(f"recorded {alias}{key} -> {f.name}", file=sys.stderr)
    return data


class Handler(BaseHTTPRequestHandler):
    """Serve recorded responses; refuse anything unrecorded."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Log requests to stderr only."""
        sys.stderr.write("mock: " + (format % args) + "\n")

    def _send(
        self, status: int, body: str, ctype: str = "application/json", headers: dict[str, str] | None = None
    ) -> None:
        raw = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        """Answer one GET."""
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/health":
            self._send(200, '{"status": "ok"}')
            return
        parts = parsed.path.split("/", 2)
        alias = parts[1] if len(parts) > 1 else ""
        rest = "/" + parts[2] if len(parts) > 2 else "/"
        key = canonical(rest, parsed.query)
        if FLAKY:
            with _lock:
                first = key not in _seen
                _seen.add(key)
            if first:
                self._send(
                    429, '{"error_code": "TooManyRequests", "message": "slow down"}', headers={"Retry-After": "0"}
                )
                return
        if alias == "private" and self.headers.get("Authorization") != f"Apikey {PRIVATE_KEY}":
            self._send(401, '{"error_code": "InvalidAPIKey", "message": "This dataset requires an API key."}')
            return
        data = load(alias, key)
        if data is None and RECORD and alias in UPSTREAM:
            data = record(alias, key)
        if data is None:
            print(f"mock: UNRECORDED {alias}{key}", file=sys.stderr)
            if MISS_LOG:
                with open(MISS_LOG, "a") as log:
                    log.write(f"{alias}{key}\n")
            msg = json.dumps({"error_code": "UnrecordedRequest", "message": f"no fixture for {alias}{key}"})
            self._send(404, msg)
            return
        self._send(int(data["status"]), data["body"], data.get("content_type") or "application/json")


def main() -> None:
    """Listen on a free port (or --port N) and print PORT:<n>."""
    port = 0
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
