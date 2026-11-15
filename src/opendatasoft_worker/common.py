"""Shared plumbing: portals, scoped API keys, and the resumable scan engine.

Every function takes the portal as its first argument: a domain such as
``opendata.paris.fr`` or a full URL (self-hosted portals and local fixtures).

API keys come only from ``opendatasoft`` secrets whose ``SCOPE`` covers the portal
URL, so a key for one portal is never sent to another. An unscoped secret is ignored.

Scans (``datasets`` and ``records``) choose how to read automatically:

* ``count``   no columns are needed (``count(*)``): one ``limit=0`` request returns
  ``total_count`` and that many empty rows are emitted.
* ``topn``    DuckDB pushed an ``ORDER BY col LIMIT n`` hint on a numeric/date column
  (n <= 10,000): the portal sorts and only n rows (non-null, then null) are fetched.
* ``records`` the filtered result has at most 10,000 rows (the Explore API's
  ``offset + limit`` cap): pages of 100 via the records endpoint.
* ``export``  more than 10,000 rows: the export endpoint is streamed (no cap),
  a batch at a time.

The ODSQL ``select``/``where``/``order_by`` are computed once, on the first call,
and frozen in the scan state with the paging cursor, so later batches (possibly
served by another process over HTTP) continue the same query.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from vgi.table_function import OutputCollector, ProcessParams
from vgi_rpc import ArrowSerializableDataclass

from .client import STREAMS, OdsClient, OdsError, normalize_portal
from .odsql import (
    PUSHABLE_RANGE,
    combine_where,
    convert_value,
    ident,
    to_arrow_batch,
    where_clauses,
)

log = logging.getLogger(__name__)

SECRET_TYPE = "opendatasoft"
SECRET_KEY_FIELD = "apikey"

PORTAL_DOC = (
    "The Opendatasoft portal: a domain such as 'opendata.paris.fr', 'public.opendatasoft.com' or "
    "'data.opendatasoft.com', or an https:// URL (a pasted dataset page URL is trimmed to the portal root)."
)
PORTAL_PATTERN = r"^(https?://)?[A-Za-z0-9.-]+"

RECORDS_CAP = 10_000
PAGE_SIZE = 100
EXPORT_BATCH = 2_000
COUNT_BATCH = 50_000


# ---------------------------------------------------------------- secrets


def _str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "as_py"):
        value = value.as_py()
    return "" if value is None else str(value)


def scope_match_length(scope: str, portal: str) -> int:
    """Length of the longest scope prefix covering ``portal`` at a path boundary, else -1."""
    best = -1
    for prefix in scope.split("\n"):
        p = prefix.strip().rstrip("/")
        if not p:
            continue
        if "://" not in p:
            p = "https://" + p
        p = p.lower() if "/" not in p.split("://", 1)[1] else p
        if (portal == p or portal.startswith(p + "/")) and len(p) > best:
            best = len(p)
    return best


def apikey_from(candidates: list[dict[str, Any]], portal: str) -> str | None:
    """Return the API key of the best-matching *scoped* ``opendatasoft`` secret, or None.

    Args:
        candidates: Secret dicts (field name to value).
        portal: The normalized portal URL.

    Returns:
        The key, or None for anonymous access.

    """
    best: tuple[int, str] | None = None
    unscoped = False
    for fields in candidates:
        key = _str(fields.get(SECRET_KEY_FIELD))
        scope = _str(fields.get("scope"))
        if not key:
            continue
        if not scope:
            unscoped = True
            continue
        n = scope_match_length(scope, portal)
        if n >= 0 and (best is None or n > best[0]):
            best = (n, key)
    if best is None and unscoped:
        log.warning("ignoring an opendatasoft secret without SCOPE; scope it to the portal URL to use it")
    return best[1] if best else None


def request_secret(params: Any, portal: str) -> bool:
    """Ask DuckDB for the ``opendatasoft`` secret scoped to ``portal`` (bind time).

    Args:
        params: Bind parameters.
        portal: The normalized portal URL.

    Returns:
        True once secrets are resolved (the bind may proceed), False while the
        framework still has to round-trip a secret-scope request to DuckDB.

    """
    params.secrets.get(SECRET_TYPE, scope=portal)
    return bool(params.secrets.all_resolved)


def bind_client(params: Any, portal: str) -> OdsClient:
    """Return a client for bind-time calls (after :func:`request_secret` returned True)."""
    secret = params.secrets.get(SECRET_TYPE, scope=portal)
    return OdsClient(portal, apikey_from([secret] if secret else [], portal))


def process_client(params: ProcessParams[Any], portal: str) -> OdsClient:
    """Return a client for this call's portal, with its scoped API key if one exists."""
    secrets = params.secrets
    candidates = list(secrets.of_type(SECRET_TYPE)) if secrets else []
    return OdsClient(portal, apikey_from(candidates, portal))


def portal_arg(params: Any) -> str:
    """Return the normalized portal URL from the ``portal`` argument."""
    return normalize_portal(str(params.args.portal))


# ---------------------------------------------------------------- scans


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor for a scan. Holds the frozen query, never credentials.

    ``phases`` are ODSQL ``where`` clauses read one after another (a sorted scan
    reads the non-null values of the sort column, then the nulls). ``order_col``
    and ``last_value`` (JSON of the last sort value emitted) let a sorted scan stop
    as soon as DuckDB's Top-N filter rules out everything still to come.
    """

    mode: str = ""
    select: str = ""
    order_by: str = ""
    phases: list[str] = field(default_factory=list)
    phase: int = 0
    offset: int = 0
    total: int = -1
    emitted: int = 0
    row_limit: int = -1
    token: str = ""
    order_col: str = ""
    nonnull_phase: int = -1
    last_value: str = ""


@dataclass
class ScanSpec:
    """How to read one scan: endpoints, base clauses and column mapping.

    Attributes:
        records_path: Paged endpoint (``.../records`` or ``/catalog/datasets``).
        export_path: Streamed export endpoint.
        export_fmt: ``jsonl`` or ``json``.
        base_where: ODSQL clauses from arguments (``filter :=``, ``q :=``).
        refine: ``refine`` values.
        extra_params: Other fixed query parameters.
        pushable: Output column -> (ODS field, ODS type) for filter pushdown.
        keys: Output column -> JSON key in each result row.
        selectable: Output column -> ODSQL select expression.
        postprocess: Optional hook turning a decoded row into the output row dict.

    """

    records_path: str
    export_path: str
    export_fmt: str
    base_where: list[str]
    refine: list[str]
    extra_params: dict[str, Any]
    pushable: dict[str, tuple[str, str]]
    keys: dict[str, str]
    selectable: dict[str, str]
    postprocess: Callable[[dict[str, Any]], dict[str, Any]] | None = None


def _start(params: ProcessParams[Any], state: ScanState, spec: ScanSpec, client: OdsClient) -> None:
    """Freeze the query and choose the read mode."""
    names = params.output_schema.names
    select_parts: list[str] = []
    for n in names:
        expr = spec.selectable.get(n)
        if expr and expr not in select_parts:
            select_parts.append(expr)
    state.select = ",".join(select_parts)
    filters = params.current_pushdown_filters
    init = params.init_call
    log.debug(
        "scan start: filters=%r order_hint=%s/%s/%s",
        getattr(filters, "filters", None),
        getattr(init, "order_by_column_name", None),
        getattr(init, "order_by_direction", None),
        getattr(init, "order_by_limit", None),
    )
    where = combine_where([*spec.base_where, *where_clauses(filters, spec.pushable)])

    if not names:
        body = client.get_json(spec.records_path, _params(spec, where=where, limit=0))
        state.total = int(body.get("total_count") or 0)
        state.mode = "count"
        state.phases = [where or ""]
        return

    hint = _order_hint(params, spec)
    if hint is not None:
        col, out_col, direction, nulls_first, limit = hint
        not_null = combine_where([where, f"{ident(col)} is not null"]) or ""
        is_null = combine_where([where, f"{ident(col)} is null"]) or ""
        state.order_by = f"{ident(col)} {direction}"
        state.phases = [is_null, not_null] if nulls_first else [not_null, is_null]
        state.nonnull_phase = 1 if nulls_first else 0
        state.order_col = out_col
        if limit is not None:
            state.mode = "topn"
            state.row_limit = limit
            return

    body = client.get_json(spec.records_path, _params(spec, where=where, limit=0))
    state.total = int(body.get("total_count") or 0)
    if not state.phases:
        state.phases = [where or ""]
    state.mode = "records" if state.total <= RECORDS_CAP else "export"
    if state.mode == "export":
        state.token = STREAMS.new_token()
        log.info("%d matching rows exceed the records API cap; streaming the export endpoint", state.total)


def _order_hint(params: ProcessParams[Any], spec: ScanSpec) -> tuple[str, str, str, bool, int | None] | None:
    """DuckDB's ORDER BY [LIMIT] hint, when the portal can sort that column exactly."""
    init = params.init_call
    if init is None or not init.order_by_column_name:
        return None
    name = init.order_by_column_name
    if name not in spec.pushable or name not in params.output_schema.names:
        return None
    ods_field, ods_type = spec.pushable[name]
    if ods_type not in PUSHABLE_RANGE:
        return None  # text collation on the portal may differ from DuckDB's
    limit: int | None = None
    if init.order_by_limit is not None and 0 < int(init.order_by_limit) <= RECORDS_CAP:
        limit = int(init.order_by_limit)
    direction = "desc" if getattr(init.order_by_direction, "name", "ASC") == "DESC" else "asc"
    nulls_first = getattr(init.order_by_null_order, "name", "NULLS_LAST") == "NULLS_FIRST"
    return ods_field, name, direction, nulls_first, limit


def _params(spec: ScanSpec, *, where: str | None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = dict(spec.extra_params)
    if where:
        out["where"] = where
    if spec.refine:
        out["refine"] = list(spec.refine)
    out.update({k: v for k, v in extra.items() if v is not None})
    return out


def _emit(
    params: ProcessParams[Any], state: ScanState, out: OutputCollector, spec: ScanSpec, rows: list[dict[str, Any]]
) -> None:
    if state.order_col:
        key = spec.keys.get(state.order_col, state.order_col)
        state.last_value = json.dumps(rows[-1].get(key))
    if spec.postprocess is not None:
        rows = [spec.postprocess(r) for r in rows]
    out.emit(to_arrow_batch(rows, params.output_schema, spec.keys))


def _past_topn_boundary(params: ProcessParams[Any], state: ScanState, spec: ScanSpec) -> bool:
    """Return True when DuckDB's dynamic Top-N filter rejects the last sort value emitted.

    The scan is sorted on ``order_col``, so every later row would be rejected too.
    """
    if not state.order_col or not state.last_value or state.phase != state.nonnull_phase:
        return False
    filters = getattr(params.current_pushdown_filters, "filters", None) or ()
    log.debug("top-n check: last=%s filters=%r", state.last_value, filters)
    relevant = [f for f in _flatten_and(filters) if getattr(f, "column_name", None) == state.order_col]
    if not relevant:
        return False
    f_out = params.output_schema.field(state.order_col)
    last = convert_value(json.loads(state.last_value), f_out)
    if last is None:
        return False
    for f in relevant:
        bound = f.value.as_py() if hasattr(f.value, "as_py") else f.value
        op = getattr(f.op, "value", str(f.op))
        try:
            passes = _COMPARE[op](last, bound)
        except (TypeError, KeyError):  # incomparable: never stop early on it
            continue
        if not passes:
            log.debug("Top-N boundary passed on %s; stopping the scan early", state.order_col)
            return True
    return False


_COMPARE: dict[str, Callable[[Any, Any], bool]] = {
    "eq": lambda a, b: bool(a == b),
    "ne": lambda a, b: bool(a != b),
    "gt": lambda a, b: bool(a > b),
    "ge": lambda a, b: bool(a >= b),
    "lt": lambda a, b: bool(a < b),
    "le": lambda a, b: bool(a <= b),
}


def _flatten_and(filters: Any) -> list[Any]:
    out: list[Any] = []
    for f in filters:
        if type(f).__name__ == "AndFilter":
            out.extend(_flatten_and(f.children))
        elif type(f).__name__ == "ConstantFilter":
            out.append(f)
    return out


def run_scan(
    params: ProcessParams[Any], state: ScanState, out: OutputCollector, spec: ScanSpec, client: OdsClient
) -> None:
    """Emit the next batch of a scan, or finish it."""
    if not state.mode:
        _start(params, state, spec, client)
    if state.mode != "done" and _past_topn_boundary(params, state, spec):
        STREAMS.drop(state.token)
        state.mode = "done"
    if state.mode == "done":
        out.finish()
        return

    if state.mode == "count":
        remaining = state.total - state.emitted
        if remaining <= 0:
            out.finish()
            return
        n = min(remaining, COUNT_BATCH)
        empty: Any = pa.array([{}] * n, type=pa.struct([]))
        out.emit(pa.RecordBatch.from_struct_array(empty))
        state.emitted += n
        return

    if state.mode in ("records", "topn"):
        _records_step(params, state, out, spec, client)
        return

    _export_step(params, state, out, spec, client)


def _records_step(
    params: ProcessParams[Any], state: ScanState, out: OutputCollector, spec: ScanSpec, client: OdsClient
) -> None:
    while state.phase < len(state.phases):
        if state.row_limit >= 0 and state.emitted >= state.row_limit:
            break
        cap = RECORDS_CAP
        if state.row_limit >= 0:
            cap = min(cap, state.offset + (state.row_limit - state.emitted))
        if state.mode == "records" and state.total >= 0 and len(state.phases) == 1:
            cap = min(cap, state.total)
        limit = min(PAGE_SIZE, cap - state.offset)
        if limit <= 0:
            state.phase += 1
            state.offset = 0
            continue
        body = client.get_json(
            spec.records_path,
            _params(
                spec,
                where=state.phases[state.phase] or None,
                select=state.select,
                order_by=state.order_by or None,
                limit=limit,
                offset=state.offset or None,
            ),
        )
        rows = list(body.get("results") or [])
        state.offset += len(rows)
        state.emitted += len(rows)
        if len(rows) < limit:
            state.phase += 1
            state.offset = 0
        if rows:
            _emit(params, state, out, spec, rows)
            return
    state.mode = "done"
    out.finish()


def _export_step(
    params: ProcessParams[Any], state: ScanState, out: OutputCollector, spec: ScanSpec, client: OdsClient
) -> None:
    while state.phase < len(state.phases):
        stream = STREAMS.get(state.token, state.offset)
        if stream is None:
            where = state.phases[state.phase] or None
            stream = client.open_stream(
                spec.export_path,
                _params(spec, where=where, select=state.select, order_by=state.order_by or None),
                fmt=spec.export_fmt,
            )
            if state.offset:
                log.info("resuming export in a new process: skipping %d rows", state.offset)
                stream.skip(state.offset)
            STREAMS.put(state.token, stream)
        rows = stream.take(EXPORT_BATCH)
        if rows:
            state.offset += len(rows)
            state.emitted += len(rows)
            _emit(params, state, out, spec, rows)
            return
        STREAMS.drop(state.token)
        state.token = STREAMS.new_token()
        state.phase += 1
        state.offset = 0
    state.mode = "done"
    out.finish()


def require_identifier(value: str, what: str) -> str:
    """Validate a dataset id (letters, digits, ``_ - . @``)."""
    v = (value or "").strip()
    if not v or any(c in v for c in "/?#&\\ \t\r\n"):
        raise OdsError(f"invalid {what} {value!r}")
    return v


def strip_html(text: str | None) -> str | None:
    """Plain text from an HTML description (tags removed, whitespace collapsed)."""
    if text is None:
        return None
    from html.parser import HTMLParser

    parts: list[str] = []

    class _P(HTMLParser):
        def handle_data(self, data: str) -> None:
            parts.append(data)

        def handle_starttag(self, tag: str, attrs: Any) -> None:
            if tag in ("p", "br", "li", "div", "h1", "h2", "h3", "h4", "tr"):
                parts.append("\n")

    p = _P(convert_charrefs=True)
    p.feed(text)
    p.close()
    lines = [" ".join(line.split()) for line in "".join(parts).split("\n")]
    return "\n".join(line for line in lines if line) or None
