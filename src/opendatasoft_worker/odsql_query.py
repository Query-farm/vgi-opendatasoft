"""``odsql(portal, dataset_id, select, ...)``: run an ODSQL query on the portal.

Aggregations (``count(*)``, ``sum``, ``avg``, ``group_by``) run server-side, so a
summary of a million-row dataset costs one request. The query runs at bind time
so the result columns can be typed: a column named like a dataset field takes that
field's type; other columns are inferred from their values (BIGINT, DOUBLE,
BOOLEAN, DATE, TIMESTAMPTZ, STRUCT(lon, lat), JSON or VARCHAR).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import OdsClient, OdsError, TtlCache
from .common import (
    PORTAL_DOC,
    PORTAL_PATTERN,
    bind_client,
    portal_arg,
    process_client,
    request_secret,
)
from .meta import object_tags
from .odsql import (
    GEO_POINT_TYPE,
    JSON_TYPE,
    ODS_TYPE_KEY,
    TIMESTAMPTZ,
    arrow_type_for,
    to_arrow_batch,
)
from .records import DATASET_ID_DOC, dataset_path, fetch_dataset

MAX_GROUPED = 20_000
MAX_UNGROUPED = 10_000
PAGE_SIZE = 100

_RESULTS = TtlCache(ttl=300.0, max_items=16)
_AGG_RE = re.compile(r"\b(count|sum|avg|min|max|median|percentile|envelope|bbox)\s*\(", re.I)
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(slots=True, frozen=True, kw_only=True)
class OdsqlArgs:
    """Arguments for ``odsql(portal, dataset_id, select, filter :=, group_by :=, order_by :=, max_rows :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[str, Arg(1, doc=DATASET_ID_DOC)]
    select: Annotated[
        str,
        Arg(2, doc="ODSQL select list, e.g. 'cou_name_en, count(*) AS n, sum(population) AS pop'."),
    ]
    filter: Annotated[
        str | None, Arg("filter", default=None, doc="ODSQL where condition, e.g. 'population > 1000000'.")
    ]
    group_by: Annotated[
        str | None, Arg("group_by", default=None, doc="ODSQL group_by, e.g. 'cou_name_en' or 'year(date) AS y'.")
    ]
    order_by: Annotated[str | None, Arg("order_by", default=None, doc="ODSQL order_by, e.g. 'n DESC'.")]
    max_rows: Annotated[
        int,
        Arg(
            "max_rows",
            default=1000,
            doc="Most rows to return: up to 20,000 with group_by, 10,000 without.",
            ge=1,
            le=MAX_GROUPED,
        ),
    ]


@dataclass(kw_only=True)
class OdsqlState(ArrowSerializableDataclass):
    """Single-shot result."""

    done: bool = False


def split_select(select: str) -> list[tuple[str, str | None]]:
    """Split a select list at top-level commas into ``(expression, alias)`` pairs."""
    parts: list[str] = []
    depth = 0
    quote: str | None = None
    cur = ""
    for ch in select:
        if quote:
            cur += ch
            if ch == quote:
                quote = None
            continue
        if ch in "\"'`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
            continue
        cur += ch
    if cur.strip():
        parts.append(cur.strip())
    out: list[tuple[str, str | None]] = []
    for p in parts:
        m = re.match(r"^(.*?)\s+as\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s*$", p, re.I | re.S)
        out.append((m.group(1).strip(), m.group(2)) if m else (p, None))
    return out


def query_pages(client: OdsClient, path: str, a: OdsqlArgs) -> list[dict[str, Any]]:
    """Run the query, paging when needed, and return all result rows."""
    base: dict[str, Any] = {
        "select": a.select,
        "where": a.filter,
        "group_by": a.group_by,
        "order_by": a.order_by,
    }
    if a.group_by:
        body = client.get_json(path, {**base, "limit": min(a.max_rows, MAX_GROUPED)})
        return list(body.get("results") or [])
    if _AGG_RE.search(a.select) and all(_AGG_RE.search(expr) for expr, _ in split_select(a.select)):
        # Whole-dataset aggregate: the portal returns one value repeated per row; ask for one.
        body = client.get_json(path, {**base, "limit": 1})
        return list(body.get("results") or [])[:1]
    want = min(a.max_rows, MAX_UNGROUPED)
    rows: list[dict[str, Any]] = []
    while len(rows) < want:
        limit = min(PAGE_SIZE, want - len(rows))
        body = client.get_json(path, {**base, "limit": limit, "offset": len(rows) or None})
        page = list(body.get("results") or [])
        rows.extend(page)
        if len(page) < limit:
            break
    return rows


def infer_type(values: list[Any]) -> tuple[pa.DataType, str]:
    """Return an Arrow type (and nominal ODS type) for a result column, inferred from its values."""
    vals = [v for v in values if v is not None]
    if not vals:
        return pa.string(), "text"
    if all(isinstance(v, bool) for v in vals):
        return pa.bool_(), "boolean"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
        return pa.int64(), "int"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
        return pa.float64(), "double"
    if all(isinstance(v, str) for v in vals):
        if all(_DATETIME_RE.match(v) for v in vals):
            return TIMESTAMPTZ, "datetime"
        if all(_DATE_RE.match(v) for v in vals):
            return pa.date32(), "date"
        return pa.string(), "text"
    if all(isinstance(v, dict) and set(v) == {"lon", "lat"} for v in vals):
        return GEO_POINT_TYPE, "geo_point_2d"
    return JSON_TYPE, "json_blob"


def result_schema(rows: list[dict[str, Any]], select: str, fields: list[dict[str, Any]]) -> pa.Schema:
    """Type the result columns from dataset fields and values."""
    by_name = {str(f.get("name")): f for f in fields}
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    if not keys:
        keys = [alias or expr.strip("`") for expr, alias in split_select(select)]
    out = []
    for k in keys:
        f = by_name.get(k)
        if f is not None:
            ods_type = str(f.get("type") or "text")
            multivalued = bool((f.get("annotations") or {}).get("multivalued"))
            t = arrow_type_for(ods_type, multivalued=multivalued)
            comment = f"Field {k} ({ods_type})"
        elif rows:
            t, ods_type = infer_type([r.get(k) for r in rows])
            comment = f"ODSQL result column {k}"
        else:
            t, ods_type = (pa.int64(), "int") if k.lower().startswith("count(") else (pa.string(), "text")
            comment = f"ODSQL result column {k}"
        out.append(pa.field(k, t, metadata={b"comment": comment.encode(), ODS_TYPE_KEY: ods_type.encode()}))
    if not out:
        raise OdsError("the select list produced no columns")
    return pa.schema(out)


def _cache_key(client: OdsClient, path: str, a: OdsqlArgs) -> str:
    return json.dumps(
        [client.base_url, bool(client.apikey), path, a.select, a.filter, a.group_by, a.order_by, a.max_rows]
    )


@init_single_worker
class Odsql(TableFunctionGenerator[OdsqlArgs, OdsqlState]):
    """Run an ODSQL select / group_by / order_by on the portal and return typed rows."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "odsql"
        description = "Run a server-side ODSQL query (aggregates, group_by) on an Opendatasoft dataset"
        tags = {
            **object_tags(
                title="Server-Side ODSQL Query",
                category="data",
                keywords="opendatasoft, odsql, aggregate, group by, count, sum, server-side, analytics",
                doc_llm=(
                    "Run an ODSQL query on the portal and return its rows. Arguments: the portal, the dataset_id, "
                    "an ODSQL `select` list, and the named arguments filter (an ODSQL condition), group_by "
                    "(grouping expressions, aliases allowed), order_by (an alias or field, optionally DESC) and "
                    "max_rows (default 1000). "
                    "The `select` list may use ODSQL aggregates (count(*), sum(x), avg(x), "
                    "min, max, percentile) and functions (year(date), ...) with AS aliases; result columns are "
                    "named by alias (or the expression text) and typed from the dataset fields or the returned "
                    "values. Use it to summarize large datasets without downloading them: one request instead "
                    "of streaming every record. group_by returns at most 20,000 groups, otherwise 10,000 rows."
                ),
                doc_md=(
                    "## odsql(portal, dataset_id, select_list, ...)\n\n"
                    "A server-side ODSQL query, e.g. counts per country over a 170,000-row dataset in one "
                    "request. Column types come from the dataset schema and the returned values."
                ),
            ),
            "vgi.result_dynamic_columns_md": (
                "One column per select expression (plus any group_by key the portal echoes), named by its "
                "alias or the expression text:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| <selected dataset field> | VARCHAR | A selected or grouped field keeps its fields() type. |\n"
                "| <alias of count(*)> | BIGINT | Integer results. |\n"
                "| <alias of sum / avg / ...> | DOUBLE | Fractional results. |\n"
                "| <alias of max(datetime) ...> | TIMESTAMPTZ | Timestamp results. |\n"
                "| <alias of max(date) ...> | DATE | Date results. |\n"
                "| <other alias> | VARCHAR | Text results. |\n"
                "| <nested alias> | JSON | Nested values. |\n\n"
                "For the GeoNames example (group_by := 'cou_name_en'):\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| cou_name_en | VARCHAR | The group key, echoed by the portal. |\n"
                "| country | VARCHAR | Alias of cou_name_en. |\n"
                "| cities | BIGINT | count(*). |\n"
                "| population | BIGINT | sum(population). |\n\n"
                "For the arrondissements example:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| arrondissements | BIGINT | count(*). |\n"
                "| km2 | DOUBLE | sum(surface) / 1000000. |"
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT country, cities, population FROM opendatasoft.main.odsql('public.opendatasoft.com', "
                    "'geonames-all-cities-with-a-population-1000', "
                    "'cou_name_en AS country, count(*) AS cities, sum(population) AS population', "
                    "group_by := 'cou_name_en', order_by := 'population DESC', max_rows := 10)"
                ),
                description="The ten countries with the largest population living in GeoNames cities",
            ),
            FunctionExample(
                sql=(
                    "SELECT arrondissements, round(km2, 1) AS km2 FROM opendatasoft.main.odsql("
                    "'opendata.paris.fr', 'arrondissements', "
                    "'count(*) AS arrondissements, sum(surface) / 1000000 AS km2')"
                ),
                description="Number of Paris arrondissements and their total area in square kilometres",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[OdsqlArgs]) -> BindResponse:
        """Run the query and type its result columns."""
        portal = portal_arg(params)
        if not request_secret(params, portal):
            return BindResponse(output_schema=pa.schema([pa.field("pending", pa.string())]))
        client = bind_client(params, portal)
        path = dataset_path(params.args.dataset_id) + "/records"
        fields = list(fetch_dataset(client, params.args.dataset_id).get("fields") or [])
        rows = query_pages(client, path, params.args)
        _RESULTS.put(_cache_key(client, path, params.args), rows)
        return BindResponse(output_schema=result_schema(rows, params.args.select, fields))

    @classmethod
    def initial_state(cls, params: ProcessParams[OdsqlArgs]) -> OdsqlState:
        """Not yet emitted."""
        return OdsqlState()

    @classmethod
    def process(cls, params: ProcessParams[OdsqlArgs], state: OdsqlState, out: OutputCollector) -> None:
        """Emit the (cached or re-run) result in one batch."""
        if state.done:
            out.finish()
            return
        portal = portal_arg(params)
        client = process_client(params, portal)
        path = dataset_path(params.args.dataset_id) + "/records"
        rows = _RESULTS.get(_cache_key(client, path, params.args))
        if rows is None:
            rows = query_pages(client, path, params.args)
        out.emit(to_arrow_batch(rows, params.output_schema))
        state.done = True
