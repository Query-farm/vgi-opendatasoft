"""``records(portal, dataset_id, ...)``: the rows of one dataset, with ODSQL pushdown.

The output columns are the dataset's fields, typed from its schema
(int -> BIGINT, double -> DOUBLE, date -> DATE, datetime -> TIMESTAMPTZ,
geo_point_2d -> STRUCT(lon, lat), geo_shape -> GeoJSON geometry as JSON, file ->
STRUCT, json_blob -> JSON, multivalued text -> VARCHAR[], other -> VARCHAR).
"""

from __future__ import annotations

import urllib.parse
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

from .client import OdsClient, OdsError, TtlCache
from .common import (
    PORTAL_DOC,
    PORTAL_PATTERN,
    ScanSpec,
    ScanState,
    bind_client,
    portal_arg,
    process_client,
    request_secret,
    require_identifier,
    run_scan,
)
from .doc_variants import EXAMPLE_VARIANTS_MD
from .meta import object_tags
from .odsql import (
    ODS_TYPE_KEY,
    PUSHABLE_EQ,
    arrow_type_for,
    field_ods_type,
    ident,
    search_clause,
)

_DATASET_CACHE = TtlCache()

DATASET_ID_DOC = (
    "Dataset identifier as shown by datasets() or in the portal URL /explore/dataset/<dataset_id>/, "
    "e.g. 'arrondissements'."
)


def dataset_path(dataset_id: str) -> str:
    """Return the API path of one dataset."""
    return "/catalog/datasets/" + urllib.parse.quote(require_identifier(dataset_id, "dataset_id"), safe="@")


def fetch_dataset(client: OdsClient, dataset_id: str) -> dict[str, Any]:
    """Dataset metadata (``fields``, ``metas`` ...), cached briefly per process."""
    path = dataset_path(dataset_id)
    key = f"{client.base_url}|{bool(client.apikey)}|{path}"
    hit = _DATASET_CACHE.get(key)
    if hit is not None:
        return dict(hit)
    body = client.get_json(path)
    if not isinstance(body, dict) or "fields" not in body:
        raise OdsError(f"unexpected response for dataset {dataset_id!r}")
    _DATASET_CACHE.put(key, body)
    return dict(body)


def field_comment(f: dict[str, Any]) -> str:
    """Column comment: the field label plus its description."""
    label = str(f.get("label") or f.get("name") or "")
    desc = f.get("description")
    text = label if not desc else f"{label}: {desc}"
    return f"{text} (Opendatasoft type {f.get('type')})"


def schema_for_fields(fields: list[dict[str, Any]]) -> pa.Schema:
    """Return the DuckDB schema of a dataset's records."""
    out: list[pa.Field[Any]] = []
    seen: set[str] = set()
    for f in fields:
        name = str(f.get("name") or "")
        if not name or name in seen:
            continue
        seen.add(name)
        ods_type = str(f.get("type") or "text")
        multivalued = bool((f.get("annotations") or {}).get("multivalued"))
        t = arrow_type_for(ods_type, multivalued=multivalued)
        meta = {b"comment": field_comment(f).encode(), ODS_TYPE_KEY: ods_type.encode()}
        out.append(pa.field(name, t, nullable=True, metadata=meta))
    if not out:
        raise OdsError("the dataset has no fields")
    return pa.schema(out)


def pushable_columns(schema: pa.Schema) -> dict[str, tuple[str, str]]:
    """Columns whose filters may be pushed (scalar types; multivalued lists excluded)."""
    out: dict[str, tuple[str, str]] = {}
    for f in schema:
        ods_type = field_ods_type(f)
        if ods_type in PUSHABLE_EQ and not pa.types.is_list(f.type):
            out[f.name] = (f.name, ods_type)
    return out


@dataclass(slots=True, frozen=True, kw_only=True)
class RecordsArgs:
    """Arguments for ``records(portal, dataset_id, filter :=, q :=, refine :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[str, Arg(1, doc=DATASET_ID_DOC)]
    filter: Annotated[
        str | None,
        Arg(
            "filter",
            default=None,
            doc="Extra raw ODSQL condition, e.g. 'distance(geo, geom\\'POINT(2.35 48.85)\\', 1km)'.",
        ),
    ]
    q: Annotated[str | None, Arg("q", default=None, doc="Full-text search over the dataset's text fields.")]
    refine: Annotated[str | None, Arg("refine", default=None, doc="Facet refinement 'facet:value', e.g. 'c_ar:4'.")]


def records_spec(params: ProcessParams[RecordsArgs]) -> ScanSpec:
    """Return the scan description for a dataset's records."""
    a = params.args
    path = dataset_path(a.dataset_id)
    schema = params.output_schema
    return ScanSpec(
        records_path=f"{path}/records",
        export_path=f"{path}/exports/jsonl",
        export_fmt="jsonl",
        base_where=[w for w in (a.filter, search_clause(a.q)) if w],
        refine=[a.refine] if a.refine else [],
        extra_params={},
        pushable=pushable_columns(schema),
        keys={},
        selectable={f.name: ident(f.name) for f in schema},
    )


@init_single_worker
class Records(TableFunctionGenerator[RecordsArgs, ScanState]):
    """Read a dataset's records with select/where/order pushdown."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "records"
        description = "Rows of one Opendatasoft dataset, typed from its schema, with ODSQL pushdown"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Dataset Records",
                category="data",
                keywords="opendatasoft, huwise, records, rows, open data, odsql, export, geo, dataset",
                doc_llm=(
                    "Read the rows of one dataset of an Opendatasoft portal, given the portal and dataset_id. "
                    "Columns are the dataset's fields (see fields(portal, dataset_id)), typed: int `BIGINT`, double "
                    "`DOUBLE`, date `DATE`, datetime `TIMESTAMPTZ`, geo_point_2d `STRUCT(lon, lat)`, geo_shape "
                    "GeoJSON geometry `JSON`, file `STRUCT(id, filename, format, url, ...)`, json_blob `JSON`, "
                    "multivalued text `VARCHAR[]`, everything else `VARCHAR`. "
                    "Selected columns become the ODSQL `select`; filters "
                    "(= <> IN IS NULL starts_with on text; ranges on numbers, dates, datetimes) become `where`; "
                    "ORDER BY <number/date column> LIMIT n is sorted and limited by the portal. Up to 10,000 "
                    "matching rows are paged from the records API; larger results are streamed from the export "
                    "endpoint automatically, so filter first. Optional: filter := raw ODSQL (e.g. geo distance), "
                    "q := full-text search, refine := 'facet:value'. For server-side GROUP BY use odsql()."
                ),
                doc_md=(
                    "## records(portal, dataset_id, filter :=, q :=, refine :=)\n\n"
                    "All rows of a dataset, with columns typed from the dataset schema.\n\n"
                    "**Pushdown.** Projections, filters and `ORDER BY ... LIMIT n` on numeric/date columns run on "
                    "the portal. Results above 10,000 rows (the records API cap) are streamed from the export "
                    "endpoint instead of paged."
                ),
            ),
            "vgi.result_dynamic_columns_md": (
                "One column per dataset field, named as in fields(portal, dataset_id); the type follows the "
                "field's Opendatasoft type:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| <int field> | BIGINT | Integer field. |\n"
                "| <double / decimal field> | DOUBLE | Numeric field. |\n"
                "| <date field> | DATE | Calendar date. |\n"
                "| <datetime field> | TIMESTAMPTZ | Timestamp, UTC. |\n"
                "| <boolean field> | BOOLEAN | Boolean field. |\n"
                "| <geo_point_2d field> | STRUCT(lon DOUBLE, lat DOUBLE) | WGS84 point. |\n"
                "| <geo_shape field> | JSON | GeoJSON geometry (use ST_GeomFromGeoJSON). |\n"
                "| <json_blob field> | JSON | Free-form JSON. |\n"
                "| <file field> | STRUCT(id VARCHAR, filename VARCHAR, format VARCHAR, url VARCHAR, "
                "width INTEGER, height INTEGER, thumbnail BOOLEAN, color_summary VARCHAR[]) | Attached file. |\n"
                "| <multivalued text field> | VARCHAR[] | List of values. |\n"
                "| <text field> | VARCHAR | Text and any other type. |" + EXAMPLE_VARIANTS_MD
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT l_aroff AS arrondissement, round(surface / 1000000, 2) AS km2 "
                    "FROM opendatasoft.main.records('opendata.paris.fr', 'arrondissements') "
                    "WHERE c_ar <= 4 ORDER BY c_ar"
                ),
                description="Name and area of Paris's first four arrondissements",
            ),
            FunctionExample(
                sql=(
                    "SELECT name, population, coordinates.lat, coordinates.lon "
                    "FROM opendatasoft.main.records('public.opendatasoft.com', "
                    "'geonames-all-cities-with-a-population-1000') "
                    "WHERE country_code = 'FR' ORDER BY population DESC LIMIT 5"
                ),
                description="The five most populous French cities, sorted and limited by the portal",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[RecordsArgs]) -> BindResponse:
        """Fetch the dataset schema and map its fields to typed columns."""
        portal = portal_arg(params)
        if not request_secret(params, portal):
            return BindResponse(output_schema=pa.schema([pa.field("pending", pa.string())]))
        body = fetch_dataset(bind_client(params, portal), params.args.dataset_id)
        return BindResponse(output_schema=schema_for_fields(list(body.get("fields") or [])))

    @classmethod
    def initial_state(cls, params: ProcessParams[RecordsArgs]) -> ScanState:
        """Return a fresh, unstarted scan."""
        return ScanState()

    @classmethod
    def process(cls, params: ProcessParams[RecordsArgs], state: ScanState, out: OutputCollector) -> None:
        """Emit the next batch of records."""
        portal = portal_arg(params)
        run_scan(params, state, out, records_spec(params), process_client(params, portal))
