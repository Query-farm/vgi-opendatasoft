"""``fields(portal, dataset_id)`` and ``facets(portal, ...)``: dataset schemas and facet counts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

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

from .client import portal_label
from .common import (
    PORTAL_DOC,
    PORTAL_PATTERN,
    portal_arg,
    process_client,
    request_secret,
)
from .meta import column, object_tags, result_columns
from .odsql import JSON_TYPE, arrow_type_for, duckdb_type_name, search_clause, to_arrow_batch
from .records import DATASET_ID_DOC, dataset_path, fetch_dataset


@dataclass(kw_only=True)
class OnceState(ArrowSerializableDataclass):
    """These functions return a single batch."""

    done: bool = False


# ---------------------------------------------------------------- fields

FIELD_COLUMNS: list[pa.Field[Any]] = [
    column("portal", pa.string(), "Portal the dataset belongs to.", nullable=False),
    column("dataset_id", pa.string(), "Dataset identifier.", nullable=False),
    column("position", pa.int32(), "1-based position of the field in the dataset schema.", nullable=False),
    column("name", pa.string(), "Field name: the column name in records() and in ODSQL.", nullable=False),
    column("label", pa.string(), "Human-readable field label."),
    column("description", pa.string(), "Field description, when the publisher wrote one."),
    column(
        "ods_type",
        pa.string(),
        "Opendatasoft type: text, int, double, date, datetime, geo_point_2d, ...",
        nullable=False,
    ),
    column("duckdb_type", pa.string(), "The DuckDB type of the column in records().", nullable=False),
    column("multivalued_separator", pa.string(), "Separator of a multivalued text field (then a VARCHAR[] column)."),
    column(
        "is_facet", pa.bool_(), "True when the field is a facet (usable with facets() and refine :=).", nullable=False
    ),
    column("is_sortable", pa.bool_(), "True when the publisher marked the field sortable.", nullable=False),
    column("is_id", pa.bool_(), "True when the field is marked as the record identifier.", nullable=False),
    column("annotations", JSON_TYPE, "All field annotations as JSON."),
]
FIELDS_SCHEMA = pa.schema(FIELD_COLUMNS)


@dataclass(slots=True, frozen=True, kw_only=True)
class FieldsArgs:
    """Arguments for ``fields(portal, dataset_id)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[str, Arg(1, doc=DATASET_ID_DOC)]


def field_rows(portal: str, dataset_id: str, fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One output row per dataset field."""
    rows = []
    for i, f in enumerate(fields, start=1):
        ann = f.get("annotations") or {}
        ods_type = str(f.get("type") or "text")
        multi = ann.get("multivalued")
        rows.append(
            {
                "portal": portal_label(portal),
                "dataset_id": dataset_id,
                "position": i,
                "name": f.get("name"),
                "label": f.get("label"),
                "description": f.get("description"),
                "ods_type": ods_type,
                "duckdb_type": duckdb_type_name(arrow_type_for(ods_type, multivalued=bool(multi))),
                "multivalued_separator": multi if isinstance(multi, str) else None,
                "is_facet": bool(ann.get("facet")),
                "is_sortable": bool(ann.get("sortable")),
                "is_id": bool(ann.get("id")),
                "annotations": ann,
            }
        )
    return rows


@init_single_worker
class Fields(TableFunctionGenerator[FieldsArgs, OnceState]):
    """List a dataset's fields with their Opendatasoft and DuckDB types."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FIELDS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "fields"
        description = "The fields (schema) of an Opendatasoft dataset with their DuckDB column types"
        tags = {
            **object_tags(
                title="Dataset Fields",
                category="catalog",
                keywords="opendatasoft, schema, fields, columns, types, facets, dataset metadata",
                doc_llm=(
                    "Describe the schema of one dataset: fields(portal, dataset_id) returns one row per field "
                    "with name (the column name in records() and ODSQL), label, description, ods_type, the "
                    "duckdb_type it gets in records(), multivalued_separator, is_facet, is_sortable, is_id and "
                    "all annotations as JSON. Call it before records() or odsql() to learn column names."
                ),
                doc_md=(
                    "## fields(portal, dataset_id)\n\n"
                    "The dataset schema: one row per field, with the DuckDB type used by `records()`."
                ),
            ),
            "vgi.result_columns_schema": result_columns(FIELD_COLUMNS),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, label, ods_type, duckdb_type FROM opendatasoft.main.fields("
                    "'opendata.paris.fr', 'arrondissements') ORDER BY position"
                ),
                description="The columns of the Paris arrondissements dataset and their types",
            ),
            FunctionExample(
                sql=(
                    "SELECT name FROM opendatasoft.main.fields('public.opendatasoft.com', "
                    "'geonames-all-cities-with-a-population-1000') WHERE is_facet ORDER BY position"
                ),
                description="Which GeoNames fields can be used as facets",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FieldsArgs]) -> BindResponse:
        """Request the portal's scoped secret."""
        request_secret(params, portal_arg(params))
        return BindResponse(output_schema=FIELDS_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FieldsArgs]) -> OnceState:
        """Not yet emitted."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[FieldsArgs], state: OnceState, out: OutputCollector) -> None:
        """Emit every field in one batch."""
        if state.done:
            out.finish()
            return
        portal = portal_arg(params)
        body = fetch_dataset(process_client(params, portal), params.args.dataset_id)
        rows = field_rows(portal, str(body.get("dataset_id") or params.args.dataset_id), list(body.get("fields") or []))
        out.emit(to_arrow_batch(rows, params.output_schema))
        state.done = True


# ---------------------------------------------------------------- facets

FACET_COLUMNS: list[pa.Field[Any]] = [
    column("portal", pa.string(), "Portal queried.", nullable=False),
    column("dataset_id", pa.string(), "Dataset whose records were counted; NULL for catalog facets."),
    column("facet", pa.string(), "Facet name, e.g. theme, publisher, keyword or a dataset field.", nullable=False),
    column("value", pa.string(), "Facet value (use it in refine := 'facet:value').", nullable=False),
    column("label", pa.string(), "Display name of the value."),
    column("count", pa.int64(), "Number of datasets (catalog) or records (dataset) with this value.", nullable=False),
    column("state", pa.string(), "Facet state: displayed, refined or excluded."),
    column("depth", pa.int32(), "0 for top-level values; deeper for hierarchical facets.", nullable=False),
    column("path", pa.string(), "Full hierarchical path, values joined by '/'.", nullable=False),
]
FACETS_SCHEMA = pa.schema(FACET_COLUMNS)


@dataclass(slots=True, frozen=True, kw_only=True)
class FacetsArgs:
    """Arguments for ``facets(portal, dataset_id :=, facet :=, filter :=, q :=, refine :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[
        str | None,
        Arg("dataset_id", default=None, doc="Count a dataset's records by facet; omit for catalog facets."),
    ]
    facet: Annotated[
        str | None,
        Arg("facet", default=None, doc="Only this facet (any field name for datasets), e.g. 'publisher'."),
    ]
    filter: Annotated[str | None, Arg("filter", default=None, doc="Raw ODSQL condition applied before counting.")]
    q: Annotated[str | None, Arg("q", default=None, doc="Full-text search applied before counting.")]
    refine: Annotated[str | None, Arg("refine", default=None, doc="Facet refinement 'facet:value'.")]


def facet_rows(portal: str, dataset_id: str | None, body: Any) -> list[dict[str, Any]]:
    """Flatten a facets response (hierarchical values included)."""
    rows: list[dict[str, Any]] = []

    def walk(facet: str, values: list[dict[str, Any]], depth: int, prefix: str) -> None:
        for v in values:
            value = str(v.get("value") if v.get("value") is not None else v.get("name"))
            path = f"{prefix}/{value}" if prefix else value
            rows.append(
                {
                    "portal": portal_label(portal),
                    "dataset_id": dataset_id,
                    "facet": facet,
                    "value": value,
                    "label": v.get("name"),
                    "count": int(v.get("count") or 0),
                    "state": v.get("state"),
                    "depth": depth,
                    "path": path,
                }
            )
            if isinstance(v.get("facets"), list):
                walk(facet, v["facets"], depth + 1, path)

    for group in (body or {}).get("facets") or []:
        walk(str(group.get("name")), list(group.get("facets") or []), 0, "")
    return rows


@init_single_worker
class Facets(TableFunctionGenerator[FacetsArgs, OnceState]):
    """Facet value counts for a portal catalog or one dataset."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FACETS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "facets"
        description = "Facet value counts (themes, publishers, keywords, or a dataset's facet fields)"
        tags = {
            **object_tags(
                title="Facet Counts",
                category="catalog",
                keywords="opendatasoft, facets, counts, themes, publishers, keywords, refine, group by",
                doc_llm=(
                    "Count values of facets on the portal side. facets(portal) counts catalog datasets per "
                    "theme, publisher, keyword, language, license, ... ; facets(portal, dataset_id := 'x') counts "
                    "a dataset's records per value of its facet fields. Columns: portal, dataset_id, facet, value, "
                    "label, count, state, depth, path (hierarchical facets are flattened). facet := limits to one "
                    "facet (for datasets any field name works); filter := raw ODSQL, q := full-text search and "
                    "refine := 'facet:value' narrow what is counted. Use a value with refine := in datasets() or "
                    "records()."
                ),
                doc_md=(
                    "## facets(portal, dataset_id :=, facet :=, filter :=, q :=, refine :=)\n\n"
                    "Value counts computed by the portal: themes and publishers of the catalog, or a "
                    "dataset's facet fields."
                ),
            ),
            "vgi.result_columns_schema": result_columns(FACET_COLUMNS),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT value AS theme, count AS datasets FROM opendatasoft.main.facets("
                    "'opendata.paris.fr', facet := 'theme') ORDER BY count DESC"
                ),
                description="How many Paris datasets each theme has",
            ),
            FunctionExample(
                sql=(
                    "SELECT value AS country, count AS cities FROM opendatasoft.main.facets("
                    "'public.opendatasoft.com', dataset_id := 'geonames-all-cities-with-a-population-1000', "
                    "facet := 'cou_name_en') ORDER BY count DESC LIMIT 10"
                ),
                description="Countries with the most cities above 1,000 inhabitants, counted by the portal",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FacetsArgs]) -> BindResponse:
        """Request the portal's scoped secret."""
        request_secret(params, portal_arg(params))
        return BindResponse(output_schema=FACETS_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FacetsArgs]) -> OnceState:
        """Not yet emitted."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[FacetsArgs], state: OnceState, out: OutputCollector) -> None:
        """Fetch and emit all facet values in one batch."""
        if state.done:
            out.finish()
            return
        a = params.args
        portal = portal_arg(params)
        client = process_client(params, portal)
        path = f"{dataset_path(a.dataset_id)}/facets" if a.dataset_id else "/catalog/facets"
        where = " and ".join(f"({w})" for w in (a.filter, search_clause(a.q)) if w) or None
        body = client.get_json(path, {"facet": a.facet, "where": where, "refine": a.refine})
        out.emit(to_arrow_batch(facet_rows(portal, a.dataset_id, body), params.output_schema))
        state.done = True
