"""``datasets(portal, ...)``: the dataset catalog of an Opendatasoft portal."""

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

from .client import dataset_page_url, portal_label
from .common import (
    PORTAL_DOC,
    PORTAL_PATTERN,
    ScanSpec,
    ScanState,
    portal_arg,
    process_client,
    request_secret,
    run_scan,
    strip_html,
)
from .meta import column, object_tags, result_columns
from .odsql import TIMESTAMPTZ, refine_values, search_clause

LIST = pa.list_(pa.string())

DATASET_COLUMNS: list[pa.Field[Any]] = [
    column(
        "portal",
        pa.string(),
        "Base URL of the portal the dataset belongs to, e.g. https://opendata.paris.fr.",
        nullable=False,
    ),
    column(
        "dataset_id", pa.string(), "Dataset identifier, the key for records(), fields() and facets().", nullable=False
    ),
    column("dataset_uid", pa.string(), "Immutable internal dataset uid (da_xxxxxx)."),
    column("title", pa.string(), "Dataset title."),
    column("description", pa.string(), "Dataset description as plain text (HTML tags removed)."),
    column("publisher", pa.string(), "Organization that publishes the dataset."),
    column("themes", LIST, "Themes (categories) assigned to the dataset."),
    column("keywords", LIST, "Free keywords assigned to the dataset."),
    column("license", pa.string(), "License name, e.g. 'Open Database License (ODbL)'."),
    column("license_url", pa.string(), "URL of the license text."),
    column("language", pa.string(), "Metadata language code, e.g. 'fr'."),
    column("territory", LIST, "Geographic coverage labels."),
    column("records_count", pa.int64(), "Number of records in the dataset."),
    column("modified", TIMESTAMPTZ, "When the dataset was last modified (UTC)."),
    column("data_processed", TIMESTAMPTZ, "When the data was last processed (UTC)."),
    column("metadata_processed", TIMESTAMPTZ, "When the metadata was last processed (UTC)."),
    column("features", LIST, "Dataset features, e.g. geo, timeserie, analyze."),
    column("has_records", pa.bool_(), "True when the dataset has queryable records."),
    column("url", pa.string(), "The dataset's page on the portal.", nullable=False),
    column("api_url", pa.string(), "The Explore API v2.1 URL of the dataset.", nullable=False),
]
OUTPUT_SCHEMA = pa.schema(DATASET_COLUMNS)

# output column -> catalog field name (select key and JSON key)
SOURCE_FIELD = {
    "dataset_id": "dataset_id",
    "dataset_uid": "dataset_uid",
    "title": "title",
    "description": "description",
    "publisher": "publisher",
    "themes": "theme",
    "keywords": "keyword",
    "license": "license",
    "license_url": "license_url",
    "language": "language",
    "territory": "territory",
    "records_count": "records_count",
    "modified": "modified",
    "data_processed": "data_processed",
    "metadata_processed": "metadata_processed",
    "features": "features",
    "has_records": "has_records",
    "portal": "dataset_id",
    "url": "dataset_id",
    "api_url": "dataset_id",
}

PUSHABLE = {
    "dataset_id": ("dataset_id", "text"),
    "title": ("title", "text"),
    "publisher": ("publisher", "text"),
    "license": ("license", "text"),
    "language": ("language", "text"),
    "records_count": ("records_count", "int"),
    "modified": ("modified", "datetime"),
    "data_processed": ("data_processed", "datetime"),
    "metadata_processed": ("metadata_processed", "datetime"),
}


@dataclass(slots=True, frozen=True, kw_only=True)
class DatasetsArgs:
    """Arguments for ``datasets(portal, q :=, filter :=, theme :=, keyword :=, refine :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    q: Annotated[str | None, Arg("q", default=None, doc="Full-text search over the catalog, e.g. 'velo'.")]
    filter: Annotated[
        str | None,
        Arg("filter", default=None, doc="Raw ODSQL condition on catalog fields, e.g. 'records_count > 1000'."),
    ]
    theme: Annotated[str | None, Arg("theme", default=None, doc="Only datasets with this theme (exact facet value).")]
    keyword: Annotated[
        str | None, Arg("keyword", default=None, doc="Only datasets with this keyword (exact facet value).")
    ]
    refine: Annotated[
        str | None,
        Arg("refine", default=None, doc="Raw facet refinement 'facet:value', e.g. 'publisher:Airparif'."),
    ]


def dataset_row(raw: dict[str, Any], portal: str) -> dict[str, Any]:
    """Map one catalog result (flat ``select`` shape or nested ``metas``) to output columns."""
    metas = raw.get("metas")
    flat = dict(raw)
    if isinstance(metas, dict) and isinstance(metas.get("default"), dict):
        flat = {**metas["default"], **{k: v for k, v in raw.items() if k != "metas"}}
    dataset_id = str(flat.get("dataset_id") or "")
    row = {out: flat.get(src) for out, src in SOURCE_FIELD.items()}
    row["description"] = strip_html(flat.get("description"))
    row["portal"] = portal_label(portal)
    row["url"] = dataset_page_url(portal, dataset_id)
    row["api_url"] = f"{portal}/api/explore/v2.1/catalog/datasets/{dataset_id}"
    return row


def datasets_spec(params: ProcessParams[DatasetsArgs], portal: str) -> ScanSpec:
    """Return the scan description for a catalog listing."""
    a = params.args
    return ScanSpec(
        records_path="/catalog/datasets",
        export_path="/catalog/exports/json",
        export_fmt="json",
        base_where=[w for w in (a.filter, search_clause(a.q)) if w],
        refine=[*refine_values(("theme", a.theme), ("keyword", a.keyword)), *([a.refine] if a.refine else [])],
        extra_params={},
        pushable=PUSHABLE,
        keys={},
        selectable=dict(SOURCE_FIELD.items()),
        postprocess=lambda r: dataset_row(r, portal),
    )


@init_single_worker
class Datasets(TableFunctionGenerator[DatasetsArgs, ScanState]):
    """List a portal's datasets, pushing filters into the catalog's ODSQL query."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OUTPUT_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "datasets"
        description = "Datasets published on an Opendatasoft portal, with ODSQL filter pushdown"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Portal Dataset Catalog",
                category="catalog",
                keywords="opendatasoft, huwise, open data, catalog, datasets, search, publisher, theme, keyword",
                doc_llm=(
                    "List the datasets of an Opendatasoft (Huwise) open-data portal, one row per dataset: "
                    "portal, dataset_id, title, description (plain text), publisher, themes, keywords, license, "
                    "language, territory, records_count, modified, data_processed, metadata_processed, features, "
                    "has_records, url, api_url. `portal` is a domain such as 'opendata.paris.fr'. Optional named "
                    "arguments: q := full-text search, theme := / keyword := exact facet values, refine := "
                    "'facet:value', filter := raw ODSQL. SQL filters on dataset_id, title, publisher, license, "
                    "language (= / IN / IS NULL / starts_with) and on records_count, modified, data_processed, "
                    "metadata_processed (ranges) are pushed into the portal's ODSQL `where`; selected columns "
                    "become its `select`. Use dataset_id with records(), fields() and facets(). Catalogs larger "
                    "than 10,000 matches are streamed from the catalog export endpoint."
                ),
                doc_md=(
                    "## datasets(portal, q :=, theme :=, keyword :=, refine :=, filter :=)\n\n"
                    "One row per dataset on the portal. Filters on `publisher`, `license`, `modified`, "
                    "`records_count` and similar columns run on the portal (ODSQL), not in DuckDB.\n\n"
                    "The columns `portal`, `dataset_id`, `title`, `description`, `publisher`, `modified` and `url` "
                    "match the other open-data workers (CKAN, Socrata) so catalogs can be combined with "
                    "`UNION ALL BY NAME`."
                ),
            ),
            "vgi.result_columns_schema": result_columns(DATASET_COLUMNS),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT publisher, count(*) AS datasets, sum(records_count) AS records "
                    "FROM opendatasoft.main.datasets('opendata.paris.fr') "
                    "GROUP BY publisher ORDER BY datasets DESC LIMIT 10"
                ),
                description="The ten most prolific publishers on the Paris open-data portal",
            ),
            FunctionExample(
                sql=(
                    "SELECT dataset_id, title, records_count FROM opendatasoft.main.datasets("
                    "'public.opendatasoft.com', q := 'population') "
                    "WHERE modified >= TIMESTAMPTZ '2025-01-01' AND records_count > 1000 ORDER BY records_count DESC"
                ),
                description="Recently updated population datasets with more than 1,000 records",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[DatasetsArgs]) -> BindResponse:
        """Validate the portal and request its scoped secret."""
        request_secret(params, portal_arg(params))
        return BindResponse(output_schema=OUTPUT_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[DatasetsArgs]) -> ScanState:
        """Return a fresh, unstarted scan."""
        return ScanState()

    @classmethod
    def process(cls, params: ProcessParams[DatasetsArgs], state: ScanState, out: OutputCollector) -> None:
        """Emit the next batch of datasets."""
        portal = portal_arg(params)
        run_scan(params, state, out, datasets_spec(params, portal), process_client(params, portal))
