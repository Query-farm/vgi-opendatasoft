"""Discovery metadata: the one file to edit when you rename or extend the worker.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Keep every object documented and ``make lint`` stays at 100.

* Catalog-level tags (title, license, author, links, long description) live in
  ``CATALOG_TAGS`` below.
* Every function and table carries ``object_tags(...)``: a title, a category
  from ``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "opendatasoft"

SOURCE_URL = "https://github.com/Query-farm/vgi-opendatasoft"

# Categories group functions in docs and agent tools. Every object's
# ``vgi.category`` must name one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "catalog",
        "title": "Catalog & Schema",
        "description": "Discover datasets, their fields and facet value counts on a portal.",
    },
    {
        "name": "data",
        "title": "Dataset Data",
        "description": "Read dataset records or run server-side ODSQL aggregations.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def result_columns(fields: list[pa.Field[Any]]) -> str:
    """Return the ``vgi.result_columns_schema`` JSON for a fixed output schema."""
    from .odsql import duckdb_type_name

    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type_name(f.type) if f.type != pa.int32() else "INTEGER",
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in fields
        ]
    )


CATALOG_COMMENT = (
    "Any Opendatasoft (Huwise) open-data portal as SQL: search dataset catalogs and read typed records, "
    "with filters, projections and aggregates pushed down as ODSQL. No API key needed for public data."
)

CATALOG_DOC_LLM = (
    "Query any Opendatasoft (Huwise) open-data portal through its Explore API v2.1: thousands of city, "
    "region and utility portals such as opendata.paris.fr, public.opendatasoft.com and the "
    "data.opendatasoft.com hub. Every function takes the portal (a domain such as 'opendata.paris.fr', or a "
    "URL) as its first argument, and most take a dataset_id, the identifier shown in the portal's "
    "/explore/dataset/<dataset_id>/ URLs. Typical flow: search the dataset catalog of a portal, inspect a "
    "dataset's fields and their types, then read its records with ordinary SQL; WHERE filters, selected "
    "columns and ORDER BY <number or date> LIMIT n are translated into the portal's own query language "
    "(ODSQL), so only matching rows travel. Results above 10,000 rows are streamed from the export "
    "endpoint automatically. For GROUP BY summaries of large datasets, send an ODSQL aggregate to the "
    "portal instead of downloading every record. Field types become real DuckDB types: `DATE`, "
    "`TIMESTAMPTZ`, `BIGINT`, `DOUBLE`, `STRUCT(lon, lat)` points and GeoJSON `JSON` shapes. Public data "
    "needs no credentials; for private datasets create a secret scoped to the portal:\n\n"
    "```sql\nCREATE SECRET (TYPE opendatasoft, APIKEY '...', SCOPE 'https://data.example.com');\n```\n\n"
    "Catalog columns portal, dataset_id, title, description, publisher, modified and url match the CKAN and "
    "Socrata workers so catalogs from several portal platforms can be combined with UNION ALL BY NAME."
)

CATALOG_DOC_MD = (
    "# Opendatasoft open-data portals in DuckDB\n\n"
    "Query **any [Opendatasoft (Huwise)](https://www.opendatasoft.com) portal** with SQL: Paris, "
    "public.opendatasoft.com, the data.opendatasoft.com hub and hundreds of European cities, regions and "
    "utilities. No API key is needed for public data.\n\n"
    "Filters, projections and `ORDER BY ... LIMIT` run on the portal as ODSQL, large results stream from "
    "the export endpoint, and aggregations can run server-side. Columns get real DuckDB types, including "
    "points as `STRUCT(lon, lat)` and shapes as GeoJSON `JSON`."
)

SCHEMA_DOC_LLM = (
    "The `main` schema holds every function: catalog discovery (dataset search with theme, keyword and "
    "publisher filters; a dataset's field list with types; facet value counts) and data access (typed "
    "records with ODSQL pushdown, and raw server-side ODSQL aggregation). `portal` is a domain such as "
    "'opendata.paris.fr' or a URL. Look up a dataset's field names before reading records; filter in SQL "
    "so the portal does the work; use server-side aggregation for GROUP BY over datasets with hundreds of "
    "thousands of rows. Named arguments: q := full-text search, filter := raw ODSQL condition, refine := "
    "'facet:value'."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Everything this worker offers lives in one schema, grouped in two categories.\n\n"
    "**Catalog** functions search a portal's datasets, list a dataset's fields with the DuckDB type each "
    "one gets, and count facet values (themes, publishers, keywords or a dataset's own facet fields).\n\n"
    "**Data** functions read a dataset's records with filters, projections and Top-N sorts pushed to the "
    "portal, or run an ODSQL aggregate on the portal and return only the summary."
)

AGENT_TEST_TASKS = [
    {
        "name": "paris_arrondissement_count",
        "prompt": (
            "Using the Opendatasoft worker and the 'arrondissements' dataset on opendata.paris.fr, how many "
            "arrondissements does Paris have?"
        ),
    },
    {
        "name": "largest_arrondissement",
        "prompt": (
            "Which Paris arrondissement (official name, column l_aroff) has the largest surface according to "
            "the 'arrondissements' dataset on opendata.paris.fr?"
        ),
    },
    {
        "name": "paris_theme_counts",
        "prompt": "On the opendata.paris.fr portal, how many datasets carry each theme?",
    },
    {
        "name": "airparif_datasets",
        "prompt": "List the dataset ids that the publisher 'Airparif' has on the opendata.paris.fr portal.",
    },
    {
        "name": "arrondissement_field_types",
        "prompt": (
            "What Opendatasoft type does the field 'geom' have in the 'arrondissements' dataset on opendata.paris.fr?"
        ),
    },
    {
        "name": "most_cities_country",
        "prompt": (
            "In the 'geonames-all-cities-with-a-population-1000' dataset on public.opendatasoft.com, which "
            "country (cou_name_en) has the most cities? Count on the portal side rather than downloading "
            "every record."
        ),
    },
    {
        "name": "quote_literal",
        "prompt": 'Quote the text O\'Brien "Jr" as an ODSQL string literal with the Opendatasoft worker.',
    },
]

CATALOG_TAGS = {
    "vgi.title": "Opendatasoft Open Data",
    "vgi.subtitle": "Any Opendatasoft (Huwise) portal's catalog and records as SQL, with ODSQL pushdown.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "opendatasoft, huwise, open data, odsql, government data, city data, catalog, records, duckdb"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
