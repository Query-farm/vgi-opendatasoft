"""Assemble the Opendatasoft functions into the ``opendatasoft`` catalog and run the worker.

Run it:

    vgi-opendatasoft-worker                      # stdio: what DuckDB launches by default
    vgi-opendatasoft-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'opendatasoft' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-opendatasoft-worker');
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, SecretTypeSpec

from .common import SECRET_KEY_FIELD, SECRET_TYPE
from .datasets import Datasets
from .discovery import Facets, Fields
from .helpers import OdsqlQuote
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .odsql_query import Odsql
from .records import Records

FUNCTIONS: list[type[Any]] = [Datasets, Fields, Facets, Records, Odsql, OdsqlQuote]

# CREATE SECRET (TYPE opendatasoft, APIKEY '...', SCOPE 'https://data.example.com')
API_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Opendatasoft API key for private datasets; scope it to the portal URL.",
    schema=pa.schema(
        [
            pa.field(
                SECRET_KEY_FIELD,
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_doc": "API key from the portal's account settings, sent as 'Authorization: Apikey ...'.",
                },
            )
        ],
        metadata={"vgi_doc_url": f"{SOURCE_URL}#private-datasets-api-keys"},
    ),
)


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; return them all for the schema."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class OpendatasoftWorker(Worker):
    """The worker process: one ``opendatasoft`` catalog, one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Opendatasoft catalog discovery and dataset records.",
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Opendatasoft Functions",
                    "domain": "open-data",
                    "vgi.keywords": keywords_array("opendatasoft, datasets, records, facets, odsql, open data"),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                },
            )
        ],
    )

    secret_types = [API_SECRET_TYPE]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    OpendatasoftWorker.main()


if __name__ == "__main__":
    main()
