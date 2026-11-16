<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Any Opendatasoft Open-Data Portal in DuckDB

> **vgi-opendatasoft** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-opendatasoft/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-opendatasoft/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Hundreds of cities, regions and utilities publish open data on
[Opendatasoft (Huwise)](https://www.opendatasoft.com): Paris (`opendata.paris.fr`),
`public.opendatasoft.com`, the `data.opendatasoft.com` hub with ~100,000 datasets, and many more.
This [VGI](https://query.farm/vgi) worker turns any of them into SQL: search the catalog, read
records with real column types, and let the portal do the filtering, sorting and aggregating
through its own query language, **ODSQL**. No API key is needed for public data.

```sql
-- Paris's first four arrondissements: the filter and the columns are sent to the portal
SELECT l_aroff AS arrondissement, round(surface / 1000000, 2) AS km2
FROM opendatasoft.main.records('opendata.paris.fr', 'arrondissements')
WHERE c_ar <= 4 ORDER BY c_ar;
```

## Functions

| Function | Returns |
|---|---|
| `datasets(portal, q :=, theme :=, keyword :=, refine :=, filter :=)` | One row per dataset: `portal, dataset_id, dataset_uid, title, description, publisher, themes, keywords, license, license_url, language, territory, records_count, modified, data_processed, metadata_processed, features, has_records, url, api_url` |
| `fields(portal, dataset_id)` | A dataset's schema: `name, label, description, ods_type, duckdb_type, multivalued_separator, is_facet, is_sortable, is_id, annotations` |
| `facets(portal, dataset_id :=, facet :=, filter :=, q :=, refine :=)` | Value counts computed by the portal (catalog themes/publishers/keywords, or a dataset's facet fields), hierarchical facets flattened (`depth`, `path`) |
| `records(portal, dataset_id, filter :=, q :=, refine :=)` | The dataset's rows, one column per field, typed (below) |
| `odsql(portal, dataset_id, select_list, filter :=, group_by :=, order_by :=, max_rows := 1000)` | The result of a server-side ODSQL query (aggregates, `group_by`) |
| `odsql_quote(text)` | `text` as an ODSQL string literal, for building `filter :=` conditions safely |

`portal` is a domain (`opendata.paris.fr`) or a URL; a pasted dataset page URL is trimmed to the
portal root, and `http://host:port/prefix` works for self-hosted portals and test fixtures.
Named arguments: `q :=` full-text search, `filter :=` a raw ODSQL condition (e.g. geo
`distance(...)`), `refine :=` a facet refinement `'facet:value'`.

**Catalog columns line up with the other open-data workers.** `portal, dataset_id, title,
description, publisher, modified, url` have the same names and types as in
[vgi-ckan](https://github.com/Query-farm/vgi-ckan) and
[vgi-socrata](https://github.com/Query-farm/vgi-socrata), so catalogs can be combined:

```sql
SELECT portal, dataset_id, title, modified FROM opendatasoft.main.datasets('opendata.paris.fr')
UNION ALL BY NAME
SELECT portal, dataset_id, title, modified FROM opendatasoft.main.datasets('public.opendatasoft.com');
```

### Column types

| Opendatasoft type | DuckDB type |
|---|---|
| `int` | `BIGINT` |
| `double`, `decimal` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `date` | `DATE` |
| `datetime` | `TIMESTAMPTZ` (UTC) |
| `geo_point_2d` | `STRUCT(lon DOUBLE, lat DOUBLE)` |
| `geo_shape` | `JSON`: the GeoJSON geometry (use `ST_GeomFromGeoJSON` from `spatial`) |
| `file` (images, PDFs) | `STRUCT(id, filename, format, url, width, height, thumbnail, color_summary)` |
| `json_blob` | `JSON` |
| `text` with a `multivalued` annotation | `VARCHAR[]` |
| `text` and anything else | `VARCHAR` |

`JSON` columns need DuckDB's `json` extension, which DuckDB autoloads.

## Pushdown: what runs on the portal

`records()` and `datasets()` translate the SQL around them into ODSQL, so only matching rows
travel. DuckDB still re-checks every pushed predicate, and a filter is only translated when the
portal's semantics can never drop a matching row (each rule below was checked against live
portals).

| SQL | Sent as ODSQL |
|---|---|
| selected columns | `select=` |
| `col = v`, `col <> v` (text, numbers, dates, booleans) | `` `col` = "v" `` (text equality is exact and case-sensitive on the portal) |
| `<`, `<=`, `>`, `>=`, `BETWEEN` on numbers, `DATE`, `TIMESTAMPTZ` | `` `col` >= 10 ``, `` `d` < date'2024-01-01' ``, `` `t` >= date'2024-01-01T00:00:00+00:00' `` |
| `col IN (...)` | `` `col` in ("a", "b") `` |
| `col IS [NOT] NULL` | `` `col` is [not] null `` |
| `col LIKE 'abc%'`, `starts_with(col, 'abc')` | `` startswith(`col`, "abc") `` |
| `ORDER BY <number/date column> [DESC] LIMIT n` | `order_by=`, and the scan stops once DuckDB's Top-N is satisfied |

Not pushed (DuckDB applies them): ranges on text (the portal's collation differs from
DuckDB's), infix `LIKE '%x%'`/`contains` (ODSQL `like` is token-based full-text matching and
would miss rows), `OR` across columns, and anything on list, struct or JSON columns.
String literals are quoted with `"` and backslash escapes; dates use ODSQL `date'...'` literals.

On `datasets()`, filters on `dataset_id, title, publisher, license, language` (equality, `IN`,
`IS NULL`, prefix) and on `records_count, modified, data_processed, metadata_processed` (ranges)
are pushed the same way. Themes and keywords are lists: use `theme :=` / `keyword :=`.

### Records API vs. export endpoint (chosen automatically)

The Explore API's records endpoint caps `offset + limit` at **10,000 rows**. Each scan first asks
the portal how many rows match (`limit=0`), then:

* **≤ 10,000 rows**: pages of 100 from `/records` (with `order_by` when DuckDB sorts on a
  numeric/date column).
* **> 10,000 rows**: streams `/exports/jsonl` (or `/catalog/exports/json` for catalogs) with the
  same `select`/`where`, decoding it incrementally, 2,000 rows per batch. There is no cap, but it
  is a full download of every matching row: filter first.
* **`ORDER BY col LIMIT n`** with no filter: `n` rows sorted by the portal (non-nulls, then nulls
  as SQL requires), no count request.

The cursor (frozen ODSQL, phase, offset, rows emitted) lives in the scan state, so a scan served
over HTTP by several worker processes resumes correctly; an export reopened in another process
skips the rows already emitted.

For `GROUP BY` over big datasets, ask the portal instead of downloading everything:

```sql
SELECT country, cities, population
FROM opendatasoft.main.odsql('public.opendatasoft.com', 'geonames-all-cities-with-a-population-1000',
  'cou_name_en AS country, count(*) AS cities, sum(population) AS population',
  group_by := 'cou_name_en', order_by := 'population DESC', max_rows := 10);
```

`odsql()` runs the query when it is planned so it can type the result columns (dataset fields keep
their types; other columns are inferred from the values). `group_by` returns at most 20,000
groups; otherwise up to 10,000 rows are paged.

## Install

Python 3.14+ and [uv](https://docs.astral.sh/uv/). Run the worker straight from a release tag
(workers are not published to PyPI):

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'opendatasoft' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-opendatasoft@v0.1.1 vgi-opendatasoft-worker');
```

From a checkout: `make build`, then `LOCATION '/abs/path/to/vgi-opendatasoft/.venv/bin/vgi-opendatasoft-worker'`.
Hosted: `vgi-opendatasoft-worker --http --port 8000` (or the Dockerfile), then
`ATTACH 'opendatasoft' (TYPE vgi, LOCATION 'https://your-host')`.

## Examples

```sql
-- Which themes does Paris publish most?
SELECT value AS theme, count AS datasets
FROM opendatasoft.main.facets('opendata.paris.fr', facet := 'theme') ORDER BY count DESC;

-- Recently updated environment datasets with at least 1,000 records
SELECT dataset_id, title, records_count, modified
FROM opendatasoft.main.datasets('opendata.paris.fr', theme := 'Environnement')
WHERE modified >= TIMESTAMPTZ '2025-01-01' AND records_count >= 1000 ORDER BY modified DESC;

-- What does a dataset look like?
SELECT name, ods_type, duckdb_type FROM opendatasoft.main.fields('opendata.paris.fr', 'arrondissements');

-- The five biggest French cities (sorted and limited by the portal)
SELECT name, population, coordinates.lat, coordinates.lon
FROM opendatasoft.main.records('public.opendatasoft.com', 'geonames-all-cities-with-a-population-1000')
WHERE country_code = 'FR' ORDER BY population DESC LIMIT 5;

-- Shapes into the spatial extension
INSTALL spatial; LOAD spatial;
SELECT l_aroff, ST_Area(ST_GeomFromGeoJSON(geom)) AS area_deg2
FROM opendatasoft.main.records('opendata.paris.fr', 'arrondissements');
```

## Private datasets (API keys)

Create a key in the portal's account settings and store it as a DuckDB secret **scoped to the
portal**. The key is sent as `Authorization: Apikey ...` only to portals whose URL starts with the
secret's `SCOPE`; an unscoped `opendatasoft` secret is ignored, so a key is never sent to the
wrong portal. Keys never appear in URLs, logs or scan state, and `duckdb_secrets()` redacts them.

```sql
CREATE SECRET paris_key (TYPE opendatasoft, APIKEY 'your-key', SCOPE 'https://data.example.com');
SELECT count(*) FROM opendatasoft.main.records('data.example.com', 'my-private-dataset');
```

Requests time out after 60 s; HTTP 429 and 5xx are retried up to 5 times with backoff that
honours `Retry-After`.

## Testing

```bash
make check                      # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-sql TRANSPORT=stdio   # one SQL lane
make lint-explain LINT_ARGS=    # vgi-lint with the examples executed against the live portals
OPENDATASOFT_LIVE=1 uv run pytest tests/test_live.py   # optional live smoke test
```

The SQL tests (`test/sql/opendatasoft.test`) run through the real `vgi` extension against
`test/mock_api/server.py`, a **strict replay server** for responses recorded once from
`opendata.paris.fr`, `public.opendatasoft.com` and `data.opendatasoft.com` (stored in
`test/fixtures/`, exports truncated to 40 rows). A request is answered only if its path and its
exact ODSQL parameters were recorded, so every query in the suite also asserts what was pushed
down: a lost filter or a differently quoted literal is an unrecorded request and fails the test.
The mock also rate-limits each URL once (429) to exercise retries, and serves a hand-made private
dataset that needs an API key. To re-record after changing a query:
`MOCK_RECORD=1 make test-sql TRANSPORT=stdio` (respect the portals' terms: keep it small).

## Support

Open an issue on [Query-farm/vgi-opendatasoft](https://github.com/Query-farm/vgi-opendatasoft/issues).
For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

Data you read through this worker is published by each portal under its own license (see the
`license` column of `datasets()`). Opendatasoft and Huwise are trademarks of their owners; this
project is not affiliated with them.
