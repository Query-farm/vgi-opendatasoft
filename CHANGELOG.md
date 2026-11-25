# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.1,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- New `serve` extra (`vgi-python[http,oauth]`); the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.1

- Fix: every query whose filter is pushed down to the worker (a `WHERE` clause, or
  `ORDER BY … LIMIT`) failed with "No DuckDB-compatible engine is installed". The SDK binds
  and evaluates pushed predicates with a local engine, which was only in the dev
  dependency group; the runtime dependency is now `vgi-python[http,haybarn]`.
- Fix: portals that gzip their responses without being asked (opendata.paris.fr) failed with
  `UnicodeDecodeError`; gzip responses are now decompressed.

## 0.1.0

First release: any Opendatasoft (Huwise) portal as SQL through the Explore API v2.1.

- `datasets`, `fields`, `facets`: catalog search (theme/keyword/refine/q/filter), dataset schemas with
  DuckDB types, portal-side facet counts.
- `records`: typed rows (BIGINT/DOUBLE/DATE/TIMESTAMPTZ/STRUCT points/GeoJSON JSON/file STRUCT/VARCHAR[])
  with select/where/Top-N pushdown into ODSQL; automatic switch to the streamed export endpoint above
  the records API's 10,000-row cap; resumable cursor state.
- `odsql`: server-side ODSQL aggregates with typed results; `odsql_quote` scalar.
- Optional `opendatasoft` secret (APIKEY, scoped to the portal URL) for private datasets.
- Strict replay fixtures recorded from opendata.paris.fr, public.opendatasoft.com and data.opendatasoft.com.

Generated from vgi-starter-python v1.3.0.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
