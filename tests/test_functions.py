"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
import io
from typing import Any

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    InFilter,
    IsNullFilter,
    OrFilter,
)

from opendatasoft_worker.client import OdsClient, OdsError, _iter_json_array, normalize_portal
from opendatasoft_worker.common import apikey_from, scope_match_length, strip_html
from opendatasoft_worker.discovery import facet_rows
from opendatasoft_worker.odsql import (
    FILE_TYPE,
    GEO_POINT_TYPE,
    JSON_TYPE,
    TIMESTAMPTZ,
    literal,
    string_literal,
    to_arrow_batch,
    translate_filter,
)
from opendatasoft_worker.odsql_query import infer_type, split_select
from opendatasoft_worker.records import schema_for_fields

SCHEMA = ["main"]
COLS = {
    "name": ("name", "text"),
    "pop": ("pop", "int"),
    "d": ("d", "date"),
    "ts": ("ts", "datetime"),
    "flag": ("flag", "boolean"),
}


def _const(col: str, op: ComparisonOp, value: Any) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value))


# ---------------------------------------------------------------- ODSQL translation


def test_string_literal_escapes_quotes_and_backslashes() -> None:
    assert string_literal('O\'Brien "x" \\') == '"O\'Brien \\"x\\" \\\\"'


def test_literals() -> None:
    assert literal(3) == "3"
    assert literal(2.5) == "2.5"
    assert literal(float("nan")) is None
    assert literal(True) == "true"
    assert literal(dt.date(2024, 1, 2)) == "date'2024-01-02'"
    ts = dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert literal(ts) == "date'2024-01-02T01:04:05+00:00'"


def test_equality_and_ranges() -> None:
    assert translate_filter(_const("name", ComparisonOp.EQ, "Paris"), COLS) == '`name` = "Paris"'
    assert translate_filter(_const("pop", ComparisonOp.GE, 10), COLS) == "`pop` >= 10"
    assert translate_filter(_const("d", ComparisonOp.LT, dt.date(2020, 1, 1)), COLS) == "`d` < date'2020-01-01'"
    assert translate_filter(_const("flag", ComparisonOp.EQ, True), COLS) == "`flag` = true"


def test_text_ranges_are_not_pushed() -> None:
    # Portal collation may differ from DuckDB's byte order.
    assert translate_filter(_const("name", ComparisonOp.GT, "M"), COLS) is None


def test_unknown_columns_are_not_pushed() -> None:
    assert translate_filter(_const("geo", ComparisonOp.EQ, "x"), COLS) is None


def test_type_mismatch_is_not_pushed() -> None:
    assert translate_filter(_const("pop", ComparisonOp.EQ, "10"), COLS) is None


def test_in_and_null() -> None:
    f = InFilter(column_name="name", column_index=0, values=pa.array(["a", 'b"']))
    assert translate_filter(f, COLS) == '`name` in ("a", "b\\"")'
    assert translate_filter(IsNullFilter(column_name="pop", column_index=0), COLS) == "`pop` is null"


def test_like_prefix_range_becomes_startswith() -> None:
    f = AndFilter(
        column_name="name",
        column_index=0,
        children=(_const("name", ComparisonOp.GE, "Lou"), _const("name", ComparisonOp.LT, "Lov")),
    )
    assert translate_filter(f, COLS) == 'startswith(`name`, "Lou")'


def test_and_keeps_translatable_parts_or_needs_all() -> None:
    good = _const("pop", ComparisonOp.GT, 1)
    bad = _const("name", ComparisonOp.GT, "x")
    assert translate_filter(AndFilter(column_name="", column_index=0, children=(good, bad)), COLS) == "(`pop` > 1)"
    assert translate_filter(OrFilter(column_name="", column_index=0, children=(good, bad)), COLS) is None
    both = OrFilter(column_name="", column_index=0, children=(good, _const("pop", ComparisonOp.LT, -1)))
    assert translate_filter(both, COLS) == "(`pop` > 1) or (`pop` < -1)"


# ---------------------------------------------------------------- types and values


def test_schema_for_fields_maps_types() -> None:
    schema = schema_for_fields(
        [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "double"},
            {"name": "c", "type": "date"},
            {"name": "d", "type": "datetime"},
            {"name": "e", "type": "geo_point_2d"},
            {"name": "f", "type": "geo_shape"},
            {"name": "g", "type": "file"},
            {"name": "h", "type": "text", "annotations": {"multivalued": ","}},
            {"name": "i", "type": "text"},
        ]
    )
    assert [f.type for f in schema] == [
        pa.int64(),
        pa.float64(),
        pa.date32(),
        TIMESTAMPTZ,
        GEO_POINT_TYPE,
        JSON_TYPE,
        FILE_TYPE,
        pa.list_(pa.string()),
        pa.string(),
    ]


def test_to_arrow_batch_converts_values() -> None:
    schema = schema_for_fields(
        [
            {"name": "n", "type": "int"},
            {"name": "t", "type": "datetime"},
            {"name": "p", "type": "geo_point_2d"},
            {"name": "s", "type": "geo_shape"},
        ]
    )
    rows: list[dict[str, Any]] = [
        {
            "n": 5,
            "t": "2024-05-01T10:00:00+02:00",
            "p": {"lon": 2.3, "lat": 48.8},
            "s": {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]}, "properties": {}},
        },
        {"n": None, "t": None, "p": None, "s": None},
    ]
    batch = to_arrow_batch(rows, schema)
    assert batch.column("n").to_pylist() == [5, None]
    assert batch.column("t")[0].as_py() == dt.datetime(2024, 5, 1, 8, 0, tzinfo=dt.UTC)
    assert batch.column("p")[0].as_py() == {"lon": 2.3, "lat": 48.8}
    assert batch.column("s").cast(pa.string()).to_pylist()[0] == '{"type": "Point", "coordinates": [1, 2]}'


# ---------------------------------------------------------------- portals, secrets, parsing


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("opendata.paris.fr", "https://opendata.paris.fr"),
        ("https://opendata.paris.fr/explore/dataset/arrondissements/table/", "https://opendata.paris.fr"),
        ("https://Data.Example.com/api/explore/v2.1/", "https://data.example.com"),
        ("http://127.0.0.1:8080/paris/", "http://127.0.0.1:8080/paris"),
    ],
)
def test_normalize_portal(value: str, expected: str) -> None:
    assert normalize_portal(value) == expected


def test_normalize_portal_rejects_garbage() -> None:
    with pytest.raises(OdsError):
        normalize_portal("ftp://x")


def test_secret_scope_matching() -> None:
    assert scope_match_length("https://opendata.paris.fr", "https://opendata.paris.fr") > 0
    assert scope_match_length("opendata.paris.fr", "https://opendata.paris.fr") > 0
    assert scope_match_length("https://opendata.paris", "https://opendata.paris.fr") == -1
    secrets = [
        {"apikey": "unscoped", "scope": ""},
        {"apikey": "other", "scope": "https://elsewhere.example.com"},
        {"apikey": "paris", "scope": "https://opendata.paris.fr"},
    ]
    assert apikey_from(secrets, "https://opendata.paris.fr") == "paris"
    assert apikey_from(secrets, "https://public.opendatasoft.com") is None


def test_url_sorts_params_and_repeats_lists() -> None:
    c = OdsClient("https://x.example")
    assert c.url("/catalog/datasets", {"where": "a = 1", "refine": ["t:b", "t:a"], "limit": 0}) == (
        "https://x.example/api/explore/v2.1/catalog/datasets?limit=0&refine=t%3Aa&refine=t%3Ab&where=a+%3D+1"
    )


def test_json_array_stream() -> None:
    raw = b'[{"a": 1}, {"a": "x,]"} ,\n {"a": [1, 2]}]'
    assert list(_iter_json_array(io.BytesIO(raw), chunk_size=3)) == [{"a": 1}, {"a": "x,]"}, {"a": [1, 2]}]
    assert list(_iter_json_array(io.BytesIO(b"[]"))) == []


def test_strip_html() -> None:
    assert strip_html("<p>Hello <b>world</b></p><p>2&amp;3</p>") == "Hello world\n2&3"


def test_split_select_and_infer() -> None:
    assert split_select("a, count(*) AS n, round(avg(x), 2) as `m`") == [
        ("a", None),
        ("count(*)", "n"),
        ("round(avg(x), 2)", "m"),
    ]
    assert infer_type([1, None, 3])[0] == pa.int64()
    assert infer_type([1, 2.5])[0] == pa.float64()
    assert infer_type(["2024-01-01T00:00:00+00:00"])[0] == TIMESTAMPTZ
    assert infer_type(["2024-01-01"])[0] == pa.date32()
    assert infer_type([{"lon": 1, "lat": 2}])[0] == GEO_POINT_TYPE


def test_facet_rows_flatten_hierarchy() -> None:
    body = {
        "facets": [
            {
                "name": "tz",
                "facets": [
                    {
                        "name": "Europe",
                        "value": "Europe",
                        "count": 3,
                        "facets": [{"name": "Paris", "value": "Paris", "count": 2}],
                    }
                ],
            }
        ]
    }
    rows = facet_rows("https://x.example", None, body)
    assert [(r["path"], r["depth"], r["count"]) for r in rows] == [("Europe", 0, 3), ("Europe/Paris", 1, 2)]


# Protocol-level calls through vgi.client.Client are not tested here: every function asks
# DuckDB for a portal-scoped secret at bind time, and the Python Client does not answer
# secret-scope requests. test/sql covers the functions end to end through DuckDB.
