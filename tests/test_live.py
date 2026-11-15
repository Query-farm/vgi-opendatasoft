"""Optional live smoke test against real portals. Skipped unless OPENDATASOFT_LIVE=1.

It checks that the Explore API v2.1 still answers the way the recorded fixtures
assume (field types, ODSQL select/where, export streaming). Keyless and tiny.
"""

from __future__ import annotations

import os

import pytest

from opendatasoft_worker.client import OdsClient, normalize_portal
from opendatasoft_worker.records import schema_for_fields

pytestmark = pytest.mark.skipif(os.environ.get("OPENDATASOFT_LIVE") != "1", reason="set OPENDATASOFT_LIVE=1")


def test_live_paris_arrondissements() -> None:
    client = OdsClient(normalize_portal("opendata.paris.fr"))
    meta = client.get_json("/catalog/datasets/arrondissements")
    schema = schema_for_fields(meta["fields"])
    assert {"c_ar", "l_aroff", "geom"} <= set(schema.names)
    body = client.get_json(
        "/catalog/datasets/arrondissements/records",
        {"select": "`l_aroff`", "where": "`c_ar` = 1", "limit": 1},
    )
    assert body["results"] == [{"l_aroff": "Louvre"}]


def test_live_export_streams() -> None:
    client = OdsClient(normalize_portal("opendata.paris.fr"))
    stream = client.open_stream(
        "/catalog/datasets/arrondissements/exports/jsonl", {"select": "`c_ar`", "order_by": "`c_ar`"}, fmt="jsonl"
    )
    try:
        rows = stream.take(3)
    finally:
        stream.close()
    assert [r["c_ar"] for r in rows] == [1, 2, 3]
