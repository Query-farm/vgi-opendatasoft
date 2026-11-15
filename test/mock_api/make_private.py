#!/usr/bin/env python3
"""Write the hand-made fixtures of the ``private`` portal (a dataset that needs an API key).

Run after changing the requests the SQL tests make against it:
``python3 test/mock_api/make_private.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from server import canonical, fixture_file

API = "/api/explore/v2.1/catalog/datasets/budget-2026"
FIELDS = [
    {"name": "poste", "label": "Poste", "type": "text", "description": None, "annotations": {"facet": True}},
    {"name": "montant", "label": "Montant (EUR)", "type": "double", "description": None, "annotations": {}},
    {"name": "vote_le", "label": "Voté le", "type": "date", "description": None, "annotations": {}},
]
ROWS = [
    {"poste": "Voirie", "montant": 1250000.5, "vote_le": "2026-01-15"},
    {"poste": "Écoles", "montant": 980000.0, "vote_le": "2026-01-15"},
    {"poste": "Parcs", "montant": None, "vote_le": None},
]
RESPONSES = {
    API: {
        "dataset_id": "budget-2026",
        "dataset_uid": "da_private",
        "has_records": True,
        "visibility": "restricted",
        "features": ["analyze"],
        "fields": FIELDS,
        "metas": {"default": {"title": "Budget 2026 (privé)", "records_count": 3}},
    },
    API + "/records?limit=0": {"total_count": 3, "results": []},
    API + "/records?limit=3&select=`poste`,`montant`,`vote_le`": {"total_count": 3, "results": ROWS},
}


def main() -> None:
    """Write every response as a fixture file."""
    for request, body in RESPONSES.items():
        path, _, query = request.partition("?")
        key = canonical(path, query)
        f = fixture_file("private", key)
        f.parent.mkdir(parents=True, exist_ok=True)
        data = {"request": key, "status": 200, "content_type": "application/json", "truncated": False}
        data["body"] = json.dumps(body, ensure_ascii=False)
        f.write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n")
        print(f.name, key)


if __name__ == "__main__":
    main()
