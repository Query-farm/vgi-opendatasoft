"""``odsql_quote(value)``: quote a value as an ODSQL string literal.

Use it to build ``filter :=`` conditions from data safely, e.g.
``filter := 'publisher = ' || opendatasoft.main.odsql_quote(name)``.
Runs locally; no network.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .odsql import string_literal


class OdsqlQuote(ScalarFunction):
    """``odsql_quote(value)``: an ODSQL double-quoted string literal."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "odsql_quote"
        description = "Quote text as an ODSQL string literal for filter := conditions"
        tags = {
            **object_tags(
                title="Quote ODSQL Literal",
                category="data",
                keywords="odsql, quote, escape, literal, string, filter, injection-safe",
                doc_llm=(
                    'Return `value` as an ODSQL string literal: wrapped in double quotes with `\\\\` and `"` '
                    "backslash-escaped, ready to concatenate into a `filter :=` condition of the other "
                    "functions, e.g. 'publisher = ' || odsql_quote(p). NULL in, NULL out. Runs locally "
                    "without contacting any portal. SQL WHERE clauses on records() do not need it: those "
                    "are translated and quoted automatically."
                ),
                doc_md=(
                    "## odsql_quote(value)\n\n"
                    "Quote text for use inside a raw ODSQL `filter :=` condition.\n\n"
                    '`odsql_quote(\'Hôtel "de" Ville\')` returns `"Hôtel \\"de\\" Ville"`.'
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Quote a value containing double quotes and an apostrophe",
                        "sql": "SELECT opendatasoft.main.odsql_quote('O''Brien \"Jr\"') AS literal",
                        "expected_result": [{"literal": '"O\'Brien \\"Jr\\""'}],
                    },
                    {
                        "description": "Build an ODSQL condition for each value of a column",
                        "sql": (
                            "SELECT 'publisher = ' || opendatasoft.main.odsql_quote(p) AS condition "
                            "FROM (VALUES ('Airparif'), ('Ville de Paris')) t(p) ORDER BY p"
                        ),
                        "expected_result": [
                            {"condition": 'publisher = "Airparif"'},
                            {"condition": 'publisher = "Ville de Paris"'},
                        ],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT opendatasoft.main.odsql_quote('O''Brien \"Jr\"') AS literal",
                description="Quote a value containing quotes for an ODSQL condition",
            ),
            FunctionExample(
                sql=(
                    "SELECT dataset_id FROM opendatasoft.main.datasets('opendata.paris.fr', "
                    "filter := 'publisher = ' || opendatasoft.main.odsql_quote('Airparif')) ORDER BY dataset_id"
                ),
                description="Use a quoted literal inside a raw ODSQL filter",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="Text to quote; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Quote every value."""
        return pa.array(
            [None if v is None else string_literal(v) for v in value.to_pylist()],
            type=pa.string(),
        )
