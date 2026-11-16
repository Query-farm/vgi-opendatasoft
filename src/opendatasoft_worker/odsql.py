r"""ODSQL translation and Opendatasoft field typing.

* ``arrow_type_for`` maps an Opendatasoft field type to a DuckDB/Arrow type.
* ``to_arrow_batch`` converts decoded JSON rows into a typed Arrow batch.
* ``where_clauses`` translates DuckDB's pushed-down filters into ODSQL ``where``
  clauses. Only translations that can never drop a matching row are made; the SDK
  re-applies every pushed predicate exactly (``auto_apply_filters``), so a clause
  that matches a superset is safe, and an untranslated filter is simply left to DuckDB.

What translates (verified against live portals, see README "Pushdown"):

==========================  =============================================
SQL                         ODSQL
==========================  =============================================
``col = v`` / ``<>``        ``\\`col\\` = "v"`` (text: exact, case-sensitive)
``<, <=, >, >=``            numeric, date and datetime columns only
                            (text ordering/collation differs, so not pushed)
``col IN (...)``            ``\\`col\\` in (...)``
``IS [NOT] NULL``           ``\\`col\\` is [not] null``
``starts_with`` / ``LIKE 'p%'``  ``startswith(\\`col\\`, "p")`` (case-sensitive)
AND / OR                    AND of translatable parts; OR only if all parts translate
==========================  =============================================

ODSQL ``like`` is token-based full-text matching (``like "*ouv*"`` does not match
"Louvre"), so infix ``LIKE``/``contains`` is never pushed.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from collections.abc import Iterable, Sequence
from decimal import Decimal
from typing import Any

import pyarrow as pa

TIMESTAMPTZ = pa.timestamp("us", tz="UTC")
JSON_TYPE: pa.DataType = pa.json_(pa.string())

GEO_POINT_TYPE = pa.struct([pa.field("lon", pa.float64()), pa.field("lat", pa.float64())])
FILE_TYPE = pa.struct(
    [
        pa.field("id", pa.string()),
        pa.field("filename", pa.string()),
        pa.field("format", pa.string()),
        pa.field("url", pa.string()),
        pa.field("width", pa.int32()),
        pa.field("height", pa.int32()),
        pa.field("thumbnail", pa.bool_()),
        pa.field("color_summary", pa.list_(pa.string())),
    ]
)

# Field metadata key carrying the Opendatasoft type of a column.
ODS_TYPE_KEY = b"ods_type"

PUSHABLE_RANGE = {"int", "double", "decimal", "date", "datetime"}
PUSHABLE_EQ = PUSHABLE_RANGE | {"text", "boolean"}

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def arrow_type_for(ods_type: str, *, multivalued: bool = False) -> pa.DataType:
    """Return the Arrow (DuckDB) type for an Opendatasoft field type.

    Args:
        ods_type: The field's ``type`` from the dataset schema.
        multivalued: True when the field has a ``multivalued`` annotation.

    Returns:
        BIGINT / DOUBLE / BOOLEAN / DATE / TIMESTAMPTZ / STRUCT / JSON / VARCHAR,
        or a LIST of VARCHAR for multivalued text.

    """
    if multivalued and ods_type == "text":
        return pa.list_(pa.string())
    mapping: dict[str, pa.DataType] = {
        "int": pa.int64(),
        "double": pa.float64(),
        "decimal": pa.float64(),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "datetime": TIMESTAMPTZ,
        "geo_point_2d": GEO_POINT_TYPE,
        "geo_shape": JSON_TYPE,
        "json_blob": JSON_TYPE,
        "file": FILE_TYPE,
    }
    return mapping.get(ods_type, pa.string())


def duckdb_type_name(t: pa.DataType) -> str:
    """Return a DuckDB type name for documentation columns."""
    if t == pa.int64():
        return "BIGINT"
    if t == pa.float64():
        return "DOUBLE"
    if t == pa.bool_():
        return "BOOLEAN"
    if t == pa.date32():
        return "DATE"
    if t == TIMESTAMPTZ:
        return "TIMESTAMPTZ"
    if t == JSON_TYPE:
        return "JSON"
    if t == GEO_POINT_TYPE:
        return "STRUCT(lon DOUBLE, lat DOUBLE)"
    if t == FILE_TYPE:
        return (
            "STRUCT(id VARCHAR, filename VARCHAR, format VARCHAR, url VARCHAR, width INTEGER, "
            "height INTEGER, thumbnail BOOLEAN, color_summary VARCHAR[])"
        )
    if pa.types.is_list(t):
        return "VARCHAR[]"
    return "VARCHAR"


# ---------------------------------------------------------------- values


def parse_datetime(value: Any) -> dt.datetime | None:
    """Parse an ISO-8601 timestamp (``Z`` or offset) to an aware UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if len(text) == 10:
            text += "T00:00:00+00:00"
        parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def parse_date(value: Any) -> dt.date | None:
    """Parse ``YYYY-MM-DD`` (or the date part of a timestamp)."""
    if value is None or value == "":
        return None
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        f = float(value)
    except TypeError, ValueError:
        return None
    return int(f) if math.isfinite(f) and f == int(f) else None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except TypeError, ValueError:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("true", "t", "yes", "1", "oui"):
        return True
    if text in ("false", "f", "no", "0", "non"):
        return False
    return None


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _to_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [v if isinstance(v, str) else json.dumps(v, ensure_ascii=False) for v in value]
    return [str(value)]


def _to_json(value: Any, ods_type: str) -> str | None:
    if value is None:
        return None
    if ods_type == "geo_shape" and isinstance(value, dict) and value.get("type") == "Feature":
        value = value.get("geometry")
        if value is None:
            return None
    if isinstance(value, str):
        # json_blob values may already be serialized; keep valid JSON only.
        try:
            json.loads(value)
            return value
        except ValueError:
            return json.dumps(value, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _to_point(value: Any) -> dict[str, float | None] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return {"lon": _to_float(value.get("lon")), "lat": _to_float(value.get("lat"))}
    if isinstance(value, (list, tuple)) and len(value) == 2:
        # Legacy [lat, lon] order.
        return {"lon": _to_float(value[1]), "lat": _to_float(value[0])}
    if isinstance(value, str) and "," in value:
        lat, lon = value.split(",", 1)
        return {"lon": _to_float(lon), "lat": _to_float(lat)}
    return None


def _to_file(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    colors = value.get("color_summary")
    return {
        "id": _to_text(value.get("id")),
        "filename": _to_text(value.get("filename")),
        "format": _to_text(value.get("format")),
        "url": _to_text(value.get("url")),
        "width": _to_int(value.get("width")),
        "height": _to_int(value.get("height")),
        "thumbnail": _to_bool(value.get("thumbnail")),
        "color_summary": [str(c) for c in colors] if isinstance(colors, list) else None,
    }


def field_ods_type(f: pa.Field[Any]) -> str:
    """Return the Opendatasoft type stored in a field's metadata, else one inferred from its Arrow type."""
    meta = f.metadata or {}
    if ODS_TYPE_KEY in meta:
        return meta[ODS_TYPE_KEY].decode()
    t = f.type
    if t == pa.int64():
        return "int"
    if t == pa.float64():
        return "double"
    if t == pa.bool_():
        return "boolean"
    if t == pa.date32():
        return "date"
    if pa.types.is_timestamp(t):
        return "datetime"
    if t == GEO_POINT_TYPE:
        return "geo_point_2d"
    if t == FILE_TYPE:
        return "file"
    if t == JSON_TYPE:
        return "json_blob"
    return "text"


def convert_value(value: Any, f: pa.Field[Any]) -> Any:
    """Convert one decoded JSON value to the Python value Arrow expects for ``f``."""
    t = f.type
    ods_type = field_ods_type(f)
    if pa.types.is_list(t):
        return _to_list(value)
    if t == pa.int64() or t == pa.int32():
        return _to_int(value)
    if t == pa.float64():
        return _to_float(value)
    if t == pa.bool_():
        return _to_bool(value)
    if t == pa.date32():
        return parse_date(value)
    if pa.types.is_timestamp(t):
        return parse_datetime(value)
    if t == GEO_POINT_TYPE:
        return _to_point(value)
    if t == FILE_TYPE:
        return _to_file(value)
    if t == JSON_TYPE:
        return _to_json(value, ods_type)
    return _to_text(value)


def to_arrow_batch(
    rows: Sequence[dict[str, Any]], schema: pa.Schema, keys: dict[str, str] | None = None
) -> pa.RecordBatch:
    """Build a typed batch from decoded rows.

    Args:
        rows: Decoded JSON objects.
        schema: The (projected) output schema.
        keys: Optional map from column name to the JSON key holding its value.

    Returns:
        A record batch with exactly ``schema``.

    """
    keys = keys or {}
    arrays = []
    for f in schema:
        key = keys.get(f.name, f.name)
        values = [convert_value(r.get(key), f) for r in rows]
        storage_type = f.type.storage_type if isinstance(f.type, pa.ExtensionType) else f.type
        arr = pa.array(values, type=storage_type)
        if isinstance(f.type, pa.ExtensionType):
            arr = pa.ExtensionArray.from_storage(f.type, arr)
        arrays.append(arr)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------- ODSQL text


def ident(name: str) -> str:
    """Quote a field name as an ODSQL identifier (backticks)."""
    return "`" + name.replace("`", "") + "`"


def string_literal(value: str) -> str:
    r"""Return an ODSQL string literal: double quotes, ``\\`` and ``\"`` escaped."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def literal(value: Any) -> str | None:
    """Render a Python value as an ODSQL literal, or None if it cannot be expressed exactly."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value) if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, dt.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return "date'" + aware.astimezone(dt.UTC).isoformat() + "'"
    if isinstance(value, dt.date):
        return "date'" + value.isoformat() + "'"
    if isinstance(value, str):
        return string_literal(value)
    return None


def literal_matches_kind(value: Any, ods_type: str) -> bool:
    """Return True if a literal of this Python type is a sound comparison for the column kind."""
    if isinstance(value, bool):
        return ods_type == "boolean"
    if isinstance(value, (int, float, Decimal)):
        return ods_type in ("int", "double", "decimal")
    if isinstance(value, dt.datetime):
        return ods_type in ("datetime", "date")
    if isinstance(value, dt.date):
        return ods_type in ("date", "datetime")
    if isinstance(value, str):
        return ods_type == "text"
    return False


_OPS = {"eq": "=", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}


def _scalar_py(value: Any) -> Any:
    return value.as_py() if hasattr(value, "as_py") else value


def translate_filter(flt: Any, columns: dict[str, tuple[str, str]]) -> str | None:
    """Translate one pushed-down filter to ODSQL, or None when it cannot be pushed safely.

    Args:
        flt: A filter object from ``vgi.table_filter_pushdown``.
        columns: Map of output column name to ``(odsql field name, ods type)`` for
            the columns that may be pushed. Columns not listed are never pushed.

    Returns:
        An ODSQL boolean expression, or None.

    """
    kind = type(flt).__name__
    if kind == "AndFilter":
        prefix = _prefix_range(flt, columns)
        if prefix is not None:
            return prefix
        parts = [p for p in (translate_filter(c, columns) for c in flt.children) if p]
        return " and ".join(f"({p})" for p in parts) if parts else None
    if kind == "OrFilter":
        maybe = [translate_filter(c, columns) for c in flt.children]
        if not maybe or any(p is None for p in maybe):
            return None
        return " or ".join(f"({p})" for p in maybe)
    name = getattr(flt, "column_name", None)
    if name not in columns:
        return None
    field, ods_type = columns[name]
    col = ident(field)
    if kind == "IsNullFilter":
        return f"{col} is null"
    if kind == "IsNotNullFilter":
        return f"{col} is not null"
    if kind == "ConstantFilter":
        op = getattr(flt.op, "value", str(flt.op))
        value = _scalar_py(flt.value)
        if op not in _OPS or not literal_matches_kind(value, ods_type):
            return None
        if op not in ("eq", "ne") and ods_type not in PUSHABLE_RANGE:
            return None
        if ods_type not in PUSHABLE_EQ:
            return None
        lit = literal(value)
        return None if lit is None else f"{col} {_OPS[op]} {lit}"
    if kind == "InFilter":
        values = flt.values.to_pylist()
        if not values or ods_type not in PUSHABLE_EQ:
            return None
        lits = []
        for v in values:
            if v is None:
                continue
            if not literal_matches_kind(v, ods_type):
                return None
            lit = literal(v)
            if lit is None:
                return None
            lits.append(lit)
        return f"{col} in ({', '.join(lits)})" if lits else None
    if kind in ("ExpressionFilter", "V2ExpressionFilter"):
        prefix = _prefix_of(flt)
        if prefix and ods_type == "text":
            return f"startswith({col}, {string_literal(prefix)})"
    return None


def _prefix_range(flt: Any, columns: dict[str, tuple[str, str]]) -> str | None:
    """Recognize DuckDB's rewrite of ``LIKE 'p%'``: ``col >= 'p' AND col < 'q'`` (q = p with last char + 1)."""
    children = list(getattr(flt, "children", ()))
    if len(children) != 2 or any(type(c).__name__ != "ConstantFilter" for c in children):
        return None
    names = {c.column_name for c in children}
    if len(names) != 1:
        return None
    name = names.pop()
    if name not in columns or columns[name][1] != "text":
        return None
    by_op = {getattr(c.op, "value", str(c.op)): _scalar_py(c.value) for c in children}
    low, high = by_op.get("ge"), by_op.get("lt")
    if not isinstance(low, str) or not isinstance(high, str) or not low:
        return None
    if high != low[:-1] + chr(ord(low[-1]) + 1):
        return None
    return f"startswith({ident(columns[name][0])}, {string_literal(low)})"


_PREFIX_RE = re.compile(r"""^\s*(?:prefix|starts_with)\s*\(\s*"?[^,]*"?\s*,\s*'((?:[^']|'')*)'\s*\)\s*$""", re.I)


def _prefix_of(flt: Any) -> str | None:
    """Extract the literal prefix of a ``starts_with(col, 'p')``/``prefix`` expression filter."""
    expr = getattr(flt, "expr", None)
    if expr is None or not hasattr(expr, "to_sql"):
        return None
    try:
        sql = expr.to_sql(flt.column_name)
    except Exception:
        return None
    m = _PREFIX_RE.match(sql)
    if not m:
        return None
    prefix = m.group(1).replace("''", "'")
    return prefix or None


def where_clauses(filters: Any, columns: dict[str, tuple[str, str]]) -> list[str]:
    """Translate the top-level (AND-ed) pushed filters; untranslatable ones are skipped."""
    out: list[str] = []
    for flt in getattr(filters, "filters", None) or ():
        clause = translate_filter(flt, columns)
        if clause:
            out.append(clause)
    return out


def combine_where(parts: Iterable[str | None]) -> str | None:
    """AND together non-empty ODSQL clauses (each parenthesized)."""
    kept = [p.strip() for p in parts if p and p.strip()]
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return " and ".join(f"({p})" for p in kept)


def search_clause(q: str | None) -> str | None:
    """ODSQL full-text search for ``q``."""
    if not q or not q.strip():
        return None
    return f"search({string_literal(q.strip())})"


def refine_values(*pairs: tuple[str, str | None]) -> list[str]:
    """``refine`` parameter values (``facet:value``) for the non-empty pairs."""
    return [f"{facet}:{value}" for facet, value in pairs if value]


def is_identifier(name: str) -> bool:
    """Return True for a plain ODS field identifier."""
    return bool(_IDENT_RE.match(name))
