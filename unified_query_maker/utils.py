from __future__ import annotations

import re
from typing import Any, Dict, Tuple

ALLOWED_OPS = {
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "nin",
    "exists",
    "nexists",
    # extended
    "between",
    "contains",
    "ncontains",
    "icontains",
    "starts_with",
    "ends_with",
    "ilike",
    "regex",
    "array_contains",
    "array_overlap",
    "array_contained",
    "geo_within",
    "geo_intersects",
}


def escape_single_quotes(s: str) -> str:
    return s.replace("'", "''")


def validate_qualified_name(
    name: str, *, allow_star: bool, allow_trailing_star: bool
) -> None:
    """
    Valid identifiers: letters, digits, underscore; segments separated by dots.

    allow_star:
      - allow the entire name to be '*'

    allow_trailing_star:
      - allow 'segment.*' (ONLY trailing)
    """
    raw = str(name).strip()
    if raw == "*":
        if allow_star:
            return
        raise ValueError("'*' is not allowed here")

    if allow_trailing_star and raw.endswith(".*"):
        base = raw[:-2]
        if not base:
            raise ValueError("Invalid qualified name")
        parts = base.split(".")
        for p in parts:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p):
                raise ValueError(f"Invalid identifier segment: {p}")
        return

    parts = raw.split(".")
    for p in parts:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p):
            raise ValueError(f"Invalid identifier segment: {p}")


def parse_condition(condition: Dict[str, Any]) -> Tuple[str, str, Any]:
    """
    Parse legacy dict condition formats:

      {"status": "active"}       -> ("status", "eq", "active")
      {"age": {"gt": 30}}        -> ("age", "gt", 30)
      {"field": {"exists": true}}-> ("field","exists",True)
    """
    if not isinstance(condition, dict) or not condition:
        raise ValueError("Condition must be a non-empty object")

    if len(condition) != 1:
        raise ValueError("Legacy condition objects must have exactly one field key")

    field, op_value = next(iter(condition.items()))
    validate_qualified_name(
        str(field).strip(), allow_star=False, allow_trailing_star=False
    )

    # {"field": value} -> eq
    if not isinstance(op_value, dict):
        op = "eq"
        value = op_value
        if op not in ALLOWED_OPS:
            raise ValueError(f"Operator {op} is not allowed")
        return field, op, value

    # {"field": {"op": value}}
    if len(op_value) != 1:
        raise ValueError("Operator object must have exactly one operator")

    op, value = next(iter(op_value.items()))
    if op not in ALLOWED_OPS:
        raise ValueError(f"Operator {op} is not allowed")

    return field, op, value
