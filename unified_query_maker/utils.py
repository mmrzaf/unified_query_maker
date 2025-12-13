from __future__ import annotations

import re
from typing import Any, Dict, Tuple, List

# Strict-ish identifier safety:
#   - segment:   [A-Za-z_][A-Za-z0-9_]*
#   - qualified: seg(.seg)*
#   - star:      "*" or "seg.*"
_IDENT_SEG_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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
}


def _is_ident_segment(seg: str) -> bool:
    return bool(_IDENT_SEG_RE.match(seg))


def validate_qualified_name(
    name: str,
    *,
    allow_star: bool = False,
    allow_trailing_star: bool = False,
) -> str:
    """
    Validates a safe identifier or qualified identifier.

    Examples:
      - "users"
      - "schema.users"
      - "users.id"
      - "*" (if allow_star)
      - "users.*" (if allow_trailing_star)
    """
    if name is None:
        raise ValueError("Name cannot be None")

    raw = str(name).strip()
    if not raw:
        raise ValueError("Name cannot be empty")

    if raw == "*":
        if allow_star:
            return raw
        raise ValueError("'*' is not allowed here")

    parts = raw.split(".")
    if any(p == "" for p in parts):
        raise ValueError(f"Invalid qualified name: {raw!r}")

    for i, p in enumerate(parts):
        if p == "*":
            if allow_trailing_star and i == len(parts) - 1 and len(parts) > 1:
                continue
            raise ValueError(f"Invalid '*' placement in name: {raw!r}")
        if not _is_ident_segment(p):
            raise ValueError(f"Unsafe identifier segment {p!r} in {raw!r}")

    return raw


def parse_condition(condition: Dict[str, Any]) -> Tuple[str, str, Any]:
    """
    Normalizes a single condition dict to (field, op, value) with validation.

    Accepts:
      {"status": "active"}             -> ("status", "eq", "active")
      {"age": {"gt": 30}}              -> ("age", "gt", 30)
      {"tags": {"in": ["a", "b"]}}     -> ("tags", "in", ["a", "b"])
      {"deleted_at": {"exists": true}} -> ("deleted_at", "exists", True)
    """
    if not isinstance(condition, dict) or len(condition) != 1:
        raise ValueError(f"Condition must be a single-key object, got: {condition!r}")

    field, op_value = next(iter(condition.items()))
    field = validate_qualified_name(field, allow_star=False, allow_trailing_star=False)

    if isinstance(op_value, dict):
        if len(op_value) != 1:
            raise ValueError(f"Operator object must be single-key, got: {op_value!r}")
        op, value = next(iter(op_value.items()))
    else:
        op, value = "eq", op_value

    if op not in ALLOWED_OPS:
        raise ValueError(f"Unsupported operator {op!r}. Allowed: {sorted(ALLOWED_OPS)}")

    if op in ("in", "nin"):
        if not isinstance(value, list):
            raise ValueError(
                f"Operator {op!r} requires a list value, got: {type(value).__name__}"
            )
        if len(value) == 0:
            raise ValueError(f"Operator {op!r} requires a non-empty list")

    if op in ("exists", "nexists"):
        # translators treat these as unary; value is allowed but ignored
        if value not in (True, False, None):
            raise ValueError(
                f"Operator {op!r} requires a boolean-ish value, got: {value!r}"
            )

    return field, op, value


def escape_single_quotes(value: str) -> str:
    """Best-effort single-quote escape for string-literal contexts."""
    return value.replace("'", "''")


def format_list_sql(values: List[Any], fmt_item) -> str:
    """Formats a list as '(a, b, c)' for SQL-like syntaxes."""
    return "(" + ", ".join(fmt_item(v) for v in values) + ")"
