from __future__ import annotations

import re


def escape_single_quotes(s: str) -> str:
    """Escape single quotes for SQL string literals by doubling them."""
    return s.replace("'", "''")


def validate_qualified_name(
    name: str, *, allow_star: bool, allow_trailing_star: bool
) -> None:
    """Validate a dotted identifier.

    Rules:
      - segments separated by dots
      - segment: [A-Za-z_][A-Za-z0-9_]*
      - allow_star: allow name == '*'
      - allow_trailing_star: allow name like 'segment.*' (ONLY trailing)

    Raises:
      ValueError on invalid input.
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
