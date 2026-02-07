from __future__ import annotations

from .base_sql import SQLTranslator


class PostgreSQLTranslator(SQLTranslator):
    """PostgreSQL specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def _escape_string(self, value: str) -> str:
        # PostgreSQL also uses backslash for escaping.
        return value.replace("'", "''").replace("\\", "\\\\")

    def _render_ilike(self, field_sql: str, pattern: object) -> str:
        return f"{field_sql} ILIKE {self._format_value(pattern)} ESCAPE '\\\\'"

    def _render_regex(self, field_sql: str, pattern: object) -> str:
        return f"{field_sql} ~ {self._format_value(pattern)}"

    def _render_array_contains(self, field_sql: str, value: object) -> str:
        return f"{self._format_value(value)} = ANY({field_sql})"

    def _render_array_overlap(self, field_sql: str, values: object) -> str:
        if not isinstance(values, list):
            raise ValueError("ARRAY_OVERLAP expects a list value")
        array_literal = (
            "ARRAY[" + ", ".join(self._format_value(v) for v in values) + "]"
        )
        return f"{field_sql} && {array_literal}"

    def _render_array_contained(self, field_sql: str, values: object) -> str:
        if not isinstance(values, list):
            raise ValueError("ARRAY_CONTAINED expects a list value")
        array_literal = (
            "ARRAY[" + ", ".join(self._format_value(v) for v in values) + "]"
        )
        return f"{field_sql} <@ {array_literal}"
