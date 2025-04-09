from .base_sql import SQLTranslator


class PostgreSQLTranslator(SQLTranslator):
    """PostgreSQL specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with double quotes in PostgreSQL"""
        return f'"{identifier}"'

    def _escape_like_value(self, value: str) -> str:
        """PostgreSQL uses ESCAPE option for LIKE patterns"""
        escaped = self._escape_string(value)
        return escaped.replace("%", "\\%").replace("_", "\\_")

    def _build_match_condition(self, field: str, value: str) -> str:
        """Build LIKE condition with ESCAPE clause for PostgreSQL"""
        field = self._escape_identifier(field)
        escaped_value = self._escape_like_value(value)
        return f"{field} LIKE '%{escaped_value}%' ESCAPE '\\'"
