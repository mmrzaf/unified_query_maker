from .base_sql import SQLTranslator


class PostgreSQLTranslator(SQLTranslator):
    """PostgreSQL specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with double quotes in PostgreSQL"""
        return f'"{identifier}"'

    def _escape_string(self, value: str) -> str:
        """
        Escape string values for PostgreSQL.
        PostgreSQL also uses backslash for escaping.
        """
        return value.replace("'", "''").replace("\\", "\\\\")
