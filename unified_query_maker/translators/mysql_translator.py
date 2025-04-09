from .base_sql import SQLTranslator


class MySQLTranslator(SQLTranslator):
    """MySQL specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with backticks in MySQL"""
        return f"`{identifier}`"

    def _escape_like_value(self, value: str) -> str:
        """MySQL uses backslash for escaping LIKE special chars"""
        escaped = self._escape_string(value)
        return escaped.replace("%", "\\%").replace("_", "\\_")
