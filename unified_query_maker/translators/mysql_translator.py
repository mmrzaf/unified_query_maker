from .base_sql import SQLTranslator

class MySQLTranslator(SQLTranslator):
    """MySQL specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with backticks in MySQL"""
        return f"`{identifier}`"
