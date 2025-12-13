from unified_query_maker.models import UQLQuery
from .base_sql import SQLTranslator


class OracleTranslator(SQLTranslator):
    """Oracle specific translator."""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with double quotes in Oracle."""
        return f'"{identifier}"'

    def _build_limit_clause(self, query: UQLQuery) -> str:
        """
        Oracle pagination semantics:

        - LIMIT n           →   FETCH FIRST n ROWS ONLY
        - LIMIT n, OFFSET m →   OFFSET m ROWS FETCH NEXT n ROWS ONLY
        - OFFSET m only     →   OFFSET m ROWS
        """
        limit = query.limit
        offset = query.offset or 0

        if limit is None and offset == 0:
            return ""

        if limit is not None and offset > 0:
            return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        if limit is not None:
            return f"FETCH FIRST {limit} ROWS ONLY"
        # offset only
        return f"OFFSET {offset} ROWS"
