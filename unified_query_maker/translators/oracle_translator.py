from typing import Dict, Any
from .base_sql import SQLTranslator


class OracleTranslator(SQLTranslator):
    """Oracle specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with double quotes in Oracle"""
        return f'"{identifier}"'

    def translate(self, query: Dict[str, Any]) -> str:
        """Oracle has specific pagination approach using ROWNUM"""
        if "limit" not in query and "offset" not in query:
            return super().translate(query)

        limit = query.get("limit")
        offset = query.get("offset", 0)

        select_clause = self._build_select_clause(query)
        from_clause = self._build_from_clause(query)
        where_clause = self._build_where_clause(query)
        order_by_clause = self._build_order_by_clause(query)

        if limit is not None and offset > 0:
            pagination = f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        elif limit is not None:
            pagination = f"FETCH FIRST {limit} ROWS ONLY"
        else:
            pagination = ""

        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            order_by_clause,
            pagination,
        ]

        sql_query = " ".join([part for part in sql_parts if part])

        return f"{sql_query}{self._get_query_terminator()}"
