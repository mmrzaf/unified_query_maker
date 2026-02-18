from unified_query_maker.models import UQLQuery

from .base_sql import SQLTranslator


class MSSQLTranslator(SQLTranslator):
    def _escape_identifier(self, identifier: str) -> str:
        return f"[{identifier}]"

    def _param_placeholder(self, index_1_based: int) -> str:
        # Typical DB-API style for SQL Server via pyodbc.
        return "?"

    def _build_order_by_clause(self, query: UQLQuery) -> str:
        base = super()._build_order_by_clause(query)
        if base:
            return base
        # SQL Server requires ORDER BY for OFFSET/FETCH usage
        if query.limit is not None or (query.offset or 0) > 0:
            return "ORDER BY (SELECT NULL)"
        return ""

    def _build_limit_clause(self, query: UQLQuery) -> str:
        offset = query.offset or 0
        limit = query.limit
        if limit is None and offset == 0:
            return ""
        if limit is None:
            return f"OFFSET {offset} ROWS"
        return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    def _format_bool(self, value: bool) -> str:
        return "1" if value else "0"
