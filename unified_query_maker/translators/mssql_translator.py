from .base_sql import SQLTranslator


class MSSQLTranslator(SQLTranslator):
    """Microsoft SQL Server specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with square brackets in MSSQL"""
        return f"[{identifier}]"

    def _build_limit_offset(self, limit: int, offset: int) -> str:
        """SQL Server uses OFFSET FETCH syntax instead of LIMIT OFFSET"""
        return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    def _build_limit_clause(self, query: Dict[str, Any]) -> str:
        """Build the LIMIT clause for SQL Server"""
        if "limit" not in query:
            return ""

        if "orderBy" not in query or not query["orderBy"]:
            # SQL Server requires ORDER BY for OFFSET/FETCH
            return f"ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {query['limit']} ROWS ONLY"

        limit = query["limit"]
        offset = query.get("offset", 0)

        return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
