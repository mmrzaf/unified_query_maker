from __future__ import annotations

from unified_query_maker.models import UQLQuery

from .base_sql import SQLTranslator


class MySQLTranslator(SQLTranslator):
    """MySQL specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    def _build_limit_clause(self, query: UQLQuery) -> str:
        limit = query.limit
        offset = query.offset or 0

        if limit is None and offset == 0:
            return ""
        if limit is None:
            # MySQL doesn't support OFFSET without LIMIT. Use a very large LIMIT.
            return f"LIMIT {offset}, 18446744073709551615"
        if offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {limit} OFFSET {offset}"

    def _render_regex(self, field_sql: str, pattern: object) -> str:
        return f"{field_sql} REGEXP {self._format_value(pattern)}"
