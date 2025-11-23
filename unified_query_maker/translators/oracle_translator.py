from typing import Dict, Any, List
from pydantic import ValidationError
from .base_sql import SQLTranslator
from unified_query_maker.models import UQLQuery

class OracleTranslator(SQLTranslator):
    """Oracle specific translator"""

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers with double quotes in Oracle"""
        return f'"{identifier}"'

    def translate(self, query: Dict[str, Any]) -> str:
        """Oracle has specific pagination approach (OFFSET FETCH)"""

        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        select_clause = self._build_select_clause(parsed_query)
        from_clause = self._build_from_clause(parsed_query)
        where_clause = self._build_where_clause(parsed_query)
        order_by_clause = self._build_order_by_clause(parsed_query)

        pagination = ""
        limit = parsed_query.limit
        offset = parsed_query.offset or 0

        if limit is not None:
            if offset > 0:
                pagination = f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
            else:
                pagination = f"FETCH FIRST {limit} ROWS ONLY"
        elif offset > 0:
             # Offset without limit
             pagination = f"OFFSET {offset} ROWS"

        sql_parts: List[str] = [
            select_clause,
            from_clause,
            where_clause,
            order_by_clause,
            pagination,
        ]

        sql_query = " ".join([part for part in sql_parts if part])
        return f"{sql_query}{self._get_query_terminator()}"
