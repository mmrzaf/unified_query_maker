from typing import Dict, Any, List
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery, QueryOutput

class CassandraTranslator(QueryTranslator):

    def translate(self, query: Dict[str, Any]) -> QueryOutput:
        """Translates UQL dict to a Cassandra Query Language (CQL) string"""
        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        select_clause = f"SELECT {', '.join(parsed_query.select)}"
        from_clause = f"FROM {parsed_query.from_table}"

        where_conditions: List[str] = []
        if parsed_query.where:
            if parsed_query.where.must:
                must_conditions = " AND ".join(
                    self._parse_condition(cond) for cond in parsed_query.where.must
                )
                where_conditions.append(f"({must_conditions})")

            if parsed_query.where.must_not:
                must_not_conditions = " AND ".join(
                    self._parse_condition(cond) for cond in parsed_query.where.must_not
                )
                # Note: Cassandra has limited NOT support. This may need refinement.
                where_conditions.append(f"NOT ({must_not_conditions})")

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # Note: orderBy, limit, and offset are not implemented here.
        # Cassandra 'ORDER BY' requires specific clustering keys.
        # 'LIMIT' is available but not 'OFFSET'.

        return f"{select_clause} {from_clause} {where_clause};"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        """Parses a single UQL condition into a CQL string fragment"""
        field, op_value = next(iter(condition.items()))

        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            cql_op_map = {
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "eq": "=",
                "neq": "!=",
            }
            cql_op = cql_op_map.get(op, "=")

            # Format value (basic)
            formatted_value = f"'{value}'" if isinstance(value, str) else str(value)
            return f"{field} {cql_op} {formatted_value}"
        else:
            # Simple equality
            formatted_value = f"'{op_value}'" if isinstance(op_value, str) else str(op_value)
            return f"{field} = {formatted_value}"
