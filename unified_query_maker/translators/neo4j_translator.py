from typing import Dict, Any, List
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery, QueryOutput


class Neo4jTranslator(QueryTranslator):
    def translate(self, query: Dict[str, Any]) -> QueryOutput:
        """Translates UQL dict to a Neo4j Cypher query string"""
        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        # Assumes 'from' is a Node Label
        match_clause = f"MATCH (n:{parsed_query.from_table})"

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
                where_conditions.append(f"NOT ({must_not_conditions})")

        where_clause = (
            "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        )

        # Use 'select' fields to build the RETURN clause
        return_fields = [f"n.{field}" for field in parsed_query.select]
        return_clause = f"RETURN {', '.join(return_fields)}"

        # TODO: Add orderBy, limit, skip (offset)
        # orderBy_clause = ...
        # skip_clause = ...
        # limit_clause = ...

        return f"{match_clause} {where_clause} {return_clause};"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        """Parses a single UQL condition into a Cypher string fragment"""
        field, op_value = next(iter(condition.items()))

        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            cypher_op_map = {
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "eq": "=",
                "neq": "<>",
            }
            cypher_op = cypher_op_map.get(op, "=")

            formatted_value = f"'{value}'" if isinstance(value, str) else str(value)
            # Prepends 'n.' to the field name
            return f"n.{field} {cypher_op} {formatted_value}"
        else:
            formatted_value = (
                f"'{op_value}'" if isinstance(op_value, str) else str(op_value)
            )
            return f"n.{field} = {formatted_value}"
