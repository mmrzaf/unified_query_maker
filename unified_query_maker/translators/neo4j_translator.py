from typing import Dict, Any, List
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery
from unified_query_maker.utils import parse_condition, escape_single_quotes


class Neo4jTranslator(QueryTranslator):
    def translate(self, uql: Dict[str, Any]) -> str:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        match_clause = f"MATCH (n:{parsed.from_table})"

        where_conditions: List[str] = []
        if parsed.where:
            if parsed.where.must:
                must_conditions = " AND ".join(
                    self._parse_condition(c) for c in parsed.where.must
                )
                where_conditions.append(f"({must_conditions})")

            if parsed.where.must_not:
                must_not_conditions = " AND ".join(
                    f"NOT ({self._parse_condition(c)})" for c in parsed.where.must_not
                )
                where_conditions.append(f"({must_not_conditions})")

        where_clause = (
            "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        )

        if not parsed.select or parsed.select == ["*"]:
            return_clause = "RETURN n"
        else:
            return_fields = [f"n.{f}" for f in parsed.select]
            return_clause = f"RETURN {', '.join(return_fields)}"

        order_by_clause = ""
        if parsed.orderBy:
            order_by_clause = "ORDER BY " + ", ".join(
                f"n.{i.field} {i.order}" for i in parsed.orderBy
            )

        skip_clause = f"SKIP {parsed.offset}" if parsed.offset else ""
        limit_clause = f"LIMIT {parsed.limit}" if parsed.limit is not None else ""

        parts = [
            match_clause,
            where_clause,
            return_clause,
            order_by_clause,
            skip_clause,
            limit_clause,
        ]
        cypher = " ".join(p for p in parts if p).strip()
        return cypher + ";"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        field, op, value = parse_condition(condition)
        lhs = f"n.{field}"

        def fmt(v: Any) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "true" if v else "false"
            if isinstance(v, (int, float)):
                return str(v)
            if isinstance(v, list):
                return "[" + ", ".join(fmt(x) for x in v) + "]"
            return f"'{escape_single_quotes(str(v))}'"

        if op == "eq" and value is None:
            return f"{lhs} IS NULL"
        if op == "neq" and value is None:
            return f"{lhs} IS NOT NULL"
        if op == "exists":
            return f"{lhs} IS NOT NULL"
        if op == "nexists":
            return f"{lhs} IS NULL"
        if op == "in":
            return f"{lhs} IN {fmt(value)}"
        if op == "nin":
            return f"NOT ({lhs} IN {fmt(value)})"

        op_map = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "eq": "=",
            "neq": "<>",
        }
        return f"{lhs} {op_map[op]} {fmt(value)}"
