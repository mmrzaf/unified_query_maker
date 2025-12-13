from typing import Dict, Any, List
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery
from unified_query_maker.utils import parse_condition, escape_single_quotes


class CassandraTranslator(QueryTranslator):
    def translate(self, uql: Dict[str, Any]) -> str:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        if parsed.offset:
            raise ValueError("Cassandra does not support OFFSET")
        if parsed.orderBy:
            # Cassandra ORDER BY is restricted to clustering keys. Don’t pretend.
            raise ValueError(
                "Cassandra ORDER BY is not implemented safely in this translator"
            )

        select_fields = parsed.select or ["*"]
        select_clause = (
            "SELECT *"
            if select_fields == ["*"]
            else f"SELECT {', '.join(select_fields)}"
        )
        from_clause = f"FROM {parsed.from_table}"

        where_conditions: List[str] = []
        if parsed.where:
            if parsed.where.must:
                must_conditions = " AND ".join(
                    self._parse_condition(c) for c in parsed.where.must
                )
                where_conditions.append(f"({must_conditions})")

            if parsed.where.must_not:
                # Cassandra NOT support is limited; negate per condition instead of NOT(...)
                must_not_conditions = " AND ".join(
                    self._negate_condition(c) for c in parsed.where.must_not
                )
                where_conditions.append(f"({must_not_conditions})")

        where_clause = (
            "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        )
        limit_clause = f"LIMIT {parsed.limit}" if parsed.limit is not None else ""

        parts = [select_clause, from_clause, where_clause, limit_clause]
        cql = " ".join(p for p in parts if p).strip()
        return cql + ";"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        field, op, value = parse_condition(condition)

        def fmt(v: Any) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "true" if v else "false"
            if isinstance(v, (int, float)):
                return str(v)
            if isinstance(v, list):
                return "(" + ", ".join(fmt(x) for x in v) + ")"
            return f"'{escape_single_quotes(str(v))}'"

        if op == "eq" and value is None:
            return f"{field} IS NULL"
        if op == "neq" and value is None:
            return f"{field} IS NOT NULL"
        if op == "exists":
            return f"{field} IS NOT NULL"
        if op == "nexists":
            return f"{field} IS NULL"
        if op == "in":
            return f"{field} IN {fmt(value)}"
        if op == "nin":
            return f"{field} NOT IN {fmt(value)}"

        op_map = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "eq": "=",
            "neq": "!=",
        }
        return f"{field} {op_map[op]} {fmt(value)}"

    def _negate_condition(self, condition: Dict[str, Any]) -> str:
        field, op, value = parse_condition(condition)
        neg = {
            "eq": "neq",
            "neq": "eq",
            "gt": "lte",
            "gte": "lt",
            "lt": "gte",
            "lte": "gt",
            "in": "nin",
            "nin": "in",
            "exists": "nexists",
            "nexists": "exists",
        }[op]
        return self._parse_condition({field: {neg: value}})
