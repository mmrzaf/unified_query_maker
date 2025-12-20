from typing import Any, Dict, List, Union

from pydantic import ValidationError

from unified_query_maker.models import UQLQuery
from unified_query_maker.models.where_model import (
    AndExpression,
    Condition,
    FilterExpression,
    FilterVisitor,
    NotExpression,
    Operator,
    OrExpression,
)
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.utils import escape_single_quotes, parse_condition


class _CassandraConditionTranslator(FilterVisitor):
    def visit_condition(self, condition: Condition) -> str:
        field = condition.field
        op = condition.operator
        value = condition.value

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

        # NULL semantics
        if op == Operator.EQ and value is None:
            return f"{field} IS NULL"
        if op == Operator.NEQ and value is None:
            return f"{field} IS NOT NULL"

        # existence
        if op == Operator.EXISTS:
            return f"{field} IS NOT NULL"
        if op == Operator.NEXISTS:
            return f"{field} IS NULL"

        # ranges
        if op == Operator.BETWEEN:
            a, b = value  # validated
            return f"({field} >= {fmt(a)} AND {field} <= {fmt(b)})"

        # membership
        if op == Operator.IN:
            return f"{field} IN {fmt(value)}"
        if op == Operator.NIN:
            return f"{field} NOT IN {fmt(value)}"

        # string ops (best-effort)
        if op == Operator.CONTAINS:
            return f"{field} LIKE {fmt('%' + str(value) + '%')}"
        if op == Operator.NCONTAINS:
            return f"{field} NOT LIKE {fmt('%' + str(value) + '%')}"
        if op == Operator.STARTS_WITH:
            return f"{field} LIKE {fmt(str(value) + '%')}"
        if op == Operator.ENDS_WITH:
            return f"{field} LIKE {fmt('%' + str(value))}"

        op_map = {
            Operator.GT: ">",
            Operator.GTE: ">=",
            Operator.LT: "<",
            Operator.LTE: "<=",
            Operator.EQ: "=",
            Operator.NEQ: "!=",
        }
        if op in op_map:
            return f"{field} {op_map[op]} {fmt(value)}"

        raise ValueError(f"Unsupported operator for Cassandra: {op}")

    def visit_and(self, and_expr: AndExpression) -> str:
        return "(" + " AND ".join(e.accept(self) for e in and_expr.expressions) + ")"

    def visit_or(self, or_expr: OrExpression) -> str:
        return "(" + " OR ".join(e.accept(self) for e in or_expr.expressions) + ")"

    def visit_not(self, not_expr: NotExpression) -> str:
        return f"NOT ({not_expr.expression.accept(self)})"


class CassandraTranslator(QueryTranslator):
    def translate(self, uql: Dict[str, Any]) -> str:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        if parsed.offset:
            raise ValueError("Cassandra does not support OFFSET")
        if parsed.orderBy:
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

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> str:
        if isinstance(condition, FilterExpression):
            return condition.accept(_CassandraConditionTranslator())
        # legacy
        field, op, value = parse_condition(condition)
        typed = Condition(field=field, operator=Operator(op), value=value)
        return typed.accept(_CassandraConditionTranslator())

    def _negate_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> str:
        # Prefer operator inversion for leaf conditions (legacy behavior), otherwise wrap NOT(...)
        if isinstance(condition, FilterExpression):
            if isinstance(condition, Condition):
                inv = {
                    Operator.EQ: Operator.NEQ,
                    Operator.NEQ: Operator.EQ,
                    Operator.GT: Operator.LTE,
                    Operator.GTE: Operator.LT,
                    Operator.LT: Operator.GTE,
                    Operator.LTE: Operator.GT,
                    Operator.IN: Operator.NIN,
                    Operator.NIN: Operator.IN,
                    Operator.EXISTS: Operator.NEXISTS,
                    Operator.NEXISTS: Operator.EXISTS,
                    Operator.CONTAINS: Operator.NCONTAINS,
                    Operator.NCONTAINS: Operator.CONTAINS,
                }.get(condition.operator)
                if inv:
                    return Condition(
                        field=condition.field, operator=inv, value=condition.value
                    ).accept(_CassandraConditionTranslator())
            return f"NOT ({self._parse_condition(condition)})"

        field, op, value = parse_condition(condition)
        inv = {
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
            "contains": "ncontains",
            "ncontains": "contains",
        }.get(op)
        if not inv:
            return f"NOT ({self._parse_condition(condition)})"
        return self._parse_condition({field: {inv: value}})
