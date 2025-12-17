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


class _Neo4jConditionTranslator(FilterVisitor):
    def visit_condition(self, condition: Condition) -> str:
        field = condition.field
        op = condition.operator
        value = condition.value
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

        if op == Operator.EQ and value is None:
            return f"{lhs} IS NULL"
        if op == Operator.NEQ and value is None:
            return f"{lhs} IS NOT NULL"

        if op == Operator.EXISTS:
            return f"{lhs} IS NOT NULL"
        if op == Operator.NEXISTS:
            return f"{lhs} IS NULL"

        if op == Operator.IN:
            return f"{lhs} IN {fmt(value)}"
        if op == Operator.NIN:
            return f"NOT ({lhs} IN {fmt(value)})"

        if op == Operator.BETWEEN:
            a, b = value
            return f"({lhs} >= {fmt(a)} AND {lhs} <= {fmt(b)})"

        if op == Operator.CONTAINS:
            return f"{lhs} CONTAINS {fmt(value)}"
        if op == Operator.STARTS_WITH:
            return f"{lhs} STARTS WITH {fmt(value)}"
        if op == Operator.ENDS_WITH:
            return f"{lhs} ENDS WITH {fmt(value)}"
        if op == Operator.REGEX:
            return f"{lhs} =~ {fmt(value)}"

        op_map = {
            Operator.GT: ">",
            Operator.GTE: ">=",
            Operator.LT: "<",
            Operator.LTE: "<=",
            Operator.EQ: "=",
            Operator.NEQ: "<>",
        }
        if op in op_map:
            return f"{lhs} {op_map[op]} {fmt(value)}"

        raise ValueError(f"Unsupported operator for Neo4j: {op}")

    def visit_and(self, and_expr: AndExpression) -> str:
        return "(" + " AND ".join(e.accept(self) for e in and_expr.expressions) + ")"

    def visit_or(self, or_expr: OrExpression) -> str:
        return "(" + " OR ".join(e.accept(self) for e in or_expr.expressions) + ")"

    def visit_not(self, not_expr: NotExpression) -> str:
        return f"NOT ({not_expr.expression.accept(self)})"


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

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> str:
        if isinstance(condition, FilterExpression):
            return condition.accept(_Neo4jConditionTranslator())
        field, op, value = parse_condition(condition)
        return Condition(field=field, operator=Operator(op), value=value).accept(
            _Neo4jConditionTranslator()
        )
