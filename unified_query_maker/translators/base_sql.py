"""
Updated SQL Translator with Where Model Support
===============================================

Supports both legacy dict-based conditions and new Where/Filter model.
"""

from typing import Any, Dict, List, Union

from pydantic import ValidationError

from unified_query_maker.models import UQLQuery

# Import the new Where model
from unified_query_maker.models.where_model import (
    AndExpression,
    Condition,
    FilterExpression,
    FilterVisitor,
    NotExpression,
    Operator,
    OrExpression,
)
from unified_query_maker.utils import parse_condition, validate_qualified_name

from .base import QueryTranslator


class SQLConditionTranslator(FilterVisitor):
    """
    Visitor that translates Where model conditions to SQL.
    """

    def __init__(self, parent_translator: "SQLTranslator"):
        self.parent = parent_translator
        self.params: List[Any] = []

    def visit_condition(self, condition: Condition) -> str:
        """Translate a single condition to SQL."""
        field_sql = self.parent._escape_column_name(condition.field)
        op = condition.operator
        value = condition.value

        # NULL semantics
        if op == Operator.EQ and value is None:
            return f"{field_sql} IS NULL"
        if op == Operator.NEQ and value is None:
            return f"{field_sql} IS NOT NULL"

        # Existence operators (unary)
        if op == Operator.EXISTS:
            return f"{field_sql} IS NOT NULL"
        if op == Operator.NEXISTS:
            return f"{field_sql} IS NULL"

        # Comparison operators
        if op == Operator.GT:
            return f"{field_sql} > {self._format_value(value)}"
        if op == Operator.GTE:
            return f"{field_sql} >= {self._format_value(value)}"
        if op == Operator.LT:
            return f"{field_sql} < {self._format_value(value)}"
        if op == Operator.LTE:
            return f"{field_sql} <= {self._format_value(value)}"
        if op == Operator.BETWEEN:
            min_val, max_val = value
            return f"{field_sql} BETWEEN {self._format_value(min_val)} AND {self._format_value(max_val)}"

        # Equality operators
        if op == Operator.EQ:
            return f"{field_sql} = {self._format_value(value)}"
        if op == Operator.NEQ:
            return f"{field_sql} != {self._format_value(value)}"

        # Membership operators
        if op == Operator.IN:
            return f"{field_sql} IN {self._format_value(value)}"
        if op == Operator.NIN:
            return f"{field_sql} NOT IN {self._format_value(value)}"

        # String operators
        if op == Operator.CONTAINS:
            return f"{field_sql} LIKE {self._format_value(f'%{value}%')}"
        if op == Operator.NCONTAINS:
            return f"{field_sql} NOT LIKE {self._format_value(f'%{value}%')}"
        if op == Operator.ICONTAINS:
            return f"LOWER({field_sql}) LIKE LOWER({self._format_value(f'%{value}%')})"
        if op == Operator.STARTS_WITH:
            return f"{field_sql} LIKE {self._format_value(f'{value}%')}"
        if op == Operator.ENDS_WITH:
            return f"{field_sql} LIKE {self._format_value(f'%{value}')}"
        if op == Operator.ILIKE:
            # Postgres has ILIKE, others use LOWER() + LIKE
            return f"LOWER({field_sql}) LIKE LOWER({self._format_value(value)})"
        if op == Operator.REGEX:
            # Postgres syntax: field ~ 'pattern'
            return f"{field_sql} ~ {self._format_value(value)}"

        # Array operators (Postgres)
        if op == Operator.ARRAY_CONTAINS:
            return f"{self._format_value(value)} = ANY({field_sql})"
        if op == Operator.ARRAY_OVERLAP:
            array_literal = (
                "ARRAY[" + ", ".join(self._format_value(v) for v in value) + "]"
            )
            return f"{field_sql} && {array_literal}"
        if op == Operator.ARRAY_CONTAINED:
            array_literal = (
                "ARRAY[" + ", ".join(self._format_value(v) for v in value) + "]"
            )
            return f"{field_sql} <@ {array_literal}"

        # Fallback
        raise ValueError(f"Unsupported operator for SQL: {op}")

    def visit_and(self, and_expr: AndExpression) -> str:
        """Translate AND expression."""
        sub_clauses = [expr.accept(self) for expr in and_expr.expressions]
        return "(" + " AND ".join(sub_clauses) + ")"

    def visit_or(self, or_expr: OrExpression) -> str:
        """Translate OR expression."""
        sub_clauses = [expr.accept(self) for expr in or_expr.expressions]
        return "(" + " OR ".join(sub_clauses) + ")"

    def visit_not(self, not_expr: NotExpression) -> str:
        """Translate NOT expression."""
        sub_clause = not_expr.expression.accept(self)
        return f"NOT ({sub_clause})"

    def _format_value(self, value: Any) -> str:
        """Format a value for SQL."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            return "(" + ", ".join(self._format_value(v) for v in value) + ")"
        # string
        s = str(value).replace("'", "''")
        return f"'{s}'"


class SQLTranslator(QueryTranslator):
    """
    SQL translator with support for both dict-based and Where model conditions.

    Backward compatible: automatically detects and handles both formats.
    """

    def translate(self, uql: Dict[str, Any]) -> str:
        try:
            query = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        select = self._build_select_clause(query)
        from_ = self._build_from_clause(query)
        where = self._build_where_clause(query)
        order_by = self._build_order_by_clause(query)
        limit = self._build_limit_clause(query)

        parts = [select, from_, where, order_by, limit]
        sql = " ".join(p for p in parts if p)
        return sql.strip() + ";"

    def _build_select_clause(self, query: UQLQuery) -> str:
        if not query.select or query.select == ["*"]:
            return "SELECT *"

        fields = [self._escape_column_name(field) for field in query.select]
        return f"SELECT {', '.join(fields)}"

    def _build_from_clause(self, query: UQLQuery) -> str:
        table = self._escape_table_name(query.from_table)
        return f"FROM {table}"

    def _build_where_clause(self, query: UQLQuery) -> str:
        if not query.where:
            return ""

        where_conditions: List[str] = []

        if query.where.must:
            must_conditions = " AND ".join(
                self._parse_condition(cond) for cond in query.where.must
            )
            where_conditions.append(f"({must_conditions})")

        if query.where.must_not:
            # Semantics: exclude each condition (NOT each one, then AND them)
            must_not_conditions = " AND ".join(
                f"NOT ({self._parse_condition(cond)})" for cond in query.where.must_not
            )
            where_conditions.append(f"({must_not_conditions})")

        return "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    def _build_order_by_clause(self, query: UQLQuery) -> str:
        if not query.orderBy:
            return ""
        clauses: List[str] = []
        for order_item in query.orderBy:
            field = self._escape_column_name(order_item.field)
            clauses.append(f"{field} {order_item.order}")
        return "ORDER BY " + ", ".join(clauses)

    def _build_limit_clause(self, query: UQLQuery) -> str:
        limit = query.limit
        offset = query.offset
        if limit is None and (offset is None or offset == 0):
            return ""
        if limit is None:
            return f"OFFSET {offset}"
        if offset is None or offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {limit} OFFSET {offset}"

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> str:
        """
        Parse a condition - supports both dict format and Where model.

        Args:
            condition: Either a dict (legacy) or FilterExpression (new)

        Returns:
            SQL condition string
        """
        # New Where model - use visitor
        if isinstance(condition, FilterExpression):
            visitor = SQLConditionTranslator(self)
            return condition.accept(visitor)

        # Legacy dict format - preserve original behavior
        return self._parse_dict_condition(condition)

    def _parse_dict_condition(self, condition: Dict[str, Any]) -> str:
        """Parse legacy dict-based condition."""
        field, op, value = parse_condition(condition)
        field_sql = self._escape_column_name(field)

        # NULL semantics
        if op == "eq" and value is None:
            return f"{field_sql} IS NULL"
        if op == "neq" and value is None:
            return f"{field_sql} IS NOT NULL"

        # Unary ops
        if op in ("exists", "nexists"):
            return f"{field_sql} {self._map_operator(op)}"

        sql_op = self._map_operator(op)
        formatted_value = self._format_value(value)
        return f"{field_sql} {sql_op} {formatted_value}"

    def _map_operator(self, op: str) -> str:
        """Map legacy operator strings to SQL operators."""
        return {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "in": "IN",
            "nin": "NOT IN",
            "exists": "IS NOT NULL",
            "nexists": "IS NULL",
            # Extended operators
            "contains": "LIKE",
            "starts_with": "LIKE",
            "ends_with": "LIKE",
        }.get(op, "=")

    def _format_value(self, value: Any) -> str:
        """Format value for SQL (legacy method)."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            return "(" + ", ".join(self._format_value(v) for v in value) + ")"
        # string
        s = str(value).replace("'", "''")
        return f"'{s}'"

    def _escape_identifier(self, identifier: str) -> str:
        """
        Dialect override point: quoting style for a single identifier segment.
        Default = no quoting.
        """
        return identifier

    def _escape_column_name(self, name: str) -> str:
        raw = str(name).strip()
        validate_qualified_name(raw, allow_star=False, allow_trailing_star=True)

        if raw.endswith(".*"):
            base = raw[:-2]
            parts = [p.strip() for p in base.split(".") if p.strip()]
            return ".".join(self._escape_identifier(p) for p in parts) + ".*"

        parts = [p.strip() for p in raw.split(".") if p.strip()]
        return ".".join(self._escape_identifier(p) for p in parts)

    def _escape_table_name(self, name: str) -> str:
        raw = str(name).strip()
        validate_qualified_name(raw, allow_star=False, allow_trailing_star=False)
        parts = [p.strip() for p in raw.split(".") if p.strip()]
        return ".".join(self._escape_identifier(p) for p in parts)


# Example: Extending with specific SQL dialect
class PostgreSQLTranslator(SQLTranslator):
    """PostgreSQL-specific translator with proper identifier quoting."""

    def _escape_identifier(self, identifier: str) -> str:
        """Use double quotes for PostgreSQL."""
        return f'"{identifier}"'


class MySQLTranslator(SQLTranslator):
    """MySQL-specific translator with backtick quoting."""

    def _escape_identifier(self, identifier: str) -> str:
        """Use backticks for MySQL."""
        return f"`{identifier}`"

    def _build_limit_clause(self, query: UQLQuery) -> str:
        """MySQL uses LIMIT offset, count syntax."""
        limit = query.limit
        offset = query.offset
        if limit is None and (offset is None or offset == 0):
            return ""
        if limit is None:
            # MySQL doesn't support OFFSET without LIMIT
            # Use a very large number
            return f"LIMIT {offset}, 18446744073709551615"
        if offset is None or offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {offset}, {limit}"
