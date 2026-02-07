from __future__ import annotations

from typing import Any, Dict, List

from pydantic import ValidationError

from unified_query_maker.models import UQLQuery
from unified_query_maker.models.where_model import (
    AndExpression,
    Condition,
    FilterVisitor,
    NotExpression,
    Operator,
    OrExpression,
)
from unified_query_maker.utils import validate_qualified_name

from .base import QueryTranslator

_LIKE_ESCAPE_CHAR = "\\"


def _escape_like_literal(value: str) -> str:
    """
    Escape a literal substring for SQL LIKE/ILIKE patterns.

    Escapes:
      - escape char itself (\\)
      - % and _ (LIKE wildcards)

    Caller adds % wildcards around it.
    """
    return (
        value.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
        .replace("%", _LIKE_ESCAPE_CHAR + "%")
        .replace("_", _LIKE_ESCAPE_CHAR + "_")
    )


class SQLConditionTranslator(FilterVisitor[str]):
    """Visitor that translates Where-model expressions to SQL condition strings."""

    def __init__(self, parent_translator: "SQLTranslator"):
        self.parent = parent_translator
        self.params: List[Any] = []  # reserved for future parameterization

    def visit_condition(self, condition: Condition) -> str:
        field_sql = self.parent._escape_column_name(condition.field)
        op = condition.operator
        value = condition.value

        # NULL semantics
        if op == Operator.EQ and value is None:
            return f"{field_sql} IS NULL"
        if op == Operator.NEQ and value is None:
            return f"{field_sql} IS NOT NULL"

        # Existence (unary)
        if op == Operator.EXISTS:
            return f"{field_sql} IS NOT NULL"
        if op == Operator.NEXISTS:
            return f"{field_sql} IS NULL"

        # Basic comparisons
        if op == Operator.EQ:
            return f"{field_sql} = {self.parent._format_value(value)}"
        if op == Operator.NEQ:
            return f"{field_sql} <> {self.parent._format_value(value)}"
        if op == Operator.GT:
            return f"{field_sql} > {self.parent._format_value(value)}"
        if op == Operator.GTE:
            return f"{field_sql} >= {self.parent._format_value(value)}"
        if op == Operator.LT:
            return f"{field_sql} < {self.parent._format_value(value)}"
        if op == Operator.LTE:
            return f"{field_sql} <= {self.parent._format_value(value)}"

        # Membership
        if op in (Operator.IN, Operator.NIN):
            if not isinstance(value, list):
                raise ValueError(f"{op} expects a list value")
            if len(value) == 0:
                raise ValueError(f"{op} expects a non-empty list value")
            in_list = self.parent._format_value(value)  # "(...)" for lists
            neg = "NOT " if op == Operator.NIN else ""
            return f"{field_sql} {neg}IN {in_list}"

        # Range
        if op == Operator.BETWEEN:
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError("BETWEEN expects a 2-item list value")
            lo, hi = value
            return (
                f"{field_sql} BETWEEN {self.parent._format_value(lo)} "
                f"AND {self.parent._format_value(hi)}"
            )

        # LIKE family (literal substring semantics)
        if op in (
            Operator.CONTAINS,
            Operator.NCONTAINS,
            Operator.ICONTAINS,
            Operator.STARTS_WITH,
            Operator.ENDS_WITH,
        ):
            if not isinstance(value, str):
                raise ValueError(f"{op} expects a string value")

            escaped = _escape_like_literal(value)

            if op in (Operator.CONTAINS, Operator.NCONTAINS, Operator.ICONTAINS):
                pattern = f"%{escaped}%"
            elif op == Operator.STARTS_WITH:
                pattern = f"{escaped}%"
            else:  # ENDS_WITH
                pattern = f"%{escaped}"

            negate = op == Operator.NCONTAINS
            case_insensitive = op == Operator.ICONTAINS

            return self.parent._render_like(
                field_sql=field_sql,
                pattern=pattern,
                negate=negate,
                case_insensitive=case_insensitive,
            )

        # ILIKE (pattern semantics)
        if op == Operator.ILIKE:
            if not isinstance(value, str):
                raise ValueError("ILIKE expects a string pattern")
            return self.parent._render_ilike(field_sql, value)

        # Regex (dialect-specific)
        if op == Operator.REGEX:
            if not isinstance(value, str):
                raise ValueError("REGEX expects a string pattern")
            return self.parent._render_regex(field_sql, value)

        # Array (dialect-specific)
        if op == Operator.ARRAY_CONTAINS:
            return self.parent._render_array_contains(field_sql, value)
        if op == Operator.ARRAY_OVERLAP:
            return self.parent._render_array_overlap(field_sql, value)
        if op == Operator.ARRAY_CONTAINED:
            return self.parent._render_array_contained(field_sql, value)

        # Geo not portable in generic SQL
        if op in (Operator.GEO_WITHIN, Operator.GEO_INTERSECTS):
            raise ValueError(f"Unsupported operator for SQL: {op}")

        raise ValueError(f"Unsupported operator for SQL: {op}")

    def visit_and(self, and_expr: AndExpression) -> str:
        return (
            "(" + " AND ".join(expr.accept(self) for expr in and_expr.expressions) + ")"
        )

    def visit_or(self, or_expr: OrExpression) -> str:
        return (
            "(" + " OR ".join(expr.accept(self) for expr in or_expr.expressions) + ")"
        )

    def visit_not(self, not_expr: NotExpression) -> str:
        return f"(NOT {not_expr.expression.accept(self)})"


class SQLTranslator(QueryTranslator):
    """SQL translator for the UQLQuery model (no legacy formats)."""

    def translate(self, uql: Dict[str, Any]) -> str:
        try:
            query = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        parts = [
            self._build_select_clause(query),
            self._build_from_clause(query),
            self._build_where_clause(query),
            self._build_order_by_clause(query),
            self._build_limit_clause(query),
        ]
        sql = " ".join(p for p in parts if p)
        return sql.strip() + ";"

    # ---------- Clause builders ----------

    def _build_select_clause(self, query: UQLQuery) -> str:
        if not query.select or query.select == ["*"]:
            return "SELECT *"
        fields = [self._escape_column_name(field) for field in query.select]
        return f"SELECT {', '.join(fields)}"

    def _build_from_clause(self, query: UQLQuery) -> str:
        return f"FROM {self._escape_table_name(query.from_table)}"

    def _build_where_clause(self, query: UQLQuery) -> str:
        if not query.where:
            return ""

        visitor = SQLConditionTranslator(self)
        where_parts: List[str] = []

        if query.where.must:
            must_parts = [expr.accept(visitor) for expr in query.where.must]
            if must_parts:
                where_parts.append("(" + " AND ".join(must_parts) + ")")

        if query.where.must_not:
            must_not_parts = [
                f"NOT ({expr.accept(visitor)})" for expr in query.where.must_not
            ]
            if must_not_parts:
                where_parts.append("(" + " AND ".join(must_not_parts) + ")")

        if not where_parts:
            return ""
        return "WHERE " + " AND ".join(where_parts)

    def _build_order_by_clause(self, query: UQLQuery) -> str:
        if not query.orderBy:
            return ""
        clauses: List[str] = []
        for item in query.orderBy:
            clauses.append(f"{self._escape_column_name(item.field)} {item.order}")
        return "ORDER BY " + ", ".join(clauses)

    def _build_limit_clause(self, query: UQLQuery) -> str:
        limit = query.limit
        offset = query.offset or 0

        if limit is None and offset == 0:
            return ""
        if limit is None:
            return f"OFFSET {offset}"
        if offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {limit} OFFSET {offset}"

    # ---------- Escaping / literals ----------

    def _escape_identifier(self, identifier: str) -> str:
        """Dialect hook: quote a single identifier segment."""
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

    def _escape_string(self, value: str) -> str:
        """Dialect hook: escape a Python string for SQL string literals."""
        return value.replace("'", "''")

    def _format_bool(self, value: bool) -> str:
        return "TRUE" if value else "FALSE"

    def _format_value(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return self._format_bool(value)
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple)):
            items = list(value)
            if len(items) == 0:
                raise ValueError("Empty lists cannot be rendered as SQL literals")
            return "(" + ", ".join(self._format_value(v) for v in items) + ")"
        if isinstance(value, str):
            return f"'{self._escape_string(value)}'"
        raise ValueError(f"Unsupported SQL literal type: {type(value)}")

    # ---------- Operator renderers (dialect hooks) ----------

    def _render_like(
        self,
        *,
        field_sql: str,
        pattern: str,
        negate: bool,
        case_insensitive: bool,
    ) -> str:
        if case_insensitive:
            clause = self._render_ilike(field_sql, pattern)
        else:
            clause = f"{field_sql} LIKE {self._format_value(pattern)} ESCAPE '{_LIKE_ESCAPE_CHAR}'"
        return f"NOT ({clause})" if negate else clause

    def _render_ilike(self, field_sql: str, pattern: object) -> str:
        # Default: case-insensitive via LOWER()
        return (
            f"LOWER({field_sql}) LIKE LOWER({self._format_value(pattern)}) "
            f"ESCAPE '{_LIKE_ESCAPE_CHAR}'"
        )

    def _render_regex(self, field_sql: str, pattern: object) -> str:
        raise ValueError("REGEX is not supported for this SQL dialect")

    def _render_array_contains(self, field_sql: str, value: object) -> str:
        raise ValueError("ARRAY_CONTAINS is not supported for this SQL dialect")

    def _render_array_overlap(self, field_sql: str, values: object) -> str:
        raise ValueError("ARRAY_OVERLAP is not supported for this SQL dialect")

    def _render_array_contained(self, field_sql: str, values: object) -> str:
        raise ValueError("ARRAY_CONTAINED is not supported for this SQL dialect")
