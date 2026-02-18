from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

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
            return f"{field_sql} = {self.parent._value(value)}"
        if op == Operator.NEQ:
            return f"{field_sql} <> {self.parent._value(value)}"
        if op == Operator.GT:
            return f"{field_sql} > {self.parent._value(value)}"
        if op == Operator.GTE:
            return f"{field_sql} >= {self.parent._value(value)}"
        if op == Operator.LT:
            return f"{field_sql} < {self.parent._value(value)}"
        if op == Operator.LTE:
            return f"{field_sql} <= {self.parent._value(value)}"

        # BETWEEN
        if op == Operator.BETWEEN:
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError("BETWEEN expects a 2-item list value")
            lo, hi = value
            return f"{field_sql} BETWEEN {self.parent._value(lo)} AND {self.parent._value(hi)}"

        # IN / NIN
        if op in (Operator.IN, Operator.NIN):
            if not isinstance(value, list) or len(value) == 0:
                raise ValueError("IN/NIN expects a non-empty list value")
            in_list = self.parent._values_list(value)
            if op == Operator.IN:
                return f"{field_sql} IN {in_list}"
            return f"{field_sql} NOT IN {in_list}"

        # Strings (built on LIKE/ILIKE)
        if op == Operator.CONTAINS:
            pat = f"%{_escape_like_literal(str(value))}%"
            return self.parent._render_like(
                field_sql=field_sql, pattern=pat, negate=False, case_insensitive=False
            )
        if op == Operator.NCONTAINS:
            pat = f"%{_escape_like_literal(str(value))}%"
            return self.parent._render_like(
                field_sql=field_sql, pattern=pat, negate=True, case_insensitive=False
            )
        if op == Operator.ICONTAINS:
            pat = f"%{_escape_like_literal(str(value))}%"
            return self.parent._render_like(
                field_sql=field_sql, pattern=pat, negate=False, case_insensitive=True
            )
        if op == Operator.STARTS_WITH:
            pat = f"{_escape_like_literal(str(value))}%"
            return self.parent._render_like(
                field_sql=field_sql, pattern=pat, negate=False, case_insensitive=False
            )
        if op == Operator.ENDS_WITH:
            pat = f"%{_escape_like_literal(str(value))}"
            return self.parent._render_like(
                field_sql=field_sql, pattern=pat, negate=False, case_insensitive=False
            )
        if op == Operator.ILIKE:
            # Treat as raw pattern (caller supplies %, _ as desired)
            return self.parent._render_like(
                field_sql=field_sql,
                pattern=str(value),
                negate=False,
                case_insensitive=True,
            )
        if op == Operator.REGEX:
            return self.parent._render_regex(field_sql, value)

        # Arrays (dialect-specific)
        if op == Operator.ARRAY_CONTAINS:
            return self.parent._render_array_contains(field_sql, value)
        if op == Operator.ARRAY_OVERLAP:
            return self.parent._render_array_overlap(field_sql, value)
        if op == Operator.ARRAY_CONTAINED:
            return self.parent._render_array_contained(field_sql, value)

        # Geo not supported in SQL translators by default
        if op in (Operator.GEO_WITHIN, Operator.GEO_INTERSECTS):
            raise ValueError("GEO operators are not supported for SQL translators")

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
    """
    Base SQL translator.

    - translate(uql) -> SQL string with literals (backwards compatible)
    - translate_with_params(uql) -> (sql, params) for safe execution
    """

    def __init__(self) -> None:
        self._params: Optional[List[Any]] = None

    # ---------- Public API ----------

    def translate(self, uql: Dict[str, Any]) -> str:
        parsed = self._parse(uql)
        self._params = None
        sql = self._build_sql(parsed)
        return sql

    def translate_with_params(self, uql: Dict[str, Any]) -> Tuple[str, List[Any]]:
        parsed = self._parse(uql)
        self._params = []
        try:
            sql = self._build_sql(parsed)
            return sql, list(self._params)
        finally:
            # make mode explicit + avoid accidental reuse
            self._params = None

    # ---------- Parsing / orchestration ----------

    def _parse(self, uql: Dict[str, Any]) -> UQLQuery:
        try:
            return UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

    def _build_sql(self, query: UQLQuery) -> str:
        parts = [
            self._build_select_clause(query),
            self._build_from_clause(query),
        ]

        where_clause = self._build_where_clause(query)
        if where_clause:
            parts.append(where_clause)

        order_by = self._build_order_by_clause(query)
        if order_by:
            parts.append(order_by)

        limit_clause = self._build_limit_clause(query)
        if limit_clause:
            parts.append(limit_clause)

        return " ".join(p for p in parts if p).strip() + ";"

    # ---------- Clause builders ----------

    def _build_select_clause(self, query: UQLQuery) -> str:
        if not query.select or query.select == ["*"]:
            return "SELECT *"
        cols = ", ".join(self._escape_column_name(c) for c in query.select)
        return f"SELECT {cols}"

    def _build_from_clause(self, query: UQLQuery) -> str:
        return f"FROM {self._escape_table_name(query.from_table)}"

    def _build_where_clause(self, query: UQLQuery) -> str:
        if not query.where:
            return ""

        visitor = SQLConditionTranslator(self)
        parts: List[str] = []

        if query.where.must:
            parts.append(
                " AND ".join(expr.accept(visitor) for expr in query.where.must)
            )
            parts[-1] = f"({parts[-1]})"

        if query.where.must_not:
            not_parts = [
                f"(NOT ({expr.accept(visitor)}))" for expr in query.where.must_not
            ]
            parts.append(" AND ".join(not_parts))
            parts[-1] = f"({parts[-1]})"

        if not parts:
            return ""

        return "WHERE " + " AND ".join(parts)

    def _build_order_by_clause(self, query: UQLQuery) -> str:
        if not query.orderBy:
            return ""
        items = []
        for item in query.orderBy:
            col = self._escape_column_name(item.field)
            items.append(f"{col} {item.order}")
        return "ORDER BY " + ", ".join(items)

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

    # ---------- Escaping / identifiers ----------

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

    # ---------- Values / parameterization ----------

    def _param_placeholder(self, index_1_based: int) -> str:
        """
        Dialect hook for placeholders in translate_with_params() mode.

        Default: DB-API 'format' style (%s).
        """
        return "%s"

    def _value(self, value: Any) -> str:
        """
        Render a scalar value as either:
          - SQL literal (translate mode), or
          - placeholder + parameter capture (translate_with_params mode)
        """
        if self._params is None:
            return self._format_value(value)

        # In param mode, bind scalars only here.
        if isinstance(value, (list, tuple)):
            raise ValueError(
                "Internal error: list value must be rendered via _values_list()"
            )

        self._params.append(value)
        return self._param_placeholder(len(self._params))

    def _values_list(self, values: List[Any]) -> str:
        """
        Render a non-empty list for IN (...) in either mode.
        """
        if not values:
            raise ValueError("Empty lists cannot be rendered")

        if self._params is None:
            # literal mode keeps existing behavior
            return self._format_value(values)

        placeholders: List[str] = []
        for v in values:
            if isinstance(v, (list, tuple)):
                raise ValueError("Nested lists are not supported in IN/NIN")
            self._params.append(v)
            placeholders.append(self._param_placeholder(len(self._params)))
        return "(" + ", ".join(placeholders) + ")"

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
            clause = (
                f"{field_sql} LIKE {self._value(pattern)} ESCAPE '{_LIKE_ESCAPE_CHAR}'"
            )
        return f"NOT ({clause})" if negate else clause

    def _render_ilike(self, field_sql: str, pattern: object) -> str:
        # Default: case-insensitive via LOWER()
        return (
            f"LOWER({field_sql}) LIKE LOWER({self._value(str(pattern))}) "
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
