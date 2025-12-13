from typing import Any, Dict, List
from pydantic import ValidationError

from .base import QueryTranslator
from unified_query_maker.models import UQLQuery
from unified_query_maker.utils import parse_condition, validate_qualified_name


class SQLTranslator(QueryTranslator):
    """
    Base class for SQL-like translators.
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
            # IMPORTANT: semantics = exclude each condition, not NOT(all-of-them)
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
        # Default SQL dialect: LIMIT then OFFSET
        limit = query.limit
        offset = query.offset
        if limit is None and (offset is None or offset == 0):
            return ""
        if limit is None:
            # Some DBs allow OFFSET without LIMIT; if yours doesn't, override in dialect.
            return f"OFFSET {offset}"
        if offset is None or offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {limit} OFFSET {offset}"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        field, op, value = parse_condition(condition)
        field_sql = self._escape_column_name(field)

        # NULL semantics
        if op == "eq" and value is None:
            return f"{field_sql} IS NULL"
        if op == "neq" and value is None:
            return f"{field_sql} IS NOT NULL"

        # unary ops
        if op in ("exists", "nexists"):
            return f"{field_sql} {self._map_operator(op)}"

        sql_op = self._map_operator(op)
        formatted_value = self._format_value(value)
        return f"{field_sql} {sql_op} {formatted_value}"

    def _map_operator(self, op: str) -> str:
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
        }[op]

    def _format_value(self, value: Any) -> str:
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
