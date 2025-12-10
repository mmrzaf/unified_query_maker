from typing import Any, Dict, List
from pydantic import ValidationError

from .base import QueryTranslator
from unified_query_maker.models import UQLQuery


class SQLTranslator(QueryTranslator):
    """Base class for SQL-based translators with common functionality.

    - Knows how to translate UQLQuery → SQL SELECT.
    - Dialects override:
        * _escape_identifier
        * _escape_string (optional)
        * _build_limit_clause / _build_limit_offset (for pagination quirks)
    """

    def translate(self, query: Dict[str, Any]) -> str:
        """Translate unified query to SQL with dialect-specific customizations."""
        # 1. Validate and parse the query using Pydantic
        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        # 2. Build query parts
        select_clause = self._build_select_clause(parsed_query)
        from_clause = self._build_from_clause(parsed_query)
        where_clause = self._build_where_clause(parsed_query)
        order_by_clause = self._build_order_by_clause(parsed_query)
        limit_clause = self._build_limit_clause(parsed_query)

        sql_parts: List[str] = [
            select_clause,
            from_clause,
            where_clause,
            order_by_clause,
            limit_clause,
        ]

        sql_query = " ".join(part for part in sql_parts if part)
        return f"{sql_query}{self._get_query_terminator()}"

    def _build_select_clause(self, query: UQLQuery) -> str:
        """Build the SELECT clause."""
        if not query.select:
            return "SELECT *"

        if len(query.select) == 1 and query.select[0].strip() == "*":
            return "SELECT *"

        # Normal case: escape column identifiers AS-IS (no dot splitting here).
        fields = [self._escape_identifier(field) for field in query.select]
        return f"SELECT {', '.join(fields)}"

    def _build_from_clause(self, query: UQLQuery) -> str:
        """Build the FROM clause."""
        # Pydantic ensures from_table exists.
        table_name = self._escape_table_name(query.from_table)
        return f"FROM {table_name}"

    def _build_where_clause(self, query: UQLQuery) -> str:
        """Build the WHERE clause."""
        if not query.where:
            return ""

        where_conditions: List[str] = []

        if query.where.must:
            must_conditions = " AND ".join(
                self._parse_condition(cond) for cond in query.where.must
            )
            where_conditions.append(f"({must_conditions})")

        if query.where.must_not:
            must_not_conditions = " AND ".join(
                self._parse_condition(cond) for cond in query.where.must_not
            )
            where_conditions.append(f"NOT ({must_not_conditions})")

        return "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    def _build_order_by_clause(self, query: UQLQuery) -> str:
        """Build the ORDER BY clause."""
        if not query.orderBy:
            return ""

        clauses: List[str] = []
        for order_item in query.orderBy:
            field = self._escape_identifier(order_item.field)
            direction = order_item.order  # "ASC" or "DESC"
            clauses.append(f"{field} {direction}")

        return f"ORDER BY {', '.join(clauses)}" if clauses else ""

    def _build_limit_clause(self, query: UQLQuery) -> str:
        """
        Build the LIMIT/OFFSET clause.

        Default SQL-92-ish behaviour:
            LIMIT n
            LIMIT n OFFSET m

        Dialects with weird pagination semantics (MSSQL, Oracle) should override.
        """
        if query.limit is None:
            return ""

        limit = query.limit
        offset = query.offset or 0

        if offset > 0:
            return self._build_limit_offset(limit, offset)
        else:
            return f"LIMIT {limit}"

    def _build_limit_offset(self, limit: int, offset: int) -> str:
        """
        Build LIMIT with OFFSET; override in subclasses for dialects that use
        OFFSET/FETCH instead of LIMIT/OFFSET.
        """
        return f"LIMIT {limit} OFFSET {offset}"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        """Parse a single condition into SQL."""
        # Assumes condition is a single-key dict like {"age": {"gt": 30}}
        field, op_value = next(iter(condition.items()))
        # IMPORTANT: treat field as a single identifier; we DO NOT split on dots here.
        field_sql = self._escape_identifier(field)

        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            sql_op = self._map_operator(op)
            formatted_value = self._format_value(value)
            return f"{field_sql} {sql_op} {formatted_value}"
        else:
            # Simple equality, e.g., {"status": "active"}
            formatted_value = self._format_value(op_value)
            return f"{field_sql} = {formatted_value}"

    def _map_operator(self, op: str) -> str:
        """Map UQL operators to SQL operators."""
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
        }.get(op, "=")  # Default to '='

    def _format_value(self, value: Any) -> str:
        """Format a value according to its type for SQL."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return str(value).upper()  # TRUE / FALSE
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            formatted_items = [self._format_value(item) for item in value]
            return f"({', '.join(formatted_items)})"
        # String-ish
        return f"'{self._escape_string(str(value))}'"

    def _escape_string(self, value: str) -> str:
        """Escape string values for SQL - override in subclasses if needed."""
        return value.replace("'", "''")

    def _escape_identifier(self, identifier: str) -> str:
        """
        Escape identifiers (table/column names) - override in subclasses.

        NOTE:
        - Base implementation does no quoting.
        - Dialects provide their own quoting style (", `, [], ...).
        """
        return identifier

    def _escape_table_name(self, name: str) -> str:
        """
        Escape a possibly-qualified TABLE name like:
          - 'table'
          - 'schema.table'
          - 'db.schema.table'

        We ONLY split on dots here, and we only use this for FROM / JOIN targets.
        Fields/columns are not split by default.
        """
        raw = str(name).strip()
        if not raw:
            raise ValueError("Table name cannot be empty")

        parts = [part.strip() for part in raw.split(".") if part.strip()]
        escaped_parts = [self._escape_identifier(p) for p in parts]
        return ".".join(escaped_parts)

    def _get_query_terminator(self) -> str:
        """SQL statement terminator."""
        return ";"
