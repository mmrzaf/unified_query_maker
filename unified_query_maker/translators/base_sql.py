from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from .base import QueryTranslator


class SQLTranslator(QueryTranslator):
    """Base class for SQL-based translators with common functionality"""

    def translate(self, query: Dict[str, Any]) -> str:
        """Translate unified query to SQL with dialect-specific customizations"""
        select_clause = self._build_select_clause(query)
        from_clause = self._build_from_clause(query)
        where_clause = self._build_where_clause(query)
        order_by_clause = self._build_order_by_clause(query)
        limit_clause = self._build_limit_clause(query)

        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            order_by_clause,
            limit_clause,
        ]

        sql_query = " ".join([part for part in sql_parts if part])

        return f"{sql_query}{self._get_query_terminator()}"

    def _build_select_clause(self, query: Dict[str, Any]) -> str:
        """Build the SELECT clause with dialect-specific handling"""
        if "select" not in query or not query["select"]:
            return "SELECT *"

        fields = [self._escape_identifier(field) for field in query["select"]]
        return f"SELECT {', '.join(fields)}"

    def _build_from_clause(self, query: Dict[str, Any]) -> str:
        """Build the FROM clause with dialect-specific handling"""
        if "from" not in query:
            raise ValueError("Query must contain a 'from' clause")

        table_name = self._escape_identifier(query["from"])
        return f"FROM {table_name}"

    def _build_where_clause(self, query: Dict[str, Any]) -> str:
        """Build the WHERE clause with dialect-specific handling"""
        if "where" not in query or not query["where"]:
            return ""

        where_conditions = []

        if "must" in query["where"] and query["where"]["must"]:
            must_conditions = " AND ".join(
                self._parse_condition(cond) for cond in query["where"]["must"]
            )
            where_conditions.append(f"({must_conditions})")

        if "must_not" in query["where"] and query["where"]["must_not"]:
            must_not_conditions = " AND ".join(
                self._parse_condition(cond) for cond in query["where"]["must_not"]
            )
            where_conditions.append(f"NOT ({must_not_conditions})")

        if "should" in query["where"] and query["where"]["should"]:
            should_conditions = " OR ".join(
                self._parse_condition(cond) for cond in query["where"]["should"]
            )
            where_conditions.append(f"({should_conditions})")

        if "match" in query["where"] and query["where"]["match"]:
            match_conditions = " AND ".join(
                self._build_match_condition(field, value)
                for field, value in query["where"]["match"].items()
            )
            where_conditions.append(f"({match_conditions})")

        if "range" in query["where"] and query["where"]["range"]:
            range_conditions = " AND ".join(
                self._build_range_condition(field, ranges)
                for field, ranges in query["where"]["range"].items()
            )
            where_conditions.append(f"({range_conditions})")

        return "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    def _build_order_by_clause(self, query: Dict[str, Any]) -> str:
        """Build the ORDER BY clause with dialect-specific handling"""
        if "orderBy" not in query or not query["orderBy"]:
            return ""

        order_clauses = []
        for order_item in query["orderBy"]:
            field = self._escape_identifier(order_item["field"])
            direction = order_item["order"]  # ASC or DESC
            order_clauses.append(f"{field} {direction}")

        return f"ORDER BY {', '.join(order_clauses)}" if order_clauses else ""

    def _build_limit_clause(self, query: Dict[str, Any]) -> str:
        """Build the LIMIT/OFFSET clause with dialect-specific handling"""
        if "limit" not in query:
            return ""

        limit = query["limit"]
        offset = query.get("offset", 0)

        if offset > 0:
            return self._build_limit_offset(limit, offset)
        else:
            return f"LIMIT {limit}"

    def _build_limit_offset(self, limit: int, offset: int) -> str:
        """Build limit with offset clause - override in subclasses for dialect differences"""
        return f"LIMIT {limit} OFFSET {offset}"

    def _parse_condition(self, condition: Dict[str, Any]) -> str:
        """Parse a single condition into SQL"""
        field, op_value = next(iter(condition.items()))
        field = self._escape_identifier(field)

        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            sql_op = self._map_operator(op)

            formatted_value = self._format_value(value)
            return f"{field} {sql_op} {formatted_value}"
        else:
            formatted_value = self._format_value(op_value)
            return f"{field} = {formatted_value}"

    def _build_match_condition(self, field: str, value: str) -> str:
        """Build LIKE condition for text matching"""
        field = self._escape_identifier(field)
        escaped_value = self._escape_like_value(value)
        return f"{field} LIKE '%{escaped_value}%'"

    def _build_range_condition(self, field: str, ranges: Dict[str, Any]) -> str:
        """Build range conditions (gt, lt, gte, lte)"""
        field = self._escape_identifier(field)
        conditions = []

        for op, value in ranges.items():
            sql_op = self._map_operator(op)
            formatted_value = self._format_value(value)
            conditions.append(f"{field} {sql_op} {formatted_value}")

        return " AND ".join(conditions)

    def _map_operator(self, op: str) -> str:
        """Map UQL operators to SQL operators"""
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
        }.get(op, "=")

    def _format_value(self, value: Any) -> str:
        """Format a value according to its type"""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return str(value).upper()  # TRUE or FALSE
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            value: List[str]
            formatted_items = [self._format_value(item) for item in value]
            return f"({', '.join(formatted_items)})"
        else:
            # String values need quotes
            return f"'{self._escape_string(str(value))}'"

    def _escape_string(self, value: str) -> str:
        """Escape string values for SQL - override in subclasses for dialect differences"""
        return value.replace("'", "''")

    def _escape_like_value(self, value: str) -> str:
        """Escape values used in LIKE patterns - override in subclasses"""
        escaped = self._escape_string(value)
        return escaped.replace("%", "\\%").replace("_", "\\_")

    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifiers (table/column names) - override in subclasses"""
        return identifier

    def _get_query_terminator(self) -> str:
        """Get the SQL statement terminator - override in subclasses if needed"""
        return ";"
