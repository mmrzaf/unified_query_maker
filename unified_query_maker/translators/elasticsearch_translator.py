"""
Updated Elasticsearch Translator with Where Model Support
=========================================================

Supports both legacy dict-based conditions and new Where/Filter model.
"""

from typing import Any, Dict, Union

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
from unified_query_maker.utils import parse_condition


class ElasticsearchConditionTranslator(FilterVisitor):
    """
    Visitor that translates Where model conditions to Elasticsearch Query DSL.
    """

    def visit_condition(self, condition: Condition) -> Dict[str, Any]:
        """Translate a single condition to Elasticsearch query."""
        field = condition.field
        op = condition.operator
        value = condition.value

        # Existence operators
        if op == Operator.EXISTS:
            return {"exists": {"field": field}}
        if op == Operator.NEXISTS:
            return {"bool": {"must_not": {"exists": {"field": field}}}}

        # Equality operators
        if op == Operator.EQ:
            # Use term query for exact match
            return {"term": {field: value}}
        if op == Operator.NEQ:
            return {"bool": {"must_not": {"term": {field: value}}}}

        # Comparison operators
        if op == Operator.GT:
            return {"range": {field: {"gt": value}}}
        if op == Operator.GTE:
            return {"range": {field: {"gte": value}}}
        if op == Operator.LT:
            return {"range": {field: {"lt": value}}}
        if op == Operator.LTE:
            return {"range": {field: {"lte": value}}}
        if op == Operator.BETWEEN:
            min_val, max_val = value
            return {"range": {field: {"gte": min_val, "lte": max_val}}}

        # Membership operators
        if op == Operator.IN:
            return {"terms": {field: value}}
        if op == Operator.NIN:
            return {"bool": {"must_not": {"terms": {field: value}}}}

        # String operators
        if op == Operator.CONTAINS:
            # Wildcard query for substring
            return {"wildcard": {field: f"*{value}*"}}
        if op == Operator.NCONTAINS:
            return {"bool": {"must_not": {"wildcard": {field: f"*{value}*"}}}}
        if op == Operator.ICONTAINS:
            # Match query with lowercase (requires text field)
            return {"match": {field: {"query": value, "operator": "and"}}}
        if op == Operator.STARTS_WITH:
            return {"prefix": {field: value}}
        if op == Operator.ENDS_WITH:
            return {"wildcard": {field: f"*{value}"}}
        if op == Operator.ILIKE:
            # Convert SQL LIKE to wildcard with lowercase
            pattern = value.replace("%", "*").replace("_", "?").lower()
            # Use lowercase field variant if available
            return {"wildcard": {f"{field}.lowercase": pattern}}
        if op == Operator.REGEX:
            return {"regexp": {field: value}}

        # Array operators
        if op == Operator.ARRAY_CONTAINS:
            return {"term": {field: value}}
        if op == Operator.ARRAY_OVERLAP:
            return {"terms": {field: value}}
        if op == Operator.ARRAY_CONTAINED:
            # All array elements must be in the provided list
            # Use script query for this
            script = f"""
                def fieldValues = doc['{field}'].values;
                def allowed = params.allowed;
                for (def v : fieldValues) {{
                    if (!allowed.contains(v)) {{
                        return false;
                    }}
                }}
                return true;
            """
            return {
                "script": {"script": {"source": script, "params": {"allowed": value}}}
            }

        # Geospatial operators
        if op == Operator.GEO_WITHIN:
            return {"geo_shape": {field: {"shape": value, "relation": "within"}}}
        if op == Operator.GEO_INTERSECTS:
            return {"geo_shape": {field: {"shape": value, "relation": "intersects"}}}

        # Fallback
        raise ValueError(f"Unsupported operator for Elasticsearch: {op}")

    def visit_and(self, and_expr: AndExpression) -> Dict[str, Any]:
        """Translate AND expression."""
        sub_queries = [expr.accept(self) for expr in and_expr.expressions]
        return {"bool": {"must": sub_queries}}

    def visit_or(self, or_expr: OrExpression) -> Dict[str, Any]:
        """Translate OR expression."""
        sub_queries = [expr.accept(self) for expr in or_expr.expressions]
        return {"bool": {"should": sub_queries, "minimum_should_match": 1}}

    def visit_not(self, not_expr: NotExpression) -> Dict[str, Any]:
        """Translate NOT expression."""
        sub_query = not_expr.expression.accept(self)
        return {"bool": {"must_not": sub_query}}


class ElasticsearchTranslator(QueryTranslator):
    """
    Elasticsearch translator with support for both dict-based and Where model conditions.

    Backward compatible: automatically detects and handles both formats.
    """

    def translate(self, uql: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        es_bool: Dict[str, Any] = {}

        if parsed.where:
            # Build must and must_not clauses
            if parsed.where.must:
                es_bool["must"] = [self._parse_condition(c) for c in parsed.where.must]
            if parsed.where.must_not:
                es_bool["must_not"] = [
                    self._parse_condition(c) for c in parsed.where.must_not
                ]

        out: Dict[str, Any] = {"query": {"bool": es_bool}}

        if parsed.select and parsed.select != ["*"]:
            out["_source"] = parsed.select
        if parsed.orderBy:
            out["sort"] = [
                {item.field: {"order": item.order.lower()}} for item in parsed.orderBy
            ]
        if parsed.limit is not None:
            out["size"] = parsed.limit
        if parsed.offset is not None:
            out["from"] = parsed.offset

        return out

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> Dict[str, Any]:
        """
        Parse a condition - supports both dict format and Where model.

        Args:
            condition: Either a dict (legacy) or FilterExpression (new)

        Returns:
            Elasticsearch query clause
        """
        # New Where model - use visitor
        if isinstance(condition, FilterExpression):
            visitor = ElasticsearchConditionTranslator()
            return condition.accept(visitor)

        # Legacy dict format - preserve original behavior
        return self._parse_dict_condition(condition)

    def _parse_dict_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
        """Parse legacy dict-based condition."""
        field, op, value = parse_condition(condition)

        if op in ("gt", "gte", "lt", "lte"):
            return {"range": {field: {op: value}}}
        if op == "eq":
            return {"term": {field: value}}
        if op == "neq":
            return {"bool": {"must_not": [{"term": {field: value}}]}}
        if op == "in":
            return {"terms": {field: value}}
        if op == "nin":
            return {"bool": {"must_not": [{"terms": {field: value}}]}}
        if op == "exists":
            return {"exists": {"field": field}}
        if op == "nexists":
            return {"bool": {"must_not": [{"exists": {"field": field}}]}}

        # Extended operators (if present in legacy format)
        if op == "contains":
            return {"wildcard": {field: f"*{value}*"}}
        if op == "starts_with":
            return {"prefix": {field: value}}
        if op == "ends_with":
            return {"wildcard": {field: f"*{value}"}}

        # Default fallback
        return {"term": {field: value}}


# Example: Extended Elasticsearch translator with aggregations
class ElasticsearchAdvancedTranslator(ElasticsearchTranslator):
    """
    Extended Elasticsearch translator with aggregation support.
    """

    def translate_with_aggs(
        self, uql: Dict[str, Any], aggregations: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Translate query with aggregations.

        Args:
            uql: UQL query
            aggregations: Elasticsearch aggregations dict

        Returns:
            Complete Elasticsearch query with aggregations
        """
        # Get base query
        query = self.translate(uql)

        # Add aggregations if provided
        if aggregations:
            query["aggs"] = aggregations

        return query

    def translate_search_after(
        self, uql: Dict[str, Any], search_after: list = None
    ) -> Dict[str, Any]:
        """
        Translate with search_after for efficient deep pagination.

        Args:
            uql: UQL query
            search_after: Values from previous page's sort

        Returns:
            Elasticsearch query with search_after
        """
        query = self.translate(uql)

        # Remove from/size for search_after pagination
        if search_after:
            query.pop("from", None)
            query["search_after"] = search_after

        return query

    def translate_with_highlighting(
        self, uql: Dict[str, Any], highlight_fields: list = None
    ) -> Dict[str, Any]:
        """
        Translate with result highlighting.

        Args:
            uql: UQL query
            highlight_fields: List of fields to highlight

        Returns:
            Elasticsearch query with highlighting
        """
        query = self.translate(uql)

        if highlight_fields:
            query["highlight"] = {"fields": {field: {} for field in highlight_fields}}

        return query


# Example: Builder for complex Elasticsearch queries
class ElasticsearchQueryBuilder:
    """
    Builder for constructing complex Elasticsearch queries with Where model.
    """

    def __init__(self):
        self.filter_expr: FilterExpression = None
        self.must_exprs: list = []
        self.must_not_exprs: list = []
        self.should_exprs: list = []
        self.minimum_should_match: int = None

    def filter(self, expr: FilterExpression) -> "ElasticsearchQueryBuilder":
        """Add filter context (no scoring)."""
        self.filter_expr = expr
        return self

    def must(self, expr: FilterExpression) -> "ElasticsearchQueryBuilder":
        """Add must clause (scoring)."""
        self.must_exprs.append(expr)
        return self

    def must_not(self, expr: FilterExpression) -> "ElasticsearchQueryBuilder":
        """Add must_not clause."""
        self.must_not_exprs.append(expr)
        return self

    def should(
        self, expr: FilterExpression, minimum_match: int = 1
    ) -> "ElasticsearchQueryBuilder":
        """Add should clause."""
        self.should_exprs.append(expr)
        if minimum_match:
            self.minimum_should_match = minimum_match
        return self

    def build(self) -> Dict[str, Any]:
        """Build the complete Elasticsearch query."""
        visitor = ElasticsearchConditionTranslator()
        bool_query = {}

        if self.filter_expr:
            bool_query["filter"] = self.filter_expr.accept(visitor)

        if self.must_exprs:
            bool_query["must"] = [expr.accept(visitor) for expr in self.must_exprs]

        if self.must_not_exprs:
            bool_query["must_not"] = [
                expr.accept(visitor) for expr in self.must_not_exprs
            ]

        if self.should_exprs:
            bool_query["should"] = [expr.accept(visitor) for expr in self.should_exprs]
            if self.minimum_should_match:
                bool_query["minimum_should_match"] = self.minimum_should_match

        return {"query": {"bool": bool_query}}
