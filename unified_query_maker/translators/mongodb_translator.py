"""
Updated MongoDB Translator with Where Model Support
===================================================

Supports both legacy dict-based conditions and new Where/Filter model.
"""

import re
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


class MongoDBConditionTranslator(FilterVisitor):
    """
    Visitor that translates Where model conditions to MongoDB queries.
    """

    def visit_condition(self, condition: Condition) -> Dict[str, Any]:
        """Translate a single condition to MongoDB query."""
        field = condition.field
        op = condition.operator
        value = condition.value

        # Existence operators
        if op == Operator.EXISTS:
            return {field: {"$exists": True, "$ne": None}}
        if op == Operator.NEXISTS:
            # Match both missing and null
            return {"$or": [{field: {"$exists": False}}, {field: None}]}

        # Equality operators
        if op == Operator.EQ:
            return {field: value}
        if op == Operator.NEQ:
            return {field: {"$ne": value}}

        # Comparison operators
        if op == Operator.GT:
            return {field: {"$gt": value}}
        if op == Operator.GTE:
            return {field: {"$gte": value}}
        if op == Operator.LT:
            return {field: {"$lt": value}}
        if op == Operator.LTE:
            return {field: {"$lte": value}}
        if op == Operator.BETWEEN:
            min_val, max_val = value
            return {field: {"$gte": min_val, "$lte": max_val}}

        # Membership operators
        if op == Operator.IN:
            return {field: {"$in": value}}
        if op == Operator.NIN:
            return {field: {"$nin": value}}

        # String operators
        if op == Operator.CONTAINS:
            # Case-sensitive regex
            escaped = re.escape(value)
            return {field: {"$regex": escaped}}
        if op == Operator.NCONTAINS:
            escaped = re.escape(value)
            return {field: {"$not": {"$regex": escaped}}}
        if op == Operator.ICONTAINS:
            # Case-insensitive contains
            escaped = re.escape(value)
            return {field: {"$regex": escaped, "$options": "i"}}
        if op == Operator.STARTS_WITH:
            escaped = re.escape(value)
            return {field: {"$regex": f"^{escaped}"}}
        if op == Operator.ENDS_WITH:
            escaped = re.escape(value)
            return {field: {"$regex": f"{escaped}$"}}
        if op == Operator.ILIKE:
            # Convert SQL LIKE pattern to regex
            pattern = value.replace("%", ".*").replace("_", ".")
            return {field: {"$regex": f"^{pattern}$", "$options": "i"}}
        if op == Operator.REGEX:
            return {field: {"$regex": value}}

        # Array operators
        if op == Operator.ARRAY_CONTAINS:
            # In MongoDB, equality on array field checks if value is an element
            return {field: value}
        if op == Operator.ARRAY_OVERLAP:
            # At least one element in common
            return {field: {"$in": value}}
        if op == Operator.ARRAY_CONTAINED:
            # All array elements must be in the provided list (subset check)
            # MongoDB: use $not + $elemMatch to check no elements outside the list
            return {field: {"$not": {"$elemMatch": {"$nin": value}}}}

        # Geospatial operators
        if op == Operator.GEO_WITHIN:
            return {field: {"$geoWithin": {"$geometry": value}}}
        if op == Operator.GEO_INTERSECTS:
            return {field: {"$geoIntersects": {"$geometry": value}}}

        # Fallback
        raise ValueError(f"Unsupported operator for MongoDB: {op}")

    def visit_and(self, and_expr: AndExpression) -> Dict[str, Any]:
        """Translate AND expression."""
        sub_queries = [expr.accept(self) for expr in and_expr.expressions]
        return {"$and": sub_queries}

    def visit_or(self, or_expr: OrExpression) -> Dict[str, Any]:
        """Translate OR expression."""
        sub_queries = [expr.accept(self) for expr in or_expr.expressions]
        return {"$or": sub_queries}

    def visit_not(self, not_expr: NotExpression) -> Dict[str, Any]:
        """Translate NOT expression."""
        sub_query = not_expr.expression.accept(self)
        return {"$nor": [sub_query]}


class MongoDBTranslator(QueryTranslator):
    """
    MongoDB translator with support for both dict-based and Where model conditions.

    Backward compatible: automatically detects and handles both formats.
    """

    def translate(self, uql: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        query_filter: Dict[str, Any] = {}

        if parsed.where:
            # Build filter from must and must_not
            conditions = []

            if parsed.where.must:
                for cond in parsed.where.must:
                    conditions.append(self._parse_condition(cond))

            if parsed.where.must_not:
                # Negate each condition explicitly
                for cond in parsed.where.must_not:
                    negated = self._negate_condition(cond)
                    conditions.append(negated)

            # Combine conditions
            if len(conditions) == 1:
                query_filter = conditions[0]
            elif len(conditions) > 1:
                query_filter = {"$and": conditions}

        out: Dict[str, Any] = {"filter": query_filter}

        if parsed.select and parsed.select != ["*"]:
            out["projection"] = {f: 1 for f in parsed.select}

        if parsed.orderBy:
            out["sort"] = [
                (item.field, 1 if item.order == "ASC" else -1)
                for item in parsed.orderBy
            ]

        if parsed.limit is not None:
            out["limit"] = parsed.limit
        if parsed.offset is not None:
            out["skip"] = parsed.offset

        return out

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> Dict[str, Any]:
        """
        Parse a condition - supports both dict format and Where model.

        Args:
            condition: Either a dict (legacy) or FilterExpression (new)

        Returns:
            MongoDB query document
        """
        # New Where model - use visitor
        if isinstance(condition, FilterExpression):
            visitor = MongoDBConditionTranslator()
            return condition.accept(visitor)

        # Legacy dict format - preserve original behavior
        return self._parse_dict_condition(condition)

    def _parse_dict_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
        """Parse legacy dict-based condition."""
        field, op, value = parse_condition(condition)

        if op == "eq":
            return {field: value}
        if op == "neq":
            return {field: {"$ne": value}}
        if op in ("gt", "gte", "lt", "lte"):
            return {field: {f"${op}": value}}
        if op == "in":
            return {field: {"$in": value}}
        if op == "nin":
            return {field: {"$nin": value}}
        if op == "exists":
            return {field: {"$exists": True}}
        if op == "nexists":
            return {field: {"$exists": False}}

        # Extended operators (if present in legacy format)
        if op == "contains":
            return {field: {"$regex": re.escape(value)}}
        if op == "starts_with":
            return {field: {"$regex": f"^{re.escape(value)}"}}
        if op == "ends_with":
            return {field: {"$regex": f"{re.escape(value)}$"}}

        return {field: value}

    def _negate_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> Dict[str, Any]:
        """
        Negate a condition for must_not semantics.

        Args:
            condition: Either dict or FilterExpression

        Returns:
            Negated MongoDB query
        """
        # For Where model, wrap in NOT expression
        if isinstance(condition, FilterExpression):
            not_expr = NotExpression(expression=condition)
            visitor = MongoDBConditionTranslator()
            return not_expr.accept(visitor)

        # For dict, use $nor
        parsed = self._parse_dict_condition(condition)
        return {"$nor": [parsed]}


# Example: Extended MongoDB translator with aggregation support
class MongoDBAdvancedTranslator(MongoDBTranslator):
    """
    Extended MongoDB translator with aggregation pipeline support.
    """

    def translate_to_pipeline(self, uql: Dict[str, Any]) -> list:
        """
        Translate to MongoDB aggregation pipeline.

        Returns:
            List of pipeline stages
        """
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        pipeline = []

        # $match stage (where clause)
        if parsed.where:
            match_filter = {}
            conditions = []

            if parsed.where.must:
                for cond in parsed.where.must:
                    conditions.append(self._parse_condition(cond))

            if parsed.where.must_not:
                for cond in parsed.where.must_not:
                    conditions.append(self._negate_condition(cond))

            if len(conditions) == 1:
                match_filter = conditions[0]
            elif len(conditions) > 1:
                match_filter = {"$and": conditions}

            if match_filter:
                pipeline.append({"$match": match_filter})

        # $sort stage
        if parsed.orderBy:
            sort_spec = {
                item.field: 1 if item.order == "ASC" else -1 for item in parsed.orderBy
            }
            pipeline.append({"$sort": sort_spec})

        # $skip stage
        if parsed.offset and parsed.offset > 0:
            pipeline.append({"$skip": parsed.offset})

        # $limit stage
        if parsed.limit is not None:
            pipeline.append({"$limit": parsed.limit})

        # $project stage (select)
        if parsed.select and parsed.select != ["*"]:
            project_spec = {f: 1 for f in parsed.select}
            pipeline.append({"$project": project_spec})

        return pipeline
