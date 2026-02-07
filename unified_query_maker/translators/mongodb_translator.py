from __future__ import annotations

import re
from typing import Any, Dict

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
from unified_query_maker.translators.base import QueryTranslator


def _sql_like_to_regex(pattern: str) -> str:
    """
    Convert SQL LIKE (% and _) to a safe regex body:
      - % -> .*
      - _ -> .
      - everything else escaped literally
    """
    out: list[str] = []
    for ch in str(pattern):
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return "".join(out)


class MongoDBConditionTranslator(FilterVisitor[Dict[str, Any]]):
    """Visitor that translates Where-model expressions to MongoDB filter documents."""

    def visit_condition(self, condition: Condition) -> Dict[str, Any]:
        field = condition.field
        op = condition.operator
        value = condition.value

        # Existence
        if op == Operator.EXISTS:
            return {field: {"$exists": True, "$ne": None}}
        if op == Operator.NEXISTS:
            return {"$or": [{field: {"$exists": False}}, {field: None}]}

        # Equality
        if op == Operator.EQ:
            return {field: value}
        if op == Operator.NEQ:
            return {field: {"$ne": value}}

        # Comparisons / ranges
        if op == Operator.GT:
            return {field: {"$gt": value}}
        if op == Operator.GTE:
            return {field: {"$gte": value}}
        if op == Operator.LT:
            return {field: {"$lt": value}}
        if op == Operator.LTE:
            return {field: {"$lte": value}}
        if op == Operator.BETWEEN:
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError("BETWEEN expects a 2-item list value")
            lo, hi = value
            return {field: {"$gte": lo, "$lte": hi}}

        # Membership
        if op == Operator.IN:
            return {field: {"$in": value}}
        if op == Operator.NIN:
            return {field: {"$nin": value}}

        # Strings
        if op == Operator.CONTAINS:
            return {field: {"$regex": re.escape(str(value))}}
        if op == Operator.NCONTAINS:
            return {field: {"$not": {"$regex": re.escape(str(value))}}}
        if op == Operator.ICONTAINS:
            return {field: {"$regex": re.escape(str(value)), "$options": "i"}}
        if op == Operator.STARTS_WITH:
            return {field: {"$regex": f"^{re.escape(str(value))}"}}
        if op == Operator.ENDS_WITH:
            return {field: {"$regex": f"{re.escape(str(value))}$"}}
        if op == Operator.ILIKE:
            body = _sql_like_to_regex(str(value))
            return {field: {"$regex": f"^{body}$", "$options": "i"}}
        if op == Operator.REGEX:
            return {field: {"$regex": str(value)}}

        # Arrays
        if op == Operator.ARRAY_CONTAINS:
            return {field: value}
        if op == Operator.ARRAY_OVERLAP:
            return {field: {"$in": value}}
        if op == Operator.ARRAY_CONTAINED:
            return {field: {"$not": {"$elemMatch": {"$nin": value}}}}

        # Geo
        if op == Operator.GEO_WITHIN:
            return {field: {"$geoWithin": {"$geometry": value}}}
        if op == Operator.GEO_INTERSECTS:
            return {field: {"$geoIntersects": {"$geometry": value}}}

        raise ValueError(f"Unsupported operator for MongoDB: {op}")

    def visit_and(self, and_expr: AndExpression) -> Dict[str, Any]:
        return {"$and": [expr.accept(self) for expr in and_expr.expressions]}

    def visit_or(self, or_expr: OrExpression) -> Dict[str, Any]:
        return {"$or": [expr.accept(self) for expr in or_expr.expressions]}

    def visit_not(self, not_expr: NotExpression) -> Dict[str, Any]:
        return {"$nor": [not_expr.expression.accept(self)]}


class MongoDBTranslator(QueryTranslator):
    """MongoDB translator for the UQLQuery model (no legacy formats)."""

    def translate(self, uql: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        visitor = MongoDBConditionTranslator()
        query_filter: Dict[str, Any] = {}

        if parsed.where:
            parts: list[Dict[str, Any]] = []

            if parsed.where.must:
                parts.extend(expr.accept(visitor) for expr in parsed.where.must)

            if parsed.where.must_not:
                parts.extend(
                    NotExpression(expression=expr).accept(visitor)
                    for expr in parsed.where.must_not
                )

            if len(parts) == 1:
                query_filter = parts[0]
            elif len(parts) > 1:
                query_filter = {"$and": parts}

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
