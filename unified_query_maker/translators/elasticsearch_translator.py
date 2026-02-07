from __future__ import annotations

from typing import Any, Dict, Optional

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


def _like_to_wildcard_pattern(pattern: str) -> str:
    """
    Convert SQL LIKE pattern (%, _) into ES wildcard (*, ?).

    - Treat backslash (\\) as escaping the next char (matching SQL ESCAPE '\\').
    - Escape ES wildcard meta characters in literal output.
    """
    out: list[str] = []
    s = str(pattern)
    i = 0

    while i < len(s):
        ch = s[i]

        if ch == "\\":
            i += 1
            if i >= len(s):
                out.append(_escape_wildcard_literal("\\"))
                break
            out.append(_escape_wildcard_literal(s[i]))
            i += 1
            continue

        if ch == "%":
            out.append("*")
        elif ch == "_":
            out.append("?")
        else:
            out.append(_escape_wildcard_literal(ch))
        i += 1

    return "".join(out)


def _escape_wildcard_literal(value: str) -> str:
    """
    Escape Elasticsearch wildcard meta characters in a *literal* string.

    In ES wildcard syntax, '*', '?', and '\\' are special.
    """
    out: list[str] = []
    for ch in str(value):
        if ch in ("*", "?", "\\"):
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


class ElasticsearchConditionTranslator(FilterVisitor[Dict[str, Any]]):
    """Visitor that translates Where-model expressions to Elasticsearch Query DSL."""

    def visit_condition(self, condition: Condition) -> Dict[str, Any]:
        field = condition.field
        op = condition.operator
        value = condition.value

        # Existence
        if op == Operator.EXISTS:
            return {"exists": {"field": field}}
        if op == Operator.NEXISTS:
            return {"bool": {"must_not": [{"exists": {"field": field}}]}}

        # Equality
        if op == Operator.EQ:
            return {"term": {field: value}}
        if op == Operator.NEQ:
            return {"bool": {"must_not": [{"term": {field: value}}]}}

        # Comparison / ranges
        if op == Operator.GT:
            return {"range": {field: {"gt": value}}}
        if op == Operator.GTE:
            return {"range": {field: {"gte": value}}}
        if op == Operator.LT:
            return {"range": {field: {"lt": value}}}
        if op == Operator.LTE:
            return {"range": {field: {"lte": value}}}
        if op == Operator.BETWEEN:
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError("BETWEEN expects a 2-item list value")
            lo, hi = value
            return {"range": {field: {"gte": lo, "lte": hi}}}

        # Membership
        if op == Operator.IN:
            return {"terms": {field: value}}
        if op == Operator.NIN:
            return {"bool": {"must_not": [{"terms": {field: value}}]}}

        # String ops (wildcard/prefix/regexp)
        if op == Operator.CONTAINS:
            lit = _escape_wildcard_literal(str(value))
            return {"wildcard": {field: f"*{lit}*"}}

        if op == Operator.NCONTAINS:
            lit = _escape_wildcard_literal(str(value))
            return {"bool": {"must_not": [{"wildcard": {field: f"*{lit}*"}}]}}

        if op == Operator.ICONTAINS:
            lit = _escape_wildcard_literal(str(value))
            return {
                "wildcard": {field: {"value": f"*{lit}*", "case_insensitive": True}}
            }

        if op == Operator.ENDS_WITH:
            lit = _escape_wildcard_literal(str(value))
            return {"wildcard": {field: f"*{lit}"}}
        if op == Operator.STARTS_WITH:
            return {"prefix": {field: value}}

        if op == Operator.ILIKE:
            if not isinstance(value, str):
                raise ValueError("ILIKE expects a string pattern")
            wildcard = _like_to_wildcard_pattern(value)
            return {"wildcard": {field: {"value": wildcard, "case_insensitive": True}}}
        if op == Operator.REGEX:
            return {"regexp": {field: value}}

        # Arrays
        if op == Operator.ARRAY_CONTAINS:
            # For arrays of primitives, term matches any element; list semantics are backend-specific.
            if isinstance(value, list):
                # Best-effort: require all provided values to be present (bool must of terms).
                return {"bool": {"must": [{"term": {field: v}} for v in value]}}
            return {"term": {field: value}}

        if op == Operator.ARRAY_OVERLAP:
            return {"terms": {field: value}}

        if op == Operator.ARRAY_CONTAINED:
            # Ensure all elements of doc[field] are within the allowed list.
            # Painless script is portable across common ES versions.
            if not isinstance(value, list):
                raise ValueError("ARRAY_CONTAINED expects a list value")
            return {
                "script": {
                    "script": {
                        "lang": "painless",
                        "source": (
                            "def vals = doc.containsKey(params.f) ? doc[params.f] : null; "
                            "if (vals == null) return true; "
                            "for (def v : vals) { if (!params.allowed.contains(v)) return false; } "
                            "return true;"
                        ),
                        "params": {"allowed": value, "f": field},
                    }
                }
            }

        # Geo
        if op == Operator.GEO_WITHIN:
            return {"geo_shape": {field: {"shape": value, "relation": "within"}}}
        if op == Operator.GEO_INTERSECTS:
            return {"geo_shape": {field: {"shape": value, "relation": "intersects"}}}

        raise ValueError(f"Unsupported operator for Elasticsearch: {op}")

    def visit_and(self, and_expr: AndExpression) -> Dict[str, Any]:
        return {"bool": {"must": [expr.accept(self) for expr in and_expr.expressions]}}

    def visit_or(self, or_expr: OrExpression) -> Dict[str, Any]:
        return {
            "bool": {
                "should": [expr.accept(self) for expr in or_expr.expressions],
                "minimum_should_match": 1,
            }
        }

    def visit_not(self, not_expr: NotExpression) -> Dict[str, Any]:
        return {"bool": {"must_not": [not_expr.expression.accept(self)]}}


class ElasticsearchTranslator(QueryTranslator):
    """Elasticsearch translator for the UQLQuery model (no legacy formats)."""

    def translate(self, uql: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        out: Dict[str, Any] = {}

        # _source (projection)
        if parsed.select and parsed.select != ["*"]:
            out["_source"] = parsed.select

        # Pagination
        if parsed.limit is not None:
            out["size"] = parsed.limit
        if parsed.offset is not None:
            out["from"] = parsed.offset

        # Sorting
        if parsed.orderBy:
            out["sort"] = [
                {item.field: {"order": item.order.lower()}} for item in parsed.orderBy
            ]

        # Query
        if parsed.where and (parsed.where.must or parsed.where.must_not):
            visitor = ElasticsearchConditionTranslator()
            bool_query: Dict[str, Any] = {}

            if parsed.where.must:
                bool_query["must"] = [
                    expr.accept(visitor) for expr in parsed.where.must
                ]

            if parsed.where.must_not:
                bool_query["must_not"] = [
                    expr.accept(visitor) for expr in parsed.where.must_not
                ]

            out["query"] = {"bool": bool_query}
        else:
            out["query"] = {"match_all": {}}

        return out


class ElasticsearchQueryBuilder:
    """Builder for constructing complex Elasticsearch queries with Where model."""

    def __init__(self) -> None:
        self.filter_expr: Optional[FilterExpression] = None
        self.must_exprs: list[FilterExpression] = []
        self.must_not_exprs: list[FilterExpression] = []
        self.should_exprs: list[FilterExpression] = []
        self.minimum_should_match: Optional[int] = None

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
        bool_query: Dict[str, Any] = {}

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
            if self.minimum_should_match is not None:
                bool_query["minimum_should_match"] = self.minimum_should_match

        return {"query": {"bool": bool_query}}
