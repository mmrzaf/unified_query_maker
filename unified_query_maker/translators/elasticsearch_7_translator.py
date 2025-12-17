from typing import Any, Dict, List, Union

from pydantic import ValidationError

from unified_query_maker.models import UQLQuery, QueryOutput
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


class _ES7ConditionTranslator(FilterVisitor):
    def visit_condition(self, condition: Condition) -> Dict[str, Any]:
        field = condition.field
        op = condition.operator
        value = condition.value

        if op in (Operator.GT, Operator.GTE, Operator.LT, Operator.LTE):
            return {"range": {field: {op.value: value}}}

        if op == Operator.EXISTS:
            return {"exists": {"field": field}}
        if op == Operator.NEXISTS:
            return {"bool": {"must_not": [{"exists": {"field": field}}]}}

        if op == Operator.IN:
            return {"terms": {field: value}}
        if op == Operator.NIN:
            return {"bool": {"must_not": [{"terms": {field: value}}]}}

        if op == Operator.NEQ:
            return {"bool": {"must_not": [{"term": {field: value}}]}}

        # default eq / term
        return {"term": {field: value}}

    def visit_and(self, and_expr: AndExpression) -> Dict[str, Any]:
        return {"bool": {"must": [e.accept(self) for e in and_expr.expressions]}}

    def visit_or(self, or_expr: OrExpression) -> Dict[str, Any]:
        return {
            "bool": {
                "should": [e.accept(self) for e in or_expr.expressions],
                "minimum_should_match": 1,
            }
        }

    def visit_not(self, not_expr: NotExpression) -> Dict[str, Any]:
        return {"bool": {"must_not": [not_expr.expression.accept(self)]}}


class Elasticsearch7Translator(QueryTranslator):
    def translate(self, query: Dict[str, Any]) -> QueryOutput:
        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        es_bool: Dict[str, List[Dict[str, Any]]] = {}

        if parsed_query.where:
            if parsed_query.where.must:
                es_bool["must"] = [
                    self._parse_condition(c) for c in parsed_query.where.must
                ]
            if parsed_query.where.must_not:
                es_bool["must_not"] = [
                    self._parse_condition(c) for c in parsed_query.where.must_not
                ]

        return {"query": {"bool": es_bool}}

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> Dict[str, Any]:
        if isinstance(condition, FilterExpression):
            return condition.accept(_ES7ConditionTranslator())

        field, op, value = parse_condition(condition)
        typed = Condition(field=field, operator=Operator(op), value=value)
        return typed.accept(_ES7ConditionTranslator())
