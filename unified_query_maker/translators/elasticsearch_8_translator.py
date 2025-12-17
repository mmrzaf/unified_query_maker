from typing import Any, Dict, List, Union

from pydantic import ValidationError

from unified_query_maker.models import UQLQuery, QueryOutput
from unified_query_maker.models.where_model import FilterExpression
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.translators.elasticsearch_7_translator import (
    _ES7ConditionTranslator,
)
from unified_query_maker.models.where_model import Condition, Operator
from unified_query_maker.utils import parse_condition


class Elasticsearch8Translator(QueryTranslator):
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

        out: Dict[str, Any] = {"query": {"bool": es_bool}}

        if parsed_query.select and parsed_query.select != ["*"]:
            out["_source"] = parsed_query.select
        if parsed_query.orderBy:
            out["sort"] = [
                {i.field: {"order": i.order.lower()}} for i in parsed_query.orderBy
            ]
        if parsed_query.limit is not None:
            out["size"] = parsed_query.limit
        if parsed_query.offset is not None:
            out["from"] = parsed_query.offset

        return out

    def _parse_condition(
        self, condition: Union[Dict[str, Any], FilterExpression]
    ) -> Dict[str, Any]:
        if isinstance(condition, FilterExpression):
            return condition.accept(_ES7ConditionTranslator())
        field, op, value = parse_condition(condition)
        return Condition(field=field, operator=Operator(op), value=value).accept(
            _ES7ConditionTranslator()
        )
