from typing import Dict, Any, List
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery, QueryOutput


class Elasticsearch8Translator(QueryTranslator):
    def translate(self, query: Dict[str, Any]) -> QueryOutput:
        """Translates UQL dict to Elasticsearch 8 query dict"""
        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        es_bool: Dict[str, List[Dict[str, Any]]] = {}

        if parsed_query.where:
            if parsed_query.where.must:
                es_bool["must"] = [
                    self._parse_condition(cond) for cond in parsed_query.where.must
                ]
            if parsed_query.where.must_not:
                es_bool["must_not"] = [
                    self._parse_condition(cond) for cond in parsed_query.where.must_not
                ]

        return {"query": {"bool": es_bool}}

    def _parse_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
        """Parses a single UQL condition into an ES condition dict"""
        field, op_value = next(iter(condition.items()))

        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            es_op_map = {
                "gt": "gt",
                "gte": "gte",
                "lt": "lt",
                "lte": "lte",
            }

            if op in es_op_map:
                return {"range": {field: {es_op_map[op]: value}}}
            elif op == "eq":
                return {"term": {field: value}}
            elif op == "neq":
                return {"bool": {"must_not": {"term": {field: value}}}}

            return {"term": {field: op_value}}
        else:
            return {"term": {field: op_value}}
