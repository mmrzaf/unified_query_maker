from typing import Dict, Any
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery
from unified_query_maker.utils import parse_condition


class Elasticsearch8Translator(QueryTranslator):
    def translate(self, uql: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        es_bool: Dict[str, Any] = {}

        if parsed.where:
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

    def _parse_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
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

        return {"term": {field: value}}
