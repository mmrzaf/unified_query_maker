from typing import Dict, Any
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery
from unified_query_maker.utils import parse_condition


class MongoDBTranslator(QueryTranslator):
    def translate(self, uql: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = UQLQuery.model_validate(uql)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        query_filter: Dict[str, Any] = {}

        if parsed.where:
            if parsed.where.must:
                query_filter["$and"] = [
                    self._parse_condition(c) for c in parsed.where.must
                ]
            if parsed.where.must_not:
                # Negate each condition explicitly
                query_filter.setdefault("$and", [])
                query_filter["$and"].extend(
                    {"$nor": [self._parse_condition(c)]} for c in parsed.where.must_not
                )

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

    def _parse_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
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

        return {field: value}
