from typing import Dict, Any, List
from pydantic import ValidationError
from unified_query_maker.translators.base import QueryTranslator
from unified_query_maker.models import UQLQuery, QueryOutput


class MongoDBTranslator(QueryTranslator):
    def translate(self, query: Dict[str, Any]) -> QueryOutput:
        """Translates UQL dict to a MongoDB query filter document"""
        try:
            parsed_query = UQLQuery.model_validate(query)
        except ValidationError as e:
            raise ValueError(f"Invalid UQL query: {e}") from e

        mongo_query: Dict[str, List[Dict[str, Any]]] = {}

        if parsed_query.where:
            if parsed_query.where.must:
                mongo_query["$and"] = [
                    self._parse_condition(cond) for cond in parsed_query.where.must
                ]

            if parsed_query.where.must_not:
                mongo_query["$nor"] = [
                    self._parse_condition(cond) for cond in parsed_query.where.must_not
                ]

        # This only returns the filter document.
        # In a real application, you'd use other parts of the query:
        # projection = {field: 1 for field in parsed_query.select}
        # sort_list = [(item.field, 1 if item.order == "ASC" else -1) for item in parsed_query.orderBy or []]
        # limit_val = parsed_query.limit
        # skip_val = parsed_query.offset

        # db.collection.find(mongo_query, projection).sort(sort_list).skip(skip_val).limit(limit_val)

        return mongo_query

    def _parse_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
        """Parses a single UQL condition into a MongoDB condition dict"""
        field, op_value = next(iter(condition.items()))

        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            mongo_op_map = {
                "gt": "$gt",
                "gte": "$gte",
                "lt": "$lt",
                "lte": "$lte",
                "eq": "$eq",
                "neq": "$ne",
            }
            mongo_op = mongo_op_map.get(op, "$eq")
            return {field: {mongo_op: value}}
        else:
            # Simple equality, e.g., {"status": "active"}
            return {field: op_value}
