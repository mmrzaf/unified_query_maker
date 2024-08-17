from paya_uni_query.translators.base import QueryTranslator


class MongoDBTranslator(QueryTranslator):
    def translate(self, query):
        mongo_query = {}

        if "must" in query["where"]:
            mongo_query.update(
                {
                    "$and": [
                        self._parse_condition(cond) for cond in query["where"]["must"]
                    ]
                }
            )

        if "must_not" in query["where"]:
            mongo_query.update(
                {
                    "$nor": [
                        self._parse_condition(cond)
                        for cond in query["where"]["must_not"]
                    ]
                }
            )

        return mongo_query

    def _parse_condition(self, condition):
        field, op_value = next(iter(condition.items()))
        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            mongo_op = {
                "gt": "$gt",
                "gte": "$gte",
                "lt": "$lt",
                "lte": "$lte",
                "eq": "$eq",
                "neq": "$ne",
            }.get(op, "$eq")
            return {field: {mongo_op: value}}
        else:
            return {field: op_value}
