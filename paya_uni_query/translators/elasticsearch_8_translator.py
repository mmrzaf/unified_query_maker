from paya_uni_query.translators.base import QueryTranslator


class Elasticsearch8Translator(QueryTranslator):
    def translate(self, query):
        es_query = {"query": {"bool": {}}}

        if "must" in query["where"]:
            es_query["query"]["bool"]["must"] = [
                self._parse_condition(cond) for cond in query["where"]["must"]
            ]

        if "must_not" in query["where"]:
            es_query["query"]["bool"]["must_not"] = [
                self._parse_condition(cond) for cond in query["where"]["must_not"]
            ]

        return es_query

    def _parse_condition(self, condition):
        field, op_value = next(iter(condition.items()))
        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            es_op = {
                "gt": "range",
                "gte": "range",
                "lt": "range",
                "lte": "range",
                "eq": "term",
                "neq": "must_not",
            }.get(op, "term")
            if es_op == "range":
                return {"range": {field: {op: value}}}
            else:
                return {es_op: {field: value}}
        else:
            return {"term": {field: op_value}}
