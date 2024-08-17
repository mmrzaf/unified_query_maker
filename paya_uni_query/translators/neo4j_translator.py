from paya_uni_query.translators.base import QueryTranslator


class Neo4jTranslator(QueryTranslator):
    def translate(self, query):
        match_clause = f"MATCH (n:{query['from']})"

        where_conditions = []

        if "must" in query["where"]:
            must_conditions = " AND ".join(
                self._parse_condition(cond) for cond in query["where"]["must"]
            )
            where_conditions.append(f"({must_conditions})")

        if "must_not" in query["where"]:
            must_not_conditions = " AND ".join(
                self._parse_condition(cond, negate=True)
                for cond in query["where"]["must_not"]
            )
            where_conditions.append(f"NOT ({must_not_conditions})")

        where_clause = (
            "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        )

        return f"{match_clause} {where_clause} RETURN n;"

    def _parse_condition(self, condition, negate=False):
        field, op_value = next(iter(condition.items()))
        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            cypher_op = {
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "eq": "=",
                "neq": "!=",
            }.get(op, "=")
            return f"n.{field} {cypher_op} {value}"
        else:
            return f"n.{field} = '{op_value}'"
