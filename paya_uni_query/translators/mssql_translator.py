from paya_uni_query.translators.base import QueryTranslator


class MSSQLTranslator(QueryTranslator):
    def translate(self, query):
        select_clause = f"SELECT {', '.join(query['select'])}"
        from_clause = f"FROM {query['from']}"

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

        return f"{select_clause} {from_clause} {where_clause};"

    def _parse_condition(self, condition, negate=False):
        field, op_value = next(iter(condition.items()))
        if isinstance(op_value, dict):
            op, value = next(iter(op_value.items()))
            sql_op = {
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "eq": "=",
                "neq": "!=",
            }.get(op, "=")
            return f"{field} {sql_op} {value}"
        else:
            return f"{field} = '{op_value}'"
