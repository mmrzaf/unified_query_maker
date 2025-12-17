from __future__ import annotations

from unified_query_maker.models import UQLQuery
from unified_query_maker.models.where_model import (
    AndExpression,
    Condition,
    FilterExpression,
    NotExpression,
    OrExpression,
)


def validate_uql_semantics(uql: UQLQuery) -> bool:
    try:
        if uql.where:
            for expr in uql.where.must or []:
                _walk(expr)
            for expr in uql.where.must_not or []:
                _walk(expr)
        return True
    except Exception:
        return False


def _walk(expr: FilterExpression) -> None:
    if isinstance(expr, Condition):
        return
    if isinstance(expr, AndExpression) or isinstance(expr, OrExpression):
        if not expr.expressions:
            raise ValueError("Boolean expression cannot be empty")
        for sub in expr.expressions:
            _walk(sub)
        return
    if isinstance(expr, NotExpression):
        _walk(expr.expression)
        return
    raise ValueError(f"Unknown filter node: {type(expr)}")
