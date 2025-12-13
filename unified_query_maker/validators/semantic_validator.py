from unified_query_maker.models import UQLQuery
from unified_query_maker.utils import parse_condition


def validate_uql_semantics(uql: UQLQuery) -> bool:
    """
    Backend-agnostic semantic validation (structure + operator sanity).

    Returns:
        True if semantically valid, else False.
    """
    try:
        if uql.where:
            for cond in uql.where.must or []:
                parse_condition(cond)
            for cond in uql.where.must_not or []:
                parse_condition(cond)

        if uql.limit is not None and uql.limit < 0:
            return False
        if uql.offset is not None and uql.offset < 0:
            return False

        return True
    except ValueError:
        return False
