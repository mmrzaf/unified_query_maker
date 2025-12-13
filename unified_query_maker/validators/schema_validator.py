from pydantic import ValidationError
from typing import Optional, Dict, Any
from unified_query_maker.models import UQLQuery


def validate_uql_schema(uql: Dict[str, Any]) -> Optional[UQLQuery]:
    """
    Validates the given raw UQL dict against the Pydantic model schema.

    Args:
        uql: Raw dict of the UQL query.

    Returns:
        A parsed UQLQuery model instance if valid, else None.
    """
    try:
        return UQLQuery.model_validate(uql)
    except ValidationError:
        return None
