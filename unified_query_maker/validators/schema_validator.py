from pydantic import ValidationError
from typing import Optional, Dict, Any
from unified_query_maker.models import UQLQuery

def validate_uql_schema(uql: Dict[str, Any]) -> Optional[UQLQuery]:
    """
    Validates the UQL query schema using Pydantic models.
    
    Args:
        uql: The raw UQL query dictionary.

    Returns:
        A parsed UQLQuery model instance if valid, else None.
    """
    try:
        return UQLQuery.model_validate(uql)
    except ValidationError as ve:
        print(f"Schema validation error: {ve}")
        return None
