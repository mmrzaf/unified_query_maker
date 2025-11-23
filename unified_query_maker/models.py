from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field, ConfigDict

# A simple value in a condition, e.g., "active", 30, true
OperatorValue = Union[str, int, float, bool, None]

# A condition, e.g., {"age": {"gt": 30}} or {"status": "active"}
Condition = Dict[str, Any]

QueryOutput = Union[str, Dict[str, Any]]


class OrderByItem(BaseModel):
    """Defines a single sorting criterion."""
    field: str
    order: Literal["ASC", "DESC"]


class WhereClause(BaseModel):
    """Defines the 'where' block with 'must' and 'must_not' conditions."""
    must: Optional[List[Condition]] = None
    must_not: Optional[List[Condition]] = None

    # Forbid extra fields like 'should' or 'match'
    model_config = ConfigDict(extra="forbid")


class UQLQuery(BaseModel):
    """
    The main Pydantic model for a Unified Query Language (UQL) query.
    """
    select: List[str]
    # Use alias to allow 'from' in the JSON but 'from_table' in Python
    from_table: str = Field(..., alias="from")
    where: Optional[WhereClause] = None
    orderBy: Optional[List[OrderByItem]] = None
    
    limit: Optional[int] = None
    offset: Optional[int] = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
    )
