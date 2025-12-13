from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field, ConfigDict, constr, field_validator
from unified_query_maker.utils import validate_qualified_name

OperatorValue = Union[str, int, float, bool, None]
NonEmptyStr = constr(min_length=1)


class OrderByItem(BaseModel):
    field: str
    order: Literal["ASC", "DESC"]

    @field_validator("field")
    @classmethod
    def _validate_order_field(cls, v: str) -> str:
        return validate_qualified_name(v, allow_star=False, allow_trailing_star=False)


class WhereClause(BaseModel):
    must: Optional[List[Dict[str, Any]]] = None
    must_not: Optional[List[Dict[str, Any]]] = None


class UQLQuery(BaseModel):
    """
    Unified Query Language (UQL) query model.
    """

    select: list[NonEmptyStr] | None = None
    from_table: NonEmptyStr = Field(..., alias="from")
    where: Optional[WhereClause] = None
    orderBy: Optional[List[OrderByItem]] = None

    limit: Optional[int] = Field(default=None, ge=0)
    offset: Optional[int] = Field(default=None, ge=0)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
    )

    @field_validator("from_table")
    @classmethod
    def _validate_from_table(cls, v: str) -> str:
        # Allow qualified table names like schema.table
        return validate_qualified_name(v, allow_star=False, allow_trailing_star=False)

    @field_validator("select")
    @classmethod
    def _validate_select(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v

        cleaned: list[str] = []
        for item in v:
            item_s = str(item).strip()
            if item_s == "*":
                cleaned.append("*")
            else:
                cleaned.append(
                    validate_qualified_name(
                        item_s, allow_star=False, allow_trailing_star=True
                    )
                )

        # Don't allow mixing "*" with other fields
        if "*" in cleaned and len(cleaned) > 1:
            raise ValueError("select cannot mix '*' with other fields")

        return cleaned


# Keep compatibility if your code imports QueryOutput
QueryOutput = Any
