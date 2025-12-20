from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, constr, field_validator

from unified_query_maker.models.where_model import (
    FilterExpressionModel,
    normalize_filter_expression,
)
from unified_query_maker.utils import validate_qualified_name

NonEmptyStr = constr(min_length=1)


class WhereClause(BaseModel):
    model_config = ConfigDict(extra="forbid")

    must: Optional[List[FilterExpressionModel]] = None
    must_not: Optional[List[FilterExpressionModel]] = None

    @field_validator("must", "must_not", mode="before")
    @classmethod
    def _normalize_expr_lists(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, list):
            raise TypeError("must/must_not must be arrays")
        return [normalize_filter_expression(item) for item in v]


class OrderByItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: NonEmptyStr
    order: str = Field("ASC", pattern="^(ASC|DESC)$")


QueryOutput = Any


class UQLQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    select: Optional[List[NonEmptyStr]] = None
    from_table: NonEmptyStr = Field(..., alias="from")
    where: Optional[WhereClause] = None
    orderBy: Optional[List[OrderByItem]] = None
    limit: Optional[int] = Field(None, ge=0)
    offset: Optional[int] = Field(None, ge=0)

    @field_validator("from_table")
    @classmethod
    def _validate_from_table(cls, v: str) -> str:
        validate_qualified_name(
            str(v).strip(), allow_star=False, allow_trailing_star=False
        )
        return v

    @field_validator("select")
    @classmethod
    def _validate_select(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return None

        fields = [str(x).strip() for x in v]
        if not fields:
            raise ValueError("select cannot be empty")

        if "*" in fields and len(fields) > 1:
            raise ValueError("select cannot contain '*' alongside explicit fields")

        for f in fields:
            if f == "*":
                continue
            validate_qualified_name(f, allow_star=False, allow_trailing_star=True)

        return fields

    @field_validator("where", mode="before")
    @classmethod
    def _normalize_where(cls, v: Any) -> Any:
        """
        Convenience:
        - If user passes a single expression (typed or legacy dict), wrap it into {"must":[...]}.
        - If user passes a WhereClause instance, accept it as-is.
        """
        if v is None:
            return None

        if isinstance(v, WhereClause):
            return v

        if isinstance(v, dict) and ("must" in v or "must_not" in v):
            return v

        return {"must": [normalize_filter_expression(v)]}
