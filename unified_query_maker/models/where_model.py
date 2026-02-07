from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Any, Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from unified_query_maker.utils import validate_qualified_name


def _jsonify_dates(value: object) -> object:
    """
    Make values JSON-friendly while preserving intent:
    - date/datetime -> ISO 8601 string
    - tuple -> list (common in Python callers)
    - recursively applies to lists/dicts
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_jsonify_dates(v) for v in value]
    if isinstance(value, list):
        return [_jsonify_dates(v) for v in value]
    if isinstance(value, dict):
        # JsonValue requires string keys; enforce early.
        out: dict[str, object] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError("Object keys in 'value' must be strings")
            out[k] = _jsonify_dates(v)
        return out
    return value


class FieldType(str, Enum):
    """
    Optional hint (not required). Keep only if you plan type-aware translation later.
    """

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY = "array"
    OBJECT = "object"
    UNKNOWN = "unknown"


class Operator(str, Enum):
    # Basic comparison
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"

    # Membership
    IN = "in"
    NIN = "nin"

    # Existence
    EXISTS = "exists"
    NEXISTS = "nexists"

    # Range / strings
    BETWEEN = "between"
    CONTAINS = "contains"
    NCONTAINS = "ncontains"
    ICONTAINS = "icontains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    ILIKE = "ilike"
    REGEX = "regex"

    # Arrays
    ARRAY_CONTAINS = "array_contains"
    ARRAY_OVERLAP = "array_overlap"
    ARRAY_CONTAINED = "array_contained"

    # Geospatial
    GEO_WITHIN = "geo_within"
    GEO_INTERSECTS = "geo_intersects"


R = TypeVar("R")


class FilterVisitor(ABC, Generic[R]):
    @abstractmethod
    def visit_condition(self, condition: "Condition") -> R: ...

    @abstractmethod
    def visit_and(self, and_expr: "AndExpression") -> R: ...

    @abstractmethod
    def visit_or(self, or_expr: "OrExpression") -> R: ...

    @abstractmethod
    def visit_not(self, not_expr: "NotExpression") -> R: ...


class FilterExpression(BaseModel):
    """
    Base class for filter AST nodes.
    Single approach: every node MUST be typed via the 'type' discriminator.
    """

    model_config = ConfigDict(extra="forbid")

    type: str

    def accept(self, visitor: FilterVisitor[R]) -> R:
        raise NotImplementedError("Subclasses must implement accept()")

    # Fluent composition
    def __and__(self, other: "FilterExpressionModel") -> "AndExpression":
        return AndExpression(expressions=[self, other])  # type: ignore[arg-type]

    def __or__(self, other: "FilterExpressionModel") -> "OrExpression":
        return OrExpression(expressions=[self, other])  # type: ignore[arg-type]

    def __invert__(self) -> "NotExpression":
        return NotExpression(expression=self)  # type: ignore[arg-type]


class Condition(FilterExpression):
    type: Literal["condition"] = "condition"

    field: str
    operator: Operator
    value: Optional[JsonValue] = None
    field_type: Optional[FieldType] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_value(cls, data: object) -> object:
        # Allow Python callers to use date/datetime; store as ISO strings for JSON portability.
        if isinstance(data, dict) and "value" in data:
            copied = dict(data)
            copied["value"] = _jsonify_dates(copied.get("value"))
            return copied
        return data

    @model_validator(mode="after")
    def _validate_condition(self) -> "Condition":
        validate_qualified_name(self.field, allow_star=False, allow_trailing_star=False)

        op = self.operator
        v = self.value

        # Unary ops
        if op in (Operator.EXISTS, Operator.NEXISTS):
            self.value = None
            return self

        # IN/NIN: list required
        if op in (Operator.IN, Operator.NIN):
            if not isinstance(v, list):
                raise ValueError(f"Operator '{op}' requires a list value")
            return self

        # BETWEEN: 2-item list required
        if op == Operator.BETWEEN:
            if not isinstance(v, list) or len(v) != 2:
                raise ValueError("Operator 'between' requires a 2-item list value")
            return self

        # String ops: string required
        if op in (
            Operator.CONTAINS,
            Operator.NCONTAINS,
            Operator.ICONTAINS,
            Operator.STARTS_WITH,
            Operator.ENDS_WITH,
            Operator.ILIKE,
            Operator.REGEX,
        ):
            if not isinstance(v, str):
                raise ValueError(f"Operator '{op}' requires a string value")
            return self

        # Array ops:
        # - array_contains: scalar membership (element-in-array)
        # - array_overlap / array_contained: list required
        if op == Operator.ARRAY_CONTAINS:
            if isinstance(v, list) or isinstance(v, dict):
                raise ValueError(
                    "Operator 'array_contains' requires a scalar JSON value"
                )
            return self

        if op in (Operator.ARRAY_OVERLAP, Operator.ARRAY_CONTAINED):
            if not isinstance(v, list):
                raise ValueError(f"Operator '{op}' requires a list value")
            return self

        # Geo: dict required
        if op in (Operator.GEO_WITHIN, Operator.GEO_INTERSECTS):
            if not isinstance(v, dict):
                raise ValueError(f"Operator '{op}' requires an object/dict value")
            return self

        return self

    def accept(self, visitor: FilterVisitor[R]) -> R:
        return visitor.visit_condition(self)


class AndExpression(FilterExpression):
    type: Literal["and"] = "and"
    expressions: list["FilterExpressionModel"] = Field(min_length=1)

    def accept(self, visitor: FilterVisitor[R]) -> R:
        return visitor.visit_and(self)


class OrExpression(FilterExpression):
    type: Literal["or"] = "or"
    expressions: list["FilterExpressionModel"] = Field(min_length=1)

    def accept(self, visitor: FilterVisitor[R]) -> R:
        return visitor.visit_or(self)


class NotExpression(FilterExpression):
    type: Literal["not"] = "not"
    expression: "FilterExpressionModel"

    def accept(self, visitor: FilterVisitor[R]) -> R:
        return visitor.visit_not(self)


FilterExpressionModel = Annotated[
    Condition | AndExpression | OrExpression | NotExpression,
    Field(discriminator="type"),
]


class Where:
    """
    Fluent builder (kept). Produces typed FilterExpression nodes only.
    """

    @staticmethod
    def field(name: str, field_type: Optional[FieldType] = None) -> "FieldRef":
        return FieldRef(name=name, field_type=field_type)

    @staticmethod
    def and_(*expressions: FilterExpressionModel) -> AndExpression:
        return AndExpression(expressions=list(expressions))

    @staticmethod
    def or_(*expressions: FilterExpressionModel) -> OrExpression:
        return OrExpression(expressions=list(expressions))

    @staticmethod
    def not_(expression: FilterExpressionModel) -> NotExpression:
        return NotExpression(expression=expression)


class FieldRef:
    def __init__(self, name: str, field_type: Optional[FieldType] = None):
        self.name = name
        self.field_type = field_type

    # comparisons
    def eq(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.EQ,
            value=value,
            field_type=self.field_type,
        )

    def neq(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.NEQ,
            value=value,
            field_type=self.field_type,
        )

    def gt(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.GT,
            value=value,
            field_type=self.field_type,
        )

    def gte(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.GTE,
            value=value,
            field_type=self.field_type,
        )

    def lt(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.LT,
            value=value,
            field_type=self.field_type,
        )

    def lte(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.LTE,
            value=value,
            field_type=self.field_type,
        )

    # membership / existence
    def in_(self, values: Sequence[JsonValue]) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.IN,
            value=list(values),
            field_type=self.field_type,
        )

    def nin(self, values: Sequence[JsonValue]) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.NIN,
            value=list(values),
            field_type=self.field_type,
        )

    def exists(self) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.EXISTS,
            value=None,
            field_type=self.field_type,
        )

    def nexists(self) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.NEXISTS,
            value=None,
            field_type=self.field_type,
        )

    # range / strings
    def between(self, min_val: JsonValue, max_val: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.BETWEEN,
            value=[min_val, max_val],
            field_type=self.field_type,
        )

    def contains(self, value: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.CONTAINS,
            value=value,
            field_type=self.field_type,
        )

    def ncontains(self, value: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.NCONTAINS,
            value=value,
            field_type=self.field_type,
        )

    def icontains(self, value: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.ICONTAINS,
            value=value,
            field_type=self.field_type,
        )

    def starts_with(self, value: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.STARTS_WITH,
            value=value,
            field_type=self.field_type,
        )

    def ends_with(self, value: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.ENDS_WITH,
            value=value,
            field_type=self.field_type,
        )

    def ilike(self, pattern: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.ILIKE,
            value=pattern,
            field_type=self.field_type,
        )

    def regex(self, pattern: str) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.REGEX,
            value=pattern,
            field_type=self.field_type,
        )

    # arrays
    def array_contains(self, value: JsonValue) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.ARRAY_CONTAINS,
            value=value,
            field_type=self.field_type,
        )

    def array_overlap(self, values: Sequence[JsonValue]) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.ARRAY_OVERLAP,
            value=list(values),
            field_type=self.field_type,
        )

    def array_contained(self, values: Sequence[JsonValue]) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.ARRAY_CONTAINED,
            value=list(values),
            field_type=self.field_type,
        )

    # geo
    def geo_within(self, shape: dict[str, Any]) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.GEO_WITHIN,
            value=shape,
            field_type=self.field_type,
        )

    def geo_intersects(self, shape: dict[str, Any]) -> Condition:
        return Condition(
            field=self.name,
            operator=Operator.GEO_INTERSECTS,
            value=shape,
            field_type=self.field_type,
        )
