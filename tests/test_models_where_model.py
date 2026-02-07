from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import TypeAdapter, ValidationError

from unified_query_maker.models.where_model import (
    AndExpression,
    Condition,
    FieldType,
    NotExpression,
    Operator,
    OrExpression,
    Where,
    FilterExpressionModel,
)


def test_where_builder_creates_conditions_and_boolean_nodes():
    a = Where.field("a")
    b = Where.field("b", field_type=FieldType.NUMBER)

    c1 = a.eq("x")
    assert isinstance(c1, Condition)
    assert c1.operator == Operator.EQ
    assert c1.value == "x"

    c2 = b.gt(10)
    assert c2.field_type == FieldType.NUMBER

    and_expr = c1 & c2
    assert isinstance(and_expr, AndExpression)
    assert len(and_expr.expressions) == 2

    or_expr = c1 | c2
    assert isinstance(or_expr, OrExpression)

    not_expr = ~c1
    assert isinstance(not_expr, NotExpression)


def test_condition_validates_field_name():
    with pytest.raises(ValidationError):
        Condition(field="bad-name", operator=Operator.EQ, value=1)


def test_condition_unary_ops_clear_value():
    c = Condition(field="a", operator=Operator.EXISTS, value=True)
    assert c.value is None

    c2 = Condition(field="a", operator=Operator.NEXISTS, value="x")
    assert c2.value is None


def test_condition_in_nin_require_list_like_and_tuple_is_normalized():
    c = Condition(field="a", operator=Operator.IN, value=(1, 2, 3))
    assert c.value == [1, 2, 3]

    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.NIN, value="not-a-list")


def test_condition_between_requires_two_items():
    Condition(field="a", operator=Operator.BETWEEN, value=[1, 2])

    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.BETWEEN, value=[1])

    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.BETWEEN, value="nope")


def test_condition_string_ops_require_string():
    ok = Condition(field="name", operator=Operator.CONTAINS, value="ali")
    assert ok.value == "ali"

    with pytest.raises(ValidationError):
        Condition(field="name", operator=Operator.ICONTAINS, value=123)


def test_condition_geo_ops_require_dict():
    with pytest.raises(ValidationError):
        Condition(field="geo", operator=Operator.GEO_WITHIN, value=["not", "dict"])

    ok = Condition(
        field="geo", operator=Operator.GEO_INTERSECTS, value={"type": "Polygon"}
    )
    assert ok.value == {"type": "Polygon"}


def test_condition_date_datetime_jsonifies_before_validation():
    dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    c = Condition(field="created_at", operator=Operator.EQ, value=dt)
    assert c.value == dt.isoformat()

    d = date(2024, 1, 2)
    c2 = Condition(field="d", operator=Operator.BETWEEN, value=[d, d])
    assert c2.value == [d.isoformat(), d.isoformat()]


def test_condition_rejects_non_string_dict_keys_in_value():
    with pytest.raises(ValidationError):
        Condition(field="obj", operator=Operator.EQ, value={1: "x"})  # type: ignore[arg-type]


def test_filter_expression_model_requires_typed_dicts():
    adapter = TypeAdapter(FilterExpressionModel)

    # Missing discriminator should fail
    with pytest.raises(ValidationError):
        adapter.validate_python({"field": "a", "operator": "eq", "value": 1})

    # Typed dict should validate
    out = adapter.validate_python(
        {"type": "condition", "field": "a", "operator": "eq", "value": 1}
    )
    assert isinstance(out, Condition)
