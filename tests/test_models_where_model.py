from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from unified_query_maker.models.where_model import (
    AndExpression,
    Condition,
    FieldType,
    NotExpression,
    Operator,
    OrExpression,
    Where,
    normalize_filter_expression,
)


def test_where_builder_fieldref_methods_create_conditions():
    w = Where.field("age")
    assert w.eq(1).operator == Operator.EQ
    assert w.neq(2).operator == Operator.NEQ
    assert w.gt(3).operator == Operator.GT
    assert w.gte(4).operator == Operator.GTE
    assert w.lt(5).operator == Operator.LT
    assert w.lte(6).operator == Operator.LTE
    assert w.in_([1, 2]).operator == Operator.IN
    assert w.nin([1, 2]).operator == Operator.NIN
    assert w.exists().operator == Operator.EXISTS
    assert w.nexists().operator == Operator.NEXISTS
    assert w.between(1, 9).operator == Operator.BETWEEN
    assert Where.field("name").contains("x").operator == Operator.CONTAINS
    assert Where.field("name").starts_with("x").operator == Operator.STARTS_WITH
    assert Where.field("name").ends_with("x").operator == Operator.ENDS_WITH
    assert Where.field("name").regex("x.*").operator == Operator.REGEX


def test_filter_expression_boolean_operators():
    a = Where.field("a").eq(1)
    b = Where.field("b").eq(2)

    expr_and = a & b
    assert isinstance(expr_and, AndExpression)
    assert len(expr_and.expressions) == 2

    expr_or = a | b
    assert isinstance(expr_or, OrExpression)
    assert len(expr_or.expressions) == 2

    expr_not = ~a
    assert isinstance(expr_not, NotExpression)
    assert isinstance(expr_not.expression, Condition)


def test_condition_validates_field_name():
    with pytest.raises(ValidationError):
        Condition(field="bad-name", operator=Operator.EQ, value=1)


def test_condition_exists_requires_no_value():
    cond = Condition(field="age", operator=Operator.EXISTS, value=123)
    assert cond.value is None

    cond2 = Condition(field="age", operator=Operator.NEXISTS, value="ignored")
    assert cond2.value is None


def test_condition_in_requires_list_like():
    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.IN, value="not-a-list")

    c = Condition(field="a", operator=Operator.IN, value=(1, 2, 3))
    assert c.value == [1, 2, 3]


def test_condition_between_requires_two_items():
    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.BETWEEN, value=[1])

    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.BETWEEN, value=[1, 2, 3])

    c = Condition(field="a", operator=Operator.BETWEEN, value=[1, 2])
    assert c.value == [1, 2]


def test_condition_string_ops_require_string():
    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.CONTAINS, value=1)

    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.STARTS_WITH, value=["x"])

    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.ENDS_WITH, value={"x": 1})

    ok = Condition(field="a", operator=Operator.CONTAINS, value="x")
    assert ok.value == "x"


def test_condition_array_ops_require_list():
    with pytest.raises(ValidationError):
        Condition(field="tags", operator=Operator.ARRAY_OVERLAP, value="x")

    ok = Condition(field="tags", operator=Operator.ARRAY_OVERLAP, value=["a", "b"])
    assert ok.value == ["a", "b"]


def test_condition_geo_ops_require_dict():
    with pytest.raises(ValidationError):
        Condition(field="geo", operator=Operator.GEO_WITHIN, value=["not", "dict"])

    ok = Condition(
        field="geo", operator=Operator.GEO_INTERSECTS, value={"type": "Polygon"}
    )
    assert ok.value == {"type": "Polygon"}


def test_condition_date_datetime_jsonifies_before_validation():
    dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    c = Condition(
        field="created_at",
        operator=Operator.GT,
        value=dt,
        field_type=FieldType.DATETIME,
    )
    assert c.value == "2024-01-02T03:04:05+00:00"

    d = date(2024, 1, 2)
    c2 = Condition(
        field="created_on", operator=Operator.EQ, value=d, field_type=FieldType.DATE
    )
    assert c2.value == "2024-01-02"

    c3 = Condition(
        field="range",
        operator=Operator.BETWEEN,
        value=[date(2024, 1, 1), date(2024, 1, 3)],
        field_type=FieldType.DATE,
    )
    assert c3.value == ["2024-01-01", "2024-01-03"]


def test_condition_jsonify_rejects_non_string_dict_keys():
    with pytest.raises(ValidationError):
        Condition(field="a", operator=Operator.EQ, value={1: "x"})


def test_normalize_filter_expression_accepts_typed_nodes():
    c = Where.field("a").gt(1)
    out = normalize_filter_expression(c)
    assert out is c


def test_normalize_filter_expression_expands_legacy_dict_condition():
    # legacy form: {"age": {"gt": 30}}
    out = normalize_filter_expression({"age": {"gt": 30}})
    assert out["type"] == "condition"
    assert out["field"] == "age"
    assert out["operator"] == "gt"
    assert out["value"] == 30


def test_normalize_filter_expression_boolean_shorthands():
    out_and = normalize_filter_expression({"and": [{"a": 1}, {"b": {"gt": 2}}]})
    assert out_and["type"] == "and"
    assert len(out_and["expressions"]) == 2
    assert out_and["expressions"][0]["type"] == "condition"

    out_or = normalize_filter_expression({"or": [{"a": 1}, {"b": 2}]})
    assert out_or["type"] == "or"
    assert len(out_or["expressions"]) == 2

    out_not = normalize_filter_expression({"not": {"a": 1}})
    assert out_not["type"] == "not"
    assert out_not["expression"]["type"] == "condition"


def test_normalize_filter_expression_rejects_unknown_shape():
    with pytest.raises(ValueError):
        normalize_filter_expression({})

    with pytest.raises(ValueError):
        normalize_filter_expression({"field": {"unknown_op": 1}})
