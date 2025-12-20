import pytest

from unified_query_maker.utils import (
    ALLOWED_OPS,
    escape_single_quotes,
    parse_condition,
    validate_qualified_name,
)


def test_escape_single_quotes():
    assert escape_single_quotes("a'b''c") == "a''b''''c"
    assert escape_single_quotes("") == ""


@pytest.mark.parametrize(
    "name",
    [
        "a",
        "a_b",
        "a1",
        "a.b",
        "schema.table",
        "_x._y",
    ],
)
def test_validate_qualified_name_accepts_normal_names(name: str):
    validate_qualified_name(name, allow_star=False, allow_trailing_star=False)


@pytest.mark.parametrize("name", ["a.*", "a.b.*"])
def test_validate_qualified_name_trailing_star(name: str):
    validate_qualified_name(name, allow_star=False, allow_trailing_star=True)


def test_validate_qualified_name_star_disallowed_when_allow_star_false():
    with pytest.raises(ValueError):
        validate_qualified_name("*", allow_star=False, allow_trailing_star=True)


@pytest.mark.parametrize("name", ["*", "a.*"])
def test_validate_qualified_name_allow_star(name: str):
    validate_qualified_name(name, allow_star=True, allow_trailing_star=True)


@pytest.mark.parametrize(
    "name",
    [
        "1abc",
        "a-1",
        "a..b",
        ".a",
        "a.",
        "a.*.b",
        "a.**",
    ],
)
def test_validate_qualified_name_rejects_invalid_names(name: str):
    with pytest.raises(ValueError):
        validate_qualified_name(name, allow_star=False, allow_trailing_star=True)


def test_parse_condition_eq_shorthand():
    field, op, value = parse_condition({"age": 10})
    assert field == "age"
    assert op == "eq"
    assert value == 10


@pytest.mark.parametrize("op", sorted(ALLOWED_OPS))
def test_parse_condition_allows_each_op(op: str):
    field, got_op, value = parse_condition({"f": {op: "x"}})
    assert field == "f"
    assert got_op == op
    assert value == "x"


def test_parse_condition_rejects_multiple_fields():
    with pytest.raises(ValueError):
        parse_condition({"a": 1, "b": 2})


def test_parse_condition_rejects_empty_dict():
    with pytest.raises(ValueError):
        parse_condition({})


def test_parse_condition_rejects_unknown_operator():
    with pytest.raises(ValueError):
        parse_condition({"a": {"not_an_op": 1}})


def test_parse_condition_rejects_multi_operator_inner_dict():
    with pytest.raises(ValueError):
        parse_condition({"a": {"gt": 1, "lt": 2}})
