import pytest

from unified_query_maker.utils import escape_single_quotes, validate_qualified_name


def test_escape_single_quotes():
    assert escape_single_quotes("a'b''c") == "a''b''''c"
    assert escape_single_quotes("") == ""


@pytest.mark.parametrize(
    "name,allow_star,allow_trailing_star,ok",
    [
        ("a", False, False, True),
        ("a.b", False, False, True),
        ("a_b.c1", False, False, True),
        ("*", True, False, True),
        ("t.*", False, True, True),
        ("t.*", False, False, False),
        ("bad-name", False, False, False),
        ("1bad", False, False, False),
        ("a..b", False, False, False),
    ],
)
def test_validate_qualified_name(name, allow_star, allow_trailing_star, ok):
    if ok:
        validate_qualified_name(
            name, allow_star=allow_star, allow_trailing_star=allow_trailing_star
        )
    else:
        with pytest.raises(ValueError):
            validate_qualified_name(
                name, allow_star=allow_star, allow_trailing_star=allow_trailing_star
            )
