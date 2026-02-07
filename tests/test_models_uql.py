from __future__ import annotations

import pytest
from pydantic import ValidationError

from unified_query_maker.models import OrderByItem, UQLQuery, Where, WhereClause


def test_uqlquery_accepts_from_alias():
    q = UQLQuery.model_validate({"select": ["id"], "from": "public.users"})
    assert q.from_table == "public.users"


def test_uqlquery_select_star_rules():
    UQLQuery.model_validate({"select": ["*"], "from": "t"})

    with pytest.raises(ValidationError):
        UQLQuery.model_validate({"select": ["*", "id"], "from": "t"})

    with pytest.raises(ValidationError):
        UQLQuery.model_validate({"select": [], "from": "t"})


def test_uqlquery_select_allows_trailing_star_segment():
    q = UQLQuery.model_validate({"select": ["public.users.*"], "from": "public.users"})
    assert q.select == ["public.users.*"]


def test_uqlquery_invalid_identifiers_rejected():
    with pytest.raises(ValidationError):
        UQLQuery.model_validate({"select": ["bad-name"], "from": "t"})

    with pytest.raises(ValidationError):
        UQLQuery.model_validate({"select": ["id"], "from": "bad-name"})


def test_uqlquery_where_autowraps_single_expression_to_must():
    q = UQLQuery.model_validate(
        {
            "select": ["id"],
            "from": "t",
            "where": {"type": "condition", "field": "a", "operator": "gt", "value": 1},
        }
    )
    assert q.where is not None
    assert len(q.where.must) == 1
    assert q.where.must_not is None


def test_where_clause_requires_list_for_must_and_must_not():
    # must must be a list
    with pytest.raises(TypeError):
        UQLQuery.model_validate(
            {"select": ["id"], "from": "t", "where": {"must": {"a": 1}}}
        )

    # must_not must be a list
    with pytest.raises(TypeError):
        UQLQuery.model_validate(
            {"select": ["id"], "from": "t", "where": {"must_not": {"b": 2}}}
        )


def test_orderby_item_defaults_and_validation():
    item = OrderByItem.model_validate({"field": "name"})
    assert item.order == "ASC"

    item2 = OrderByItem.model_validate({"field": "name", "order": "DESC"})
    assert item2.order == "DESC"

    with pytest.raises(ValidationError):
        OrderByItem.model_validate({"field": "", "order": "ASC"})

    with pytest.raises(ValidationError):
        OrderByItem.model_validate({"field": "name", "order": "UP"})


def test_uqlquery_where_model_roundtrip():
    where = WhereClause(must=[Where.field("age").gt(10)])
    q = UQLQuery.model_validate({"select": ["id"], "from": "t", "where": where})
    assert q.where.must[0].field == "age"


def test_uqlquery_rejects_empty_and_expression_via_schema():
    # AndExpression has min_length=1, so this should fail normal validation.
    with pytest.raises(ValidationError):
        UQLQuery.model_validate(
            {
                "select": ["id"],
                "from": "t",
                "where": {"must": [{"type": "and", "expressions": []}]},
            }
        )
