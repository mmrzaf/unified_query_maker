from __future__ import annotations

from unified_query_maker.models.where_model import AndExpression
from unified_query_maker.validators.schema_validator import validate_uql_schema
from unified_query_maker.validators.semantic_validator import validate_uql_semantics


def test_validate_uql_schema_valid_returns_model():
    q = validate_uql_schema({"select": ["id"], "from": "t"})
    assert q is not None
    assert q.from_table == "t"


def test_validate_uql_schema_invalid_returns_none():
    assert validate_uql_schema({"select": [], "from": "t"}) is None
    assert validate_uql_schema({"select": ["id"]}) is None


def test_validate_uql_semantics_basic_true():
    q = validate_uql_schema(
        {"select": ["id"], "from": "t", "where": {"must": [{"a": {"gt": 1}}]}}
    )
    assert q is not None
    assert validate_uql_semantics(q) is True


def test_validate_uql_semantics_detects_empty_and_expression():
    # semantic validator should reject empty AND, even if constructed bypassing validation
    bad_and = AndExpression.model_construct(type="and", expressions=[])
    q = validate_uql_schema(
        {"select": ["id"], "from": "t", "where": {"must": [bad_and]}}
    )
    assert q is not None
    assert validate_uql_semantics(q) is False
