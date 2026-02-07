from __future__ import annotations

from unified_query_maker.models.where_model import Condition, Operator, Where
from unified_query_maker.translators.elasticsearch_translator import (
    ElasticsearchQueryBuilder,
    ElasticsearchTranslator,
)


def test_elasticsearch_translator_typed_conditions():
    tr = ElasticsearchTranslator()
    out = tr.translate(
        {
            "select": ["id", "name"],
            "from": "idx",
            "where": {
                "must": [
                    {"type": "condition", "field": "age", "operator": "gt", "value": 30}
                ],
                "must_not": [
                    {
                        "type": "condition",
                        "field": "status",
                        "operator": "eq",
                        "value": "inactive",
                    }
                ],
            },
            "orderBy": [{"field": "age", "order": "DESC"}],
            "limit": 10,
            "offset": 20,
        }
    )
    assert out["_source"] == ["id", "name"]
    assert out["size"] == 10
    assert out["from"] == 20
    assert out["sort"] == [{"age": {"order": "desc"}}]
    assert "bool" in out["query"]
    assert "must" in out["query"]["bool"]
    assert "must_not" in out["query"]["bool"]


def test_elasticsearch_translator_where_model_array_contained_emits_script():
    tr = ElasticsearchTranslator()
    out = tr.translate(
        {
            "select": ["id"],
            "from": "idx",
            "where": {
                "must": [
                    Condition(
                        field="tags",
                        operator=Operator.ARRAY_CONTAINED,
                        value=["a", "b"],
                    )
                ]
            },
        }
    )
    clause = out["query"]["bool"]["must"][0]
    assert "script" in clause
    assert clause["script"]["script"]["params"]["allowed"] == ["a", "b"]


def test_elasticsearch_query_builder_builds_bool_query():
    qb = ElasticsearchQueryBuilder()
    built = (
        qb.filter(Where.field("status").eq("active"))
        .must(Where.field("age").gt(18))
        .must_not(Where.field("banned").eq(True))
        .should(Where.field("tier").eq("pro"), minimum_match=1)
        .build()
    )
    assert "query" in built and "bool" in built["query"]
    b = built["query"]["bool"]
    assert "filter" in b and "must" in b and "must_not" in b and "should" in b
    assert b["minimum_should_match"] == 1


def test_elasticsearch_contains_escapes_wildcard_metacharacters():
    tr = ElasticsearchTranslator()
    out = tr.translate(
        {
            "from": "idx",
            "where": {"must": [Where.field("name").contains(r"a*b?c\d")]},
        }
    )
    clause = out["query"]["bool"]["must"][0]
    assert clause == {"wildcard": {"name": "*a\\*b\\?c\\\\d*"}}
