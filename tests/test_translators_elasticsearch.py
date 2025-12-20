from __future__ import annotations

from unified_query_maker.models.where_model import Condition, Operator, Where
from unified_query_maker.translators.elasticsearch_translator import (
    ElasticsearchAdvancedTranslator,
    ElasticsearchQueryBuilder,
    ElasticsearchTranslator,
)


def test_elasticsearch_translator_legacy_dict_conditions():
    tr = ElasticsearchTranslator()
    out = tr.translate(
        {
            "select": ["id", "name"],
            "from": "idx",
            "where": {
                "must": [{"age": {"gt": 30}}],
                "must_not": [{"status": "inactive"}],
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


def test_elasticsearch_advanced_translator_aggs_and_highlighting_and_search_after():
    tr = ElasticsearchAdvancedTranslator()
    base = {"select": ["id"], "from": "idx", "where": {"must": [{"a": 1}]}}
    out = tr.translate_with_aggs(base, aggregations={"x": {"terms": {"field": "a"}}})
    assert "aggs" in out
    assert out["aggs"]["x"]["terms"]["field"] == "a"

    out2 = tr.translate_with_highlighting(base, highlight_fields=["name", "title"])
    assert "highlight" in out2
    assert "name" in out2["highlight"]["fields"]

    out3 = tr.translate_search_after(base, search_after=[123, "abc"])
    assert "search_after" in out3
    assert "from" not in out3


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
