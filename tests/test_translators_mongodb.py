from __future__ import annotations

from unified_query_maker.models.where_model import Condition, Operator, Where
from unified_query_maker.translators.mongodb_translator import (
    MongoDBAdvancedTranslator,
    MongoDBTranslator,
)


def test_mongodb_translator_legacy_dict_conditions():
    tr = MongoDBTranslator()
    out = tr.translate(
        {
            "select": ["id", "name"],
            "from": "ignored_by_translator",
            "where": {
                "must": [{"age": {"gt": 30}}],
                "must_not": [{"status": "inactive"}],
            },
            "orderBy": [{"field": "age", "order": "DESC"}],
            "limit": 10,
            "offset": 20,
        }
    )
    assert out["projection"] == {"id": 1, "name": 1}
    assert out["sort"] == [("age", -1)]
    assert out["limit"] == 10
    assert out["skip"] == 20
    assert "$and" in out["filter"]


def test_mongodb_translator_where_model_geo_and_exists():
    tr = MongoDBTranslator()
    out = tr.translate(
        {
            "select": ["id"],
            "from": "x",
            "where": {
                "must": [
                    Where.field("a").exists(),
                    Condition(
                        field="geo",
                        operator=Operator.GEO_WITHIN,
                        value={"type": "Polygon"},
                    ),
                ],
                "must_not": [Where.field("deleted").eq(True)],
            },
        }
    )
    f = out["filter"]
    assert "$and" in f
    # must_not should appear as a $nor clause inside the $and list
    assert any("$nor" in part for part in f["$and"])


def test_mongodb_advanced_pipeline_translation():
    tr = MongoDBAdvancedTranslator()
    pipeline = tr.translate_to_pipeline(
        {
            "select": ["id", "name"],
            "from": "x",
            "where": {"must": [{"age": {"gte": 10}}]},
            "orderBy": [{"field": "age", "order": "ASC"}],
            "offset": 5,
            "limit": 10,
        }
    )
    assert pipeline[0].get("$match")
    assert pipeline[1] == {"$sort": {"age": 1}}
    assert pipeline[2] == {"$skip": 5}
    assert pipeline[3] == {"$limit": 10}
    assert pipeline[4] == {"$project": {"id": 1, "name": 1}}
