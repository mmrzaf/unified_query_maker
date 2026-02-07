from __future__ import annotations

from unified_query_maker.models.where_model import Condition, Operator, Where
from unified_query_maker.translators.mongodb_translator import MongoDBTranslator


def test_mongodb_translator_typed_conditions():
    tr = MongoDBTranslator()
    out = tr.translate(
        {
            "select": ["id", "name"],
            "from": "ignored_by_translator",
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
