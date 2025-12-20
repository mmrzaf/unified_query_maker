from __future__ import annotations

import pytest

from unified_query_maker.models.where_model import Where
from unified_query_maker.translators.cassandra_translator import CassandraTranslator
from unified_query_maker.translators.neo4j_translator import Neo4jTranslator
from unified_query_maker.translators.orientdb_translator import OrientDBTranslator

from .conftest import squash_ws


def test_orientdb_basic_translation():
    tr = OrientDBTranslator()
    q = {
        "select": ["id", "name"],
        "from": "Users",
        "where": {"must": [{"age": {"gt": 30}}], "must_not": [{"status": "inactive"}]},
        "orderBy": [{"field": "age", "order": "DESC"}],
        "limit": 10,
        "offset": 5,
    }
    out = tr.translate(q)
    s = squash_ws(out)
    assert s.startswith("SELECT id, name FROM Users")
    assert "WHERE (" in s
    assert "ORDER BY age DESC" in s
    assert "SKIP 5" in s
    assert "LIMIT 10" in s
    assert s.endswith(";")


def test_neo4j_basic_translation_with_where_model():
    tr = Neo4jTranslator()
    q = {
        "select": ["id", "name"],
        "from": "User",
        "where": {
            "must": [Where.field("age").gt(18)],
            "must_not": [Where.field("banned").eq(True)],
        },
        "orderBy": [{"field": "age", "order": "DESC"}],
        "limit": 10,
        "offset": 5,
    }
    out = tr.translate(q)
    s = squash_ws(out)
    assert s.startswith("MATCH (n:User)")
    assert "WHERE" in s
    assert "RETURN n.id, n.name" in s
    assert "ORDER BY n.age DESC" in s
    assert "SKIP 5" in s
    assert "LIMIT 10" in s
    assert s.endswith(";")


def test_cassandra_rejects_order_by_and_offset():
    tr = CassandraTranslator()
    with pytest.raises(ValueError):
        tr.translate(
            {
                "select": ["id"],
                "from": "t",
                "orderBy": [{"field": "id", "order": "ASC"}],
            }
        )
    with pytest.raises(ValueError):
        tr.translate({"select": ["id"], "from": "t", "offset": 10})


def test_cassandra_must_not_inverts_leaf_conditions_when_possible():
    tr = CassandraTranslator()
    out = tr.translate(
        {
            "select": ["id"],
            "from": "users",
            "where": {"must_not": [{"status": "inactive"}]},
        }
    )
    s = squash_ws(out)
    # Must-not leaf eq should invert to != via negate logic (not wrap NOT(...))
    assert "status !=" in s
    assert "NOT (" not in s  # leaf inversion avoids NOT wrapper when possible


def test_cassandra_must_not_wraps_complex_expressions_with_not():
    tr = CassandraTranslator()
    out = tr.translate(
        {
            "select": ["id"],
            "from": "users",
            "where": {"must_not": [(Where.field("a").eq(1) & Where.field("b").eq(2))]},
        }
    )
    s = squash_ws(out)
    assert "NOT ((" in s
    assert "a = 1" in s and "b = 2" in s
