from __future__ import annotations

import pytest

from unified_query_maker.models.where_model import Condition, Operator, Where
from unified_query_maker.translators.mariadb_translator import MariaDBTranslator
from unified_query_maker.translators.mysql_translator import MySQLTranslator
from unified_query_maker.translators.postgresql_translator import PostgreSQLTranslator

from .conftest import squash_ws


def test_postgresql_translator_legacy_dict_conditions():
    tr = PostgreSQLTranslator()
    sql = tr.translate(
        {
            "select": ["id", "name"],
            "from": "public.users",
            "where": {
                "must": [{"age": {"gt": 30}}, {"active": True}],
                "must_not": [{"status": "inactive"}],
            },
            "orderBy": [{"field": "name", "order": "DESC"}],
            "limit": 10,
            "offset": 20,
        }
    )
    assert squash_ws(sql) == squash_ws(
        'SELECT "id", "name" FROM "public"."users" '
        'WHERE ("age" > 30 AND "active" = TRUE) AND (NOT ("status" = \'inactive\')) '
        'ORDER BY "name" DESC LIMIT 10 OFFSET 20;'
    )


def test_postgresql_translator_where_model_extended_string_ops():
    tr = PostgreSQLTranslator()
    sql = tr.translate(
        {
            "select": ["id"],
            "from": "public.users",
            "where": {
                "must": [
                    Where.field("name").contains("ali"),
                    Where.field("name").starts_with("a"),
                ],
                "must_not": [Where.field("name").ends_with("z")],
            },
        }
    )
    s = squash_ws(sql)
    assert "WHERE (" in s
    assert '"name" LIKE ' in s
    assert "%ali%" in s  # contains adds wildcards via visitor
    assert "a%" in s  # starts_with
    assert "%z" in s  # ends_with inside must_not


def test_postgresql_translator_where_model_array_overlap():
    tr = PostgreSQLTranslator()
    sql = tr.translate(
        {
            "select": ["id"],
            "from": "public.items",
            "where": {
                "must": [
                    Condition(
                        field="tags", operator=Operator.ARRAY_OVERLAP, value=["a", "b"]
                    )
                ]
            },
        }
    )
    s = squash_ws(sql)
    assert "\"tags\" && ARRAY['a', 'b']" in s


def test_mysql_and_mariadb_quote_identifiers():
    q = {"select": ["id"], "from": "public.users"}
    mysql = MySQLTranslator().translate(q)
    mariadb = MariaDBTranslator().translate(q)

    assert squash_ws(mysql) == squash_ws("SELECT `id` FROM `public`.`users`;")
    assert squash_ws(mariadb) == squash_ws("SELECT `id` FROM `public`.`users`;")

    # offset-only behavior is dialect-sensitive; base SQLTranslator emits OFFSET-only.
    # MySQL generally prefers LIMIT ... OFFSET or LIMIT offset,count; this is a stabilization target.
    sql_offset_only = MySQLTranslator().translate(
        {"select": ["id"], "from": "t", "offset": 5}
    )
    assert "OFFSET 5" in sql_offset_only


@pytest.mark.xfail(
    strict=True,
    reason="MySQL offset-only pagination should be emitted as LIMIT offset, big-number (see base_sql example).",
)
def test_mysql_offset_only_should_not_emit_bare_offset():
    sql_offset_only = MySQLTranslator().translate(
        {"select": ["id"], "from": "t", "offset": 5}
    )
    assert "LIMIT 5, 18446744073709551615" in sql_offset_only
