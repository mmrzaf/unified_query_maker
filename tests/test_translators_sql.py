from __future__ import annotations

import pytest

from unified_query_maker.models.where_model import Where
from unified_query_maker.translators.mssql_translator import MSSQLTranslator
from unified_query_maker.translators.mysql_translator import MySQLTranslator
from unified_query_maker.translators.postgresql_translator import PostgreSQLTranslator

from .conftest import squash_ws


def test_postgresql_translator_typed_conditions():
    tr = PostgreSQLTranslator()
    sql = tr.translate(
        {
            "select": ["id", "name"],
            "from": "public.users",
            "where": {
                "must": [
                    {"type": "condition", "field": "age", "operator": "gt", "value": 30},
                    {"type": "condition", "field": "active", "operator": "eq", "value": True},
                ],
                "must_not": [
                    {"type": "condition", "field": "status", "operator": "eq", "value": "inactive"}
                ],
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


def test_mysql_translator_offset_only_uses_big_limit():
    tr = MySQLTranslator()
    sql = tr.translate({"select": ["id"], "from": "t", "offset": 5})
    s = squash_ws(sql)
    assert "LIMIT 5, 18446744073709551615" in s


def test_mssql_translator_rejects_regex():
    tr = MSSQLTranslator()
    with pytest.raises(ValueError):
        tr.translate(
            {
                "select": ["id"],
                "from": "t",
                "where": {"must": [Where.field("name").regex(".*")]},
            }
        )
