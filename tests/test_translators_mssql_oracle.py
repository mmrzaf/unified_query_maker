from __future__ import annotations

from unified_query_maker.translators.mssql_translator import MSSQLTranslator
from unified_query_maker.translators.oracle_translator import OracleTranslator

from .conftest import squash_ws


def test_mssql_injects_order_by_when_paginating_without_order():
    tr = MSSQLTranslator()
    sql = tr.translate(
        {"select": ["id"], "from": "dbo.Users", "limit": 10, "offset": 5}
    )
    s = squash_ws(sql)
    assert "ORDER BY (SELECT NULL)" in s
    assert "OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY" in s
    assert s.startswith("SELECT [id] FROM [dbo].[Users]")


def test_oracle_limit_only():
    tr = OracleTranslator()
    sql = tr.translate({"select": ["id"], "from": "T", "limit": 10})
    s = squash_ws(sql)
    assert s.endswith("FETCH FIRST 10 ROWS ONLY;")
    assert s.startswith('SELECT "id" FROM "T"')


def test_oracle_offset_only():
    tr = OracleTranslator()
    sql = tr.translate({"select": ["id"], "from": "T", "offset": 7})
    s = squash_ws(sql)
    assert s.endswith("OFFSET 7 ROWS;")
    assert s.startswith('SELECT "id" FROM "T"')


def test_oracle_limit_and_offset():
    tr = OracleTranslator()
    sql = tr.translate({"select": ["id"], "from": "T", "limit": 10, "offset": 7})
    s = squash_ws(sql)
    assert s.endswith("OFFSET 7 ROWS FETCH NEXT 10 ROWS ONLY;")
