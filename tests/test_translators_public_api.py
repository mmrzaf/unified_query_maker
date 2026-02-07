from unified_query_maker import (
    ElasticsearchTranslator,
    MongoDBTranslator,
    MSSQLTranslator,
    MySQLTranslator,
    OracleTranslator,
    PostgreSQLTranslator,
    validate_uql_schema,
    validate_uql_semantics,
)


def test_public_api_exports_are_importable():
    assert callable(validate_uql_schema)
    assert callable(validate_uql_semantics)

    # instantiation sanity
    PostgreSQLTranslator()
    MySQLTranslator()
    MSSQLTranslator()
    OracleTranslator()
    MongoDBTranslator()
    ElasticsearchTranslator()
