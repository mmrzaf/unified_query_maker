from unified_query_maker import (
    CassandraTranslator,
    ElasticsearchTranslator,
    MariaDBTranslator,
    MongoDBTranslator,
    MSSQLTranslator,
    MySQLTranslator,
    Neo4jTranslator,
    OracleTranslator,
    OrientDBTranslator,
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
    MariaDBTranslator()
    MSSQLTranslator()
    OracleTranslator()
    MongoDBTranslator()
    ElasticsearchTranslator()
    CassandraTranslator()
    OrientDBTranslator()
    Neo4jTranslator()
