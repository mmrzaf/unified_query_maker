# Unified Query Maker

### One Query Language → Many Backends

Unified Query Maker lets you describe queries once, in a structured JSON/Pydantic UQL model, and translate them into real queries for multiple database engines:

* Relational
  PostgreSQL, MySQL, MariaDB, MSSQL, Oracle
* Document / Search
  Elasticsearch (DSL), MongoDB
* Columnar / Wide
  Cassandra
* Graph / Document Graph
  Neo4j (Cypher), OrientDB

It provides:

* A **typed UQL model** with strict validation
* A **powerful Where/Filter expression system** (typed; Boolean graph; array ops; dates; text)
* **Forward-compatible translators** (legacy dict filters supported, but typed model is recommended)
* Consistent error semantics
* Clean Python API

This library is designed for real BI systems, analytics services, internal platforms, and serious backend workloads—not for toy scripts.

---

## Install

```
pip install unified-query-maker
```

Supports Python 3.9+ and Pydantic 2.x.

---

## Core Concepts

### 1️⃣ UQL Query Model

A `UQLQuery` describes:

* fields to select
* source (table / index / collection)
* optional filters
* ordering
* pagination

Example:

```python
from unified_query_maker import UQLQuery, OrderByItem

query = UQLQuery(
    select=["id", "name", "age"],
    from_="public.users",
    where=None,
    order_by=[OrderByItem(field="age", direction="desc")],
    limit=50,
    offset=0,
)
```

---

### 2️⃣ Strong Where Model (Recommended)

Typed, composable, expressive.

```python
from unified_query_maker import Where

where = Where.must([
    Where.field("age").gte(18),
    Where.field("status").eq("active"),
    Where.field("tags").contains_any(["premium", "gold"]),
])
```

Supports:

* `eq`, `neq`
* `gt`, `gte`, `lt`, `lte`
* `in_`, `nin`
* `contains`, `contains_any`, `contains_all`
* `exists`, `not_exists`
* date / datetime values
* nested AND / OR / NOT expressions

Boolean graph rules are strict:

* empty AND / OR / NOT is invalid
* malformed expressions are rejected at validation

---

### 3️⃣ Legacy Filter Support (Still Works)

If you have existing JSON filters, they continue to work:

```python
legacy_where = {
  "must": [
    {"field": "age", "op": "gte", "value": 18}
  ]
}
```

However: the typed `Where` model is the future. Migration is encouraged.

---

## Translators

Each translator turns UQL into the native engine query.

---

### PostgreSQL / MySQL / MariaDB / MSSQL / Oracle

```python
from unified_query_maker import PostgreSQLTranslator

sql = PostgreSQLTranslator().translate(query, where=where)
print(sql.query)
print(sql.params)
```

Behavior:

* Identifier validation / quoting
* Safe parameterization
* Proper pagination semantics
* Strict validation before translation

MySQL note: offset-only semantics are currently aligned with existing implementation; documented in tests.

---

### Elasticsearch

Works with DSL query objects. Supports both basic and **advanced mode**.

```python
from unified_query_maker import ElasticsearchTranslator

body = ElasticsearchTranslator().translate(query, where=where)
```

Supports:

* term, range, bool
* must / should / must_not
* search_after
* highlight
* aggregations (advanced translator)

---

### MongoDB

Supports both simple find queries and advanced pipelines.

```python
from unified_query_maker import MongoDBTranslator

doc = MongoDBTranslator().translate(query, where=where)
```

Handles:

* logical operators
* arrays
* dates
* existence operators

---

### Cassandra

Translates to valid Cassandra query expressions.
Respects Cassandra’s real-world constraints.

* unsupported ordering / offset is explicitly rejected
* certain negations are rewritten safely when possible

---

### Neo4j (Cypher)

Produces Cypher with filter conditions mapped appropriately.

---

### OrientDB

Graph/document hybrid translation mapped from UQL constructs.

---

## Validation

Two layers of protection.

### Schema Validation

```python
from unified_query_maker import validate_uql_schema

validate_uql_schema(query)
```

Ensures structure is correct.

---

### Semantic Validation

```python
from unified_query_maker import validate_uql_semantics

validate_uql_semantics(query.where)
```

Ensures:

* boolean expressions are meaningful
* filters are not empty nonsense
* operators make sense for the structure

If something is invalid, it fails early and loudly.
This is on purpose.

---

## Public API Surface

Everything you’re meant to use is cleanly exported:

```python
from unified_query_maker import (
    UQLQuery,
    Where,
    OrderByItem,
    validate_uql_schema,
    validate_uql_semantics,

    PostgreSQLTranslator,
    MySQLTranslator,
    MariaDBTranslator,
    MSSQLTranslator,
    OracleTranslator,

    ElasticsearchTranslator,
    MongoDBTranslator,

    CassandraTranslator,
    Neo4jTranslator,
    OrientDBTranslator,
)
```

---

## Status & Expectations

This library is built to be used in production analytics / data platforms.
It is not a toy. Its constraints are deliberate.

We prefer correctness, explicitness, and predictable failures over “magically doing something”.

If that is your philosophy too, you will be comfortable here.

---

## License

MIT.
