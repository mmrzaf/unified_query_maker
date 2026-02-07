Below is a **clean, updated README** aligned with your current direction:

* **One canonical model only** (typed UQL + typed filter AST)
* **No legacy filter formats**
* **Translators take a single `dict`/`UQLQuery` and return native query output**
* **No claims of parameterization/`params` unless you actually return them**
* **No claims of advanced features (search_after, highlight, aggregations) unless implemented**

Copy-paste the whole thing as `README.md`.

---

# Unified Query Maker

## One Query Language → Many Backends

Unified Query Maker lets you describe a query once using a strict, typed **UQL** (JSON/Pydantic) model and translate it into native queries for multiple backends.

Supported backends:

* **Relational (SQL)**: PostgreSQL, MySQL, MariaDB, MSSQL, Oracle
* **Document / Search**: Elasticsearch (Query DSL), MongoDB
* **Columnar / Wide**: Cassandra
* **Graph / Document Graph**: Neo4j (Cypher), OrientDB

What you get:

* A **single canonical UQL schema** (Pydantic v2)
* A **typed filter AST** (`condition`, `and`, `or`, `not`) with strict validation
* Translators that **only** accept the canonical model (no legacy formats)
* Predictable failures: invalid queries are rejected early

This library is built for production systems that value correctness over “it kinda works”.

---

## Install

```bash
pip install unified-query-maker
```

Supports Python **3.9+** and **Pydantic 2.x**.

---

## UQL in 60 seconds

A UQL query describes:

* `select`: fields to fetch (or `["*"]`)
* `from`: source (table / index / collection)
* `where`: optional `must` / `must_not` filter lists
* `orderBy`: ordering
* `limit` / `offset`: pagination (where supported by the backend)

### Minimal example

```python
from unified_query_maker import PostgreSQLTranslator

uql = {
  "select": ["id", "name"],
  "from": "public.users",
  "where": {
    "must": [
      {"type": "condition", "field": "status", "operator": "eq", "value": "active"}
    ]
  },
  "orderBy": [{"field": "id", "order": "ASC"}],
  "limit": 50,
  "offset": 0
}

sql = PostgreSQLTranslator().translate(uql)
print(sql)
```

---

## Filters (Where model)

Filters are a **typed AST**. Every node must include a `type` discriminator.

### Condition node

```json
{
  "type": "condition",
  "field": "age",
  "operator": "gte",
  "value": 18
}
```

### Boolean nodes

```json
{
  "type": "and",
  "expressions": [
    { "type": "condition", "field": "status", "operator": "eq", "value": "active" },
    { "type": "condition", "field": "age", "operator": "gte", "value": 18 }
  ]
}
```

```json
{
  "type": "not",
  "expression": {
    "type": "condition",
    "field": "country",
    "operator": "in",
    "value": ["RU", "BY"]
  }
}
```

### WhereClause structure

UQL uses an explicit `must` / `must_not` container:

```python
uql = {
  "from": "users",
  "where": {
    "must": [
      {"type": "condition", "field": "status", "operator": "eq", "value": "active"}
    ],
    "must_not": [
      {"type": "condition", "field": "email", "operator": "icontains", "value": "test"}
    ]
  }
}
```

Rules are strict:

* `and/or` must have at least one expression
* `not` must have exactly one expression
* invalid operator/value combinations fail validation

---

## Fluent filter builder (Python)

You can build typed filters without writing dicts:

```python
from unified_query_maker import Where, PostgreSQLTranslator

where = Where.and_(
    Where.field("age").gte(18),
    Where.field("status").eq("active"),
    ~Where.field("email").icontains("test"),
)

uql = {
  "select": ["id", "name", "age"],
  "from": "public.users",
  "where": {"must": [where]},
  "orderBy": [{"field": "age", "order": "DESC"}],
  "limit": 50,
}

print(PostgreSQLTranslator().translate(uql))
```

---

## Operator reference (filters)

### Comparison

* `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
* `between` (requires `[min, max]`)

### Membership / existence

* `in` (requires list)
* `nin` (requires list)
* `exists`, `nexists` (no value)

### Strings

* `contains`, `ncontains`, `icontains`
* `starts_with`, `ends_with`
* `ilike` (pattern; backend-specific implementation)
* `regex` (backend-specific support)

### Arrays

* `array_contains` (scalar membership in array)
* `array_overlap` (requires list)
* `array_contained` (requires list)

### Geo

* `geo_within`, `geo_intersects` (requires object/dict)

> Backend support varies. If an operator is not supported for a translator/dialect, translation fails with a clear error.

---

## Translators

Translators accept a UQL dict (or a `UQLQuery`-compatible structure) and return a native query representation.

### SQL translators (PostgreSQL / MySQL / MariaDB / MSSQL / Oracle)

```python
from unified_query_maker import PostgreSQLTranslator

sql = PostgreSQLTranslator().translate({
  "select": ["id", "name"],
  "from": "public.users",
  "where": {"must": [
    {"type": "condition", "field": "age", "operator": "gte", "value": 18}
  ]},
  "limit": 10,
})
print(sql)
```

Notes:

* This library outputs SQL text. (If you need parameterized queries, add a `(sql, params)` API in a future release.)
* Identifier segments are validated before translation.

### Elasticsearch (Query DSL)

```python
from unified_query_maker import ElasticsearchTranslator

body = ElasticsearchTranslator().translate({
  "from": "users",
  "where": {"must": [
    {"type": "condition", "field": "status", "operator": "eq", "value": "active"},
    {"type": "condition", "field": "name", "operator": "icontains", "value": "anna"},
  ]},
  "limit": 25,
  "offset": 0,
})
print(body)
```

### MongoDB

```python
from unified_query_maker import MongoDBTranslator

doc = MongoDBTranslator().translate({
  "from": "users",
  "where": {"must": [
    {"type": "condition", "field": "status", "operator": "eq", "value": "active"}
  ]},
  "limit": 50,
})
print(doc)
```

---

## Validation

Validation is built-in. UQL is parsed via Pydantic models; invalid queries fail early.

If you want explicit validation:

```python
from unified_query_maker.models import UQLQuery

UQLQuery.model_validate({
  "from": "users",
  "where": {"must": [
    {"type": "condition", "field": "age", "operator": "between", "value": [18, 30]}
  ]}
})
```

---

## Public API

Typical imports:

```python
from unified_query_maker import (
    Where,

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

## Versioning & breaking changes

This library is strict by design. When the schema changes, it may be a breaking change.

If you upgrade across versions, review the changelog and update your UQL accordingly.

---

## License

MIT

