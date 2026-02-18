# Unified Query Maker (UQM)

UQM defines a small, **typed** query DSL (**UQL**) and translators that turn it into:

- **SQL** (PostgreSQL, MySQL/MariaDB, SQL Server, Oracle)
- **Elasticsearch** Query DSL (`dict`)
- **MongoDB** filter/projection/sort (`dict`)

This README is the **public contract**. If you build apps on UQM, you should be able to rely on **this README alone** (no source reading required).

---

## Install

Pin to a tag for reproducible builds:

```bash
pip install git+https://github.com/mmrzaf/unified_query_maker.git@<TAG>
```

Or install the latest commit:

```bash
pip install git+https://github.com/mmrzaf/unified_query_maker.git
```

---

## Public API

### Imports (stable)

```python
from unified_query_maker import (
    PostgreSQLTranslator,
    MySQLTranslator,
    MSSQLTranslator,
    OracleTranslator,
    ElasticsearchTranslator,
    MongoDBTranslator,
    validate_uql_schema,
    validate_uql_semantics,
)
```

What’s exported at the top-level is exactly the list above.

### Translation methods

**SQL translators**

- `translate(uql: dict) -> str`
  Returns a SQL string (literals inlined) and ends with `;`.
- `translate_with_params(uql: dict) -> tuple[str, list]` (**recommended**)
  Returns `(sql, params)` for safe execution.

**Elasticsearch / MongoDB**

- `translate(uql: dict) -> dict`

---

## Validation API (use this at your boundary)

### `validate_uql_schema(uql: dict) -> UQLQuery | None`

- Returns a parsed model if valid; otherwise returns `None`.

### `validate_uql_semantics(uql_model: UQLQuery) -> bool`

- Returns `True/False` (never throws) by walking the filter tree and rejecting invalid boolean nodes (e.g., empty `and/or` if constructed unsafely).

---

## UQL: exact wire format (copy/paste types)

These are the **only accepted shapes**. “Legacy” untyped filters are **not** accepted; every filter node **must** include `"type"`.

```python
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict, Union

# -------------------------
# JSON value rules
# -------------------------
# UQM expects JSON-compatible values.
# - dict keys MUST be strings
# - date/datetime are accepted by Python callers and are converted to ISO strings
JsonScalar = Union[str, int, float, bool, None]
JsonValue = Union[JsonScalar, List["JsonValue"], Dict[str, "JsonValue"]]

# -------------------------
# Sorting
# -------------------------
SortOrder = Literal["ASC", "DESC"]

class OrderByItem(TypedDict, total=False):
    field: str                 # required
    order: SortOrder           # optional; defaults to "ASC"

# -------------------------
# Optional field type hint
# -------------------------
FieldType = Literal[
    "string",
    "number",
    "boolean",
    "date",
    "datetime",
    "array",
    "object",
    "unknown",
]

# -------------------------
# Operators
# -------------------------
Operator = Literal[
    # comparison
    "eq", "neq", "gt", "gte", "lt", "lte",
    # membership
    "in", "nin",
    # existence
    "exists", "nexists",
    # range / strings
    "between", "contains", "ncontains", "icontains",
    "starts_with", "ends_with", "ilike", "regex",
    # arrays
    "array_contains", "array_overlap", "array_contained",
    # geo
    "geo_within", "geo_intersects",
]

# -------------------------
# Filter AST (typed, discriminated)
# -------------------------
class Condition(TypedDict, total=False):
    type: Literal["condition"]          # required, must be "condition"
    field: str                         # required
    operator: Operator                 # required
    value: JsonValue                   # required/optional depending on operator
    field_type: FieldType              # optional hint

class AndExpression(TypedDict, total=False):
    type: Literal["and"]               # required
    expressions: List["WhereExpr"]     # required, must be non-empty

class OrExpression(TypedDict, total=False):
    type: Literal["or"]                # required
    expressions: List["WhereExpr"]     # required, must be non-empty

class NotExpression(TypedDict, total=False):
    type: Literal["not"]               # required
    expression: "WhereExpr"            # required

WhereExpr = Union[Condition, AndExpression, OrExpression, NotExpression]

class WhereClause(TypedDict, total=False):
    must: List[WhereExpr]              # optional (but must be a list if present)
    must_not: List[WhereExpr]          # optional (but must be a list if present)

# -------------------------
# UQL Query
# -------------------------

SelectItem = str  # identifier OR trailing-star identifier (e.g. "schema.table.*") OR "*"

# "where" can be:
#   - a WhereClause
#   - OR a single typed WhereExpr (auto-wrapped into {"must":[...]} by the model)
WhereInput = Union[WhereClause, WhereExpr]

class UQLQueryDict(TypedDict, total=False):
    select: List[SelectItem]           # optional; if omitted => translators treat as "*"
    from: str                          # REQUIRED
    where: WhereInput                  # optional
    orderBy: List[OrderByItem]         # optional
    limit: int                         # optional, >= 0
    offset: int                        # optional, >= 0
```

Key behavior that apps must rely on:

- `"from"` is required for **all** translators (even ES/Mongo).
- `orderBy[].order` defaults to `"ASC"` if omitted.
- `where` may be a single typed node (example below) and will be treated as `{"must": [node]}`.
- Filter nodes **must** include `"type"` (missing discriminator is rejected).

---

## Identifier rules (table/column names)

Identifiers are validated as dotted names:

- Segments separated by `.`
- Each segment: `[A-Za-z_][A-Za-z0-9_]*`
- No empty segments, no hyphens, no leading digits

Allowed:

- `from`: `table`, `schema.table`, `db.schema.table` (depends on your DB naming; still validated with the rule above)
- `select` item: same as above for columns, plus **trailing star**: `schema.table.*`
- `select` may also be exactly `"*"` (but then must be the only select item)

This is enforced by schema validation.

---

## `select` rules (important)

- If `select` is omitted or `select == ["*"]`, SQL translators emit `SELECT *`.
- `select` **cannot** be an empty list (`[]`).
- `"*"` cannot be combined with explicit fields (invalid).
- `select` supports **only** trailing-star segments like `"public.users.*"` as a field item.

---

## Operator semantics (value rules)

These rules are validated by the typed model.

### Comparison

- `eq`, `neq`, `gt`, `gte`, `lt`, `lte`: `value` is any JSON value
- Special case in SQL: `eq None` → `IS NULL`, `neq None` → `IS NOT NULL`.

### Existence (unary)

- `exists`, `nexists`: `value` is ignored and will be forced to `null` internally.

### Membership

- `in`, `nin`: `value` must be a list (tuples are accepted in Python and normalized to lists).

### Range

- `between`: `value` must be a 2-item list `[min, max]`.

### Strings (value must be a string)

- `contains`, `ncontains`, `icontains`, `starts_with`, `ends_with`, `ilike`, `regex`

### Arrays

- `array_contains`: value must be a scalar JSON value (not list/dict).
- `array_overlap`, `array_contained`: value must be a list.

### Geo (value must be a dict/object)

- `geo_within`, `geo_intersects`: value must be an object/dict.

### JSON portability rule

- Dict keys inside `value` must be **strings** (non-string keys are rejected).
- Python `date` / `datetime` values are converted to ISO strings before validation.

---

## Translator support matrix (what works where)

If an operator isn’t supported by a translator, translation raises `ValueError`.

### SQL translators (PostgreSQL / MySQL / MSSQL / Oracle)

Supported in all SQL dialects:

- comparisons, `between`, `in/nin`, `exists/nexists`
- string ops via `LIKE`-style translation (including escaping)

Dialect differences:

- **Geo ops are not supported in SQL translators** (always raises).
- **Regex**
  - PostgreSQL: `~`
  - MySQL: `REGEXP`
  - Oracle: `REGEXP_LIKE(...)`
  - MSSQL: **rejects regex** (raises)

- **Arrays**
  - PostgreSQL supports `array_contains`, `array_overlap`, `array_contained`
  - Other SQL dialects: `array_contained` is explicitly unsupported (raises).

Pagination differences:

- MySQL does not support `OFFSET` without `LIMIT`; UQM emits a huge `LIMIT` when only `offset` is provided.
- MSSQL requires `ORDER BY` for `OFFSET/FETCH`; UQM injects `ORDER BY (SELECT NULL)` when paginating without an order.
- Oracle uses `FETCH FIRST` and/or `OFFSET ... ROWS FETCH NEXT ...` forms.

### Elasticsearch translator

- If no `where` (or both lists empty), it emits `{ "query": { "match_all": {} } }`.
- Array ops:
  - `array_contained` uses a Painless `script` clause.

- Geo ops:
  - `geo_within` / `geo_intersects` map to `geo_shape`.

- `ilike` converts SQL-LIKE patterns (`%`/`_`, with backslash escaping) to ES `wildcard` patterns.
- `contains` escapes ES wildcard meta chars so user input is treated literally.

### MongoDB translator

Outputs a dict shaped like:

```python
{
  "filter": <mongo filter dict>,
  "projection": {field: 1, ...},  # only if select provided and not ["*"]
  "sort": [(field, 1|-1), ...],   # only if orderBy provided
  "limit": <int>,                 # optional
  "skip": <int>,                  # optional
}
```

Geo ops:

- `geo_within` → `$geoWithin: { $geometry: ... }`
- `geo_intersects` → `$geoIntersects: { $geometry: ... }`

---

## Parameterized SQL placeholders

Use `translate_with_params()` for any untrusted input.

Placeholder style:

- PostgreSQL / MySQL: `%s` (DB-API style)
- MSSQL: `?`
- Oracle: `:1, :2, ...`

---

## Examples

### 1) Minimal valid query (all backends)

```python
uql = {"from": "public.users"}
```

### 2) WhereClause with `must` + `must_not`

```python
uql = {
    "select": ["id", "name"],
    "from": "public.users",
    "where": {
        "must": [
            {"type": "condition", "field": "age", "operator": "gte", "value": 18},
            {"type": "condition", "field": "status", "operator": "eq", "value": "active"},
        ],
        "must_not": [
            {"type": "condition", "field": "email", "operator": "icontains", "value": "test"},
        ],
    },
    "orderBy": [{"field": "name"}],  # order defaults to "ASC"
    "limit": 50,
    "offset": 0,
}
```

### 3) `where` as a _single_ typed node (auto-wrapped into `must`)

```python
uql = {
    "select": ["id"],
    "from": "t",
    "where": {"type": "condition", "field": "a", "operator": "gt", "value": 1},
}
```

This is guaranteed behavior.

### 4) Nested boolean filters (and/or/not)

```python
uql = {
    "from": "t",
    "where": {
        "must": [
            {
                "type": "and",
                "expressions": [
                    {"type": "condition", "field": "age", "operator": "gte", "value": 18},
                    {
                        "type": "or",
                        "expressions": [
                            {"type": "condition", "field": "tier", "operator": "eq", "value": "pro"},
                            {"type": "condition", "field": "tier", "operator": "eq", "value": "team"},
                        ],
                    },
                ],
            }
        ],
        "must_not": [{"type": "condition", "field": "banned", "operator": "eq", "value": True}],
    },
}
```

### 5) SQL safe execution (PostgreSQL)

```python
from unified_query_maker import PostgreSQLTranslator

sql, params = PostgreSQLTranslator().translate_with_params(uql)
cursor.execute(sql, params)
```

### 6) Elasticsearch

```python
from unified_query_maker import ElasticsearchTranslator

es_query = ElasticsearchTranslator().translate(uql)
```

If you omit `where`, it becomes `match_all`.

### 7) MongoDB

```python
from unified_query_maker import MongoDBTranslator

mongo = MongoDBTranslator().translate(uql)
# mongo["filter"], mongo["projection"], mongo["sort"], mongo["limit"], mongo["skip"]
```

---

## Optional: Python builder (`Where`) for typed filters

If you don’t want to hand-write dict AST nodes, build typed nodes in Python.

Import from models:

```python
from unified_query_maker.models import Where, FieldType
```

Usage:

```python
uql = {
    "select": ["id"],
    "from": "public.users",
    "where": {
        "must": [
            Where.field("age", field_type=FieldType.NUMBER).gte(18),
            Where.field("status").eq("active"),
        ],
        "must_not": [Where.field("banned").eq(True)],
    },
}
```

The builder also supports boolean composition:

- `a & b` → `and`
- `a | b` → `or`
- `~a` → `not`

---

## Error behavior (what you should map to HTTP 400)

- Invalid UQL structure/types/identifiers → validation fails (`validate_uql_schema` returns `None`)
- Translating an invalid query dict → translator raises `ValueError("Invalid UQL query: ...")`
- Unsupported operator for a backend → translator raises `ValueError`

---

## Production guardrails (you should enforce these)

If your app accepts user-provided UQL:

1. Use `translate_with_params()` for SQL.
2. Cap complexity:
   - max total filter nodes
   - max nesting depth
   - max list lengths (`in/nin/array_*`)
   - max string length (especially `regex` / `ilike`)

3. Consider gating expensive features:
   - `regex` (Mongo / ES)
   - ES `array_contained` (script)

---
