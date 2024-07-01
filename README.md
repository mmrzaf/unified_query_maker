# paya-uni-query

The **Paya Uniquery** library is designed to be a versatile query translation library that can be integrated into various programs.
Its main functionality is to accept a unified query language (UQL) and translate it into database-specific query formats.

## Architecture

## Example
```json
{
  "type": "object",
  "properties": {
    "select": { "type": "array", "items": { "type": "string" } },
    "from": { "type": "string" },
    "where": {
      "type": "object",
      "properties": {
        "must": { "type": "array", "items": { "type": "object" } },
        "must_not": { "type": "array", "items": { "type": "object" } },
        "should": { "type": "array", "items": { "type": "object" } },
        "match": { "type": "object" }
      }
    }
  },
  "required": ["select", "from"]
}
```
```json
{
  "select": ["name", "age"],
  "from": "users",
  "where": {
    "must": [
      { "age": { "gt": 30 } },
      { "status": "active" }
    ],
    "must_not": [
      { "role": "admin" }
    ],
    "should": [
      { "location": "New York" },
      { "department": "Sales" }
    ],
    "match": {
      "description": "engineer"
    }
  }
}
```

# Usage
```python
from paya_uni_query import QueryTranslator, MySQLTranslator, validate_uql


if validate_uql(uql):
    translator = MySQLTranslator()
    sql_query = translator.translate(uql)
    print(sql_query)
```
