# Paya Uniquery

The **Paya Uniquery** library is designed to be a versatile query translation library that can be integrated into various programs. Its main functionality is to accept a Unified Query Language (UQL) and translate it into database-specific query formats.

## Architecture

The library is organized into the following components:
- **Parsers**: Responsible for parsing UQL queries.
- **Translators**: Contains various translators to convert UQL into specific database query formats (e.g., MySQL, Elasticsearch).
- **Validators**: Ensures the correctness of UQL queries through schema and semantic validation.

## JSON Schema

The Unified Query Language (UQL) follows this JSON schema:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Unified Query Language (UQL) Schema",
  "type": "object",
  "properties": {
    "select": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "Fields to retrieve from the data source."
    },
    "from": {
      "type": "string",
      "description": "Data source or table to query from."
    },
    "where": {
      "type": "object",
      "properties": {
        "must": {
          "type": "array",
          "items": {
            "type": "object"
          },
          "description": "Conditions that must be met for a record to be included."
        },
        "must_not": {
          "type": "array",
          "items": {
            "type": "object"
          },
          "description": "Conditions that must not be met for a record to be included."
        }
      },
      "description": "Filtering criteria for the query."
    },
    "orderBy": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "field": {
            "type": "string",
            "description": "Field to sort by."
          },
          "order": {
            "type": "string",
            "enum": ["ASC", "DESC"],
            "description": "Sort order."
          }
        },
        "required": ["field", "order"],
        "description": "Sorting criteria for the query results."
      }
    }
  },
  "required": ["select", "from"],
  "additionalProperties": false
}
```

## Example UQL Query

Here is an example of a UQL query that conforms to the schema:

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
    ]
  },
  "orderBy": [
    { "field": "age", "order": "DESC" }
  ]
}
```

## Usage

Here’s how you can use the Paya Uniquery library in your code:

```python
from paya_uni_query import QueryTranslator, MySQLTranslator, validate_uql

uql = {
  "select": ["name", "age"],
  "from": "users",
  "where": {
    "must": [
      { "age": { "gt": 30 } },
      { "status": "active" }
    ],
    "must_not": [
      { "role": "admin" }
    ]
  },
  "orderBy": [
    { "field": "age", "order": "DESC" }
  ]
}

# Validate the UQL query
if validate_uql(uql):
    # Create a translator for MySQL
    translator = MySQLTranslator()
    
    # Translate UQL to SQL
    sql_query = translator.translate(uql)
    
    print(sql_query)
else:
    print("Invalid UQL query.")
```


### **Changes Made:**
1. **Updated JSON Schema**: Removed `should` and `match` clauses to reflect the simplified schema.
2. **Example Query**: Updated example to match the simplified schema, including only `must` and `must_not` in the `where` clause.
3. **Usage Example**: Adjusted the example to show the simplified schema and usage in Python code.
