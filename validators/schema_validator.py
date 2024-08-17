import jsonschema

UQL_SCHEMA = {
    "type": "object",
    "properties": {
        "select": {
            "type": "array",
            "items": { "type": "string" }
        },
        "from": {
            "type": "string"
        },
        "where": {
            "type": "object",
            "properties": {
                "must": {
                    "type": "array",
                    "items": { "type": "object" }
                },
                "must_not": {
                    "type": "array",
                    "items": { "type": "object" }
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
    "additionalProperties": False
}


def validate_uql_schema(uql):
    try:
        jsonschema.validate(instance=uql, schema=UQL_SCHEMA)
        return True
    except jsonschema.exceptions.ValidationError as ve:
        print(f"Schema validation error: {ve}")
        return False
