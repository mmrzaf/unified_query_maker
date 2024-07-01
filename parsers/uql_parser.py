import jsonschema

UQL_SCHEMA = {
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

def parse_uql(uql):
    jsonschema.validate(instance=uql, schema=UQL_SCHEMA)
    return uql
