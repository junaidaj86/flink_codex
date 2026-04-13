---
name: nested-json-to-flat-json
description: Use this skill when the user wants the `nested_json_to_flat_json` pattern only: nested JSON source, flat JSON target, flattening nested paths into top-level output fields. Trigger for requests like "flatten nested JSON", "write nested JSON as flat JSON", or "turn data.orderId into order_id".
---

# Nested JSON to Flat JSON Skill

## Purpose
Create a Flink job for the `nested_json_to_flat_json` pattern only. The job reads nested JSON, flattens selected nested paths into top-level JSON output fields, and optionally filters records.

## When to Use
Use this skill when:
- source format is JSON
- target format is JSON
- nested fields must become top-level fields
- flattening is required

Do not use this skill when:
- output must remain nested
- target format is Avro

## Schema Rules
- `source_schema` is required to deterministically generate the new destination JSON schema
- `flatten_rules` must be present
- destination schema must differ from source structure

## Processing Rules
- Flatten nested fields using the configured separator
- Preserve field types
- Use top-level destination field names after flattening

## Expected Inputs
- source topic
- target topic
- `source_schema`
- mapping definition
- `flatten_rules`
- optional filter conditions

## Workflow
1. Build or confirm a `JobRequest` with:
   - `source_format="json"`
   - `target_format="json"`
   - `pattern_type="nested_json_to_flat_json"`
2. Require `source_schema`
3. Call `validate_job_request`
4. Call `generate_normalized_request`
5. If sample records exist, call preview tools
6. Call `generate_job_spec`
7. Call `generate_flink_sql`

## Output Expectations
- flat JSON output
- destination field names derived from nested paths and `flatten_rules`
- destination schema differs from source structure
- deterministic SQL generation

## Guardrails
- Do not preserve nested structure in output
- Do not omit `flatten_rules`
- Do not reuse the source schema as the destination schema

## Template Usage
Use:
- [nested_json_to_flat_json.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/nested_json_to_flat_json.sql.j2)

## Example
Input:
```json
{
  "eventId": "E-1001",
  "data": {
    "orderId": "O-1001",
    "totalAmount": 450.0
  }
}
```

Filter:
```text
data.totalAmount > 300
```

Output:
```json
{
  "eventId": "E-1001",
  "data_orderId": "O-1001",
  "data_totalAmount": 450.0
}
```
