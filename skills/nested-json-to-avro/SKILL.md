---
name: nested-json-to-avro
description: Use this skill when the user wants the `nested_json_to_avro` pattern only: nested JSON source to Avro target, with or without filters, and with deterministic output schema derived from inline schema or source schema. Trigger for requests like "convert nested JSON to Avro", "filter order events into Avro", "write nested JSON fields to an Avro topic", or "project nested JSON into Avro without filtering".
---

# Nested JSON to Avro Skill

## Purpose
Create a Flink job for the `nested_json_to_avro` pattern only. The job reads nested JSON, applies optional filtering and projection, and writes Avro output for the transformed structure.

## When to Use
Use this skill when:
- source format is JSON
- target format is Avro
- nested fields are referenced
- filter conditions may or may not be present

Do not use this skill when:
- output should remain JSON
- flattening to flat JSON is desired

## Schema Rules
- `source_schema` is required for this pattern
- `inline_schema` is preferred when the target Avro contract already exists
- otherwise `source_schema` is used so destination Avro schema can be generated deterministically
- `schema_reference` is metadata only

## Processing Rules
- Nested fields may be mapped into Avro output
- Destination schema must match the transformed output structure
- No registry fetch
- Filter conditions are optional
- If no filter exists, generate projection-only SQL
- Do not assume the sink contract remains structurally nested unless the generated SQL and Avro schema prove it

## Workflow
1. Build or confirm a `JobRequest` with `pattern_type="nested_json_to_avro"`
2. Require `source_schema`
3. If an explicit Avro contract already exists, include `inline_schema`
4. Run validation, normalization, optional preview, job spec generation, and SQL generation

## Expected Inputs
- source topic
- target topic
- `source_schema`
- optional `inline_schema`
- mapping definition
- optional filter conditions

## Output Expectations
- Avro output aligned to the transformed nested JSON field selection
- deterministic destination Avro schema
- filtered output written to a new Avro topic
- if no filter exists, projection-only Avro output written to a new topic

## Guardrails
- Do not reuse the original nested JSON schema as an Avro schema without transformation
- Do not continue without a valid Avro contract

## Template Usage
Use:
- [nested_json_to_avro.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/nested_json_to_avro.sql.j2)

## Example
Input:
```json
{
  "eventId": "E-1001",
  "data": {
    "orderId": "O-1001",
    "totalAmount": 450.0,
    "currency": "EUR"
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
  "order_id": "O-1001",
  "total_amount": 450.0,
  "currency": "EUR"
}
```

Example without filter:

Input:
```json
{
  "eventId": "E-1001",
  "data": {
    "orderId": "O-1001",
    "currency": "EUR"
  }
}
```

Filter:
```text
No filter
```

Output:
```json
{
  "order_id": "O-1001",
  "currency": "EUR"
}
```
