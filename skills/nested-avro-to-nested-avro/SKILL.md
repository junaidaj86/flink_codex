---
name: nested-avro-to-nested-avro
description: Use this skill when the user wants the `nested_avro_to_nested_avro` pattern only: nested Avro source with nested-field selection into an Avro target, without separator-based flattening. Trigger for requests like "filter nested Avro events", "select nested Avro fields", or "transform nested Avro without flattening field names".
---

# Nested Avro to Nested Avro Skill

## Purpose
Create a Flink job for the `nested_avro_to_nested_avro` pattern only. The job reads nested Avro data, applies filtering and projection on nested source fields, and writes Avro output without separator-based flattening.

## When to Use
Use this skill when:
- source format is Avro
- target format is Avro
- nested fields are part of filtering or projection
- the request is about nested source-field handling rather than flattening

Do not use this skill when:
- output must be flattened
- target format is JSON

## Schema Rules
- `source_schema` is required for this pattern
- `inline_schema` is preferred
- otherwise use `source_schema` so a destination Avro schema can be generated
- if a truly nested destination Avro sink contract is required, verify the generated SQL and Avro schema explicitly before using this pattern

## Workflow
1. Build or confirm a `JobRequest` with `pattern_type="nested_avro_to_nested_avro"`
2. Require `source_schema`
3. If a specific Avro sink contract already exists, include `inline_schema`
4. Run the normal MCP validation -> normalization -> preview -> spec -> SQL flow

## Expected Inputs
- source topic
- target topic
- `source_schema`
- optional `inline_schema`
- mapping definition
- optional filter conditions

## Output Expectations
- Avro output generated from nested source-field selection
- destination schema aligned to selected fields
- deterministic SQL generation

## Guardrails
- Do not use separator-based flattening
- Do not switch to `nested_avro_to_flat_avro`
- Do not assume the sink contract is structurally nested unless the generated SQL and Avro schema prove it

## Template Usage
Use:
- [nested_avro_to_nested_avro.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/nested_avro_to_nested_avro.sql.j2)

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
  "totalAmount": 450.0
}
```
