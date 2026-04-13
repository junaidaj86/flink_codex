---
name: nested-avro-to-flat-avro
description: Use this skill when the user wants the `nested_avro_to_flat_avro` pattern only: nested Avro source flattened into flat Avro output. Trigger for requests like "flatten Avro fields", "write nested Avro as flat Avro", or "convert nested Avro records into top-level columns".
---

# Nested Avro to Flat Avro Skill

## Purpose
Create a Flink job for the `nested_avro_to_flat_avro` pattern only. The job reads nested Avro input, flattens selected fields into top-level Avro output columns, and optionally filters records.

## When to Use
Use this skill when:
- source format is Avro
- target format is Avro
- flattening is required

Do not use this skill when:
- output should remain nested
- target format is JSON

## Schema Rules
- `flatten_rules` is required
- `source_schema` is required for this pattern
- `inline_schema` is preferred for the destination Avro contract
- otherwise use `source_schema` so the flattened destination Avro schema can be generated

## Workflow
1. Build or confirm a `JobRequest` with `pattern_type="nested_avro_to_flat_avro"`
2. Require `flatten_rules`
3. Require `source_schema`
4. If a specific Avro sink contract already exists, include `inline_schema`
5. Run validation, normalization, optional preview, spec generation, and SQL generation

## Expected Inputs
- source topic
- target topic
- `source_schema`
- optional `inline_schema`
- mapping definition
- `flatten_rules`
- optional filter conditions

## Output Expectations
- flat Avro output
- destination field names derived from flattened paths
- deterministic destination Avro schema
- deterministic SQL generation

## Guardrails
- Do not preserve nested output structure
- Do not omit `flatten_rules`
- Do not reuse the original nested schema as the sink schema

## Template Usage
Use:
- [nested_avro_to_flat_avro.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/nested_avro_to_flat_avro.sql.j2)

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
