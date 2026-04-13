---
name: json-to-avro
description: Use this skill when the user wants the `json_to_avro` pattern only: flat JSON source to Avro target with filtering or projection. Trigger for requests like "convert JSON to Avro", "write filtered JSON records to an Avro topic", or "use source schema to derive an Avro contract".
---

# JSON to Avro Skill

## Purpose
Create a Flink job for the `json_to_avro` pattern only. The job reads JSON, applies optional filtering and projection, and writes Avro output.

## When to Use
Use this skill when:
- source format is JSON
- target format is Avro
- flattening is not required

Do not use this skill when:
- source fields are nested and need nested-path handling
- output should remain JSON

## Schema Rules
- `source_schema` is required for this pattern
- `inline_schema` is preferred when the Avro contract is already defined
- otherwise use `source_schema` so the destination Avro schema can be generated deterministically
- `schema_reference` is metadata only in this repo

## Processing Rules
- Preserve top-level field meaning and types
- No registry fetch
- Destination Avro schema must exist before SQL generation

## Expected Inputs
- source topic
- target topic
- `source_schema`
- optional `inline_schema`
- mapping definition
- optional filter conditions

## Workflow
1. Build or confirm a `JobRequest` with `pattern_type="json_to_avro"`
2. Require `source_schema`
3. If an explicit Avro contract already exists, include `inline_schema`
4. Call validation, normalization, preview if applicable, spec generation, and SQL generation

## Output Expectations
- Avro output written to the destination topic
- top-level field types preserved or reflected in the destination Avro schema
- deterministic Avro contract established before SQL generation

## Guardrails
- Do not fetch from Schema Registry
- Do not treat `schema_reference` as executable input
- Do not proceed without an Avro contract

## Template Usage
Use:
- [json_to_avro.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/json_to_avro.sql.j2)

## Example
Input:
```json
{
  "order_id": "O-1001",
  "amount": 200.5,
  "status": "CREATED"
}
```

Filter:
```text
amount > 100
```

Output:
```json
{
  "order_id": "O-1001",
  "amount": 200.5,
  "status": "CREATED"
}
```
