---
name: avro-to-avro-simple-filter
description: Use this skill when the user wants the `avro_to_avro_simple_filter` pattern only: top-level Avro source to top-level Avro target with filtering or projection and no structural changes. Trigger for requests like "filter Avro records", "copy Avro fields to a new topic", or "create a simple Avro-to-Avro clean job".
---

# Avro to Avro Simple Filter Skill

## Purpose
Create a Flink job for the `avro_to_avro_simple_filter` pattern only. The job reads Avro data, applies top-level filtering and projection, and writes Avro output without structural changes.

## When to Use
Use this skill when:
- source format is Avro
- target format is Avro
- only top-level fields are involved
- no flattening is required

Do not use this skill when:
- nested fields are central to the request
- flattening is required

## Schema Rules
- `source_schema` is required for this pattern
- `inline_schema` is preferred for the destination contract
- otherwise use `source_schema` so a destination Avro schema can be generated
- `schema_reference` is metadata only

## Workflow
1. Build or confirm a `JobRequest` with `pattern_type="avro_to_avro_simple_filter"`
2. Require `source_schema`
3. If an explicit Avro contract already exists, include `inline_schema`
4. Run validation, normalization, optional preview, spec generation, and SQL generation

## Expected Inputs
- source topic
- target topic
- `source_schema`
- optional `inline_schema`
- mapping definition
- optional filter conditions

## Output Expectations
- top-level Avro output
- no structural changes
- deterministic SQL generation with an Avro sink contract

## Guardrails
- Do not flatten nested fields
- Do not cast fields
- Do not fetch schemas externally

## Template Usage
Use:
- [avro_to_avro_simple_filter.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/avro_to_avro_simple_filter.sql.j2)

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
