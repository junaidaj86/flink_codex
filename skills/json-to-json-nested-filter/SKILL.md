---
name: json-to-json-nested-filter
description: Use this skill when the user wants the `json_to_json_nested_filter` pattern only: JSON source to JSON target, filtering on nested paths while preserving nested output structure. Trigger for requests like "filter nested JSON records", "keep nested JSON structure", or "filter order events where data.totalAmount > 300".
---

# JSON to JSON Nested Filter Skill

## Purpose
Create a Flink job for the `json_to_json_nested_filter` pattern only. The job reads nested JSON data, applies filtering that may reference nested paths, and writes JSON output while preserving nested structure.

## When to Use
Use this skill when:
- source format is JSON
- target format is JSON
- nested fields are referenced in filters or projection
- output should remain nested
- no flattening is required

Do not use this skill when:
- the output must be flattened
- field types must change
- target format is Avro

## Schema Rules
- `source_schema` is required for this pattern
- nested destination structure is derived from the selected source fields
- do not flatten nested paths in this skill

## Processing Rules
- Nested dot-path filtering is allowed
- Nested projection is allowed
- Preserve output nesting
- No casting unless required to materialize nested `ROW` structure in SQL

## Expected Inputs
- source topic
- target topic
- `source_schema`
- mapping definition
- optional filter conditions

## Workflow
1. Build or confirm a `JobRequest` with:
   - `source_format="json"`
   - `target_format="json"`
   - `pattern_type="json_to_json_nested_filter"`
2. Require `source_schema`
3. Call `validate_job_request`
4. Call `generate_normalized_request`
5. If sample records exist, call:
   - `generate_transform_preview`
   - `render_source_sample_table`
   - `render_destination_sample_table`
6. Call `generate_job_spec`
7. Call `generate_flink_sql`

## Output Expectations
- nested JSON structure preserved
- nested fields remain nested in the destination contract
- deterministic SQL generation
- filtered output written to a new JSON topic

## Guardrails
- Do not flatten nested fields
- Do not rename nested fields into flat names
- Do not switch to `nested_json_to_flat_json`

## Template Usage
Use:
- [json_to_json_nested_filter.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/json_to_json_nested_filter.sql.j2)

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
  "eventId": "E-1001",
  "data": {
    "orderId": "O-1001",
    "totalAmount": 450.0,
    "currency": "EUR"
  }
}
```
