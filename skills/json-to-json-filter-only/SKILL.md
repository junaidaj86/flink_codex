---
name: json-to-json-filter-only
description: Use this skill when the user wants the `json_to_json_simple_filter` pattern only: JSON source to JSON target, top-level filtering and optional projection, with no field renaming, no field type changes, no flattening, and no output structure changes. Trigger for requests like "filter JSON records", "create a filtered JSON topic", or "copy selected top-level JSON fields with the same structure".
---

# JSON to JSON Filter Only Skill

## Purpose
Create a Flink job for the `json_to_json_simple_filter` pattern only. The job reads JSON data, applies top-level record filtering and optional top-level field projection, and writes the result to a new JSON topic without changing field names, field types, or structure.

## When to Use
Use this skill when:
- source format is JSON
- target format is JSON
- the requested pattern is top-level filtering only
- no field type changes are required
- no field renaming is required
- no flattening is required
- no nested hierarchy changes are required
- filter fields are top-level fields

Do not use this skill when:
- any field type must change
- structure must change
- nested content must be flattened
- filter conditions depend on nested paths like `data.totalAmount`
- nested output structure must be preserved explicitly
- output schema differs from input schema

## Schema Rules
- `source_schema` is required for this pattern
- source and target structure must be the same
- field types must remain unchanged
- if a schema artifact is supplied, it must describe the same top-level structure for source and sink
- do not switch to a structure-changing pattern

## Processing Rules
- Filtering is allowed
- Projection is allowed only for top-level fields and only if it does not change the output structure
- Field names must remain unchanged
- Nested paths should not be used in this skill
- No casting is allowed

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
   - `pattern_type="json_to_json_simple_filter"`
2. Require `source_schema`
3. Call `validate_job_request`
4. If validation passes, call `generate_normalized_request`
5. If sample records are present, call:
   - `generate_transform_preview`
   - `render_source_sample_table`
   - `render_destination_sample_table`
6. Call `generate_job_spec`
7. Call `generate_flink_sql`

## Output Expectations
- same JSON structure
- same field types
- same top-level field names preserved
- filtered output written to new topic
- deterministic SQL generation

## Guardrails
- Do not cast any field
- Do not flatten any field
- Do not rename fields
- Do not switch to `json_to_json_nested_filter` or `nested_json_to_flat_json`
- Do not infer missing fields

## Template Usage
Use the existing pattern template:
- [json_to_json_simple_filter.sql.j2](/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex/templates/json_to_json_simple_filter.sql.j2)

Template filling rules:
- sink columns must remain top-level
- where clause must be omitted if no filter exists
- projection must preserve original field types and field names

## Example
Input:
```json
{
  "orderId": "O-1001",
  "status": "CREATED",
  "amount": 200.5
}
```

Filter:
```text
status = 'CREATED'
```

Output:
```json
{
  "orderId": "O-1001",
  "status": "CREATED",
  "amount": 200.5
}
```
