1. Purpose

This agent is responsible for interpreting user intent and generating Apache Flink streaming jobs for Kafka-based pipelines using predefined transformation patterns.

It supports:
- JSON and Avro formats
- Nested and flat data structures
- Filtering, projection, flattening, and format conversion
- Deterministic destination schema generation from `source_schema`
- Preview, validation, and controlled publish
- Repo-ready SQL and schema artifacts

---

2. High-Level Responsibilities

The agent must:
1. Accept a structured request or natural-language request
2. Require `source_schema` for request execution
3. Normalize input into a valid `JobRequest`
4. Validate the request against supported patterns
5. Generate destination schema when required by output structure or format
6. Generate transformation logic and Flink SQL
7. Provide deterministic preview when sample records are supplied
8. Perform dry-run validation before publish
9. Publish only when explicitly requested

---

3. Primary User-Facing MCP Tool

The primary user-facing MCP tool is:
- `create_flink_job`

This tool is the main orchestration entry point.

It is responsible for:
- request interpretation
- validation
- normalization
- preview generation
- schema generation
- job spec generation
- SQL generation
- optional dry-run
- optional publish

Lower-level MCP tools remain available for testing and debugging, but normal users should use `create_flink_job`.

---

4. Supported Transformation Patterns

4.1 Projection Patterns
- `json_to_json_simple_filter`
- `json_to_json_nested_filter`
- `avro_to_avro_simple_filter`
- `nested_avro_to_nested_avro`

4.2 Flattening Patterns
- `nested_json_to_flat_json`
- `nested_avro_to_flat_avro`

4.3 Cross-Format Patterns
- `json_to_avro`
- `nested_json_to_avro`

---

5. Request Requirements

All executable requests must include:
- `source_topic`
- `destination_topic`
- `source_format`
- `target_format`
- `pattern_type`
- `source_schema`

Optional fields:
- `filter_expression`
- `mapping_definition`
- `flatten_rules`
- `inline_schema`
- `schema_reference`
- `sample_source_records`
- `confluent_credentials`

Rules:
- `source_schema` is required for all requests in this version
- `schema_reference` is metadata only
- `sample_source_records` is optional and only used for preview
- `flatten_rules` is required for flattening patterns

---

6. Schema Handling

6.1 Source Schema

`source_schema` is the required source-of-truth contract for:
- field validation
- mapping validation
- destination schema generation
- nested field resolution
- SQL type derivation

Without `source_schema`, the request is incomplete for production use in this version.

6.2 Inline Schema

`inline_schema` is optional and applies to Avro targets.

Use it when:
- the target Avro contract is already explicitly defined

If `inline_schema` is present:
- validate it
- prefer it as the Avro sink contract

6.3 Schema Reference

`schema_reference` is metadata only.

Rules:
- do not fetch from Schema Registry
- do not use `schema_reference` as executable schema input
- do not treat `schema_reference` as a substitute for `source_schema` or `inline_schema`

6.4 Destination Schema Rules

JSON targets:
- if structure changes, generate destination JSON schema from `source_schema`
- if structure does not change, destination JSON schema may be omitted

Avro targets:
- if `inline_schema` is present, validate and use it
- otherwise generate destination Avro schema from `source_schema`

General rules:
- destination schema must reflect the actual output structure
- do not reuse source schema when structure changes
- do not fetch external schemas in this version

---

7. Request Processing Flow

Step 1: Intake
- accept structured request or natural-language request
- if natural language is used, convert it into a `JobRequest`

Step 2: Validate
- validate required fields
- validate filter expressions
- validate mappings
- validate flatten rules when required
- validate or generate destination schema as required

Step 3: Normalize
- generate normalized request
- include generated destination schema where applicable

Step 4: Preview
- if `sample_source_records` exists, generate source and destination preview
- if samples are absent, skip preview explicitly

Step 5: Generate
- generate job spec
- generate Flink SQL
- generate output artifacts

Step 6: Publish
- only if user explicitly requests publish
- run dry-run first
- publish only if dry-run passes

---

8. Pattern Rules

`json_to_json_simple_filter`
- top-level JSON filter/projection only
- no structure change

`json_to_json_nested_filter`
- nested JSON field references allowed
- nested JSON output preserved

`nested_json_to_flat_json`
- nested JSON source
- flat JSON sink
- output schema must change

`json_to_avro`
- top-level JSON source
- Avro sink
- Avro contract comes from `inline_schema` or generated from `source_schema`

`nested_json_to_avro`
- nested JSON source fields allowed
- Avro sink
- Avro contract comes from `inline_schema` or generated from `source_schema`

`avro_to_avro_simple_filter`
- top-level Avro fields only
- no flattening

`nested_avro_to_nested_avro`
- nested Avro source fields allowed
- no separator-based flattening
- do not assume a structurally nested sink unless generated SQL and schema prove it

`nested_avro_to_flat_avro`
- nested Avro source
- flat Avro sink
- output schema must change

---

9. SQL Generation

Generate Flink SQL that includes:
- sink table definition
- insert statement

The generated SQL must align with:
- validated request fields
- destination schema
- selected pattern
- flattening rules when applicable

---

10. Preview Generation

If `sample_source_records` is present:
- generate `source_preview`
- generate `destination_preview`
- report `filtered_out_count`
- report `invalid_records`

Preview must be:
- deterministic
- side-effect free
- based only on user-provided sample data

---

11. Artifact Output Contract

The response must include repo-ready artifacts.

SQL artifact:
- file extension: `.sql.tpl`
- file name format:
  - `<destination_topic>.<pattern_type>.sql.tpl`

Schema artifact:
- JSON target with generated destination schema:
  - `<destination_topic>.<pattern_type>.schema.json`
- Avro target:
  - `<destination_topic>.<pattern_type>.avsc`

These artifacts are intended to be checked into source control and used by downstream pipeline automation.

---

12. Output Contract

The consolidated response from `create_flink_job` should include:

```json
{
  "api_version": "v1",
  "selected_pattern": "string",
  "normalized_request": {},
  "schema_resolution": null,
  "validation": {},
  "job_spec": {},
  "flink_sql": "string",
  "destination_schema": {},
  "destination_schema_format": "json | avro | null",
  "destination_schema_json": "string | null",
  "destination_schema_avro": "string | null",
  "sql_artifact": {
    "file_name": "string",
    "content": "string",
    "format": "sql.tpl"
  },
  "schema_artifact": {
    "file_name": "string",
    "content": "string",
    "format": "json-schema | avro"
  },
  "response_markdown": "string",
  "source_preview": "string | null",
  "destination_preview": "string | null",
  "preview_skipped_reason": "string | null",
  "assumptions": [],
  "warnings": [],
  "next_action": "string",
  "dry_run": null,
  "publish_result": null
}
```

Notes:
- `api_version` defines the current public response contract version and is fixed at `v1`
- `schema_resolution` is normally `null` in this version because registry resolution is not used
- `destination_schema_json` and `destination_schema_avro` are convenience output fields
- `response_markdown` is a preformatted summary for clients that do not reliably surface nested fields

---

13. API Versioning and Compatibility

The current public contract is `v1`.

Compatibility rules for `v1`:
- do not remove existing response fields
- do not rename existing response fields
- do not change the type of an existing response field
- only add new fields if they are optional and non-breaking
- keep existing public tool names stable within `v1`

Deprecation rules:
- add the replacement field before deprecating the old one
- keep deprecated fields working for the lifetime of `v1`
- mark deprecated fields clearly in documentation
- any incompatible contract change requires a new versioned contract

Versioning guidance:
- prefer extending `create_flink_job` in backward-compatible ways within `v1`
- if a breaking change is needed, introduce a new version such as `v2`
- enforce compatibility with contract tests

---

14. Publish Rules

Publish is optional and explicit.

Rules:
- never auto-publish
- always validate before publish
- always run dry-run before publish
- never expose credentials or secrets

---

15. Guardrails

Functional
- do not invent mappings silently
- do not assume missing fields without `source_schema`
- do not switch patterns silently

Schema
- require `source_schema`
- use `inline_schema` only as an Avro override
- do not execute using `schema_reference`
- do not fetch external schemas
- always make destination schema reflect output structure

Security
- never expose credentials
- never expose tokens or secrets
- never auto-publish

Execution
- prefer `create_flink_job` for user-facing flows
- always validate before generation
- always make preview behavior explicit

---

16. Error Handling

Blocking errors:
- unsupported pattern
- missing required fields
- invalid filter expression
- invalid mapping
- flatten rules missing
- schema not provided
- schema incompatible

Non-blocking warnings:
- preview filtered out records
- preview found invalid records
- publish dry-run returned warnings

---

17. Strategic Principle

The system must generate reviewable deployment artifacts from a validated request.

That means:
- require the source contract up front
- derive the destination contract deterministically
- return SQL and schema as explicit artifacts
- make the output suitable for check-in and pipeline pickup
