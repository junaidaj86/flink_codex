"""FastAPI and FastMCP entry point."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from fastmcp import FastMCP

from flink_codex.catalog import catalog
from flink_codex.interpreter import interpret_customer_request as interpret_customer_request_impl
from flink_codex.models import (
    ConfluentCloudCredentials,
    FilterExpression,
    JobRequest,
    JobSpec,
    MappingDefinition,
    NormalizedJobRequest,
    PatternDefinition,
    SchemaResolutionResult,
    TransformPreview,
    ValidationResult,
)
from flink_codex.preview import generate_transform_preview
from flink_codex.publisher import dry_run_publish as dry_run_publish_impl
from flink_codex.publisher import publish_job as publish_job_impl
from flink_codex.schema_generator import generate_avro_schema, generate_json_schema
from flink_codex.sql_generator import (
    generate_flink_sql as generate_flink_sql_impl,
    generate_job_spec as generate_job_spec_impl,
    render_destination_sample_table as render_destination_sample_table_impl,
    render_source_sample_table as render_source_sample_table_impl,
)
from flink_codex.validation import (
    validate_avro_schema as validate_avro_schema_impl,
    validate_filter_expression as validate_filter_expression_impl,
    validate_job_request as validate_job_request_impl,
    validate_schema_mapping as validate_schema_mapping_impl,
)

app = FastAPI(title="Flink Codex", version="0.1.0")
mcp = FastMCP("flink-clean-job-creator")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Health endpoint."""
    return {"status": "ok"}


@mcp.tool(name="list_supported_patterns", description="List all supported Flink clean job patterns.")
async def list_supported_patterns() -> list[PatternDefinition]:
    """Return all supported patterns."""
    return list(catalog.patterns)


@mcp.tool(name="describe_pattern", description="Describe a single supported pattern by id.")
async def describe_pattern(pattern_type: str) -> PatternDefinition | ValidationResult:
    """Describe a pattern or return an unsupported pattern error."""
    pattern = catalog.by_id(pattern_type)
    if pattern is None:
        return ValidationResult(valid=False, error_code="UNSUPPORTED_PATTERN", error_message=f"Unsupported pattern: {pattern_type}")
    return pattern


@mcp.tool(name="list_required_fields", description="List required, optional, and conditional fields for a pattern.")
async def list_required_fields(pattern_type: str) -> dict[str, list[str]]:
    """Return required field metadata for a pattern."""
    return catalog.required_fields(pattern_type)


@mcp.tool(name="interpret_customer_request", description="Convert a natural-language customer request into a deterministic JobRequest.")
async def interpret_customer_request(user_query: str, source_schema: dict | None = None) -> JobRequest | ValidationResult:
    """Interpret a natural-language request into a structured job request."""
    return interpret_customer_request_impl(user_query, source_schema)


@mcp.tool(name="validate_job_request", description="Validate a Flink clean job request.")
async def validate_job_request(request: JobRequest) -> ValidationResult:
    """Validate a job request."""
    return await validate_job_request_impl(request)


@mcp.tool(name="validate_filter_expression", description="Validate a filter expression against an optional source schema.")
async def validate_filter_expression(expression: FilterExpression, source_schema: dict | None = None) -> ValidationResult:
    """Validate filter syntax and field references."""
    return await validate_filter_expression_impl(expression, source_schema)


@mcp.tool(name="validate_schema_mapping", description="Validate schema mapping references against a source schema.")
async def validate_schema_mapping(
    mapping: MappingDefinition,
    source_schema: dict,
    target_schema: dict | None = None,
) -> ValidationResult:
    """Validate a mapping definition."""
    return await validate_schema_mapping_impl(mapping, source_schema, target_schema)


@mcp.tool(name="validate_avro_schema", description="Validate an Avro schema string.")
async def validate_avro_schema(schema_string: str) -> ValidationResult:
    """Validate an Avro schema."""
    return await validate_avro_schema_impl(schema_string)


@mcp.tool(name="generate_normalized_request", description="Normalize a validated Flink clean job request.")
async def generate_normalized_request(request: JobRequest) -> NormalizedJobRequest:
    """Normalize a request, applying defaults and resolving schema when needed."""
    resolved_schema = None
    generated_json_schema = generate_json_schema(request) if request.source_schema else None
    generated_avro_schema = None
    if request.target_format == "avro" and request.inline_schema is not None:
        resolved_schema = SchemaResolutionResult(
            resolved=True,
            subject_name=request.schema_reference,
            schema_string=request.inline_schema,
        )
    elif request.target_format == "avro" and request.source_schema is not None:
        generated_avro_schema = generate_avro_schema(request)
    return NormalizedJobRequest(
        **request.model_dump(),
        resolved_schema=resolved_schema,
        generated_json_schema=generated_json_schema,
        generated_avro_schema=generated_avro_schema,
        normalized_at=datetime.now(timezone.utc),
    )


@mcp.tool(name="generate_transform_preview", description="Generate a deterministic transform preview from user records.")
async def generate_transform_preview_tool(request: JobRequest) -> TransformPreview | ValidationResult:
    """Generate a preview or return a sample-records validation error."""
    return await generate_transform_preview(request)


@mcp.tool(name="render_source_sample_table", description="Render the source sample table as markdown.")
async def render_source_sample_table(preview: TransformPreview) -> str:
    """Render the source preview table."""
    return await render_source_sample_table_impl(preview)


@mcp.tool(name="render_destination_sample_table", description="Render the destination sample table as markdown.")
async def render_destination_sample_table(preview: TransformPreview) -> str:
    """Render the destination preview table."""
    return await render_destination_sample_table_impl(preview)


@mcp.tool(name="generate_job_spec", description="Generate a job specification from a normalized request.")
async def generate_job_spec(request: NormalizedJobRequest) -> JobSpec:
    """Generate a job spec."""
    return await generate_job_spec_impl(request)


@mcp.tool(name="generate_flink_sql", description="Generate Flink SQL from a job spec.")
async def generate_flink_sql(spec: JobSpec) -> str:
    """Return the Flink SQL contained by a job spec."""
    return await generate_flink_sql_impl(spec)


@mcp.tool(name="dry_run_publish", description="Run publish precondition checks without side effects.")
async def dry_run_publish(spec: JobSpec, credentials: ConfluentCloudCredentials):
    """Perform a dry run publish."""
    return await dry_run_publish_impl(spec, credentials)


@mcp.tool(name="publish_job", description="Publish a Flink SQL job to Confluent Cloud.")
async def publish_job(spec: JobSpec, credentials: ConfluentCloudCredentials):
    """Publish a job after validation preconditions pass."""
    return await publish_job_impl(spec, credentials)

if __name__ == "__main__":
    #mcp.run(host="0.0.0.0", port=8000, transport="http")
    mcp.run()
