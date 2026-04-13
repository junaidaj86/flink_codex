"""FastAPI and FastMCP entry point."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from fastapi import FastAPI
from fastmcp import FastMCP

from flink_codex.catalog import catalog
from flink_codex.interpreter import interpret_customer_request as interpret_customer_request_impl
from flink_codex.models import (
    ConfluentCloudCredentials,
    DryRunResult,
    FilterExpression,
    JobRequest,
    JobSpec,
    MappingDefinition,
    NormalizedJobRequest,
    PatternDefinition,
    PublishResult,
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


def _next_action_for_success(*, has_samples: bool, publish_requested: bool) -> str:
    """Return the next operational step for a successful generation flow."""
    if publish_requested:
        return "Review the generated Flink SQL and publish result."
    if not has_samples:
        return "Provide sample_source_records to generate deterministic source and destination previews."
    return "Review the generated Flink SQL and run dry_run_publish before publishing."


def _build_create_response(
    *,
    selected_pattern: str | None,
    normalized_request: NormalizedJobRequest | None,
    schema_resolution: SchemaResolutionResult | None,
    validation: ValidationResult,
    job_spec: JobSpec | None,
    flink_sql: str | None,
    source_preview: str | None,
    destination_preview: str | None,
    preview_skipped_reason: str | None,
    assumptions: list[str],
    warnings: list[str],
    next_action: str,
    dry_run: DryRunResult | None = None,
    publish_result: PublishResult | None = None,
) -> dict[str, Any]:
    """Build the consolidated create_flink_job response payload."""
    destination_schema: dict[str, Any] | str | None = None
    destination_schema_format: str | None = None
    destination_schema_json: str | None = None
    destination_schema_avro: str | None = None
    if job_spec is not None:
        if job_spec.generated_json_schema is not None:
            destination_schema = job_spec.generated_json_schema
            destination_schema_format = "json"
            destination_schema_json = json.dumps(job_spec.generated_json_schema, indent=2)
        elif job_spec.inline_schema is not None:
            destination_schema = job_spec.inline_schema
            destination_schema_format = "avro"
            destination_schema_avro = job_spec.inline_schema
        elif job_spec.generated_avro_schema is not None:
            destination_schema = job_spec.generated_avro_schema
            destination_schema_format = "avro"
            destination_schema_avro = job_spec.generated_avro_schema
    elif normalized_request is not None:
        if normalized_request.generated_json_schema is not None:
            destination_schema = normalized_request.generated_json_schema
            destination_schema_format = "json"
            destination_schema_json = json.dumps(normalized_request.generated_json_schema, indent=2)
        elif normalized_request.inline_schema is not None:
            destination_schema = normalized_request.inline_schema
            destination_schema_format = "avro"
            destination_schema_avro = normalized_request.inline_schema
        elif normalized_request.generated_avro_schema is not None:
            destination_schema = normalized_request.generated_avro_schema
            destination_schema_format = "avro"
            destination_schema_avro = normalized_request.generated_avro_schema

    return {
        "selected_pattern": selected_pattern,
        "normalized_request": normalized_request.model_dump(mode="json") if normalized_request else None,
        "schema_resolution": schema_resolution.model_dump(mode="json") if schema_resolution else None,
        "validation": validation.model_dump(mode="json"),
        "job_spec": job_spec.model_dump(mode="json") if job_spec else None,
        "flink_sql": flink_sql,
        "destination_schema": destination_schema,
        "destination_schema_format": destination_schema_format,
        "destination_schema_json": destination_schema_json,
        "destination_schema_avro": destination_schema_avro,
        "source_preview": source_preview,
        "destination_preview": destination_preview,
        "preview_skipped_reason": preview_skipped_reason,
        "assumptions": assumptions,
        "warnings": warnings,
        "next_action": next_action,
        "dry_run": dry_run.model_dump(mode="json") if dry_run else None,
        "publish_result": publish_result.model_dump(mode="json") if publish_result else None,
    }


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


@mcp.tool(name="create_flink_job", description="Create a Flink job from either a natural-language request or a structured JobRequest.")
async def create_flink_job(
    user_query: str | None = None,
    request: JobRequest | None = None,
    source_schema: dict | None = None,
    sample_source_records: list[dict[str, Any]] | None = None,
    inline_schema: str | None = None,
    publish: bool = False,
    credentials: ConfluentCloudCredentials | None = None,
) -> dict[str, Any]:
    """Run the end-to-end job creation flow and return a consolidated result."""
    assumptions: list[str] = []
    warnings: list[str] = []

    if request is None and user_query is None:
        validation = ValidationResult(
            valid=False,
            error_code="MISSING_REQUIRED_FIELD",
            error_message="Provide either user_query or request.",
        )
        return _build_create_response(
            selected_pattern=None,
            normalized_request=None,
            schema_resolution=None,
            validation=validation,
            job_spec=None,
            flink_sql=None,
            source_preview=None,
            destination_preview=None,
            preview_skipped_reason=None,
            assumptions=[],
            warnings=[],
            next_action="Provide either a natural-language user_query or a structured request.",
        )

    if request is not None:
        effective_request = request.model_copy(
            update={
                "source_schema": source_schema or request.source_schema,
                "sample_source_records": sample_source_records or request.sample_source_records,
                "inline_schema": inline_schema or request.inline_schema,
            }
        )
    else:
        interpreted = interpret_customer_request_impl(user_query or "", source_schema)
        if isinstance(interpreted, ValidationResult):
            return _build_create_response(
                selected_pattern=None,
                normalized_request=None,
                schema_resolution=None,
                validation=interpreted,
                job_spec=None,
                flink_sql=None,
                source_preview=None,
                destination_preview=None,
                preview_skipped_reason=None,
                assumptions=[],
                warnings=[],
                next_action="Provide a clearer request with source topic, destination topic, filter condition, and source_schema.",
            )
        effective_request = interpreted.model_copy(
            update={
                "source_schema": source_schema or interpreted.source_schema,
                "sample_source_records": sample_source_records,
                "inline_schema": inline_schema,
            }
        )
        assumptions.append("The natural-language request was interpreted into a supported pattern before validation.")

    selected_pattern = effective_request.pattern_type
    validation = await validate_job_request_impl(effective_request)
    warnings.extend(validation.warnings)
    if not validation.valid:
        return _build_create_response(
            selected_pattern=selected_pattern,
            normalized_request=None,
            schema_resolution=None,
            validation=validation,
            job_spec=None,
            flink_sql=None,
            source_preview=None,
            destination_preview=None,
            preview_skipped_reason=None,
            assumptions=assumptions,
            warnings=warnings,
            next_action="Correct the validation error and resubmit the request.",
        )

    normalized_request = await generate_normalized_request(effective_request)
    schema_resolution = normalized_request.resolved_schema

    source_preview: str | None = None
    destination_preview: str | None = None
    preview_skipped_reason: str | None = None
    if effective_request.sample_source_records:
        preview_result = await generate_transform_preview(effective_request)
        if isinstance(preview_result, ValidationResult):
            warnings.append(preview_result.error_message or "Preview generation failed.")
            preview_skipped_reason = preview_result.error_message or "Preview generation failed."
        else:
            source_preview = await render_source_sample_table_impl(preview_result)
            destination_preview = await render_destination_sample_table_impl(preview_result)
            if preview_result.filtered_out_count:
                warnings.append(f"Preview filtered out {preview_result.filtered_out_count} record(s).")
            if preview_result.invalid_records:
                warnings.append(f"Preview found invalid records at indexes: {preview_result.invalid_records}.")
    else:
        preview_skipped_reason = "sample_source_records not provided"

    job_spec = await generate_job_spec_impl(normalized_request)
    flink_sql = await generate_flink_sql_impl(job_spec)

    dry_run_result: DryRunResult | None = None
    publish_result: PublishResult | None = None
    if publish:
        if credentials is None:
            validation = ValidationResult(
                valid=False,
                error_code="CREDENTIAL_MISSING",
                error_message="Publishing requires credentials.",
                warnings=warnings,
            )
            return _build_create_response(
                selected_pattern=selected_pattern,
                normalized_request=normalized_request,
                schema_resolution=schema_resolution,
                validation=validation,
                job_spec=job_spec,
                flink_sql=flink_sql,
                source_preview=source_preview,
                destination_preview=destination_preview,
                preview_skipped_reason=preview_skipped_reason,
                assumptions=assumptions,
                warnings=warnings,
                next_action="Provide valid Confluent credentials before publishing.",
            )
        dry_run_result = await dry_run_publish_impl(job_spec, credentials)
        if not dry_run_result.passed:
            warnings.extend(dry_run_result.validation.warnings)
            return _build_create_response(
                selected_pattern=selected_pattern,
                normalized_request=normalized_request,
                schema_resolution=schema_resolution,
                validation=validation,
                job_spec=job_spec,
                flink_sql=flink_sql,
                source_preview=source_preview,
                destination_preview=destination_preview,
                preview_skipped_reason=preview_skipped_reason,
                assumptions=assumptions,
                warnings=warnings,
                next_action="Resolve the dry-run errors before publishing.",
                dry_run=dry_run_result,
            )
        publish_result = await publish_job_impl(job_spec, credentials)
        if not publish_result.published and publish_result.error_message:
            warnings.append(publish_result.error_message)

    return _build_create_response(
        selected_pattern=selected_pattern,
        normalized_request=normalized_request,
        schema_resolution=schema_resolution,
        validation=validation,
        job_spec=job_spec,
        flink_sql=flink_sql,
        source_preview=source_preview,
        destination_preview=destination_preview,
        preview_skipped_reason=preview_skipped_reason,
        assumptions=assumptions,
        warnings=warnings,
        next_action=_next_action_for_success(
            has_samples=bool(effective_request.sample_source_records),
            publish_requested=publish,
        ),
        dry_run=dry_run_result,
        publish_result=publish_result,
    )

if __name__ == "__main__":
    #mcp.run(host="0.0.0.0", port=8000, transport="http")
    mcp.run(transport="stdio")
