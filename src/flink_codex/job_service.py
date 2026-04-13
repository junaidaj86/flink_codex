"""Application service for end-to-end Flink job creation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .interpreter import interpret_customer_request as interpret_customer_request_impl
from .models import (
    ConfluentCloudCredentials,
    DryRunResult,
    JobRequest,
    JobSpec,
    NormalizedJobRequest,
    PublishResult,
    SchemaResolutionResult,
    ValidationResult,
)
from .preview import generate_transform_preview
from .publisher import dry_run_publish as dry_run_publish_impl
from .publisher import publish_job as publish_job_impl
from .schema_generator import generate_avro_schema, generate_json_schema
from .sql_generator import (
    generate_flink_sql as generate_flink_sql_impl,
    generate_job_spec as generate_job_spec_impl,
    render_destination_sample_table as render_destination_sample_table_impl,
    render_source_sample_table as render_source_sample_table_impl,
)
from .validation import validate_job_request as validate_job_request_impl


def next_action_for_success(*, has_samples: bool, publish_requested: bool) -> str:
    """Return the next operational step for a successful generation flow."""
    if publish_requested:
        return "Review the generated Flink SQL and publish result."
    if not has_samples:
        return "Provide sample_source_records to generate deterministic source and destination previews."
    return "Review the generated Flink SQL and run dry_run_publish before publishing."


def normalize_request(request: JobRequest) -> NormalizedJobRequest:
    """Normalize a validated request, applying schema derivation."""
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


def _artifact_stem(
    selected_pattern: str | None,
    normalized_request: NormalizedJobRequest | None,
    job_spec: JobSpec | None,
) -> str | None:
    """Build a deterministic artifact name stem from destination topic and job type."""
    destination_topic = None
    if job_spec is not None:
        destination_topic = job_spec.destination_topic
    elif normalized_request is not None:
        destination_topic = normalized_request.destination_topic
    if destination_topic is None:
        return None
    topic_slug = destination_topic.replace(".", "_").replace("-", "_")
    if selected_pattern is None:
        return topic_slug
    return f"{topic_slug}.{selected_pattern}"


def _extract_destination_schema(
    normalized_request: NormalizedJobRequest | None,
    job_spec: JobSpec | None,
) -> tuple[dict[str, Any] | str | None, str | None, str | None, str | None]:
    """Return destination schema payload and serialized variants."""
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
        return (
            destination_schema,
            destination_schema_format,
            destination_schema_json,
            destination_schema_avro,
        )

    if normalized_request is not None:
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

    return (
        destination_schema,
        destination_schema_format,
        destination_schema_json,
        destination_schema_avro,
    )


def _build_artifacts(
    *,
    artifact_stem: str | None,
    flink_sql: str | None,
    destination_schema_json: str | None,
    destination_schema_avro: str | None,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    """Build SQL and schema artifact metadata."""
    sql_artifact = None
    schema_artifact = None

    if flink_sql is not None and artifact_stem is not None:
        sql_artifact = {
            "file_name": f"{artifact_stem}.sql.tpl",
            "content": flink_sql,
            "format": "sql.tpl",
        }

    if artifact_stem is not None:
        if destination_schema_json is not None:
            schema_artifact = {
                "file_name": f"{artifact_stem}.schema.json",
                "content": destination_schema_json,
                "format": "json-schema",
            }
        elif destination_schema_avro is not None:
            schema_artifact = {
                "file_name": f"{artifact_stem}.avsc",
                "content": destination_schema_avro,
                "format": "avro",
            }

    return sql_artifact, schema_artifact


def _build_response_markdown(
    *,
    selected_pattern: str | None,
    validation: ValidationResult,
    flink_sql: str | None,
    destination_schema_json: str | None,
    destination_schema_avro: str | None,
    source_preview: str | None,
    destination_preview: str | None,
    preview_skipped_reason: str | None,
    warnings: list[str],
    next_action: str,
) -> str:
    """Build a preformatted user-facing response block."""
    parts: list[str] = []
    if selected_pattern is not None:
        parts.append(f"Selected pattern: `{selected_pattern}`")
    parts.append(f"Validation: `{'passed' if validation.valid else 'failed'}`")

    if flink_sql is not None:
        parts.append(f"Flink SQL:\n```sql\n{flink_sql.rstrip()}\n```")
    if destination_schema_json is not None:
        parts.append(f"Destination schema:\n```json\n{destination_schema_json.rstrip()}\n```")
    elif destination_schema_avro is not None:
        parts.append(f"Destination schema:\n```json\n{destination_schema_avro.rstrip()}\n```")
    if source_preview is not None:
        parts.append(f"Source preview:\n{source_preview.rstrip()}")
    if destination_preview is not None:
        parts.append(f"Destination preview:\n{destination_preview.rstrip()}")
    if preview_skipped_reason is not None:
        parts.append(f"Preview: {preview_skipped_reason}")
    if warnings:
        parts.append("Warnings:\n- " + "\n- ".join(warnings))
    parts.append(f"Next action: {next_action}")
    return "\n\n".join(parts)


def build_create_response(
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
    (
        destination_schema,
        destination_schema_format,
        destination_schema_json,
        destination_schema_avro,
    ) = _extract_destination_schema(normalized_request, job_spec)
    artifact_stem = _artifact_stem(selected_pattern, normalized_request, job_spec)
    sql_artifact, schema_artifact = _build_artifacts(
        artifact_stem=artifact_stem,
        flink_sql=flink_sql,
        destination_schema_json=destination_schema_json,
        destination_schema_avro=destination_schema_avro,
    )
    response_markdown = _build_response_markdown(
        selected_pattern=selected_pattern,
        validation=validation,
        flink_sql=flink_sql,
        destination_schema_json=destination_schema_json,
        destination_schema_avro=destination_schema_avro,
        source_preview=source_preview,
        destination_preview=destination_preview,
        preview_skipped_reason=preview_skipped_reason,
        warnings=warnings,
        next_action=next_action,
    )

    return {
        "api_version": "v1",
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
        "sql_artifact": sql_artifact,
        "schema_artifact": schema_artifact,
        "response_markdown": response_markdown,
        "source_preview": source_preview,
        "destination_preview": destination_preview,
        "preview_skipped_reason": preview_skipped_reason,
        "assumptions": assumptions,
        "warnings": warnings,
        "next_action": next_action,
        "dry_run": dry_run.model_dump(mode="json") if dry_run else None,
        "publish_result": publish_result.model_dump(mode="json") if publish_result else None,
    }


async def create_flink_job_service(
    *,
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
        return build_create_response(
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
            return build_create_response(
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
        return build_create_response(
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

    normalized_request = normalize_request(effective_request)
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
            publish_validation = ValidationResult(
                valid=False,
                error_code="CREDENTIAL_MISSING",
                error_message="Publishing requires credentials.",
                warnings=warnings,
            )
            return build_create_response(
                selected_pattern=selected_pattern,
                normalized_request=normalized_request,
                schema_resolution=schema_resolution,
                validation=publish_validation,
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
            return build_create_response(
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

    return build_create_response(
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
        next_action=next_action_for_success(
            has_samples=bool(effective_request.sample_source_records),
            publish_requested=publish,
        ),
        dry_run=dry_run_result,
        publish_result=publish_result,
    )
