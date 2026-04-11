"""Dry-run and publish orchestration."""

from __future__ import annotations

from datetime import datetime, timezone

from .confluent_client import publish_statement
from .models import ConfluentCloudCredentials, DryRunResult, JobRequest, JobSpec, PublishResult
from .validation import validate_job_request, validation_error


async def dry_run_publish(spec: JobSpec, credentials: ConfluentCloudCredentials) -> DryRunResult:
    """Perform publish precondition checks without network calls."""
    del credentials
    validation = (
        validation_error("PUBLISH_PRECONDITION_FAILED", "Job spec validation_status must be passed before publishing.")
        if spec.validation_status != "passed"
        else await validate_job_request(
            JobRequest(
                source_topic=spec.source_topic,
                destination_topic=spec.destination_topic,
                source_format=spec.source_format,
                target_format=spec.target_format,
                pattern_type=spec.pattern_type,
                schema_reference=spec.schema_reference,
                inline_schema=spec.inline_schema,
                sample_source_records=None,
            )
        )
    )
    passed = validation.valid and spec.validation_status == "passed"
    return DryRunResult(passed=passed, validation=validation, sql_preview=spec.flink_sql)


async def publish_job(spec: JobSpec, credentials: ConfluentCloudCredentials) -> PublishResult:
    """Publish a job only after validation preconditions succeed."""
    if spec.validation_status != "passed":
        return PublishResult(
            published=False,
            error_code="PUBLISH_PRECONDITION_FAILED",
            error_message="Job spec validation_status must be passed before publishing.",
        )

    validation = await validate_job_request(
        JobRequest(
            source_topic=spec.source_topic,
            destination_topic=spec.destination_topic,
            source_format=spec.source_format,
            target_format=spec.target_format,
            pattern_type=spec.pattern_type,
            schema_reference=spec.schema_reference,
            inline_schema=spec.inline_schema,
            confluent_credentials=credentials,
        )
    )
    if not validation.valid:
        return PublishResult(
            published=False,
            error_code="PUBLISH_PRECONDITION_FAILED",
            error_message=validation.error_message,
        )

    result = await publish_statement(spec.flink_sql, credentials)
    if result.published:
        spec.created_at = datetime.now(timezone.utc)
    return result
