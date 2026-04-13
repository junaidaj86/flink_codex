"""Domain models for the Flink Codex project."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, field_validator


ERROR_CODES = {
    "UNSUPPORTED_PATTERN",
    "MISSING_REQUIRED_FIELD",
    "INVALID_FILTER_EXPRESSION",
    "INVALID_MAPPING",
    "SCHEMA_NOT_PROVIDED",
    "SCHEMA_FETCH_FAILED",
    "SCHEMA_SUBJECT_NOT_FOUND",
    "SCHEMA_INCOMPATIBLE",
    "FLATTEN_RULES_MISSING",
    "CREDENTIAL_INVALID",
    "CREDENTIAL_MISSING",
    "OAUTH_TOKEN_FETCH_FAILED",
    "PUBLISH_PRECONDITION_FAILED",
    "SAMPLE_RECORDS_MISSING",
}


class ConfluentCloudCredentials(BaseModel):
    """OAuth client credentials and endpoints for Confluent Cloud."""

    oauth_client_id: str
    oauth_client_secret: SecretStr
    identity_provider_url: str
    oauth_audience: str
    cloud_base_url: str = "https://api.confluent.cloud"
    schema_registry_url: str

    @field_validator("identity_provider_url", "cloud_base_url", "schema_registry_url")
    @classmethod
    def must_be_https(cls, value: str) -> str:
        """Ensure all URLs are HTTPS."""
        if not value.startswith("https://"):
            raise ValueError("URL must use HTTPS")
        return value.rstrip("/")


class FilterExpression(BaseModel):
    """Field selection and filter conditions."""

    include_fields: list[str]
    conditions: list[str] = Field(default_factory=list)


class FlattenRules(BaseModel):
    """Flattening configuration."""

    separator: str = "_"


class MappingDefinition(BaseModel):
    """Explicit source-to-destination field mappings."""

    mappings: dict[str, str]


class ValidationResult(BaseModel):
    """Validation status with canonical errors."""

    valid: bool
    error_code: str | None = None
    error_message: str | None = None
    warnings: list[str] = Field(default_factory=list)

    @field_validator("error_code")
    @classmethod
    def validate_error_code(cls, value: str | None) -> str | None:
        """Ensure validation uses only the approved error codes."""
        if value is not None and value not in ERROR_CODES:
            raise ValueError("Unsupported error code")
        return value


class SchemaResolutionResult(BaseModel):
    """Schema Registry lookup result."""

    resolved: bool
    subject_name: str | None = None
    schema_id: int | None = None
    schema_string: str | None = None
    error_message: str | None = None


class PatternDefinition(BaseModel):
    """Catalog entry for a supported transformation pattern."""

    id: str
    name: str
    description: str
    source_format: Literal["json", "avro"]
    target_format: Literal["json", "avro"]
    requires_flatten_rules: bool
    requires_schema_reference: bool
    supports_nested_source: bool
    supports_nested_target: bool


class JobRequest(BaseModel):
    """Input contract for validation, preview, and generation."""

    source_topic: str
    destination_topic: str
    source_format: Literal["json", "avro"]
    target_format: Literal["json", "avro"]
    pattern_type: str
    filter_expression: FilterExpression | None = None
    mapping_definition: MappingDefinition | None = None
    flatten_rules: FlattenRules | None = None
    schema_reference: str | None = None
    inline_schema: str | None = None
    source_schema: dict[str, Any] | None = None
    sample_source_records: list[dict[str, Any]] | None = None
    confluent_credentials: ConfluentCloudCredentials | None = None


class NormalizedJobRequest(JobRequest):
    """Normalized request with resolved schema and timestamp."""

    resolved_schema: SchemaResolutionResult | None = None
    generated_json_schema: dict[str, Any] | None = None
    generated_avro_schema: str | None = None
    normalized_at: datetime


class SamplePreviewTable(BaseModel):
    """Preview table with columns and user-derived rows."""

    columns: list[str]
    rows: list[list[Any]]


class TransformPreview(BaseModel):
    """Deterministic preview for source and destination samples."""

    pattern_type: str
    source_table: SamplePreviewTable
    destination_table: SamplePreviewTable
    transformation_summary: str
    mapping_explanation: list[str]
    filtered_out_count: int = 0
    invalid_records: list[int] = Field(default_factory=list)


class JobSpec(BaseModel):
    """Generated Flink job specification."""

    spec_id: str
    pattern_type: str
    flink_sql: str
    source_topic: str
    destination_topic: str
    source_format: str
    target_format: str
    schema_reference: str | None
    filter_expression: FilterExpression | None = None
    mapping_definition: MappingDefinition | None = None
    flatten_rules: FlattenRules | None = None
    inline_schema: str | None = None
    source_schema: dict[str, Any] | None = None
    generated_json_schema: dict[str, Any] | None = None
    generated_avro_schema: str | None = None
    validation_status: Literal["pending", "passed", "failed"] = "pending"
    created_at: datetime


class DryRunResult(BaseModel):
    """Dry-run publish result."""

    passed: bool
    validation: ValidationResult
    sql_preview: str


class PublishResult(BaseModel):
    """Publish outcome for a Flink SQL statement."""

    published: bool
    statement_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None
