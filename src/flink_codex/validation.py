"""Validation logic for Flink Codex requests."""

from __future__ import annotations

import json
import re
from typing import Any

from fastavro import parse_schema

from .catalog import catalog
from .models import (
    FilterExpression,
    JobRequest,
    MappingDefinition,
    ValidationResult,
)
from .schema_generator import destination_structure_differs, generate_avro_schema, generate_json_schema

CONDITION_PATTERN = re.compile(
    r"^\s*([A-Za-z_][\w.]*)\s*(==|!=|>=|<=|>|<)\s*(?:'[^']*'|\"[^\"]*\"|-?\d+(?:\.\d+)?|true|false|null)\s*$",
    re.IGNORECASE,
)


def validation_error(code: str, message: str) -> ValidationResult:
    """Build a failed validation result."""
    return ValidationResult(valid=False, error_code=code, error_message=message)


def validation_success(*, warnings: list[str] | None = None) -> ValidationResult:
    """Build a successful validation result."""
    return ValidationResult(valid=True, warnings=warnings or [])


def _flatten_record(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested record into dot-notation paths."""
    result: dict[str, Any] = {}
    for key, value in record.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(_flatten_record(value, path))
        else:
            result[path] = value
    return result


def _derive_source_schema(request: JobRequest) -> dict[str, Any] | None:
    """Derive a best-effort source schema from sample records."""
    if request.source_schema:
        return flatten_record_paths_from_schema(request.source_schema)
    if not request.sample_source_records:
        return None
    fields: dict[str, Any] = {}
    for record in request.sample_source_records:
        fields.update(_flatten_record(record))
    return fields


def flatten_record_paths_from_schema(schema: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten leaf paths from a JSON schema for validation checks."""
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        schema_type = next((item for item in schema_type if item != "null"), "string")
    if schema_type != "object":
        return {prefix: schema} if prefix else {}

    flattened: dict[str, Any] = {}
    for key, child in schema.get("properties", {}).items():
        path = f"{prefix}.{key}" if prefix else key
        child_type = child.get("type")
        if isinstance(child_type, list):
            child_type = next((item for item in child_type if item != "null"), "string")
        if child_type == "object":
            flattened.update(flatten_record_paths_from_schema(child, path))
        else:
            flattened[path] = child
    return flattened


def _extract_condition_fields(expression: FilterExpression) -> list[str]:
    """Return field paths referenced by filter conditions."""
    fields: list[str] = []
    for condition in expression.conditions:
        match = CONDITION_PATTERN.match(condition)
        if match:
            fields.append(match.group(1))
    return fields


async def validate_filter_expression(
    expression: FilterExpression,
    source_schema: dict[str, Any] | None,
) -> ValidationResult:
    """Validate filter syntax and optional field references."""
    for field_path in expression.include_fields:
        if not field_path or " " in field_path:
            return validation_error("INVALID_FILTER_EXPRESSION", f"Invalid include field path: {field_path!r}")
        if source_schema is not None and field_path not in source_schema:
            return validation_error("INVALID_FILTER_EXPRESSION", f"Include field not found in source schema: {field_path}")

    for condition in expression.conditions:
        match = CONDITION_PATTERN.match(condition)
        if not match:
            return validation_error("INVALID_FILTER_EXPRESSION", f"Invalid filter condition syntax: {condition}")
        field_name = match.group(1)
        if source_schema is not None and field_name not in source_schema:
            return validation_error("INVALID_FILTER_EXPRESSION", f"Filter field not found in source schema: {field_name}")

    return validation_success()


async def validate_schema_mapping(
    mapping: MappingDefinition,
    source_schema: dict[str, Any],
    target_schema: dict[str, Any] | None,
) -> ValidationResult:
    """Validate mapping references against known schema fields."""
    del target_schema
    for source_field, destination_field in mapping.mappings.items():
        if source_field not in source_schema:
            return validation_error("INVALID_MAPPING", f"Source field not found in schema: {source_field}")
        if not destination_field:
            return validation_error("INVALID_MAPPING", f"Destination field is missing for source field: {source_field}")
    return validation_success()


async def validate_avro_schema(schema_string: str) -> ValidationResult:
    """Validate an Avro schema string with fastavro."""
    try:
        parse_schema(json.loads(schema_string))
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        return validation_error("SCHEMA_INCOMPATIBLE", f"Invalid Avro schema: {exc}")
    return validation_success()


async def _validate_schema_requirements(request: JobRequest) -> ValidationResult:
    """Validate that Avro targets have a schema reference or can resolve one."""
    if request.target_format == "avro":
        if request.inline_schema:
            return await validate_avro_schema(request.inline_schema)
        generated_schema = generate_avro_schema(request) if request.source_schema else None
        if generated_schema is not None:
            return await validate_avro_schema(generated_schema)
        return validation_error(
            "SCHEMA_NOT_PROVIDED",
            "Avro target requires inline_schema or source_schema. schema_reference is metadata only in this version.",
        )

    if request.target_format == "json" and destination_structure_differs(request):
        if request.source_schema is None:
            return validation_error(
                "SCHEMA_NOT_PROVIDED",
                "Structure-changing JSON targets require source_schema so a destination schema can be generated.",
            )
        generated_schema = generate_json_schema(request)
        if generated_schema is None:
            return validation_error(
                "SCHEMA_NOT_PROVIDED",
                "Destination JSON schema could not be generated from the provided source_schema.",
            )
    return validation_success()


async def validate_job_request(request: JobRequest) -> ValidationResult:
    """Validate a job request in deterministic order and stop on first failure."""
    pattern = catalog.by_id(request.pattern_type)
    if pattern is None:
        return validation_error("UNSUPPORTED_PATTERN", f"Unsupported pattern: {request.pattern_type}")

    required = {
        "source_topic": request.source_topic,
        "destination_topic": request.destination_topic,
        "source_format": request.source_format,
        "target_format": request.target_format,
        "pattern_type": request.pattern_type,
    }
    for field_name, value in required.items():
        if value in (None, ""):
            return validation_error("MISSING_REQUIRED_FIELD", f"Missing required field: {field_name}")

    source_schema = _derive_source_schema(request)
    if request.filter_expression is not None:
        result = await validate_filter_expression(request.filter_expression, source_schema)
        if not result.valid:
            return result

    if request.mapping_definition is not None and source_schema is not None:
        result = await validate_schema_mapping(request.mapping_definition, source_schema, None)
        if not result.valid:
            return result

    if pattern.requires_flatten_rules and request.flatten_rules is None:
        return validation_error("FLATTEN_RULES_MISSING", "This pattern requires flatten_rules.")

    return await _validate_schema_requirements(request)
