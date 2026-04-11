"""Validation tests."""

from __future__ import annotations

import pytest

from flink_codex.models import FilterExpression, FlattenRules, JobRequest, MappingDefinition
from flink_codex.server import validate_job_request


@pytest.mark.asyncio
async def test_avro_target_without_schema_and_no_credentials_fails(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "schema_reference": None,
            "inline_schema": None,
            "confluent_credentials": None,
        }
    )
    result = await validate_job_request(request)
    assert result.valid is False
    assert result.error_code == "SCHEMA_NOT_PROVIDED"


@pytest.mark.asyncio
async def test_flatten_pattern_without_flatten_rules_fails(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={"pattern_type": "nested_json_to_flat_json", "flatten_rules": None}
    )
    result = await validate_job_request(request)
    assert result.error_code == "FLATTEN_RULES_MISSING"


@pytest.mark.asyncio
async def test_filter_expression_validated_before_schema(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "schema_reference": None,
            "confluent_credentials": None,
            "filter_expression": FilterExpression(include_fields=["order_id"], conditions=["bad condition"]),
        }
    )
    result = await validate_job_request(request)
    assert result.error_code == "INVALID_FILTER_EXPRESSION"


@pytest.mark.asyncio
async def test_avro_target_with_schema_reference_only_fails(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "schema_reference": "orders-value",
            "inline_schema": None,
        }
    )
    result = await validate_job_request(request)
    assert result.valid is False
    assert result.error_code == "SCHEMA_NOT_PROVIDED"


@pytest.mark.asyncio
async def test_avro_target_with_source_schema_generates_contract(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "schema_reference": None,
            "inline_schema": None,
            "source_schema": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string"},
                    "status": {"type": "string"},
                    "amount": {"type": "number"},
                },
            },
        }
    )
    result = await validate_job_request(request)
    assert result.valid is True


@pytest.mark.asyncio
async def test_structure_changing_json_without_source_schema_fails(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "pattern_type": "nested_json_to_flat_json",
            "filter_expression": FilterExpression(include_fields=["order.id"], conditions=[]),
            "mapping_definition": MappingDefinition(mappings={"order.id": "order_id"}),
            "flatten_rules": FlattenRules(separator="_"),
            "source_schema": None,
            "sample_source_records": None,
        }
    )
    result = await validate_job_request(request)
    assert result.valid is False
    assert result.error_code == "SCHEMA_NOT_PROVIDED"


@pytest.mark.asyncio
async def test_valid_json_to_json_request_passes(sample_json_request: JobRequest) -> None:
    result = await validate_job_request(sample_json_request)
    assert result.valid is True


@pytest.mark.asyncio
async def test_avro_target_with_inline_schema_passes(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "schema_reference": None,
            "inline_schema": '{"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"status","type":"string"}]}',
        }
    )
    result = await validate_job_request(request)
    assert result.valid is True


@pytest.mark.asyncio
async def test_invalid_inline_avro_schema_fails(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "schema_reference": None,
            "inline_schema": '{"type":"record","name":"Broken","fields":[{"name":"order_id"}]}',
        }
    )
    result = await validate_job_request(request)
    assert result.error_code == "SCHEMA_INCOMPATIBLE"
