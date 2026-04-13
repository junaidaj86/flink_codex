"""End-to-end create_flink_job tool tests."""

from __future__ import annotations

import pytest

from flink_codex.models import FilterExpression, JobRequest, MappingDefinition
from flink_codex.server import create_flink_job


@pytest.mark.asyncio
async def test_create_flink_job_from_structured_request(sample_json_request: JobRequest) -> None:
    result = await create_flink_job(
        request=sample_json_request.model_copy(
            update={
                "sample_source_records": None,
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
    )
    assert result["api_version"] == "v1"
    assert result["selected_pattern"] == "json_to_json_simple_filter"
    assert result["validation"]["valid"] is True
    assert result["job_spec"] is not None
    assert "CREATE TABLE" in result["flink_sql"]
    assert result["destination_schema_format"] is None
    assert result["destination_schema"] is None
    assert result["destination_schema_json"] is None
    assert result["destination_schema_avro"] is None
    assert result["sql_artifact"]["file_name"] == "orders_cleaned.json_to_json_simple_filter.sql.tpl"
    assert result["sql_artifact"]["format"] == "sql.tpl"
    assert result["schema_artifact"] is None
    assert "Flink SQL:" in result["response_markdown"]
    assert result["preview_skipped_reason"] == "sample_source_records not provided"


@pytest.mark.asyncio
async def test_create_flink_job_from_natural_language_with_source_schema() -> None:
    result = await create_flink_job(
        user_query="create a flink job for order events filter for totalamount greater than 300 into topic filter_order to avro",
        source_schema={
            "type": "object",
            "properties": {
                "eventId": {"type": "string"},
                "data": {
                    "type": "object",
                    "properties": {
                        "orderId": {"type": "string"},
                        "totalAmount": {"type": "number"},
                        "currency": {"type": "string"},
                    },
                },
            },
        },
    )
    assert result["api_version"] == "v1"
    assert result["selected_pattern"] == "nested_json_to_avro"
    assert result["validation"]["valid"] is True
    assert result["normalized_request"]["generated_avro_schema"] is not None
    assert result["destination_schema_format"] == "avro"
    assert result["destination_schema"] is not None
    assert result["destination_schema_json"] is None
    assert result["destination_schema_avro"] is not None
    assert result["sql_artifact"]["file_name"] == "filter_order.nested_json_to_avro.sql.tpl"
    assert result["schema_artifact"]["file_name"] == "filter_order.nested_json_to_avro.avsc"
    assert result["schema_artifact"]["format"] == "avro"
    assert "'value.avro-schema'" in result["flink_sql"]
    assert "Destination schema:" in result["response_markdown"]


@pytest.mark.asyncio
async def test_create_flink_job_generates_destination_json_schema_for_flattening() -> None:
    result = await create_flink_job(
        user_query="Create a Flink job from topic order_events where data.totalAmount is greater than 300 into topic flat_order_events and flatten the output",
        source_schema={
            "type": "object",
            "properties": {
                "eventId": {"type": "string"},
                "eventType": {"type": "string"},
                "eventTime": {"type": "string"},
                "source": {"type": "string"},
                "traceId": {"type": "string"},
                "data": {
                    "type": "object",
                    "properties": {
                        "orderId": {"type": "string"},
                        "customerId": {"type": "string"},
                        "status": {"type": "string"},
                        "totalAmount": {"type": "number"},
                        "currency": {"type": "string"},
                    },
                },
            },
        },
    )
    assert result["api_version"] == "v1"
    assert result["selected_pattern"] == "nested_json_to_flat_json"
    assert result["validation"]["valid"] is True
    assert result["normalized_request"]["generated_json_schema"] is not None
    assert result["destination_schema_format"] == "json"
    assert result["destination_schema"] is not None
    assert result["destination_schema_json"] is not None
    assert result["destination_schema_avro"] is None
    assert result["sql_artifact"]["file_name"] == "flat_order_events.nested_json_to_flat_json.sql.tpl"
    assert result["schema_artifact"]["file_name"] == "flat_order_events.nested_json_to_flat_json.schema.json"
    assert result["schema_artifact"]["format"] == "json-schema"
    assert "data_total_amount" in result["normalized_request"]["generated_json_schema"]["properties"]
    assert result["job_spec"]["generated_json_schema"] is not None
    assert "\"data_total_amount\"" in result["response_markdown"]


@pytest.mark.asyncio
async def test_create_flink_job_validation_failure_returns_consolidated_response() -> None:
    result = await create_flink_job(
        request=JobRequest(
            source_topic="orders",
            destination_topic="orders_clean",
            source_format="json",
            target_format="avro",
            pattern_type="json_to_avro",
            filter_expression=FilterExpression(include_fields=["order_id"], conditions=["amount > 100"]),
            mapping_definition=MappingDefinition(mappings={"order_id": "order_id"}),
            source_schema=None,
            inline_schema=None,
        )
    )
    assert result["api_version"] == "v1"
    assert result["validation"]["valid"] is False
    assert result["validation"]["error_code"] == "SCHEMA_NOT_PROVIDED"
    assert result["job_spec"] is None
    assert result["flink_sql"] is None
    assert result["sql_artifact"] is None
    assert result["schema_artifact"] is None


@pytest.mark.asyncio
async def test_create_flink_job_v1_contract_contains_required_fields(
    sample_json_request: JobRequest,
) -> None:
    result = await create_flink_job(
        request=sample_json_request.model_copy(
            update={
                "sample_source_records": None,
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
    )

    required_keys = {
        "api_version",
        "selected_pattern",
        "normalized_request",
        "schema_resolution",
        "validation",
        "job_spec",
        "flink_sql",
        "destination_schema",
        "destination_schema_format",
        "destination_schema_json",
        "destination_schema_avro",
        "sql_artifact",
        "schema_artifact",
        "response_markdown",
        "source_preview",
        "destination_preview",
        "preview_skipped_reason",
        "assumptions",
        "warnings",
        "next_action",
        "dry_run",
        "publish_result",
    }

    assert result["api_version"] == "v1"
    assert required_keys.issubset(result.keys())
    assert isinstance(result["validation"], dict)
    assert isinstance(result["assumptions"], list)
    assert isinstance(result["warnings"], list)
    assert isinstance(result["next_action"], str)
