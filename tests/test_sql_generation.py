"""SQL generation tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from flink_codex.catalog import catalog
from flink_codex.models import FilterExpression, FlattenRules, JobRequest, MappingDefinition, NormalizedJobRequest
from flink_codex.sql_generator import build_flink_sql, generate_job_spec
from flink_codex.server import generate_normalized_request


def _normalize(request: JobRequest) -> NormalizedJobRequest:
    return NormalizedJobRequest(**request.model_dump(), resolved_schema=None, normalized_at=datetime.now(timezone.utc))


@pytest.mark.asyncio
async def test_sql_contains_source_topic(sample_json_request: JobRequest) -> None:
    sql = await build_flink_sql(_normalize(sample_json_request))
    assert sample_json_request.source_topic in sql


@pytest.mark.asyncio
async def test_sql_contains_separate_create_and_insert(sample_json_request: JobRequest) -> None:
    sql = await build_flink_sql(_normalize(sample_json_request))
    assert "CREATE TABLE" in sql
    assert "INSERT INTO" in sql


@pytest.mark.asyncio
async def test_sql_contains_where_clause_for_filter(sample_json_request: JobRequest) -> None:
    sql = await build_flink_sql(_normalize(sample_json_request))
    assert "WHERE status == 'ACTIVE'" in sql


@pytest.mark.asyncio
async def test_sql_contains_avro_format_hint_for_avro_target(sample_avro_request: JobRequest) -> None:
    sql = await build_flink_sql(_normalize(sample_avro_request))
    assert "VALUE_FORMAT = 'AVRO'" in sql or "'value.format' = 'AVRO'" in sql


@pytest.mark.asyncio
async def test_sql_contains_inline_avro_schema_when_provided(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "target_format": "avro",
            "pattern_type": "json_to_avro",
            "inline_schema": '{"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"status","type":"string"}]}',
            "schema_reference": None,
        }
    )
    sql = await build_flink_sql(_normalize(request))
    assert "'value.avro-schema'" in sql


@pytest.mark.asyncio
async def test_normalized_request_generates_destination_json_schema_for_flatten(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(
        update={
            "pattern_type": "nested_json_to_flat_json",
            "filter_expression": FilterExpression(include_fields=["order.id"], conditions=[]),
            "mapping_definition": MappingDefinition(mappings={"order.id": "order.id"}),
            "flatten_rules": FlattenRules(separator="_"),
            "source_schema": {
                "type": "object",
                "properties": {
                    "order": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                        },
                    },
                },
            },
        }
    )
    normalized = await generate_normalized_request(request)
    assert normalized.generated_json_schema is not None
    assert "order_id" in normalized.generated_json_schema["properties"]


@pytest.mark.asyncio
async def test_normalized_request_generates_destination_avro_schema_from_source_schema(sample_json_request: JobRequest) -> None:
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
    normalized = await generate_normalized_request(request)
    assert normalized.generated_avro_schema is not None
    assert "\"type\": \"record\"" in normalized.generated_avro_schema


@pytest.mark.asyncio
async def test_nested_json_sql_uses_row_type_and_no_dotted_aliases() -> None:
    request = JobRequest(
        source_topic="order_events",
        destination_topic="filter_order",
        source_format="json",
        target_format="json",
        pattern_type="json_to_json_nested_filter",
        filter_expression=FilterExpression(
            include_fields=[
                "eventId",
                "eventType",
                "eventTime",
                "source",
                "traceId",
                "data.orderId",
                "data.customerId",
                "data.status",
                "data.totalAmount",
                "data.currency",
                "data.items",
                "data.shippingAddress.street",
                "data.shippingAddress.city",
                "data.shippingAddress.postalCode",
                "data.shippingAddress.country",
            ],
            conditions=["data.totalAmount > 300"],
        ),
        mapping_definition=MappingDefinition(
            mappings={
                "eventId": "eventId",
                "eventType": "eventType",
                "eventTime": "eventTime",
                "source": "source",
                "traceId": "traceId",
                "data.orderId": "data.orderId",
                "data.customerId": "data.customerId",
                "data.status": "data.status",
                "data.totalAmount": "data.totalAmount",
                "data.currency": "data.currency",
                "data.items": "data.items",
                "data.shippingAddress.street": "data.shippingAddress.street",
                "data.shippingAddress.city": "data.shippingAddress.city",
                "data.shippingAddress.postalCode": "data.shippingAddress.postalCode",
                "data.shippingAddress.country": "data.shippingAddress.country",
            }
        ),
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
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "productId": {"type": "string"},
                                    "quantity": {"type": "integer"},
                                    "price": {"type": "number"},
                                },
                            },
                        },
                        "shippingAddress": {
                            "type": "object",
                            "properties": {
                                "street": {"type": "string"},
                                "city": {"type": "string"},
                                "postalCode": {"type": "string"},
                                "country": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
    )
    sql = await build_flink_sql(_normalize(request))
    assert "data ROW<" in sql
    assert "totalAmount DOUBLE" in sql
    assert "AS data.orderId" not in sql
    assert "CAST(ROW(" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("pattern_id", [pattern.id for pattern in catalog.patterns])
async def test_each_pattern_generates_valid_sql(pattern_id: str, sample_json_request: JobRequest, sample_avro_request: JobRequest) -> None:
    request = sample_avro_request if "avro" in pattern_id and not pattern_id.startswith("json") else sample_json_request
    request = request.model_copy(update={"pattern_type": pattern_id, "flatten_rules": None})
    pattern = catalog.by_id(pattern_id)
    if pattern and pattern.requires_flatten_rules:
        request = request.model_copy(update={"flatten_rules": FlattenRules(separator="_")})
    if pattern and pattern.target_format == "avro":
        request = request.model_copy(
            update={
                "target_format": "avro",
                "schema_reference": "subject-value",
                "inline_schema": '{"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"status","type":"string"}]}',
            }
        )
    sql = await build_flink_sql(_normalize(request))
    assert sql.strip()
