"""Pattern catalog tests."""

from __future__ import annotations

import pytest

from flink_codex.catalog import catalog
from flink_codex.models import JobRequest
from flink_codex.server import describe_pattern, interpret_customer_request, validate_job_request


def test_all_8_patterns_registered() -> None:
    assert len(catalog.patterns) == 8


@pytest.mark.asyncio
async def test_unsupported_pattern_returns_error(sample_json_request: JobRequest) -> None:
    request = sample_json_request.model_copy(update={"pattern_type": "unknown"})
    result = await validate_job_request(request)
    assert result.valid is False
    assert result.error_code == "UNSUPPORTED_PATTERN"


@pytest.mark.asyncio
async def test_pattern_lookup_by_id() -> None:
    result = await describe_pattern("json_to_json_simple_filter")
    assert result.id == "json_to_json_simple_filter"


@pytest.mark.asyncio
async def test_interpret_customer_request_chooses_nested_json_pattern() -> None:
    schema = {
        "type": "object",
        "properties": {
            "eventId": {"type": "string"},
            "data": {
                "type": "object",
                "properties": {
                    "totalAmount": {"type": "number"},
                    "orderId": {"type": "string"},
                },
            },
        },
    }
    result = await interpret_customer_request(
        "create a flink job for order events filter for totalamount greater than 300 and pushed to destination topic filter order",
        source_schema=schema,
    )
    assert result.pattern_type == "json_to_json_nested_filter"
    assert result.filter_expression.conditions == ["data.totalAmount > 300"]


@pytest.mark.asyncio
async def test_interpret_customer_request_chooses_flatten_pattern_when_requested() -> None:
    schema = {
        "type": "object",
        "properties": {
            "data": {
                "type": "object",
                "properties": {
                    "totalAmount": {"type": "number"},
                },
            },
        },
    }
    result = await interpret_customer_request(
        "create a flink job from order events flatten output and filter for totalamount greater than 300 into topic filter order",
        source_schema=schema,
    )
    assert result.pattern_type == "nested_json_to_flat_json"
    assert result.flatten_rules is not None


@pytest.mark.asyncio
async def test_interpret_customer_request_chooses_simple_json_pattern_for_top_level_field() -> None:
    result = await interpret_customer_request(
        "create a flink job from topic orders where amount greater than 100 into topic final_order"
    )
    assert result.pattern_type == "json_to_json_simple_filter"
    assert result.filter_expression.conditions == ["amount > 100"]


@pytest.mark.asyncio
async def test_interpret_customer_request_handles_typo_heavy_destination_phrase() -> None:
    schema = {
        "type": "object",
        "properties": {
            "eventId": {"type": "string"},
            "data": {
                "type": "object",
                "properties": {
                    "totalAmount": {"type": "number"},
                },
            },
        },
    }
    result = await interpret_customer_request(
        "create a flink job for order events filter for totalamount gretaer tha 300 adn psuhed to detiantoon topic filter order",
        source_schema=schema,
    )
    assert result.pattern_type == "json_to_json_nested_filter"
    assert result.destination_topic == "filter_order"
    assert result.filter_expression.conditions == ["data.totalAmount > 300"]
