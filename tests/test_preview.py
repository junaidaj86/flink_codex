"""Preview generation tests."""

from __future__ import annotations

import pytest

from flink_codex.models import FilterExpression, FlattenRules, MappingDefinition
from flink_codex.preview import generate_transform_preview


@pytest.mark.asyncio
async def test_preview_fails_without_sample_records(sample_json_request) -> None:
    request = sample_json_request.model_copy(update={"sample_source_records": None})
    result = await generate_transform_preview(request)
    assert result.error_code == "SAMPLE_RECORDS_MISSING"


@pytest.mark.asyncio
async def test_preview_fails_with_empty_sample_records(sample_json_request) -> None:
    request = sample_json_request.model_copy(update={"sample_source_records": []})
    result = await generate_transform_preview(request)
    assert result.error_code == "SAMPLE_RECORDS_MISSING"


@pytest.mark.asyncio
async def test_source_table_row_count_matches_input(sample_json_request) -> None:
    preview = await generate_transform_preview(sample_json_request)
    assert len(preview.source_table.rows) == len(sample_json_request.sample_source_records)


@pytest.mark.asyncio
async def test_destination_table_excludes_filtered_records(sample_json_request) -> None:
    preview = await generate_transform_preview(sample_json_request)
    assert len(preview.destination_table.rows) == 1
    assert preview.filtered_out_count == 1


@pytest.mark.asyncio
async def test_invalid_record_tracked_not_fatal(sample_json_request) -> None:
    request = sample_json_request.model_copy(
        update={
            "mapping_definition": MappingDefinition(mappings={"order_id": "order_id", "missing": "missing"}),
            "filter_expression": FilterExpression(include_fields=["order_id", "missing"], conditions=[]),
            "sample_source_records": [{"order_id": "A1"}, {"order_id": "A2", "missing": "x"}],
        }
    )
    preview = await generate_transform_preview(request)
    assert preview.invalid_records == [0]
    assert len(preview.destination_table.rows) == 1


@pytest.mark.asyncio
async def test_flattening_uses_separator_in_column_names(sample_json_request) -> None:
    request = sample_json_request.model_copy(
        update={
            "pattern_type": "nested_json_to_flat_json",
            "flatten_rules": FlattenRules(separator="_"),
            "filter_expression": FilterExpression(include_fields=["order.id"], conditions=["order.id == 'A1'"]),
            "mapping_definition": MappingDefinition(mappings={"order.id": "order.id"}),
            "sample_source_records": [{"order": {"id": "A1"}}],
        }
    )
    preview = await generate_transform_preview(request)
    assert preview.destination_table.columns == ["order_id"]


@pytest.mark.asyncio
async def test_preview_is_deterministic(sample_json_request) -> None:
    preview1 = await generate_transform_preview(sample_json_request)
    preview2 = await generate_transform_preview(sample_json_request)
    assert preview1.model_dump() == preview2.model_dump()


@pytest.mark.asyncio
async def test_no_synthetic_rows_ever_added(sample_json_request) -> None:
    preview = await generate_transform_preview(sample_json_request)
    assert len(preview.destination_table.rows) <= len(sample_json_request.sample_source_records)
