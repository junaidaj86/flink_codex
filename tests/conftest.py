"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from flink_codex.models import ConfluentCloudCredentials, FilterExpression, JobRequest, MappingDefinition


@pytest.fixture
def sample_credentials() -> ConfluentCloudCredentials:
    """Return dummy Confluent credentials."""
    return ConfluentCloudCredentials(
        oauth_client_id="client-id",
        oauth_client_secret="client-secret",
        identity_provider_url="https://identity.example.com",
        oauth_audience="audience-a",
        cloud_base_url="https://api.confluent.cloud",
        schema_registry_url="https://schema.example.com",
    )


@pytest.fixture
def sample_json_request() -> JobRequest:
    """Return a valid JSON request."""
    return JobRequest(
        source_topic="orders.raw",
        destination_topic="orders.cleaned",
        source_format="json",
        target_format="json",
        pattern_type="json_to_json_simple_filter",
        filter_expression=FilterExpression(
            include_fields=["order_id", "status"],
            conditions=["status == 'ACTIVE'"],
        ),
        mapping_definition=MappingDefinition(mappings={"order_id": "order_id", "status": "status"}),
        sample_source_records=[
            {"order_id": "A1", "status": "ACTIVE"},
            {"order_id": "A2", "status": "INACTIVE"},
        ],
    )


@pytest.fixture
def sample_avro_request() -> JobRequest:
    """Return a valid Avro request."""
    return JobRequest(
        source_topic="orders.raw.avro",
        destination_topic="orders.cleaned.avro",
        source_format="avro",
        target_format="avro",
        pattern_type="avro_to_avro_simple_filter",
        schema_reference="orders.cleaned.avro-value",
        inline_schema='{"type":"record","name":"Order","fields":[{"name":"order_id","type":"string"},{"name":"status","type":"string"}]}',
        filter_expression=FilterExpression(
            include_fields=["order_id", "status"],
            conditions=["status == 'ACTIVE'"],
        ),
        mapping_definition=MappingDefinition(mappings={"order_id": "order_id", "status": "status"}),
        sample_source_records=[
            {"order_id": "A1", "status": "ACTIVE"},
        ],
    )


@pytest.fixture
def mock_oauth_token(monkeypatch):
    """Patch token fetch to return a deterministic token."""
    async def _mock_token(credentials):
        del credentials
        return "test-token"

    monkeypatch.setattr("flink_codex.confluent_client.fetch_oauth_token", _mock_token)
    return _mock_token
