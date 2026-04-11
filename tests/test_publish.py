"""Publish tests."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from flink_codex.models import JobSpec
from flink_codex.publisher import dry_run_publish, publish_job


class PublishClientStub:
    """Async client stub for publish tests."""

    calls: list[dict] = []
    queued_responses: list[httpx.Response] = []

    def __init__(self, *args, **kwargs) -> None:
        del args, kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb

    async def request(self, method, url, headers=None, data=None, json=None, timeout=None):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "data": data, "json": json, "timeout": timeout}
        )
        return self.queued_responses.pop(0)


def _spec(status: str = "pending") -> JobSpec:
    return JobSpec(
        spec_id="spec-1",
        pattern_type="json_to_json_simple_filter",
        flink_sql="SELECT * FROM `orders.raw`;",
        source_topic="orders.raw",
        destination_topic="orders.cleaned",
        source_format="json",
        target_format="json",
        schema_reference=None,
        validation_status=status,
        created_at=datetime.now(timezone.utc),
    )


@pytest.fixture(autouse=True)
def reset_publish_stub():
    PublishClientStub.calls = []
    PublishClientStub.queued_responses = []


@pytest.fixture
def patch_publish_client(monkeypatch):
    monkeypatch.setattr("flink_codex.confluent_client.httpx.AsyncClient", PublishClientStub)

    async def _token(credentials):
        del credentials
        return "test-token"

    monkeypatch.setattr("flink_codex.confluent_client.fetch_oauth_token", _token)


@pytest.mark.asyncio
async def test_publish_without_prior_validation_fails(sample_credentials) -> None:
    result = await publish_job(_spec("pending"), sample_credentials)
    assert result.error_code == "PUBLISH_PRECONDITION_FAILED"


@pytest.mark.asyncio
async def test_dry_run_makes_zero_http_calls(sample_credentials) -> None:
    result = await dry_run_publish(_spec("passed"), sample_credentials)
    assert result.passed is True
    assert PublishClientStub.calls == []


@pytest.mark.asyncio
async def test_publish_uses_bearer_auth(sample_credentials, patch_publish_client) -> None:
    PublishClientStub.queued_responses = [httpx.Response(200, json={"id": "stmt-1"})]
    await publish_job(_spec("passed"), sample_credentials)
    auth = PublishClientStub.calls[0]["headers"]["Authorization"]
    assert auth.startswith("Bearer ")


@pytest.mark.asyncio
async def test_publish_calls_correct_endpoint(sample_credentials, patch_publish_client) -> None:
    PublishClientStub.queued_responses = [httpx.Response(200, json={"id": "stmt-1"})]
    await publish_job(_spec("passed"), sample_credentials)
    assert PublishClientStub.calls[0]["url"] == "https://api.confluent.cloud/sql/v1/statements"
