"""OAuth client tests."""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from flink_codex.confluent_client import _TOKEN_CACHE, ConfluentClientError, fetch_oauth_token
from flink_codex.models import ConfluentCloudCredentials


class RecordingAsyncClient:
    """Minimal async client stub for token tests."""

    calls: list[dict] = []
    queued_responses: list[httpx.Response] = []
    queued_exception: Exception | None = None

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
        if self.queued_exception is not None:
            raise self.queued_exception
        return self.queued_responses.pop(0)


@pytest.fixture(autouse=True)
def clear_cache():
    _TOKEN_CACHE.clear()
    RecordingAsyncClient.calls = []
    RecordingAsyncClient.queued_responses = []
    RecordingAsyncClient.queued_exception = None


@pytest.fixture
def patched_async_client(monkeypatch):
    monkeypatch.setattr("flink_codex.confluent_client.httpx.AsyncClient", RecordingAsyncClient)


@pytest.mark.asyncio
async def test_token_fetch_posts_to_correct_endpoint(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    RecordingAsyncClient.queued_responses = [httpx.Response(200, json={"access_token": "token-a", "expires_in": 60})]
    await fetch_oauth_token(sample_credentials)
    assert RecordingAsyncClient.calls[0]["url"] == "https://identity.example.com/oauth2/token"


@pytest.mark.asyncio
async def test_token_fetch_sends_client_credentials_grant(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    RecordingAsyncClient.queued_responses = [httpx.Response(200, json={"access_token": "token-a", "expires_in": 60})]
    await fetch_oauth_token(sample_credentials)
    sent = RecordingAsyncClient.calls[0]["data"]
    assert sent["grant_type"] == "client_credentials"
    assert sent["client_id"] == "client-id"
    assert sent["client_secret"] == "client-secret"
    assert sent["audience"] == "audience-a"


@pytest.mark.asyncio
async def test_token_cached_after_first_fetch(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    RecordingAsyncClient.queued_responses = [httpx.Response(200, json={"access_token": "token-a", "expires_in": 60})]
    token1 = await fetch_oauth_token(sample_credentials)
    token2 = await fetch_oauth_token(sample_credentials)
    assert token1 == token2 == "token-a"
    assert len(RecordingAsyncClient.calls) == 1


@pytest.mark.asyncio
async def test_token_cache_keyed_per_client_and_audience(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    other = sample_credentials.model_copy(update={"oauth_client_id": "client-id-2"})
    RecordingAsyncClient.queued_responses = [
        httpx.Response(200, json={"access_token": "token-a", "expires_in": 60}),
        httpx.Response(200, json={"access_token": "token-b", "expires_in": 60}),
    ]
    await fetch_oauth_token(sample_credentials)
    await fetch_oauth_token(other)
    assert len(RecordingAsyncClient.calls) == 2


@pytest.mark.asyncio
async def test_token_expiry_triggers_refetch(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    RecordingAsyncClient.queued_responses = [
        httpx.Response(200, json={"access_token": "token-a", "expires_in": 1}),
        httpx.Response(200, json={"access_token": "token-b", "expires_in": 1}),
    ]
    await fetch_oauth_token(sample_credentials)
    await asyncio.sleep(2)
    await fetch_oauth_token(sample_credentials)
    assert len(RecordingAsyncClient.calls) == 2


@pytest.mark.asyncio
async def test_401_raises_credential_invalid(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    RecordingAsyncClient.queued_responses = [httpx.Response(401, json={})]
    with pytest.raises(ConfluentClientError) as exc:
        await fetch_oauth_token(sample_credentials)
    assert exc.value.error_code == "CREDENTIAL_INVALID"


@pytest.mark.asyncio
async def test_network_error_raises_oauth_token_fetch_failed(sample_credentials: ConfluentCloudCredentials, patched_async_client) -> None:
    RecordingAsyncClient.queued_exception = httpx.ConnectError("boom")
    with pytest.raises(ConfluentClientError) as exc:
        await fetch_oauth_token(sample_credentials)
    assert exc.value.error_code == "OAUTH_TOKEN_FETCH_FAILED"


@pytest.mark.asyncio
async def test_token_not_in_logs(sample_credentials: ConfluentCloudCredentials, patched_async_client, caplog) -> None:
    caplog.set_level(logging.INFO)
    RecordingAsyncClient.queued_responses = [httpx.Response(200, json={"access_token": "super-secret-token", "expires_in": 60})]
    await fetch_oauth_token(sample_credentials)
    assert "super-secret-token" not in caplog.text


@pytest.mark.asyncio
async def test_client_secret_not_in_logs(sample_credentials: ConfluentCloudCredentials, patched_async_client, caplog) -> None:
    caplog.set_level(logging.INFO)
    RecordingAsyncClient.queued_responses = [httpx.Response(200, json={"access_token": "token-a", "expires_in": 60})]
    await fetch_oauth_token(sample_credentials)
    assert "client-secret" not in caplog.text
