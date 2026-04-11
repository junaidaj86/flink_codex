"""Async Confluent Cloud client helpers."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from .models import ConfluentCloudCredentials, PublishResult
from .utils.credential_masking import masked_credential_log

LOGGER = logging.getLogger(__name__)
_TOKEN_CACHE: dict[tuple[str, str], tuple[str, float]] = {}
_TOKEN_LOCK = asyncio.Lock()


class ConfluentClientError(Exception):
    """Structured client exception with canonical error code."""

    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
) -> httpx.Response:
    """Issue an HTTP request with timeout and a single 429 retry."""
    response = await client.request(
        method,
        url,
        headers=headers,
        data=data,
        json=json_body,
        timeout=10.0,
    )
    if response.status_code == 429:
        await asyncio.sleep(1)
        response = await client.request(
            method,
            url,
            headers=headers,
            data=data,
            json=json_body,
            timeout=10.0,
        )
    return response


def _require_credentials(credentials: ConfluentCloudCredentials) -> None:
    """Raise if any required OAuth field is missing."""
    if (
        not credentials.oauth_client_id
        or not credentials.oauth_client_secret.get_secret_value()
        or not credentials.identity_provider_url
        or not credentials.oauth_audience
    ):
        raise ConfluentClientError("CREDENTIAL_MISSING", "Required Confluent Cloud credential fields are missing.")


async def fetch_oauth_token(credentials: ConfluentCloudCredentials) -> str:
    """Fetch and cache an OAuth token keyed by client id and audience."""
    _require_credentials(credentials)
    cache_key = (credentials.oauth_client_id, credentials.oauth_audience)
    now = time.monotonic()

    async with _TOKEN_LOCK:
        cached = _TOKEN_CACHE.get(cache_key)
        if cached is not None and cached[1] > now:
            return cached[0]

    url = f"{credentials.identity_provider_url}/oauth2/token"
    form_data = {
        "grant_type": "client_credentials",
        "client_id": credentials.oauth_client_id,
        "client_secret": credentials.oauth_client_secret.get_secret_value(),
        "audience": credentials.oauth_audience,
    }
    LOGGER.info("Fetching OAuth token for %s", masked_credential_log(credentials))

    try:
        async with httpx.AsyncClient() as client:
            response = await _request_with_retry(client, "POST", url, data=form_data)
    except httpx.HTTPError as exc:
        raise ConfluentClientError("OAUTH_TOKEN_FETCH_FAILED", f"OAuth token fetch failed: {exc.__class__.__name__}") from exc

    if response.status_code in {401, 403}:
        raise ConfluentClientError("CREDENTIAL_INVALID", "OAuth client credentials were rejected.")
    if response.status_code >= 400:
        raise ConfluentClientError("OAUTH_TOKEN_FETCH_FAILED", "OAuth token endpoint returned an unexpected error.")

    payload = response.json()
    token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))
    if not token:
        raise ConfluentClientError("OAUTH_TOKEN_FETCH_FAILED", "OAuth token endpoint did not return an access token.")

    async with _TOKEN_LOCK:
        _TOKEN_CACHE[cache_key] = (token, time.monotonic() + max(expires_in - 1, 0))
    return token


async def _authorized_request(
    method: str,
    url: str,
    credentials: ConfluentCloudCredentials,
    *,
    json_body: dict[str, Any] | None = None,
) -> httpx.Response:
    """Send an authorized request and refresh the token once on auth failure."""
    token = await fetch_oauth_token(credentials)
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient() as client:
        try:
            response = await _request_with_retry(client, method, url, headers=headers, json_body=json_body)
        except httpx.HTTPError as exc:
            raise ConfluentClientError("SCHEMA_FETCH_FAILED", f"Confluent request failed: {exc.__class__.__name__}") from exc

    if response.status_code in {401, 403}:
        async with _TOKEN_LOCK:
            _TOKEN_CACHE.pop((credentials.oauth_client_id, credentials.oauth_audience), None)
        token = await fetch_oauth_token(credentials)
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient() as client:
            try:
                response = await _request_with_retry(client, method, url, headers=headers, json_body=json_body)
            except httpx.HTTPError as exc:
                raise ConfluentClientError("SCHEMA_FETCH_FAILED", f"Confluent request failed: {exc.__class__.__name__}") from exc
    return response


async def publish_statement(
    sql: str,
    credentials: ConfluentCloudCredentials,
) -> PublishResult:
    """Publish a Flink SQL statement to Confluent Cloud."""
    url = f"{credentials.cloud_base_url}/sql/v1/statements"
    response = await _authorized_request("POST", url, credentials, json_body={"statement": sql})
    if response.status_code in {401, 403}:
        return PublishResult(published=False, error_code="CREDENTIAL_INVALID", error_message="OAuth credentials were rejected.")
    if response.status_code >= 400:
        return PublishResult(
            published=False,
            error_code="SCHEMA_FETCH_FAILED",
            error_message="Confluent Cloud statement publish failed.",
        )

    payload = response.json()
    return PublishResult(
        published=True,
        statement_id=payload.get("id") or payload.get("statement_id"),
        error_code=None,
        error_message=None,
    )
