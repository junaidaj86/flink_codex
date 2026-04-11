"""Helpers for safe credential logging."""

from __future__ import annotations

from ..models import ConfluentCloudCredentials


def masked_credential_log(credentials: ConfluentCloudCredentials) -> dict[str, str]:
    """Return a logging-safe representation of Confluent credentials."""
    return {
        "oauth_client_id": credentials.oauth_client_id,
        "oauth_client_secret": "***",
        "identity_provider_url": credentials.identity_provider_url,
        "oauth_audience": credentials.oauth_audience,
        "cloud_base_url": credentials.cloud_base_url,
        "schema_registry_url": credentials.schema_registry_url,
    }
