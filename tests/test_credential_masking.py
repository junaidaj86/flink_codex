"""Credential masking tests."""

from __future__ import annotations

from flink_codex.utils.credential_masking import masked_credential_log


def test_secret_str_not_in_repr(sample_credentials) -> None:
    assert "client-secret" not in repr(sample_credentials)


def test_masked_log_helper_hides_secret(sample_credentials) -> None:
    masked = masked_credential_log(sample_credentials)
    assert masked["oauth_client_secret"] == "***"


def test_masked_log_helper_shows_client_id(sample_credentials) -> None:
    masked = masked_credential_log(sample_credentials)
    assert masked["oauth_client_id"] == "client-id"


def test_masked_log_helper_shows_urls(sample_credentials) -> None:
    masked = masked_credential_log(sample_credentials)
    assert masked["identity_provider_url"] == "https://identity.example.com"
    assert masked["cloud_base_url"] == "https://api.confluent.cloud"
    assert masked["schema_registry_url"] == "https://schema.example.com"
