# Flink Codex

Deterministic Codex + MCP project for generating, validating, previewing, and optionally publishing Flink SQL clean jobs on Confluent Cloud.

## Features

- Eight canonical transformation patterns across JSON and Avro
- Deterministic validation order and canonical error taxonomy
- OAuth client credentials for Confluent Cloud publish
- In-memory token cache keyed by `(oauth_client_id, oauth_audience)`
- Jinja2-driven SQL generation
- Preview generation from user-supplied records only
- FastMCP decorator-based tool server
- Async-first HTTP integration with `httpx`

## Project layout

- `src/flink_codex/models.py`: Pydantic v2 domain models
- `src/flink_codex/catalog.py`: pattern catalog and required field metadata
- `src/flink_codex/validation.py`: deterministic request validation
- `src/flink_codex/confluent_client.py`: OAuth and publish
- `src/flink_codex/preview.py`: deterministic preview generation
- `src/flink_codex/sql_generator.py`: Jinja2 SQL and markdown rendering
- `src/flink_codex/publisher.py`: dry-run and publish orchestration
- `src/flink_codex/server.py`: FastAPI + FastMCP tool entrypoint

## Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Run tests

```bash
pytest
```

## Run the API

```bash
uvicorn flink_codex.server:app --reload
```

## Security notes

- OAuth secrets are modeled with `SecretStr`
- Tokens are cached in memory and refreshed on expiry
- Request headers are never logged
- Published errors are sanitized to avoid leaking credentials
