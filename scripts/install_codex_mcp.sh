#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_NAME="${1:-flink-codex}"
VENV_FASTMCP="${PROJECT_DIR}/.venv/bin/fastmcp"
SERVER_FILE="${PROJECT_DIR}/src/flink_codex/server.py:mcp"
PYTHONPATH_DIR="${PROJECT_DIR}/src"

if ! command -v codex >/dev/null 2>&1; then
  echo "Error: codex CLI is not installed or not on PATH." >&2
  exit 1
fi

if [[ ! -x "${VENV_FASTMCP}" ]]; then
  echo "Error: ${VENV_FASTMCP} not found or not executable." >&2
  echo "Create the virtualenv and install dependencies first, for example:" >&2
  echo "  pip install -e '.[dev]'" >&2
  exit 1
fi

if codex mcp get "${SERVER_NAME}" >/dev/null 2>&1; then
  echo "MCP server '${SERVER_NAME}' already exists in Codex."
  echo "Removing existing entry so it can be reinstalled with current paths..."
  codex mcp remove "${SERVER_NAME}"
fi

echo "Installing MCP server '${SERVER_NAME}' into Codex..."
codex mcp add "${SERVER_NAME}" \
  --env "PYTHONPATH=${PYTHONPATH_DIR}" \
  -- \
  "${VENV_FASTMCP}" \
  run \
  "${SERVER_FILE}" \
  --transport \
  stdio

echo
echo "Installed MCP server '${SERVER_NAME}'."
echo "Verify with:"
echo "  codex mcp list"
echo "  codex mcp get ${SERVER_NAME}"
