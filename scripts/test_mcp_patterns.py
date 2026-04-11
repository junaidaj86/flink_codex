#!/usr/bin/env python3
"""Exercise the Flink Codex MCP server over stdio for all supported patterns."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

PROJECT_DIR = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_DIR / "reports"
JSON_REPORT = REPORTS_DIR / "mcp_pattern_test_results.json"
MARKDOWN_REPORT = REPORTS_DIR / "MCP_PATTERN_TEST_RESULTS.md"
INLINE_AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "OrderRecord",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "customer_name", "type": ["null", "string"], "default": None},
            {"name": "amount", "type": ["null", "double"], "default": None},
            {"name": "amount_total", "type": ["null", "double"], "default": None},
            {"name": "total_amount", "type": ["null", "double"], "default": None},
            {"name": "status", "type": ["null", "string"], "default": None},
        ],
    }
)


def _server_params() -> StdioServerParameters:
    """Build stdio server parameters for the local FastMCP server."""
    return StdioServerParameters(
        command=str(PROJECT_DIR / ".venv" / "bin" / "fastmcp"),
        args=[
            "run",
            f"{PROJECT_DIR / 'src' / 'flink_codex' / 'server.py'}:mcp",
            "--transport",
            "stdio",
        ],
        env={"PYTHONPATH": str(PROJECT_DIR / "src")},
    )


def _parse_tool_result(payload: dict[str, Any]) -> Any:
    """Parse FastMCP tool output from the CallToolResult payload."""
    content = payload.get("content", [])
    if not content:
        return None
    if len(content) == 1 and content[0].get("type") == "text":
        text = content[0].get("text", "")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    parsed: list[Any] = []
    for item in content:
        text = item.get("text", "")
        try:
            parsed.append(json.loads(text))
        except json.JSONDecodeError:
            parsed.append(text)
    return parsed


async def _call_tool(session: ClientSession, name: str, arguments: dict[str, Any] | None = None) -> Any:
    """Call a tool and return parsed content."""
    result = await session.call_tool(name, arguments or {})
    payload = result.model_dump(mode="json")
    return {
        "is_error": payload.get("isError", False),
        "parsed": _parse_tool_result(payload),
        "raw": payload,
    }


def _scenario_requests() -> list[dict[str, Any]]:
    """Return deterministic test requests for all eight patterns."""
    return [
        {
            "customer_request": "Create a JSON Flink job from orders.raw to orders.cleaned, keep order_id and status, and filter where status is ACTIVE.",
            "request": {
                "source_topic": "orders.raw",
                "destination_topic": "orders.cleaned",
                "source_format": "json",
                "target_format": "json",
                "pattern_type": "json_to_json_simple_filter",
                "filter_expression": {
                    "include_fields": ["order_id", "status"],
                    "conditions": ["status == 'ACTIVE'"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order_id": "order_id",
                        "status": "status",
                    }
                },
                "sample_source_records": [
                    {"order_id": "A1", "status": "ACTIVE"},
                    {"order_id": "A2", "status": "INACTIVE"},
                ],
            },
        },
        {
            "customer_request": "Create a nested JSON Flink job from orders.raw.nested to orders.cleaned.nested, keep order.id and customer.name, and filter for order.id equal to A1.",
            "request": {
                "source_topic": "orders.raw.nested",
                "destination_topic": "orders.cleaned.nested",
                "source_format": "json",
                "target_format": "json",
                "pattern_type": "json_to_json_nested_filter",
                "filter_expression": {
                    "include_fields": ["order.id", "customer.name"],
                    "conditions": ["order.id == 'A1'"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order.id": "order_id",
                        "customer.name": "customer_name",
                    }
                },
                "sample_source_records": [
                    {"order": {"id": "A1"}, "customer": {"name": "Junaid"}},
                    {"order": {"id": "A2"}, "customer": {"name": "Sara"}},
                ],
            },
        },
        {
            "customer_request": "Create a Flink job from orders.raw.deep to orders.cleaned.flat, flatten order.id and amount.total with underscores, and filter where amount.total is greater than 0.",
            "request": {
                "source_topic": "orders.raw.deep",
                "destination_topic": "orders.cleaned.flat",
                "source_format": "json",
                "target_format": "json",
                "pattern_type": "nested_json_to_flat_json",
                "filter_expression": {
                    "include_fields": ["order.id", "amount.total"],
                    "conditions": ["amount.total > 0"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order.id": "order.id",
                        "amount.total": "amount.total",
                    }
                },
                "flatten_rules": {"separator": "_"},
                "sample_source_records": [
                    {"order": {"id": "A1"}, "amount": {"total": 99.5}},
                    {"order": {"id": "A2"}, "amount": {"total": 0}},
                ],
            },
        },
        {
            "customer_request": "Create an Avro Flink job from orders.raw.avro to orders.cleaned.avro, keep order_id and status, filter for ACTIVE, and use schema subject orders.cleaned.avro-value.",
            "request": {
                "source_topic": "orders.raw.avro",
                "destination_topic": "orders.cleaned.avro",
                "source_format": "avro",
                "target_format": "avro",
                "pattern_type": "avro_to_avro_simple_filter",
                "filter_expression": {
                    "include_fields": ["order_id", "status"],
                    "conditions": ["status == 'ACTIVE'"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order_id": "order_id",
                        "status": "status",
                    }
                },
                "inline_schema": INLINE_AVRO_SCHEMA,
                "sample_source_records": [
                    {"order_id": "A1", "status": "ACTIVE"},
                    {"order_id": "A2", "status": "INACTIVE"},
                ],
            },
        },
        {
            "customer_request": "Create a nested Avro Flink job from orders.raw.avro.nested to orders.cleaned.avro.nested, keep order.id and customer.name, filter for order.id equal to A1, and use schema subject orders.cleaned.avro.nested-value.",
            "request": {
                "source_topic": "orders.raw.avro.nested",
                "destination_topic": "orders.cleaned.avro.nested",
                "source_format": "avro",
                "target_format": "avro",
                "pattern_type": "nested_avro_to_nested_avro",
                "filter_expression": {
                    "include_fields": ["order.id", "customer.name"],
                    "conditions": ["order.id == 'A1'"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order.id": "order_id",
                        "customer.name": "customer_name",
                    }
                },
                "inline_schema": INLINE_AVRO_SCHEMA,
                "sample_source_records": [
                    {"order": {"id": "A1"}, "customer": {"name": "Junaid"}},
                    {"order": {"id": "A2"}, "customer": {"name": "Sara"}},
                ],
            },
        },
        {
            "customer_request": "Create a flattened Avro Flink job from orders.raw.avro.deep to orders.cleaned.avro.flat, flatten order.id and amount.total with underscores, filter where amount.total is greater than 0, and use schema subject orders.cleaned.avro.flat-value.",
            "request": {
                "source_topic": "orders.raw.avro.deep",
                "destination_topic": "orders.cleaned.avro.flat",
                "source_format": "avro",
                "target_format": "avro",
                "pattern_type": "nested_avro_to_flat_avro",
                "filter_expression": {
                    "include_fields": ["order.id", "amount.total"],
                    "conditions": ["amount.total > 0"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order.id": "order.id",
                        "amount.total": "amount.total",
                    }
                },
                "flatten_rules": {"separator": "_"},
                "inline_schema": INLINE_AVRO_SCHEMA,
                "sample_source_records": [
                    {"order": {"id": "A1"}, "amount": {"total": 50}},
                    {"order": {"id": "A2"}, "amount": {"total": -1}},
                ],
            },
        },
        {
            "customer_request": "Create a Flink job from JSON topic orders.raw.cross to Avro topic orders.cleaned.cross.avro, map order.id to order_id and amount.total to total_amount, filter where amount.total is greater than 0, and use schema subject orders.cleaned.cross.avro-value.",
            "request": {
                "source_topic": "orders.raw.cross",
                "destination_topic": "orders.cleaned.cross.avro",
                "source_format": "json",
                "target_format": "avro",
                "pattern_type": "nested_json_to_avro",
                "filter_expression": {
                    "include_fields": ["order.id", "amount.total"],
                    "conditions": ["amount.total > 0"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order.id": "order_id",
                        "amount.total": "total_amount",
                    }
                },
                "inline_schema": INLINE_AVRO_SCHEMA,
                "sample_source_records": [
                    {"order": {"id": "A1"}, "amount": {"total": 120.5}},
                    {"order": {"id": "A2"}, "amount": {"total": 0}},
                ],
            },
        },
        {
            "customer_request": "Create a Flink job from JSON topic orders.raw.cross.simple to Avro topic orders.cleaned.cross.simple.avro, keep order_id and amount, filter where amount is greater than 0, and use schema subject orders.cleaned.cross.simple.avro-value.",
            "request": {
                "source_topic": "orders.raw.cross.simple",
                "destination_topic": "orders.cleaned.cross.simple.avro",
                "source_format": "json",
                "target_format": "avro",
                "pattern_type": "json_to_avro",
                "filter_expression": {
                    "include_fields": ["order_id", "amount"],
                    "conditions": ["amount > 0"],
                },
                "mapping_definition": {
                    "mappings": {
                        "order_id": "order_id",
                        "amount": "amount",
                    }
                },
                "inline_schema": INLINE_AVRO_SCHEMA,
                "sample_source_records": [
                    {"order_id": "A1", "amount": 12.0},
                    {"order_id": "A2", "amount": 0},
                ],
            },
        },
    ]


def _build_markdown(report: dict[str, Any]) -> str:
    """Render a markdown summary from the JSON report."""
    lines = [
        "# MCP Pattern Test Results",
        "",
        f"Generated at: `{report['generated_at']}`",
        f"MCP server: `{report['server_name']}`",
        f"Discovered tools: `{len(report['discovered_tools'])}`",
        f"Pattern runs: `{len(report['scenario_results'])}`",
        "",
        "## Tool Discovery",
        "",
        "```json",
        json.dumps(report["discovered_tools"], indent=2),
        "```",
        "",
        "## Scenario Results",
        "",
    ]

    for scenario in report["scenario_results"]:
        lines.extend(
            [
                f"### `{scenario['pattern_type']}`",
                "",
                "Customer request:",
                f"`{scenario['customer_request']}`",
                "",
                "Payload sent over MCP:",
                "```json",
                json.dumps(scenario["request"], indent=2),
                "```",
                "",
                "Observed MCP results:",
                "```json",
                json.dumps(scenario["results"], indent=2),
                "```",
                "",
            ]
        )

    return "\n".join(lines) + "\n"


async def main() -> None:
    """Run the end-to-end MCP pattern tests and save reports."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    params = _server_params()
    scenarios = _scenario_requests()

    async with stdio_client(params) as streams:
        async with ClientSession(*streams) as session:
            init = await session.initialize()
            tools_result = await session.list_tools()
            discovered_tools = [tool.name for tool in tools_result.tools]

            scenario_results: list[dict[str, Any]] = []
            for scenario in scenarios:
                request = scenario["request"]
                validate = await _call_tool(session, "validate_job_request", {"request": request})
                normalized = await _call_tool(session, "generate_normalized_request", {"request": request})
                preview = await _call_tool(session, "generate_transform_preview", {"request": request})
                spec = await _call_tool(session, "generate_job_spec", {"request": normalized["parsed"]})
                sql = await _call_tool(session, "generate_flink_sql", {"spec": spec["parsed"]})

                scenario_results.append(
                    {
                        "pattern_type": request["pattern_type"],
                        "customer_request": scenario["customer_request"],
                        "request": request,
                        "results": {
                            "validate_job_request": validate["parsed"],
                            "generate_normalized_request": normalized["parsed"],
                            "generate_transform_preview": preview["parsed"],
                            "generate_job_spec": spec["parsed"],
                            "generate_flink_sql": sql["parsed"],
                        },
                    }
                )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "server_name": "flink-clean-job-creator",
        "discovered_tools": discovered_tools,
        "scenario_results": scenario_results,
    }
    JSON_REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    MARKDOWN_REPORT.write_text(_build_markdown(report), encoding="utf-8")
    print(str(JSON_REPORT))
    print(str(MARKDOWN_REPORT))


if __name__ == "__main__":
    asyncio.run(main())
