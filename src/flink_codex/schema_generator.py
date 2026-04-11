"""Deterministic destination schema generation."""

from __future__ import annotations

import json
from typing import Any

from .models import JobRequest, NormalizedJobRequest


RequestLike = JobRequest | NormalizedJobRequest


STRUCTURE_CHANGING_PATTERNS = {
    "nested_json_to_flat_json",
    "nested_avro_to_flat_avro",
    "json_to_avro",
    "nested_json_to_avro",
}


def flatten_record(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested record into dot-notation paths."""
    result: dict[str, Any] = {}
    for key, value in record.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten_record(value, path))
        else:
            result[path] = value
    return result


def normalize_destination_name(request: RequestLike, destination_name: str) -> str:
    """Normalize destination names for flattened patterns."""
    if request.flatten_rules is None:
        return destination_name
    return destination_name.replace(".", request.flatten_rules.separator)


def infer_scalar_type(values: list[Any]) -> str:
    """Infer a deterministic scalar type from observed values."""
    present = [value for value in values if value is not None]
    if not present:
        return "string"
    if all(isinstance(value, bool) for value in present):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in present):
        return "long"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in present):
        return "double"
    return "string"


def avro_to_sql_type(avro_type: str) -> str:
    """Map Avro scalar types to Flink SQL column types."""
    return {
        "boolean": "BOOLEAN",
        "long": "BIGINT",
        "double": "DOUBLE",
        "string": "STRING",
    }.get(avro_type, "STRING")


def avro_to_json_type(avro_type: str) -> str:
    """Map Avro scalar types to JSON Schema types."""
    return {
        "boolean": "boolean",
        "long": "integer",
        "double": "number",
        "string": "string",
    }.get(avro_type, "string")


def json_schema_to_sql_type(schema: dict[str, Any]) -> str:
    """Map a JSON Schema fragment to a Flink SQL type."""
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        schema_type = next((item for item in schema_type if item != "null"), "string")

    if schema_type == "object":
        fields = [
            f"{name} {json_schema_to_sql_type(child)}"
            for name, child in schema.get("properties", {}).items()
        ]
        return f"ROW<{', '.join(fields)}>"
    if schema_type == "array":
        item_schema = schema.get("items", {"type": "string"})
        return f"ARRAY<{json_schema_to_sql_type(item_schema)}>"
    if schema_type == "integer":
        return "BIGINT"
    if schema_type == "number":
        return "DOUBLE"
    if schema_type == "boolean":
        return "BOOLEAN"
    return "STRING"


def json_schema_to_avro_type(schema: dict[str, Any]) -> Any:
    """Map a JSON Schema fragment to an Avro type."""
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        schema_type = next((item for item in schema_type if item != "null"), "string")

    if schema_type == "object":
        return {
            "type": "record",
            "name": schema.get("title", "NestedRecord"),
            "fields": [
                {
                    "name": name,
                    "type": ["null", json_schema_to_avro_type(child)],
                    "default": None,
                }
                for name, child in schema.get("properties", {}).items()
            ],
        }
    if schema_type == "array":
        return {"type": "array", "items": json_schema_to_avro_type(schema.get("items", {"type": "string"}))}
    if schema_type == "integer":
        return "long"
    if schema_type == "number":
        return "double"
    if schema_type == "boolean":
        return "boolean"
    return "string"


def _infer_type_for_path(request: RequestLike, source_path: str) -> str:
    """Infer a scalar type for a flat path from schema or sample records."""
    schema_node = get_schema_node(request.source_schema, source_path) if request.source_schema else None
    if schema_node is not None:
        return json_type_to_avro_type(schema_node)
    flattened_records = [flatten_record(record) for record in (request.sample_source_records or [])]
    observed_values = [record.get(source_path) for record in flattened_records]
    return infer_scalar_type(observed_values)


def json_type_to_avro_type(schema_node: dict[str, Any]) -> str:
    """Map a JSON schema node to a scalar avro-ish type."""
    schema_type = schema_node.get("type")
    if isinstance(schema_type, list):
        schema_type = next((item for item in schema_type if item != "null"), "string")
    if schema_type == "integer":
        return "long"
    if schema_type == "number":
        return "double"
    if schema_type == "boolean":
        return "boolean"
    return "string"


def get_schema_node(schema: dict[str, Any] | None, path: str) -> dict[str, Any] | None:
    """Resolve a dot path into a schema node."""
    if schema is None:
        return None
    current = schema
    for part in path.split("."):
        if current.get("type") != "object":
            return None
        current = current.get("properties", {}).get(part)
        if current is None:
            return None
    return current


def destination_columns(request: RequestLike) -> list[dict[str, str]]:
    """Build deterministic destination column metadata for flat outputs."""
    include_fields = request.filter_expression.include_fields if request.filter_expression else []
    mappings = request.mapping_definition.mappings if request.mapping_definition else {field: field for field in include_fields}

    columns: list[dict[str, str]] = []
    for source_path, destination_name in mappings.items():
        normalized_name = normalize_destination_name(request, destination_name)
        avro_type = _infer_type_for_path(request, source_path)
        columns.append(
            {
                "source_path": source_path,
                "name": normalized_name,
                "avro_type": avro_type,
                "type": avro_to_sql_type(avro_type),
                "sql_type": avro_to_sql_type(avro_type),
                "json_type": avro_to_json_type(avro_type),
            }
        )
    return columns


def _build_path_tree(paths: list[str]) -> dict[str, Any]:
    """Build a nested tree from dot paths."""
    tree: dict[str, Any] = {}
    for path in paths:
        current = tree
        for part in path.split("."):
            current = current.setdefault(part, {})
    return tree


def _prune_schema(schema: dict[str, Any], tree: dict[str, Any], title: str | None = None) -> dict[str, Any]:
    """Prune a JSON schema to the requested path tree."""
    properties: dict[str, Any] = {}
    for name, subtree in tree.items():
        child = schema.get("properties", {}).get(name, {"type": "string"})
        child_type = child.get("type")
        if isinstance(child_type, list):
            child_type = next((item for item in child_type if item != "null"), "string")

        if child_type == "object" and subtree:
            properties[name] = _prune_schema(child, subtree, title=name)
        else:
            properties[name] = child

    pruned: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
        "additionalProperties": False,
    }
    if title:
        pruned["title"] = title
    return pruned


def nested_json_schema(request: RequestLike) -> dict[str, Any] | None:
    """Return a nested destination schema for nested JSON outputs."""
    if request.pattern_type != "json_to_json_nested_filter" or request.source_schema is None:
        return None
    include_fields = request.filter_expression.include_fields if request.filter_expression else []
    if not include_fields:
        return None
    return _prune_schema(
        request.source_schema,
        _build_path_tree(include_fields),
        title=request.destination_topic.replace(".", "_"),
    )


def render_column_definitions(request: RequestLike) -> str:
    """Render CREATE TABLE column definitions."""
    nested_schema = nested_json_schema(request)
    if nested_schema is not None:
        lines = [
            f"{name} {json_schema_to_sql_type(child)}"
            for name, child in nested_schema.get("properties", {}).items()
        ]
        return ",\n".join(lines)

    return ",\n".join(
        f"{column['name']} {column['type']}"
        for column in destination_columns(request)
    )


def _render_nested_object_expr(field_name: str, schema: dict[str, Any], source_prefix: str) -> str:
    """Render a nested object expression with an explicit ROW cast."""
    items: list[str] = []
    for child_name, child_schema in schema.get("properties", {}).items():
        child_path = f"{source_prefix}.{child_name}" if source_prefix else child_name
        child_type = child_schema.get("type")
        if isinstance(child_type, list):
            child_type = next((item for item in child_type if item != "null"), "string")

        if child_type == "object":
            items.append(_render_nested_object_expr(child_name, child_schema, child_path))
        else:
            items.append(child_path)

    row_type = json_schema_to_sql_type(schema)
    return f"CAST(ROW({', '.join(items)}) AS {row_type}) AS {field_name}"


def render_select_expressions(request: RequestLike) -> str:
    """Render INSERT SELECT expressions."""
    nested_schema = nested_json_schema(request)
    if nested_schema is not None:
        expressions: list[str] = []
        for name, child_schema in nested_schema.get("properties", {}).items():
            child_type = child_schema.get("type")
            if isinstance(child_type, list):
                child_type = next((item for item in child_type if item != "null"), "string")
            if child_type == "object":
                expressions.append(_render_nested_object_expr(name, child_schema, name))
            else:
                expressions.append(name)
        return ",\n".join(expressions)

    return ",\n".join(
        f"{column['source_path']} AS {column['name']}"
        for column in destination_columns(request)
    )


def destination_structure_differs(request: RequestLike) -> bool:
    """Return whether the destination structure differs from the source selection."""
    if request.pattern_type in STRUCTURE_CHANGING_PATTERNS:
        return True
    if request.pattern_type == "json_to_json_nested_filter":
        return False
    include_fields = request.filter_expression.include_fields if request.filter_expression else []
    destination_names = [column["name"] for column in destination_columns(request)]
    if request.source_format != request.target_format:
        return True
    return include_fields != destination_names


def generate_json_schema(request: RequestLike) -> dict[str, Any] | None:
    """Generate a destination JSON Schema when the structure differs."""
    if request.target_format != "json":
        return None
    nested_schema = nested_json_schema(request)
    if nested_schema is not None:
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": request.destination_topic.replace(".", "_"),
            **nested_schema,
        }
    if not destination_structure_differs(request):
        return None

    properties = {
        column["name"]: {"type": column["json_type"]}
        for column in destination_columns(request)
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": request.destination_topic.replace(".", "_"),
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
        "additionalProperties": False,
    }


def generate_avro_schema(request: RequestLike) -> str | None:
    """Generate a destination Avro schema when needed."""
    if request.target_format != "avro":
        return None
    record_name = "".join(part.capitalize() for part in request.destination_topic.replace("-", "_").split("."))
    nested_schema = nested_json_schema(request)
    if nested_schema is not None:
        schema = {
            "type": "record",
            "name": record_name or "GeneratedRecord",
            "fields": [
                {
                    "name": name,
                    "type": ["null", json_schema_to_avro_type(child)],
                    "default": None,
                }
                for name, child in nested_schema.get("properties", {}).items()
            ],
        }
        return json.dumps(schema)

    schema = {
        "type": "record",
        "name": record_name or "GeneratedRecord",
        "fields": [
            {
                "name": column["name"],
                "type": ["null", column["avro_type"]],
                "default": None,
            }
            for column in destination_columns(request)
        ],
    }
    return json.dumps(schema)
