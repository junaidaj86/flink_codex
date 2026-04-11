"""Deterministic preview generation from user-supplied records."""

from __future__ import annotations

from typing import Any

from .catalog import catalog
from .models import JobRequest, SamplePreviewTable, TransformPreview
from .validation import validation_error


def _get_path(record: dict[str, Any], path: str) -> Any:
    """Resolve a dot-notation path from a nested record."""
    current: Any = record
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(path)
        current = current[part]
    return current


def _to_literal(value: str) -> Any:
    """Parse a simple literal from a filter expression."""
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    if "." in value:
        return float(value)
    return int(value)


def _evaluate_condition(record: dict[str, Any], condition: str) -> bool:
    """Evaluate a validated simple condition against a record."""
    for operator in ("==", "!=", ">=", "<=", ">", "<"):
        if operator in condition:
            left, right = condition.split(operator, 1)
            field = left.strip()
            expected = _to_literal(right.strip())
            actual = _get_path(record, field)
            if operator == "==":
                return actual == expected
            if operator == "!=":
                return actual != expected
            if operator == ">=":
                return actual >= expected
            if operator == "<=":
                return actual <= expected
            if operator == ">":
                return actual > expected
            if operator == "<":
                return actual < expected
    raise ValueError(f"Unsupported condition: {condition}")


def _source_columns(request: JobRequest) -> list[str]:
    """Return source columns for preview rendering."""
    if request.filter_expression and request.filter_expression.include_fields:
        return list(request.filter_expression.include_fields)
    if request.mapping_definition:
        return list(request.mapping_definition.mappings.keys())
    if request.sample_source_records:
        first = request.sample_source_records[0]
        return list(_flatten_record(first).keys())
    return []


def _flatten_record(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested record into dot-notation paths."""
    result: dict[str, Any] = {}
    for key, value in record.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(_flatten_record(value, path))
        else:
            result[path] = value
    return result


def _destination_columns(request: JobRequest) -> list[str]:
    """Return destination columns after mapping and optional flattening."""
    pattern = catalog.by_id(request.pattern_type)
    mappings = request.mapping_definition.mappings if request.mapping_definition else {}
    if mappings:
        columns = list(mappings.values())
        if pattern and pattern.requires_flatten_rules and request.flatten_rules:
            return [column.replace(".", request.flatten_rules.separator) for column in columns]
        return columns
    source_columns = _source_columns(request)
    if pattern and pattern.requires_flatten_rules and request.flatten_rules:
        return [column.replace(".", request.flatten_rules.separator) for column in source_columns]
    return list(source_columns)


def _transform_record(request: JobRequest, record: dict[str, Any]) -> dict[str, Any]:
    """Apply mapping and optional flattening to a record."""
    pattern = catalog.by_id(request.pattern_type)
    fields = request.filter_expression.include_fields if request.filter_expression else list(_flatten_record(record).keys())
    transformed: dict[str, Any] = {}
    mappings = request.mapping_definition.mappings if request.mapping_definition else {}

    for field in fields:
        value = _get_path(record, field)
        destination = mappings.get(field, field)
        if pattern and pattern.requires_flatten_rules and request.flatten_rules:
            destination = destination.replace(".", request.flatten_rules.separator)
            if destination == field:
                destination = field.replace(".", request.flatten_rules.separator)
        transformed[destination] = value
    return transformed


async def generate_transform_preview(request: JobRequest):
    """Generate a deterministic preview from user-supplied sample records only."""
    if not request.sample_source_records:
        return validation_error("SAMPLE_RECORDS_MISSING", "Preview requires sample_source_records.")

    source_columns = _source_columns(request)
    destination_columns = _destination_columns(request)
    source_rows: list[list[Any]] = []
    destination_rows: list[list[Any]] = []
    invalid_records: list[int] = []
    filtered_out_count = 0

    for index, record in enumerate(request.sample_source_records):
        flattened = _flatten_record(record)
        source_rows.append([flattened.get(column) for column in source_columns])
        try:
            conditions = request.filter_expression.conditions if request.filter_expression else []
            if conditions and not all(_evaluate_condition(record, condition) for condition in conditions):
                filtered_out_count += 1
                continue
            transformed = _transform_record(request, record)
        except KeyError:
            invalid_records.append(index)
            continue
        except (TypeError, ValueError):
            invalid_records.append(index)
            continue
        destination_rows.append([transformed.get(column) for column in destination_columns])

    mapping_explanation = []
    if request.mapping_definition:
        mapping_explanation = [
            f"{source} -> {destination}"
            for source, destination in request.mapping_definition.mappings.items()
        ]
    else:
        mapping_explanation = [f"{column} -> {column}" for column in destination_columns]

    return TransformPreview(
        pattern_type=request.pattern_type,
        source_table=SamplePreviewTable(columns=source_columns, rows=source_rows),
        destination_table=SamplePreviewTable(columns=destination_columns, rows=destination_rows),
        transformation_summary=f"Applied pattern {request.pattern_type} to {len(request.sample_source_records)} source records.",
        mapping_explanation=mapping_explanation,
        filtered_out_count=filtered_out_count,
        invalid_records=invalid_records,
    )
