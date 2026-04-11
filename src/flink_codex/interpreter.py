"""Deterministic natural-language request interpretation."""

from __future__ import annotations

import re
from typing import Any

from .models import FilterExpression, FlattenRules, JobRequest, MappingDefinition, ValidationResult

TOPIC_STOP_WORDS = (" where ", " filter ", " into ", " to ", " and ", " with ", " using ", " schema")


def interpretation_error(message: str) -> ValidationResult:
    """Return a deterministic interpretation failure."""
    return ValidationResult(valid=False, error_code="MISSING_REQUIRED_FIELD", error_message=message)


def _normalize_token(value: str) -> str:
    """Normalize text for fuzzy field matching."""
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _slugify_topic(value: str) -> str:
    """Normalize topic names from customer text."""
    cleaned = value.strip().strip(".,;:")
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned.lower()


def _extract_segment(query: str, pattern: str) -> str | None:
    """Extract a topic-like segment from the query."""
    match = re.search(pattern, query, flags=re.IGNORECASE)
    if match is None:
        return None
    segment = match.group(1).strip()
    lowered = segment.lower()
    cut_index = len(segment)
    for stop_word in TOPIC_STOP_WORDS:
        idx = lowered.find(stop_word)
        if idx != -1:
            cut_index = min(cut_index, idx)
    return segment[:cut_index].strip(" .,;:")


def _extract_source_topic(query: str) -> str | None:
    """Extract a source topic from the query."""
    patterns = (
        r"job for ([a-zA-Z0-9_.\-\s]+)",
        r"from topic called ([a-zA-Z0-9_.\-\s]+)",
        r"from topic ([a-zA-Z0-9_.\-\s]+)",
        r"read from topic called ([a-zA-Z0-9_.\-\s]+)",
        r"read from topic ([a-zA-Z0-9_.\-\s]+)",
        r"from ([a-zA-Z0-9_.\-\s]+)",
    )
    for pattern in patterns:
        segment = _extract_segment(query, pattern)
        if segment:
            return _slugify_topic(segment)
    return None


def _extract_destination_topic(query: str) -> str | None:
    """Extract a destination topic from the query."""
    patterns = (
        r"destination topic ([a-zA-Z0-9_.\-\s]+)",
        r"into topic ([a-zA-Z0-9_.\-\s]+)",
        r"to topic ([a-zA-Z0-9_.\-\s]+)",
        r"pushed to ([a-zA-Z0-9_.\-\s]+)",
        r"write(?: the result)? to ([a-zA-Z0-9_.\-\s]+)",
    )
    for pattern in patterns:
        segment = _extract_segment(query, pattern)
        if segment:
            return _slugify_topic(segment)
    topic_segments = re.findall(r"topic ([a-zA-Z0-9_.\-\s]+)", query, flags=re.IGNORECASE)
    if topic_segments:
        segment = topic_segments[-1].strip()
        lowered = segment.lower()
        cut_index = len(segment)
        for stop_word in TOPIC_STOP_WORDS:
            idx = lowered.find(stop_word)
            if idx != -1:
                cut_index = min(cut_index, idx)
        return _slugify_topic(segment[:cut_index])
    return None


def _extract_numeric_condition(query: str) -> tuple[str, str] | None:
    """Extract a simple greater-than style condition."""
    match = re.search(
        r"(?:where|filter(?: for)?)\s+([a-zA-Z0-9_.]+)\s+(?:is\s+)?(?:greater\s+than|gre[a-z]*\s+tha[nr]?|more\s+than|>)\s+([0-9]+(?:\.[0-9]+)?)",
        query,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    return match.group(1), match.group(2)


def _extract_leaf_paths_from_json_schema(schema: dict[str, Any], prefix: str = "") -> list[str]:
    """Extract leaf paths from a JSON schema."""
    schema_type = schema.get("type")
    if schema_type == "object":
        paths: list[str] = []
        for key, child in schema.get("properties", {}).items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            child_paths = _extract_leaf_paths_from_json_schema(child, child_prefix)
            if child_paths:
                paths.extend(child_paths)
            else:
                paths.append(child_prefix)
        return paths
    if schema_type == "array":
        return [prefix] if prefix else []
    return [prefix] if prefix else []


def _resolve_field_path(field_name: str, source_schema: dict[str, Any] | None) -> str:
    """Resolve a field name against optional schema paths."""
    if source_schema is None:
        return field_name
    leaf_paths = _extract_leaf_paths_from_json_schema(source_schema)
    normalized_field = _normalize_token(field_name)
    exact_matches = [path for path in leaf_paths if _normalize_token(path.split(".")[-1]) == normalized_field]
    if len(exact_matches) == 1:
        return exact_matches[0]
    broad_matches = [path for path in leaf_paths if _normalize_token(path) == normalized_field]
    if len(broad_matches) == 1:
        return broad_matches[0]
    return field_name


def _select_pattern(
    source_format: str,
    target_format: str,
    filter_field_path: str,
    query: str,
) -> str:
    """Select the transformation pattern deterministically."""
    wants_flatten = any(token in query.lower() for token in ("flatten", "flat", "flattened"))
    is_nested = "." in filter_field_path

    if source_format == "json" and target_format == "json":
        if wants_flatten:
            return "nested_json_to_flat_json"
        if is_nested:
            return "json_to_json_nested_filter"
        return "json_to_json_simple_filter"
    if source_format == "json" and target_format == "avro":
        if is_nested:
            return "nested_json_to_avro"
        return "json_to_avro"
    if source_format == "avro" and target_format == "avro":
        if wants_flatten:
            return "nested_avro_to_flat_avro"
        if is_nested:
            return "nested_avro_to_nested_avro"
        return "avro_to_avro_simple_filter"
    return "json_to_json_simple_filter"


def _build_include_fields(filter_field_path: str, source_schema: dict[str, Any] | None) -> list[str]:
    """Build included fields from optional schema."""
    if source_schema is None:
        return [filter_field_path]
    leaf_paths = _extract_leaf_paths_from_json_schema(source_schema)
    top_level_first = [
        path
        for path in leaf_paths
        if path in {"eventId", "eventType", "eventTime", "source", "traceId"}
    ]
    remaining = [path for path in leaf_paths if path not in top_level_first]
    if filter_field_path not in top_level_first and filter_field_path not in remaining:
        remaining.append(filter_field_path)
    return top_level_first + remaining


def _build_mapping(include_fields: list[str], pattern_type: str) -> MappingDefinition:
    """Build a deterministic mapping definition."""
    flatten = pattern_type in {"nested_json_to_flat_json", "nested_avro_to_flat_avro"}
    mappings: dict[str, str] = {}
    for field_path in include_fields:
        if flatten:
            destination = re.sub(r"(?<!^)(?=[A-Z])", "_", field_path.split(".")[-1]).lower()
            if "." in field_path:
                destination = field_path.replace(".", "_")
                destination = re.sub(r"(?<!^)(?=[A-Z])", "_", destination).lower()
            mappings[field_path] = destination
        else:
            mappings[field_path] = field_path
    return MappingDefinition(mappings=mappings)


def interpret_customer_request(
    user_query: str,
    source_schema: dict[str, Any] | None = None,
) -> JobRequest | ValidationResult:
    """Convert a customer request into a deterministic JobRequest."""
    source_topic = _extract_source_topic(user_query)
    if source_topic is None:
        return interpretation_error("Could not determine source_topic from the request.")

    destination_topic = _extract_destination_topic(user_query)
    if destination_topic is None:
        return interpretation_error("Could not determine destination_topic from the request.")

    condition = _extract_numeric_condition(user_query)
    if condition is None:
        return interpretation_error("Could not determine filter condition from the request.")

    raw_field, threshold = condition
    source_format = "avro" if "avro" in user_query.lower() else "json"
    target_format = "avro" if " to avro" in user_query.lower() or "as avro" in user_query.lower() or "avro topic" in user_query.lower() else "json"
    filter_field_path = _resolve_field_path(raw_field, source_schema)
    pattern_type = _select_pattern(source_format, target_format, filter_field_path, user_query)
    include_fields = _build_include_fields(filter_field_path, source_schema)

    request = JobRequest(
        source_topic=source_topic,
        destination_topic=destination_topic,
        source_format=source_format,
        target_format=target_format,
        pattern_type=pattern_type,
        filter_expression=FilterExpression(
            include_fields=include_fields,
            conditions=[f"{filter_field_path} > {threshold}"],
        ),
        mapping_definition=_build_mapping(include_fields, pattern_type),
        flatten_rules=FlattenRules(separator="_") if pattern_type in {"nested_json_to_flat_json", "nested_avro_to_flat_avro"} else None,
        source_schema=source_schema,
    )
    return request
