"""Pattern catalog for supported transformation flows."""

from __future__ import annotations

from dataclasses import dataclass

from .models import PatternDefinition


@dataclass(frozen=True)
class PatternCatalog:
    """In-memory catalog for deterministic pattern lookups."""

    patterns: tuple[PatternDefinition, ...]

    def by_id(self, pattern_id: str) -> PatternDefinition | None:
        """Return a pattern by identifier."""
        return next((pattern for pattern in self.patterns if pattern.id == pattern_id), None)

    def required_fields(self, pattern_id: str) -> dict[str, list[str]]:
        """Return required, optional, and conditional fields for a pattern."""
        pattern = self.by_id(pattern_id)
        if pattern is None:
            return {"required": [], "optional": [], "conditional": []}

        required = [
            "source_topic",
            "destination_topic",
            "source_format",
            "target_format",
            "pattern_type",
        ]
        optional = [
            "filter_expression",
            "mapping_definition",
            "schema_reference",
            "inline_schema",
            "source_schema",
            "sample_source_records",
            "confluent_credentials",
        ]
        conditional: list[str] = []
        if pattern.requires_flatten_rules:
            conditional.append("flatten_rules")
        conditional.append("source_schema")
        if pattern.target_format == "avro":
            conditional.append("inline_schema|source_schema")
        return {"required": required, "optional": optional, "conditional": conditional}


catalog = PatternCatalog(
    patterns=(
        PatternDefinition(
            id="json_to_json_simple_filter",
            name="JSON to JSON Simple Filter",
            description="Top-level JSON field selection and filter.",
            source_format="json",
            target_format="json",
            requires_flatten_rules=False,
            requires_schema_reference=False,
            supports_nested_source=False,
            supports_nested_target=False,
        ),
        PatternDefinition(
            id="json_to_json_nested_filter",
            name="JSON to JSON Nested Filter",
            description="Nested JSON field selection and filtering.",
            source_format="json",
            target_format="json",
            requires_flatten_rules=False,
            requires_schema_reference=False,
            supports_nested_source=True,
            supports_nested_target=True,
        ),
        PatternDefinition(
            id="nested_json_to_flat_json",
            name="Nested JSON to Flat JSON",
            description="Flatten nested JSON into flat JSON columns with filtering.",
            source_format="json",
            target_format="json",
            requires_flatten_rules=True,
            requires_schema_reference=False,
            supports_nested_source=True,
            supports_nested_target=False,
        ),
        PatternDefinition(
            id="avro_to_avro_simple_filter",
            name="Avro to Avro Simple Filter",
            description="Top-level Avro field selection and filter.",
            source_format="avro",
            target_format="avro",
            requires_flatten_rules=False,
            requires_schema_reference=False,
            supports_nested_source=False,
            supports_nested_target=False,
        ),
        PatternDefinition(
            id="nested_avro_to_nested_avro",
            name="Nested Avro to Nested Avro",
            description="Nested Avro selection and nested Avro output with filtering.",
            source_format="avro",
            target_format="avro",
            requires_flatten_rules=False,
            requires_schema_reference=False,
            supports_nested_source=True,
            supports_nested_target=True,
        ),
        PatternDefinition(
            id="nested_avro_to_flat_avro",
            name="Nested Avro to Flat Avro",
            description="Flatten nested Avro into flat Avro fields with filtering.",
            source_format="avro",
            target_format="avro",
            requires_flatten_rules=True,
            requires_schema_reference=False,
            supports_nested_source=True,
            supports_nested_target=False,
        ),
        PatternDefinition(
            id="nested_json_to_avro",
            name="Nested JSON to Avro",
            description="Nested JSON input mapped to Avro output with filtering.",
            source_format="json",
            target_format="avro",
            requires_flatten_rules=False,
            requires_schema_reference=False,
            supports_nested_source=True,
            supports_nested_target=False,
        ),
        PatternDefinition(
            id="json_to_avro",
            name="JSON to Avro",
            description="JSON input mapped to Avro output with filtering.",
            source_format="json",
            target_format="avro",
            requires_flatten_rules=False,
            requires_schema_reference=False,
            supports_nested_source=False,
            supports_nested_target=False,
        ),
    )
)
