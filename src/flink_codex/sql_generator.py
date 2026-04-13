"""Jinja2-based SQL and markdown rendering."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .models import JobSpec, NormalizedJobRequest, TransformPreview
from .schema_generator import destination_columns, render_column_definitions, render_select_expressions

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "templates"
ENV = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(default_for_string=False, disabled_extensions=("j2",)),
    trim_blocks=True,
    lstrip_blocks=True,
)

def _sql_context(request: NormalizedJobRequest) -> dict[str, object]:
    """Build SQL template context shared by all patterns."""
    include_fields = request.filter_expression.include_fields if request.filter_expression else []
    filter_conditions = request.filter_expression.conditions if request.filter_expression else []
    mappings = request.mapping_definition.mappings if request.mapping_definition else {field: field for field in include_fields}
    return {
        "source_topic": request.source_topic,
        "destination_topic": request.destination_topic,
        "source_format": request.source_format,
        "target_format": request.target_format,
        "source_fields": include_fields,
        "filter_conditions": filter_conditions,
        "field_mappings": mappings,
        "destination_columns": destination_columns(request),
        "destination_column_definitions": render_column_definitions(request),
        "destination_select_expressions": render_select_expressions(request),
        "flatten_separator": request.flatten_rules.separator if request.flatten_rules else None,
        "avro_schema_ref": request.schema_reference or (request.resolved_schema.subject_name if request.resolved_schema else None),
        "inline_avro_schema": request.inline_schema or request.generated_avro_schema or (request.resolved_schema.schema_string if request.resolved_schema else None),
        "watermark_field": None,
    }


async def generate_flink_sql(spec: JobSpec) -> str:
    """Return the rendered Flink SQL from a generated job spec."""
    return spec.flink_sql


async def build_flink_sql(request: NormalizedJobRequest) -> str:
    """Render Flink SQL from a normalized request."""
    template = ENV.get_template(f"{request.pattern_type}.sql.j2")
    return template.render(**_sql_context(request)).strip() + "\n"


async def generate_job_spec(request: NormalizedJobRequest) -> JobSpec:
    """Generate a job spec for a normalized request."""
    sql = await build_flink_sql(request)
    return JobSpec(
        spec_id=str(uuid4()),
        pattern_type=request.pattern_type,
        flink_sql=sql,
        source_topic=request.source_topic,
        destination_topic=request.destination_topic,
        source_format=request.source_format,
        target_format=request.target_format,
        schema_reference=request.schema_reference or (request.resolved_schema.subject_name if request.resolved_schema else None),
        filter_expression=request.filter_expression,
        mapping_definition=request.mapping_definition,
        flatten_rules=request.flatten_rules,
        inline_schema=request.inline_schema or (request.resolved_schema.schema_string if request.resolved_schema else None),
        source_schema=request.source_schema,
        generated_json_schema=request.generated_json_schema,
        generated_avro_schema=request.generated_avro_schema,
        validation_status="passed",
        created_at=request.normalized_at,
    )


async def render_preview_table(preview: TransformPreview, *, destination: bool) -> str:
    """Render either the source or destination markdown preview table."""
    template = ENV.get_template("preview_table.md.j2")
    return template.render(
        pattern_type=preview.pattern_type,
        source_columns=preview.source_table.columns,
        source_rows=preview.source_table.rows,
        destination_columns=preview.destination_table.columns,
        destination_rows=preview.destination_table.rows,
        transformation_summary=preview.transformation_summary,
        mapping_explanation=preview.mapping_explanation,
        filtered_out_count=preview.filtered_out_count,
        invalid_records=preview.invalid_records,
        table_title="Destination Preview" if destination else "Source Preview",
        table_columns=preview.destination_table.columns if destination else preview.source_table.columns,
        table_rows=preview.destination_table.rows if destination else preview.source_table.rows,
    ).strip() + "\n"


async def render_source_sample_table(preview: TransformPreview) -> str:
    """Render the source markdown preview table."""
    return await render_preview_table(preview, destination=False)


async def render_destination_sample_table(preview: TransformPreview) -> str:
    """Render the destination markdown preview table."""
    return await render_preview_table(preview, destination=True)
