# Flink Codex: Tool Usage Guide

This guide explains how to use the MCP tools to generate and validate Flink clean jobs on Confluent Cloud.

## 1. Tool Execution Order (The Workflow)

To create a job from scratch, follow this sequence:

1.  **Discovery**: `list_supported_patterns` to see available job types.
2.  **Validation**: `validate_job_request` to ensure your configuration matches the pattern requirements.
3.  **Normalization**: `generate_normalized_request` to resolve defaults.
4.  **Preview**: `generate_transform_preview` with sample data to verify the SQL logic before deployment.
5.  **Spec Generation**: `generate_job_spec` to create the final deployment object.
6.  **SQL Generation**: `generate_flink_sql` to see the actual SQL that will run.
7.  **Dry Run**: `dry_run_publish` to check credentials and permissions.
8.  **Deployment**: `publish_job` to push the job to Confluent Cloud.

---

## 2. Example: JSON to JSON Simple Filter

This example demonstrates a job that reads from `raw_orders` and filters for orders with an amount > 100, writing to `high_value_orders`.

### Step 1: Validate the Request
**Tool**: `validate_job_request`
**Arguments**:
```json
{
  "pattern_type": "json_to_json_simple_filter",
  "source_topic": "raw_orders",
  "destination_topic": "high_value_orders",
  "source_format": "json",
  "target_format": "json",
  "filter_expression": {
    "include_fields": ["order_id", "order_amount"],
    "conditions": ["order_amount > 100"]
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "order_amount": "order_amount"
    }
  }
}
```

### Step 2: Generate Preview (Verification)
To ensure the filter works, provide sample records.
**Tool**: `generate_transform_preview`
**Arguments**:
```json
{
  "pattern_type": "json_to_json_simple_filter",
  "source_topic": "raw_orders",
  "destination_topic": "high_value_orders",
  "source_format": "json",
  "target_format": "json",
  "filter_expression": {
    "include_fields": ["order_id", "order_amount"],
    "conditions": ["order_amount > 100"]
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "order_amount": "order_amount"
    }
  },
  "sample_source_records": [
    {"order_id": 1, "order_amount": 50},
    {"order_id": 2, "order_amount": 150}
  ]
}
```
*Expected Result*: The `destination_records` in the output should only contain `order_id: 2`.

### Step 3: Generate Flink SQL
**Tool**: `generate_flink_sql`
(Requires the output from `generate_job_spec` first). It will output something like:
```sql
CREATE TABLE `high_value_orders` (
  order_id BIGINT,
  order_amount BIGINT
) WITH (
  'connector' = 'confluent',
  'topic' = 'high_value_orders',
  'value.format' = 'JSON'
);

INSERT INTO `high_value_orders`
SELECT
  order_id AS order_id,
  order_amount AS order_amount
FROM `raw_orders`
WHERE order_amount > 100;
```

---

## 3. How to use in the MCP Inspector
1. Start the server: `fastmcp dev src/flink_codex/server.py`
2. Open `http://localhost:5173`
3. Go to the **Tools** tab.
4. Select a tool and paste the JSON arguments from the examples above.
