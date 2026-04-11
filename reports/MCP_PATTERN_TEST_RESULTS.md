# MCP Pattern Test Results

Generated at: `2026-04-09T12:18:46.823527+00:00`
MCP server: `flink-clean-job-creator`
Discovered tools: `16`
Pattern runs: `8`

## Tool Discovery

```json
[
  "list_supported_patterns",
  "describe_pattern",
  "list_required_fields",
  "validate_job_request",
  "validate_filter_expression",
  "validate_schema_mapping",
  "resolve_schema_from_confluent_cloud",
  "validate_avro_schema",
  "generate_normalized_request",
  "generate_transform_preview",
  "render_source_sample_table",
  "render_destination_sample_table",
  "generate_job_spec",
  "generate_flink_sql",
  "dry_run_publish",
  "publish_job"
]
```

## Scenario Results

### `json_to_json_simple_filter`

Customer request:
`Create a JSON Flink job from orders.raw to orders.cleaned, keep order_id and status, and filter where status is ACTIVE.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw",
  "destination_topic": "orders.cleaned",
  "source_format": "json",
  "target_format": "json",
  "pattern_type": "json_to_json_simple_filter",
  "filter_expression": {
    "include_fields": [
      "order_id",
      "status"
    ],
    "conditions": [
      "status == 'ACTIVE'"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "status": "status"
    }
  },
  "sample_source_records": [
    {
      "order_id": "A1",
      "status": "ACTIVE"
    },
    {
      "order_id": "A2",
      "status": "INACTIVE"
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw",
    "destination_topic": "orders.cleaned",
    "source_format": "json",
    "target_format": "json",
    "pattern_type": "json_to_json_simple_filter",
    "filter_expression": {
      "include_fields": [
        "order_id",
        "status"
      ],
      "conditions": [
        "status == 'ACTIVE'"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order_id": "order_id",
        "status": "status"
      }
    },
    "flatten_rules": null,
    "schema_reference": null,
    "inline_schema": null,
    "sample_source_records": [
      {
        "order_id": "A1",
        "status": "ACTIVE"
      },
      {
        "order_id": "A2",
        "status": "INACTIVE"
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": null,
    "normalized_at": "2026-04-09T12:18:46.459682Z"
  },
  "generate_transform_preview": {
    "pattern_type": "json_to_json_simple_filter",
    "source_table": {
      "columns": [
        "order_id",
        "status"
      ],
      "rows": [
        [
          "A1",
          "ACTIVE"
        ],
        [
          "A2",
          "INACTIVE"
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "status"
      ],
      "rows": [
        [
          "A1",
          "ACTIVE"
        ]
      ]
    },
    "transformation_summary": "Applied pattern json_to_json_simple_filter to 2 source records.",
    "mapping_explanation": [
      "order_id -> order_id",
      "status -> status"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "d22097d7-4246-493b-af04-25c72add25fe",
    "pattern_type": "json_to_json_simple_filter",
    "flink_sql": "CREATE TABLE `orders.cleaned` (\norder_id STRING,\nstatus STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned',\n  'value.format' = 'JSON'\n) ;\n\nINSERT INTO `orders.cleaned`\nSELECT\norder_id AS order_id,\nstatus AS status\nFROM `orders.raw`\nWHERE status == 'ACTIVE';\n",
    "source_topic": "orders.raw",
    "destination_topic": "orders.cleaned",
    "source_format": "json",
    "target_format": "json",
    "schema_reference": null,
    "inline_schema": null,
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.459682Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned` (\norder_id STRING,\nstatus STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned',\n  'value.format' = 'JSON'\n) ;\n\nINSERT INTO `orders.cleaned`\nSELECT\norder_id AS order_id,\nstatus AS status\nFROM `orders.raw`\nWHERE status == 'ACTIVE';\n"
}
```

### `json_to_json_nested_filter`

Customer request:
`Create a nested JSON Flink job from orders.raw.nested to orders.cleaned.nested, keep order.id and customer.name, and filter for order.id equal to A1.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.nested",
  "destination_topic": "orders.cleaned.nested",
  "source_format": "json",
  "target_format": "json",
  "pattern_type": "json_to_json_nested_filter",
  "filter_expression": {
    "include_fields": [
      "order.id",
      "customer.name"
    ],
    "conditions": [
      "order.id == 'A1'"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order.id": "order_id",
      "customer.name": "customer_name"
    }
  },
  "sample_source_records": [
    {
      "order": {
        "id": "A1"
      },
      "customer": {
        "name": "Junaid"
      }
    },
    {
      "order": {
        "id": "A2"
      },
      "customer": {
        "name": "Sara"
      }
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.nested",
    "destination_topic": "orders.cleaned.nested",
    "source_format": "json",
    "target_format": "json",
    "pattern_type": "json_to_json_nested_filter",
    "filter_expression": {
      "include_fields": [
        "order.id",
        "customer.name"
      ],
      "conditions": [
        "order.id == 'A1'"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order.id": "order_id",
        "customer.name": "customer_name"
      }
    },
    "flatten_rules": null,
    "schema_reference": null,
    "inline_schema": null,
    "sample_source_records": [
      {
        "order": {
          "id": "A1"
        },
        "customer": {
          "name": "Junaid"
        }
      },
      {
        "order": {
          "id": "A2"
        },
        "customer": {
          "name": "Sara"
        }
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": null,
    "normalized_at": "2026-04-09T12:18:46.501745Z"
  },
  "generate_transform_preview": {
    "pattern_type": "json_to_json_nested_filter",
    "source_table": {
      "columns": [
        "order.id",
        "customer.name"
      ],
      "rows": [
        [
          "A1",
          "Junaid"
        ],
        [
          "A2",
          "Sara"
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "customer_name"
      ],
      "rows": [
        [
          "A1",
          "Junaid"
        ]
      ]
    },
    "transformation_summary": "Applied pattern json_to_json_nested_filter to 2 source records.",
    "mapping_explanation": [
      "order.id -> order_id",
      "customer.name -> customer_name"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "392aad97-7596-443e-972b-5fb4c44d5bbd",
    "pattern_type": "json_to_json_nested_filter",
    "flink_sql": "CREATE TABLE `orders.cleaned.nested` (\norder_id STRING,\ncustomer_name STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.nested',\n  'value.format' = 'JSON'\n) ;\n\nINSERT INTO `orders.cleaned.nested`\nSELECT\norder.id AS order_id,\ncustomer.name AS customer_name\nFROM `orders.raw.nested`\nWHERE order.id == 'A1';\n",
    "source_topic": "orders.raw.nested",
    "destination_topic": "orders.cleaned.nested",
    "source_format": "json",
    "target_format": "json",
    "schema_reference": null,
    "inline_schema": null,
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.501745Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.nested` (\norder_id STRING,\ncustomer_name STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.nested',\n  'value.format' = 'JSON'\n) ;\n\nINSERT INTO `orders.cleaned.nested`\nSELECT\norder.id AS order_id,\ncustomer.name AS customer_name\nFROM `orders.raw.nested`\nWHERE order.id == 'A1';\n"
}
```

### `nested_json_to_flat_json`

Customer request:
`Create a Flink job from orders.raw.deep to orders.cleaned.flat, flatten order.id and amount.total with underscores, and filter where amount.total is greater than 0.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.deep",
  "destination_topic": "orders.cleaned.flat",
  "source_format": "json",
  "target_format": "json",
  "pattern_type": "nested_json_to_flat_json",
  "filter_expression": {
    "include_fields": [
      "order.id",
      "amount.total"
    ],
    "conditions": [
      "amount.total > 0"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order.id": "order.id",
      "amount.total": "amount.total"
    }
  },
  "flatten_rules": {
    "separator": "_"
  },
  "sample_source_records": [
    {
      "order": {
        "id": "A1"
      },
      "amount": {
        "total": 99.5
      }
    },
    {
      "order": {
        "id": "A2"
      },
      "amount": {
        "total": 0
      }
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.deep",
    "destination_topic": "orders.cleaned.flat",
    "source_format": "json",
    "target_format": "json",
    "pattern_type": "nested_json_to_flat_json",
    "filter_expression": {
      "include_fields": [
        "order.id",
        "amount.total"
      ],
      "conditions": [
        "amount.total > 0"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order.id": "order.id",
        "amount.total": "amount.total"
      }
    },
    "flatten_rules": {
      "separator": "_"
    },
    "schema_reference": null,
    "inline_schema": null,
    "sample_source_records": [
      {
        "order": {
          "id": "A1"
        },
        "amount": {
          "total": 99.5
        }
      },
      {
        "order": {
          "id": "A2"
        },
        "amount": {
          "total": 0
        }
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": null,
    "normalized_at": "2026-04-09T12:18:46.538588Z"
  },
  "generate_transform_preview": {
    "pattern_type": "nested_json_to_flat_json",
    "source_table": {
      "columns": [
        "order.id",
        "amount.total"
      ],
      "rows": [
        [
          "A1",
          99.5
        ],
        [
          "A2",
          0
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "amount_total"
      ],
      "rows": [
        [
          "A1",
          99.5
        ]
      ]
    },
    "transformation_summary": "Applied pattern nested_json_to_flat_json to 2 source records.",
    "mapping_explanation": [
      "order.id -> order.id",
      "amount.total -> amount.total"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "a2f75320-899a-456f-898c-e796fa617701",
    "pattern_type": "nested_json_to_flat_json",
    "flink_sql": "CREATE TABLE `orders.cleaned.flat` (\norder_id STRING,\namount_total DOUBLE\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.flat',\n  'value.format' = 'JSON'\n) ;\n\nINSERT INTO `orders.cleaned.flat`\nSELECT\norder.id AS order_id,\namount.total AS amount_total\nFROM `orders.raw.deep`\nWHERE amount.total > 0;\n",
    "source_topic": "orders.raw.deep",
    "destination_topic": "orders.cleaned.flat",
    "source_format": "json",
    "target_format": "json",
    "schema_reference": null,
    "inline_schema": null,
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.538588Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.flat` (\norder_id STRING,\namount_total DOUBLE\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.flat',\n  'value.format' = 'JSON'\n) ;\n\nINSERT INTO `orders.cleaned.flat`\nSELECT\norder.id AS order_id,\namount.total AS amount_total\nFROM `orders.raw.deep`\nWHERE amount.total > 0;\n"
}
```

### `avro_to_avro_simple_filter`

Customer request:
`Create an Avro Flink job from orders.raw.avro to orders.cleaned.avro, keep order_id and status, filter for ACTIVE, and use schema subject orders.cleaned.avro-value.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.avro",
  "destination_topic": "orders.cleaned.avro",
  "source_format": "avro",
  "target_format": "avro",
  "pattern_type": "avro_to_avro_simple_filter",
  "filter_expression": {
    "include_fields": [
      "order_id",
      "status"
    ],
    "conditions": [
      "status == 'ACTIVE'"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "status": "status"
    }
  },
  "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
  "sample_source_records": [
    {
      "order_id": "A1",
      "status": "ACTIVE"
    },
    {
      "order_id": "A2",
      "status": "INACTIVE"
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.avro",
    "destination_topic": "orders.cleaned.avro",
    "source_format": "avro",
    "target_format": "avro",
    "pattern_type": "avro_to_avro_simple_filter",
    "filter_expression": {
      "include_fields": [
        "order_id",
        "status"
      ],
      "conditions": [
        "status == 'ACTIVE'"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order_id": "order_id",
        "status": "status"
      }
    },
    "flatten_rules": null,
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "sample_source_records": [
      {
        "order_id": "A1",
        "status": "ACTIVE"
      },
      {
        "order_id": "A2",
        "status": "INACTIVE"
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": {
      "resolved": true,
      "subject_name": null,
      "schema_id": null,
      "schema_string": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
      "error_message": null
    },
    "normalized_at": "2026-04-09T12:18:46.575264Z"
  },
  "generate_transform_preview": {
    "pattern_type": "avro_to_avro_simple_filter",
    "source_table": {
      "columns": [
        "order_id",
        "status"
      ],
      "rows": [
        [
          "A1",
          "ACTIVE"
        ],
        [
          "A2",
          "INACTIVE"
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "status"
      ],
      "rows": [
        [
          "A1",
          "ACTIVE"
        ]
      ]
    },
    "transformation_summary": "Applied pattern avro_to_avro_simple_filter to 2 source records.",
    "mapping_explanation": [
      "order_id -> order_id",
      "status -> status"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "4cd0f2d9-a18f-420a-83fd-8fda4ad97344",
    "pattern_type": "avro_to_avro_simple_filter",
    "flink_sql": "CREATE TABLE `orders.cleaned.avro` (\norder_id STRING,\nstatus STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.avro`\nSELECT\norder_id AS order_id,\nstatus AS status\nFROM `orders.raw.avro`\nWHERE status == 'ACTIVE';\n",
    "source_topic": "orders.raw.avro",
    "destination_topic": "orders.cleaned.avro",
    "source_format": "avro",
    "target_format": "avro",
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.575264Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.avro` (\norder_id STRING,\nstatus STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.avro`\nSELECT\norder_id AS order_id,\nstatus AS status\nFROM `orders.raw.avro`\nWHERE status == 'ACTIVE';\n"
}
```

### `nested_avro_to_nested_avro`

Customer request:
`Create a nested Avro Flink job from orders.raw.avro.nested to orders.cleaned.avro.nested, keep order.id and customer.name, filter for order.id equal to A1, and use schema subject orders.cleaned.avro.nested-value.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.avro.nested",
  "destination_topic": "orders.cleaned.avro.nested",
  "source_format": "avro",
  "target_format": "avro",
  "pattern_type": "nested_avro_to_nested_avro",
  "filter_expression": {
    "include_fields": [
      "order.id",
      "customer.name"
    ],
    "conditions": [
      "order.id == 'A1'"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order.id": "order_id",
      "customer.name": "customer_name"
    }
  },
  "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
  "sample_source_records": [
    {
      "order": {
        "id": "A1"
      },
      "customer": {
        "name": "Junaid"
      }
    },
    {
      "order": {
        "id": "A2"
      },
      "customer": {
        "name": "Sara"
      }
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.avro.nested",
    "destination_topic": "orders.cleaned.avro.nested",
    "source_format": "avro",
    "target_format": "avro",
    "pattern_type": "nested_avro_to_nested_avro",
    "filter_expression": {
      "include_fields": [
        "order.id",
        "customer.name"
      ],
      "conditions": [
        "order.id == 'A1'"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order.id": "order_id",
        "customer.name": "customer_name"
      }
    },
    "flatten_rules": null,
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "sample_source_records": [
      {
        "order": {
          "id": "A1"
        },
        "customer": {
          "name": "Junaid"
        }
      },
      {
        "order": {
          "id": "A2"
        },
        "customer": {
          "name": "Sara"
        }
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": {
      "resolved": true,
      "subject_name": null,
      "schema_id": null,
      "schema_string": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
      "error_message": null
    },
    "normalized_at": "2026-04-09T12:18:46.612841Z"
  },
  "generate_transform_preview": {
    "pattern_type": "nested_avro_to_nested_avro",
    "source_table": {
      "columns": [
        "order.id",
        "customer.name"
      ],
      "rows": [
        [
          "A1",
          "Junaid"
        ],
        [
          "A2",
          "Sara"
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "customer_name"
      ],
      "rows": [
        [
          "A1",
          "Junaid"
        ]
      ]
    },
    "transformation_summary": "Applied pattern nested_avro_to_nested_avro to 2 source records.",
    "mapping_explanation": [
      "order.id -> order_id",
      "customer.name -> customer_name"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "78bf3f0b-301b-4b98-af98-d056da5853ed",
    "pattern_type": "nested_avro_to_nested_avro",
    "flink_sql": "CREATE TABLE `orders.cleaned.avro.nested` (\norder_id STRING,\ncustomer_name STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro.nested',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.avro.nested`\nSELECT\norder.id AS order_id,\ncustomer.name AS customer_name\nFROM `orders.raw.avro.nested`\nWHERE order.id == 'A1';\n",
    "source_topic": "orders.raw.avro.nested",
    "destination_topic": "orders.cleaned.avro.nested",
    "source_format": "avro",
    "target_format": "avro",
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.612841Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.avro.nested` (\norder_id STRING,\ncustomer_name STRING\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro.nested',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.avro.nested`\nSELECT\norder.id AS order_id,\ncustomer.name AS customer_name\nFROM `orders.raw.avro.nested`\nWHERE order.id == 'A1';\n"
}
```

### `nested_avro_to_flat_avro`

Customer request:
`Create a flattened Avro Flink job from orders.raw.avro.deep to orders.cleaned.avro.flat, flatten order.id and amount.total with underscores, filter where amount.total is greater than 0, and use schema subject orders.cleaned.avro.flat-value.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.avro.deep",
  "destination_topic": "orders.cleaned.avro.flat",
  "source_format": "avro",
  "target_format": "avro",
  "pattern_type": "nested_avro_to_flat_avro",
  "filter_expression": {
    "include_fields": [
      "order.id",
      "amount.total"
    ],
    "conditions": [
      "amount.total > 0"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order.id": "order.id",
      "amount.total": "amount.total"
    }
  },
  "flatten_rules": {
    "separator": "_"
  },
  "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
  "sample_source_records": [
    {
      "order": {
        "id": "A1"
      },
      "amount": {
        "total": 50
      }
    },
    {
      "order": {
        "id": "A2"
      },
      "amount": {
        "total": -1
      }
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.avro.deep",
    "destination_topic": "orders.cleaned.avro.flat",
    "source_format": "avro",
    "target_format": "avro",
    "pattern_type": "nested_avro_to_flat_avro",
    "filter_expression": {
      "include_fields": [
        "order.id",
        "amount.total"
      ],
      "conditions": [
        "amount.total > 0"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order.id": "order.id",
        "amount.total": "amount.total"
      }
    },
    "flatten_rules": {
      "separator": "_"
    },
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "sample_source_records": [
      {
        "order": {
          "id": "A1"
        },
        "amount": {
          "total": 50
        }
      },
      {
        "order": {
          "id": "A2"
        },
        "amount": {
          "total": -1
        }
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": {
      "resolved": true,
      "subject_name": null,
      "schema_id": null,
      "schema_string": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
      "error_message": null
    },
    "normalized_at": "2026-04-09T12:18:46.648435Z"
  },
  "generate_transform_preview": {
    "pattern_type": "nested_avro_to_flat_avro",
    "source_table": {
      "columns": [
        "order.id",
        "amount.total"
      ],
      "rows": [
        [
          "A1",
          50
        ],
        [
          "A2",
          -1
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "amount_total"
      ],
      "rows": [
        [
          "A1",
          50
        ]
      ]
    },
    "transformation_summary": "Applied pattern nested_avro_to_flat_avro to 2 source records.",
    "mapping_explanation": [
      "order.id -> order.id",
      "amount.total -> amount.total"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "cb76b606-f659-4110-bdee-8b20942440c4",
    "pattern_type": "nested_avro_to_flat_avro",
    "flink_sql": "CREATE TABLE `orders.cleaned.avro.flat` (\norder_id STRING,\namount_total BIGINT\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro.flat',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.avro.flat`\nSELECT\norder.id AS order_id,\namount.total AS amount_total\nFROM `orders.raw.avro.deep`\nWHERE amount.total > 0;\n",
    "source_topic": "orders.raw.avro.deep",
    "destination_topic": "orders.cleaned.avro.flat",
    "source_format": "avro",
    "target_format": "avro",
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.648435Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.avro.flat` (\norder_id STRING,\namount_total BIGINT\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro.flat',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.avro.flat`\nSELECT\norder.id AS order_id,\namount.total AS amount_total\nFROM `orders.raw.avro.deep`\nWHERE amount.total > 0;\n"
}
```

### `nested_json_to_avro`

Customer request:
`Create a Flink job from JSON topic orders.raw.cross to Avro topic orders.cleaned.cross.avro, map order.id to order_id and amount.total to total_amount, filter where amount.total is greater than 0, and use schema subject orders.cleaned.cross.avro-value.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.cross",
  "destination_topic": "orders.cleaned.cross.avro",
  "source_format": "json",
  "target_format": "avro",
  "pattern_type": "nested_json_to_avro",
  "filter_expression": {
    "include_fields": [
      "order.id",
      "amount.total"
    ],
    "conditions": [
      "amount.total > 0"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order.id": "order_id",
      "amount.total": "total_amount"
    }
  },
  "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
  "sample_source_records": [
    {
      "order": {
        "id": "A1"
      },
      "amount": {
        "total": 120.5
      }
    },
    {
      "order": {
        "id": "A2"
      },
      "amount": {
        "total": 0
      }
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.cross",
    "destination_topic": "orders.cleaned.cross.avro",
    "source_format": "json",
    "target_format": "avro",
    "pattern_type": "nested_json_to_avro",
    "filter_expression": {
      "include_fields": [
        "order.id",
        "amount.total"
      ],
      "conditions": [
        "amount.total > 0"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order.id": "order_id",
        "amount.total": "total_amount"
      }
    },
    "flatten_rules": null,
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "sample_source_records": [
      {
        "order": {
          "id": "A1"
        },
        "amount": {
          "total": 120.5
        }
      },
      {
        "order": {
          "id": "A2"
        },
        "amount": {
          "total": 0
        }
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": {
      "resolved": true,
      "subject_name": null,
      "schema_id": null,
      "schema_string": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
      "error_message": null
    },
    "normalized_at": "2026-04-09T12:18:46.683521Z"
  },
  "generate_transform_preview": {
    "pattern_type": "nested_json_to_avro",
    "source_table": {
      "columns": [
        "order.id",
        "amount.total"
      ],
      "rows": [
        [
          "A1",
          120.5
        ],
        [
          "A2",
          0
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "total_amount"
      ],
      "rows": [
        [
          "A1",
          120.5
        ]
      ]
    },
    "transformation_summary": "Applied pattern nested_json_to_avro to 2 source records.",
    "mapping_explanation": [
      "order.id -> order_id",
      "amount.total -> total_amount"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "0bda5249-555b-480e-b5dd-b0f8cf38a323",
    "pattern_type": "nested_json_to_avro",
    "flink_sql": "CREATE TABLE `orders.cleaned.cross.avro` (\norder_id STRING,\ntotal_amount DOUBLE\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.cross.avro',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.cross.avro`\nSELECT\norder.id AS order_id,\namount.total AS total_amount\nFROM `orders.raw.cross`\nWHERE amount.total > 0;\n",
    "source_topic": "orders.raw.cross",
    "destination_topic": "orders.cleaned.cross.avro",
    "source_format": "json",
    "target_format": "avro",
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.683521Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.cross.avro` (\norder_id STRING,\ntotal_amount DOUBLE\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.cross.avro',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.cross.avro`\nSELECT\norder.id AS order_id,\namount.total AS total_amount\nFROM `orders.raw.cross`\nWHERE amount.total > 0;\n"
}
```

### `json_to_avro`

Customer request:
`Create a Flink job from JSON topic orders.raw.cross.simple to Avro topic orders.cleaned.cross.simple.avro, keep order_id and amount, filter where amount is greater than 0, and use schema subject orders.cleaned.cross.simple.avro-value.`

Payload sent over MCP:
```json
{
  "source_topic": "orders.raw.cross.simple",
  "destination_topic": "orders.cleaned.cross.simple.avro",
  "source_format": "json",
  "target_format": "avro",
  "pattern_type": "json_to_avro",
  "filter_expression": {
    "include_fields": [
      "order_id",
      "amount"
    ],
    "conditions": [
      "amount > 0"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "amount": "amount"
    }
  },
  "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
  "sample_source_records": [
    {
      "order_id": "A1",
      "amount": 12.0
    },
    {
      "order_id": "A2",
      "amount": 0
    }
  ]
}
```

Observed MCP results:
```json
{
  "validate_job_request": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "generate_normalized_request": {
    "source_topic": "orders.raw.cross.simple",
    "destination_topic": "orders.cleaned.cross.simple.avro",
    "source_format": "json",
    "target_format": "avro",
    "pattern_type": "json_to_avro",
    "filter_expression": {
      "include_fields": [
        "order_id",
        "amount"
      ],
      "conditions": [
        "amount > 0"
      ]
    },
    "mapping_definition": {
      "mappings": {
        "order_id": "order_id",
        "amount": "amount"
      }
    },
    "flatten_rules": null,
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "sample_source_records": [
      {
        "order_id": "A1",
        "amount": 12.0
      },
      {
        "order_id": "A2",
        "amount": 0
      }
    ],
    "confluent_credentials": null,
    "resolved_schema": {
      "resolved": true,
      "subject_name": null,
      "schema_id": null,
      "schema_string": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
      "error_message": null
    },
    "normalized_at": "2026-04-09T12:18:46.718588Z"
  },
  "generate_transform_preview": {
    "pattern_type": "json_to_avro",
    "source_table": {
      "columns": [
        "order_id",
        "amount"
      ],
      "rows": [
        [
          "A1",
          12.0
        ],
        [
          "A2",
          0
        ]
      ]
    },
    "destination_table": {
      "columns": [
        "order_id",
        "amount"
      ],
      "rows": [
        [
          "A1",
          12.0
        ]
      ]
    },
    "transformation_summary": "Applied pattern json_to_avro to 2 source records.",
    "mapping_explanation": [
      "order_id -> order_id",
      "amount -> amount"
    ],
    "filtered_out_count": 1,
    "invalid_records": []
  },
  "generate_job_spec": {
    "spec_id": "f563c4e4-9b76-44d1-991f-1636ef5d22b1",
    "pattern_type": "json_to_avro",
    "flink_sql": "CREATE TABLE `orders.cleaned.cross.simple.avro` (\norder_id STRING,\namount DOUBLE\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.cross.simple.avro',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.cross.simple.avro`\nSELECT\norder_id AS order_id,\namount AS amount\nFROM `orders.raw.cross.simple`\nWHERE amount > 0;\n",
    "source_topic": "orders.raw.cross.simple",
    "destination_topic": "orders.cleaned.cross.simple.avro",
    "source_format": "json",
    "target_format": "avro",
    "schema_reference": null,
    "inline_schema": "{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}",
    "validation_status": "passed",
    "created_at": "2026-04-09T12:18:46.718588Z"
  },
  "generate_flink_sql": "CREATE TABLE `orders.cleaned.cross.simple.avro` (\norder_id STRING,\namount DOUBLE\n) WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.cross.simple.avro',\n  'value.format' = 'AVRO',\n  'value.avro-schema' = '{\"type\": \"record\", \"name\": \"OrderRecord\", \"fields\": [{\"name\": \"order_id\", \"type\": \"string\"}, {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null}, {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"amount_total\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null}, {\"name\": \"status\", \"type\": [\"null\", \"string\"], \"default\": null}]}') ;\n\nINSERT INTO `orders.cleaned.cross.simple.avro`\nSELECT\norder_id AS order_id,\namount AS amount\nFROM `orders.raw.cross.simple`\nWHERE amount > 0;\n"
}
```

