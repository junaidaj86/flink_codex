# Customer Test Results

Date: 2026-04-09
Tester role: Simulated customer acceptance pass
Workspace: `/Users/jja8go/Documents/work/Scania/Code/spec_driven_development/flink_codex`

## Summary

- Automated regression suite: `48 passed in 2.45s`
- Scenario payload runs executed: `8`
- Guardrail runs executed: `5`
- Overall result: `PASS`
- Important note: the current project validates and generates jobs from structured `JobRequest` input. In this report, each customer natural-language request is mapped to the interpreted payload that was executed.

## Scenario Payloads and Results

### `json_to_json_simple_filter`

Customer natural-language request:
`Create a Flink job for topic orders.raw, keep order_id and status, filter only ACTIVE records, and write the result to topic orders.cleaned as JSON.`

Interpreted payload:
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": null,
  "source_columns": [
    "order_id",
    "status"
  ],
  "source_rows": [
    [
      "A1",
      "ACTIVE"
    ],
    [
      "A2",
      "INACTIVE"
    ]
  ],
  "destination_columns": [
    "order_id",
    "status"
  ],
  "destination_rows": [
    [
      "A1",
      "ACTIVE"
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned',\n  'value.format' = 'JSON'\n) AS\nSELECT\norder_id AS order_id,\nstatus AS status\nFROM `orders.raw`\nWHERE status == 'ACTIVE';\n"
}
```

### `json_to_json_nested_filter`

Customer natural-language request:
`Create a Flink job from orders.raw.nested to orders.cleaned.nested, keep order.id and customer.name, only include records where order.id is A1, and keep the output as JSON.`

Interpreted payload:
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": null,
  "source_columns": [
    "order.id",
    "customer.name"
  ],
  "source_rows": [
    [
      "A1",
      "Junaid"
    ],
    [
      "A2",
      "Sara"
    ]
  ],
  "destination_columns": [
    "order_id",
    "customer_name"
  ],
  "destination_rows": [
    [
      "A1",
      "Junaid"
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.nested` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.nested',\n  'value.format' = 'JSON'\n) AS\nSELECT\norder.id AS order_id,\ncustomer.name AS customer_name\nFROM `orders.raw.nested`\nWHERE order.id == 'A1';\n"
}
```

### `nested_json_to_flat_json`

Customer natural-language request:
`Create a Flink job from orders.raw.deep into orders.cleaned.flat, flatten order.id and amount.total using underscores, keep only rows where amount.total is greater than 0, and output JSON.`

Interpreted payload:
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": null,
  "source_columns": [
    "order.id",
    "amount.total"
  ],
  "source_rows": [
    [
      "A1",
      99.5
    ],
    [
      "A2",
      0
    ]
  ],
  "destination_columns": [
    "order_id",
    "amount_total"
  ],
  "destination_rows": [
    [
      "A1",
      99.5
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.flat` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.flat',\n  'value.format' = 'JSON'\n) AS\nSELECT\norder.id AS order_id,\namount.total AS amount_total\nFROM `orders.raw.deep`\nWHERE amount.total > 0;\n"
}
```

### `avro_to_avro_simple_filter`

Customer natural-language request:
`Create a Flink Avro cleaning job from orders.raw.avro to orders.cleaned.avro, keep order_id and status, filter to ACTIVE records, and use schema subject orders.cleaned.avro-value.`

Interpreted payload:
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
  "schema_reference": "orders.cleaned.avro-value",
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": "orders.cleaned.avro-value",
  "source_columns": [
    "order_id",
    "status"
  ],
  "source_rows": [
    [
      "A1",
      "ACTIVE"
    ],
    [
      "A2",
      "INACTIVE"
    ]
  ],
  "destination_columns": [
    "order_id",
    "status"
  ],
  "destination_rows": [
    [
      "A1",
      "ACTIVE"
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.avro` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro',\n  'value.format' = 'AVRO',\n  'avro-confluent.subject' = 'orders.cleaned.avro-value'\n) AS\nSELECT\norder_id AS order_id,\nstatus AS status\nFROM `orders.raw.avro`\nWHERE status == 'ACTIVE';\n"
}
```

### `nested_avro_to_nested_avro`

Customer natural-language request:
`Create a nested Avro Flink job from orders.raw.avro.nested to orders.cleaned.avro.nested, select order.id and customer.name, filter to order.id equal to A1, and use schema subject orders.cleaned.avro.nested-value.`

Interpreted payload:
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
  "schema_reference": "orders.cleaned.avro.nested-value",
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": "orders.cleaned.avro.nested-value",
  "source_columns": [
    "order.id",
    "customer.name"
  ],
  "source_rows": [
    [
      "A1",
      "Junaid"
    ],
    [
      "A2",
      "Sara"
    ]
  ],
  "destination_columns": [
    "order_id",
    "customer_name"
  ],
  "destination_rows": [
    [
      "A1",
      "Junaid"
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.avro.nested` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro.nested',\n  'value.format' = 'AVRO',\n  'avro-confluent.subject' = 'orders.cleaned.avro.nested-value'\n) AS\nSELECT\norder.id AS order_id,\ncustomer.name AS customer_name\nFROM `orders.raw.avro.nested`\nWHERE order.id == 'A1';\n"
}
```

### `nested_avro_to_flat_avro`

Customer natural-language request:
`Create a Flink job from orders.raw.avro.deep to orders.cleaned.avro.flat, flatten order.id and amount.total with underscores, keep only rows where amount.total is greater than 0, and publish as Avro with schema subject orders.cleaned.avro.flat-value.`

Interpreted payload:
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
  "schema_reference": "orders.cleaned.avro.flat-value",
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": "orders.cleaned.avro.flat-value",
  "source_columns": [
    "order.id",
    "amount.total"
  ],
  "source_rows": [
    [
      "A1",
      50
    ],
    [
      "A2",
      -1
    ]
  ],
  "destination_columns": [
    "order_id",
    "amount_total"
  ],
  "destination_rows": [
    [
      "A1",
      50
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.avro.flat` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.avro.flat',\n  'value.format' = 'AVRO',\n  'avro-confluent.subject' = 'orders.cleaned.avro.flat-value'\n) AS\nSELECT\norder.id AS order_id,\namount.total AS amount_total\nFROM `orders.raw.avro.deep`\nWHERE amount.total > 0;\n"
}
```

### `nested_json_to_avro`

Customer natural-language request:
`Create a Flink job from JSON topic orders.raw.cross into Avro topic orders.cleaned.cross.avro, map order.id to order_id and amount.total to total_amount, keep only totals greater than 0, and use schema subject orders.cleaned.cross.avro-value.`

Interpreted payload:
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
  "schema_reference": "orders.cleaned.cross.avro-value",
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": "orders.cleaned.cross.avro-value",
  "source_columns": [
    "order.id",
    "amount.total"
  ],
  "source_rows": [
    [
      "A1",
      120.5
    ],
    [
      "A2",
      0
    ]
  ],
  "destination_columns": [
    "order_id",
    "total_amount"
  ],
  "destination_rows": [
    [
      "A1",
      120.5
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.cross.avro` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.cross.avro',\n  'value.format' = 'AVRO',\n  'avro-confluent.subject' = 'orders.cleaned.cross.avro-value'\n) AS\nSELECT\norder.id AS order_id,\namount.total AS total_amount\nFROM `orders.raw.cross`\nWHERE amount.total > 0;\n"
}
```

### `json_to_avro`

Customer natural-language request:
`Create a Flink job for topic orders.raw.cross.simple where amount is greater than 0, keep order_id and amount, and write the result to Avro topic orders.cleaned.cross.simple.avro using schema subject orders.cleaned.cross.simple.avro-value.`

Interpreted payload:
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
  "schema_reference": "orders.cleaned.cross.simple.avro-value",
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

Observed result after execution:
```json
{
  "validation": {
    "valid": true,
    "error_code": null,
    "error_message": null,
    "warnings": []
  },
  "normalized_schema_reference": "orders.cleaned.cross.simple.avro-value",
  "source_columns": [
    "order_id",
    "amount"
  ],
  "source_rows": [
    [
      "A1",
      12.0
    ],
    [
      "A2",
      0
    ]
  ],
  "destination_columns": [
    "order_id",
    "amount"
  ],
  "destination_rows": [
    [
      "A1",
      12.0
    ]
  ],
  "filtered_out_count": 1,
  "invalid_records": [],
  "sql": "CREATE TABLE `orders.cleaned.cross.simple.avro` WITH (\n  'connector' = 'confluent',\n  'topic' = 'orders.cleaned.cross.simple.avro',\n  'value.format' = 'AVRO',\n  'avro-confluent.subject' = 'orders.cleaned.cross.simple.avro-value'\n) AS\nSELECT\norder_id AS order_id,\namount AS amount\nFROM `orders.raw.cross.simple`\nWHERE amount > 0;\n"
}
```

## Guardrail Payloads and Results

### `preview_without_sample_records`

Customer natural-language request:
`Show me a preview for orders.raw to orders.cleaned but I am not giving you any sample records.`

Interpreted payload:
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
  "sample_source_records": null
}
```

Observed result after execution:
```json
{
  "error_code": "SAMPLE_RECORDS_MISSING"
}
```

### `invalid_filter_before_schema`

Customer natural-language request:
`Create a Flink job for topic orders.raw.cross.simple where amoint is crerater than 1000 into topic final.`

Interpreted payload:
```json
{
  "source_topic": "orders.raw.cross.simple",
  "destination_topic": "orders.cleaned.cross.simple.avro",
  "source_format": "json",
  "target_format": "avro",
  "pattern_type": "json_to_avro",
  "filter_expression": {
    "include_fields": [
      "order_id"
    ],
    "conditions": [
      "not valid"
    ]
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "amount": "amount"
    }
  },
  "schema_reference": null,
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
  "confluent_credentials": null
}
```

Observed result after execution:
```json
{
  "error_code": "INVALID_FILTER_EXPRESSION"
}
```

### `dry_run_publish_pending_spec`

Customer natural-language request:
`Please publish this Flink job now even though it has not been validated yet.`

Interpreted payload:
```json
{
  "spec_id": "spec-pending",
  "pattern_type": "json_to_json_simple_filter",
  "flink_sql": "SELECT * FROM `orders.raw`;",
  "source_topic": "orders.raw",
  "destination_topic": "orders.cleaned",
  "source_format": "json",
  "target_format": "json",
  "schema_reference": null,
  "validation_status": "pending"
}
```

Observed result after execution:
```json
{
  "passed": false,
  "validation": {
    "valid": false,
    "error_code": "PUBLISH_PRECONDITION_FAILED",
    "error_message": "Job spec validation_status must be passed before publishing.",
    "warnings": []
  },
  "sql_preview": "SELECT * FROM `orders.raw`;"
}
```

### `publish_pending_spec`

Customer natural-language request:
`Publish this Flink SQL immediately.`

Interpreted payload:
```json
{
  "spec_id": "spec-pending",
  "pattern_type": "json_to_json_simple_filter",
  "flink_sql": "SELECT * FROM `orders.raw`;",
  "source_topic": "orders.raw",
  "destination_topic": "orders.cleaned",
  "source_format": "json",
  "target_format": "json",
  "schema_reference": null,
  "validation_status": "pending"
}
```

Observed result after execution:
```json
{
  "published": false,
  "statement_id": null,
  "error_code": "PUBLISH_PRECONDITION_FAILED",
  "error_message": "Job spec validation_status must be passed before publishing."
}
```

### `preview_with_invalid_record`

Customer natural-language request:
`Show me a preview for orders.raw to orders.cleaned using order_id and missing, even if one record is malformed.`

Interpreted payload:
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
      "missing"
    ],
    "conditions": []
  },
  "mapping_definition": {
    "mappings": {
      "order_id": "order_id",
      "missing": "missing"
    }
  },
  "sample_source_records": [
    {
      "order_id": "A1"
    },
    {
      "order_id": "A2",
      "missing": "x"
    }
  ]
}
```

Observed result after execution:
```json
{
  "pattern_type": "json_to_json_simple_filter",
  "source_table": {
    "columns": [
      "order_id",
      "missing"
    ],
    "rows": [
      [
        "A1",
        null
      ],
      [
        "A2",
        "x"
      ]
    ]
  },
  "destination_table": {
    "columns": [
      "order_id",
      "missing"
    ],
    "rows": [
      [
        "A2",
        "x"
      ]
    ]
  },
  "transformation_summary": "Applied pattern json_to_json_simple_filter to 2 source records.",
  "mapping_explanation": [
    "order_id -> order_id",
    "missing -> missing"
  ],
  "filtered_out_count": 0,
  "invalid_records": [
    0
  ]
}
```
