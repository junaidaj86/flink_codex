---
name: avro-patterns
description: Focus on Avro source and target workflows.
---

**Name:** Avro Patterns

Focus on Avro source and target workflows.

- Require either `schema_reference` or resolvable Confluent credentials for Avro targets
- Validate Avro schemas with `fastavro.parse_schema`
- Use Schema Registry subject order: `{topic}-value`, then `{topic}`
