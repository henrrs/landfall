# Adding a new source

Implement `SourcePort` in `adapters/sources/<connector>` and register it at the CLI composition boundary. The core contract already treats `source.resource` as connector-specific. Your adapter validates its ResourceSchema, contracted field existence and type mappings, filters, source position strategy and capability promises.

For an Oracle connector, map physical types to canonical types, expose metadata/profile signals, implement a safe filter predicate compiler, capture a deterministic HIGH, and stream Arrow RecordBatches. Keep credentials behind `SecretProviderPort`; do not place a connection string in the contract. If Oracle supports `ordered_fields`, prove a unique tie breaker and implement lexicographic `(LOW,HIGH]`; otherwise implement `source_native` and durable cursor serialization.

Write conformance tests comparing a reference single read with partitioned reads, including nulls, gaps, skew, new source columns and resume. The planner only consumes normalized profiles/capabilities, so source-specific SQL never enters `core`.
