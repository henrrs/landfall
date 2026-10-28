# SourcePosition

`SourcePosition` is durable progress **between runs**. `Partition` is traversal **within a run**. SQL Server V1 uses lexicographic `ordered_fields`; the fields must include a non-null unique source key to make progress deterministic. `source_native` is represented in the contract model but awaits a connector with native cursor semantics.

An incremental SQL run captures `HIGH` at start and reads `(LOW, HIGH]` with the contract filters. `initialPosition: beginning` uses no lower bound. `initialPosition: current` sets LOW to the captured HIGH and creates an empty first capture. A successful manifest advances state to HIGH; failed or partial runs do not advance it.

The checkpoint is stored under `_ingestion/state/current.json`, never in the contract. The run manifest stores previous and committed positions for audit. A backfill must use separate state; no backfill command is exposed in V1.

The lease serializes writers for a dataset. A resume loads the saved HIGH from `run.json`; it never asks SQL Server for a new HIGH. Physical partition keys can differ from position fields, as in the lab's `customer_id` ranges over `(updated_at, customer_id)` progression.

Durable position values are JSON, but HIGH/LOW comparison converts them using contracted canonical types. Decimal positions compare numerically, and timestamp positions compare as instants across offsets. SQL datetimeoffset values are normalized to UTC when serialized.
