# SourcePosition

`SourcePosition` is durable progress **between runs**. `Partition` is traversal **within a run**. SQL Server V1 uses lexicographic `ordered_fields`; the final fields must include the primary key to make progress deterministic. `source_native` is represented in the contract model but awaits a connector with native cursor semantics.

An incremental SQL run captures `HIGH` at start and reads `(LOW, HIGH]` with the contract filters. `initialPosition: beginning` uses no lower bound. `initialPosition: current` sets LOW to the captured HIGH and creates an empty first capture. A successful manifest advances state to HIGH; failed or partial runs do not advance it.

The checkpoint is stored under `_ingestion/state/current.json`, never in the contract. The run manifest stores previous and committed positions for audit. A backfill must use separate state; no backfill command is exposed in V1.
