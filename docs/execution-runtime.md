# Execution runtime

`ExecutionRuntimePort` is implemented by `LocalRuntimeAdapter` and `SparkRuntimeAdapter`. The local adapter observes CPU slots and available memory. The Spark capacity adapter translates Spark task slots and executor memory into the same `RuntimeCapacity` model. The Planner uses runtime capacity with source and platform caps to choose `max_in_flight`; it has no Spark imports or runtime-specific branches.

Local execution uses a bounded thread pool. Spark execution distributes only logical partition descriptors with `DataFrame.mapInArrow`, in waves no larger than `max_in_flight`. Each worker resolves the source connection reference, uses the same `SqlServerConnector`, streams Arrow batches into the same immutable IPC file layout, and returns metadata only. Speculation is rejected and Spark task retries are configured to one attempt so Landfall owns the bounded wave retry. `observedPeakSqlConnections` remains null because the runtime does not measure SQL sessions.

The application writes all partition evidence, manifest, and checkpoint. Workers publish immutable data files but cannot advance state. Pending runs record runtime type and mode; resume requires the same identity. See [Spark runtime](spark-runtime.md) for topology, worker packaging, limitations, and Databricks boundaries.
