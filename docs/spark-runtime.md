# Spark runtime

## Boundary

Spark is an execution runtime, not a source connector. `SqlServerConnector` remains the only SQL Server source adapter. The planner emits the same technology independent `PhysicalReadPlan` for local and Spark execution. Core and planner modules do not import PySpark.

The production data path is:

```text
SQL Server → TDS → mssql-python → cursor.arrow_reader()
           → PyArrow RecordBatch → Arrow IPC file → Landing
```

The Spark adapter does not use JDBC, `mapInPandas`, `mapPartitions`, or a driver-side source read. `DataFrame.mapInArrow` distributes control descriptors. Workers return only one small `PartitionExecutionResult` per logical partition; the source rows stay inside the worker and are written to the existing Arrow IPC storage layout.

## Configuration and optional dependency

Local use and wheel import do not require PySpark. Install `landfall[spark]` for Spark clients, or install the same `landfall` wheel and pinned PySpark distribution in the Spark worker image. The extra also installs Pandas because PySpark 4.0.1 internally builds `mapInArrow` through its `pandas_udf` helper and checks for Pandas. Landfall does not convert source records to Pandas: source batches and map input/output remain `pyarrow.RecordBatch`. The lab pins PySpark 4.0.1 and Java 21. A managed Spark service can supply its own Spark distribution; the Landfall Spark extra is optional.

Select the runtime in Platform Config, never in the Ingestion Contract:

```yaml
runtime:
  type: spark
  spark:
    mode: local
    master: local[4]
    cores: 4
    executors: 1
```

For a Spark Standalone cluster, set `mode: cluster` and `master: spark://spark-master:7077`. The adapter translates observed Spark default parallelism plus configured core and executor memory limits into `RuntimeCapacity`. The Planner still applies `safetyMaxConnections` and the contract's `maxSourceConnections` hard cap.

## Work waves and source protection

The driver derives small descriptors from each `PhysicalReadPlan`: logical partition ID and predicate fields/bounds, run ID, and `connectionRef`. Passwords, connection strings, tokens, and resolved secrets are not included. Each wave has no more descriptors than `maxInFlight`; the control DataFrame is repartitioned to that wave size before `mapInArrow`. A Spark task processes its descriptor sequentially and opens at most one SQL connection.

The adapter rejects `spark.speculation=true` and configures `spark.task.maxFailures=1`. Landfall retries an unsuccessful wave once; a retry reuses and validates already published partition files. A full run restart reuses the same pending run and deterministic `run_id + logical_partition_id` output path. Worker attempts cannot advance state or write the final manifest.

Landfall measures concurrent Spark task intervals from worker results. It does not query SQL Server session telemetry, so `observedPeakSqlConnections` is `null`; configured connection ceilings and observed task concurrency are reported separately. The worker reports `SPARK_EXECUTOR_ID` when available. If Spark does not pass it into the Python worker, the adapter emits a host-derived executor identity and marks `executorIdSources` as `host-fallback`; distinct hosts remain a separate field. SQL retry, timeout, and adaptive split details appear in partition evidence when observed. A worker loss, permanent wave failure, and Spark adaptive split are not yet part of the automated Docker fault matrix.

## Arrow, publication, and state

Each worker resolves `connectionRef` through the configured secret provider, constructs the existing `SqlServerConnector`, streams `cursor.arrow_reader()` batches, casts each batch to the contract Arrow schema, writes a temporary IPC file, validates its schema and row count, computes a streaming SHA-256, and publishes it through the configured storage adapter. No whole partition is held in memory.

Spark retries can race to publish the same logical output. The final path is immutable. A task that observes an existing path downloads and validates its Arrow IPC schema and reuses it; workers never write partition manifests, dataset state, or checkpoints. The application collects result metadata, persists partition evidence, writes the final manifest, checks the dataset lease, then advances state with CAS.

Pending runs record their runtime type and mode. Resume requires a compatible runtime identity. Physical plans contain no Spark sessions, executor IDs, or Spark partition IDs; those stay in execution evidence.

## Lab modes and reports

```bash
make e2e-spark-single
make e2e-spark-multi
make test-all-spark
make test-all
LANDFALL_TEST_SCALE=full make test-all
```

The single-node mode uses `local[4]`. The Standalone mode starts a master and two workers; both worker images install the built Landfall wheel, `mssql-python`, PyArrow, PySpark, the Pandas compatibility package required by PySpark, Java dependencies, and Azure adapters. The same filtered 10k–200k row SQL scenario runs through Reference SINGLE, Local, Spark single-node, and Spark multi-worker. Reports include runtime version/mode, host and executor distribution, waves, task concurrency, source and captured rows, plan strategy, partition balance, file/checksum evidence, Arrow and IPC bytes, duration, throughput, retries, lost rows, and duplicates.

Structured results are written under `build/test-runs/<run-id>/` by default as `summary.md`, `report.json`, `scenarios.json`, `pytest.xml`, and logs. The `test-all` orchestrator reads JUnit XML and scenario JSON; it does not parse human console output.

## Secrets and storage

The worker closure contains secret-provider configuration and references only. For the lab, workers resolve the SQL secret from the Key Vault emulator and the ADLS connection through an environment variable used by the existing storage adapter. Tests inspect the physical plan, descriptor rows, serialized worker configuration, and returned results for credential values. A real Azure identity and storage authorization deployment remains an environment-specific validation task.

## Databricks boundary

The adapter uses public PySpark DataFrame APIs and a wheel-installed worker environment, but Databricks and Unity Catalog External Location access are not certified here. This phase does not distribute Storage Account credentials to bypass UC. A future Databricks certification must prove executor access under the workspace's UC configuration.

## Validation evidence and limits

The Spark adapter has unit evidence for `mapInArrow`, descriptor-only batches, runtime capacity, and credential-free serialization. Docker certification proves worker SQL reads and runtime equivalence only when `make test-all-spark` completes successfully. Benchmarks are separate from the default gate. Shared SQL snapshot isolation across independent readers remains unavailable, and observed SQL sessions remain unmeasured.

References: [Spark `DataFrame.mapInArrow`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInArrow.html), [Microsoft mssql-python Arrow integration](https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/arrow-integration?view=sql-server-ver16).
