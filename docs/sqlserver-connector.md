# SQL Server connector

The adapter resolves `connectionRef`, connects with `mssql-python`, and validates `database/schema/table`. Identifiers are validated and quoted; values are bound parameters. `ordered_fields` must include a non-null source unique key (PK or unique index) for deterministic progress.

Profiling first reads `INFORMATION_SCHEMA.COLUMNS`, `sys.indexes`, `sys.index_columns`, `sys.dm_db_partition_stats` (or `sys.partitions` fallback), `sys.stats`, `sys.dm_db_stats_properties` and `sys.dm_db_stats_histogram`. It records simple/composite keys, unique and non-unique index key order, clustered quality, sample fraction, modification counter, histogram boundaries, approximate distinct/NULL/skew signals and statistics age. Indexed numeric/decimal/temporal extrema use `TOP 1 ORDER BY`. A bounded Arrow `TOP` probe records `RecordBatch.nbytes / rows` and throughput; static type widths remain a fallback. No full-table `COUNT(*)` is used. Missing statistics remain unknown rather than triggering a scan.

The extraction query projects only contracted fields. Filters compile from the contract AST to parameterized SQL; incremental bounds use lexicographic predicates; physical range, NULL, value, value-range or complement predicates add another conjunct. `cursor.arrow_reader()` streams RecordBatches directly. The adapter closes each connection when its partition ends. Platform Config controls query timeout and calibration bounds.

`tinyint` maps to `int16` because SQL Server tinyint is unsigned. SQL `timestamp`/`rowversion` maps to `binary`, never a temporal type. `time` has no V1 canonical scalar and fails explicitly. Unsupported or mismatched source types fail before capture. SQL Server source-native CDC is outside V1.

The profiler detects `SNAPSHOT_ISOLATION_AVAILABLE` and `READ_COMMITTED_SNAPSHOT_ON` database options, but does not change settings or claim a shared snapshot. RCSI provides statement-level views, not a run-wide view. Index seeks, histogram reads and the bounded probe still have source-side cost; stale histograms can mislead sizing. See [failure recovery](failure-recovery.md).
