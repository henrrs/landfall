# Testing strategy

## Gates

The standard `make test` target runs architecture, normative contract and SQL conformance, planner, property, and local capture tests. `make contract-lock` runs artifact-integrity checks separately from parser conformance. Golden SHA-256 values are checked as stored; tests never rewrite them.

The complete gates are:

```bash
make test-all-nospark
make test-all-spark
make test-all
LANDFALL_TEST_SCALE=full make test-all
```

`test-all-nospark` runs the lock check, lint, non-Spark Pytest suites, installed-wheel validation, the LocalRuntime SQL Server/Key Vault/Floci E2E, and `git diff --check`. It does not start Spark master or workers. `test-all-spark` runs the lock check, Spark architecture/runtime unit tests, builds the wheel, and runs the Spark SQL E2E with a single-node runtime and a two-worker Standalone cluster. `test-all` runs both gates and records one combined result. Spark is an optional dependency; the Spark gate installs it in the driver and worker images. Machine-readable artifacts are written under `build/test-runs/` by default and can be redirected with `LANDFALL_ARTIFACT_ROOT`.

Scale profiles are `tiny` (10,000 rows), `ci` (20,000 rows, default), and `full` (200,000 rows). Each Spark scenario has multiple Landfall logical partitions. The full profile is opt-in and is not a performance SLO.

## Evidence

Conventional tests remain Pytest and produce JUnit XML. The SQL lab writes structured `ScenarioEvidence` directly from manifests and runtime results; it does not parse console text. Each run stores `summary.md`, `report.json`, `scenarios.json`, `pytest.xml`, and per-command logs under `build/test-runs/<run-id>/` by default. Set `LANDFALL_ARTIFACT_ROOT` to change the host path. CI uploads these files even when a gate fails.

Runtime evidence includes source and eligible row counts, captured rows, strategy, partitions, files, Arrow and IPC bytes, duration, rows/s, MB/s, partition min/mean/p50/p95/max and imbalance, retries, timeouts, splits, runtime mode, Spark tasks, waves, executor/host distribution, observed task concurrency, lost rows, duplicates, and schema/canonical hash equality. SQL session counts are reported as unobserved (`null`) unless measured directly.

## SQL and runtime matrix

The SQL Server lab verifies metadata, statistics, histogram and Arrow calibration; ranges, quantiles, NULL, hot and composite predicates; wide SQL types; filters; incremental boundaries; and schema drift. The runtime conformance scenario uses the same contract against Reference SINGLE, LocalRuntime, Spark `local[4]`, and Spark Standalone. It compares rows by canonical multiset hash and checks schema equality, partition distribution, and output artifacts.

Architecture tests keep PySpark outside Core, prohibit JDBC production paths, confirm `mapInArrow` is the Spark execution mechanism, and inspect serialized plans, descriptors, work configuration, and results for credentials. The SQL connector data-path test exercises `cursor.arrow_reader()` and rejects `fetchall()`/`to_pylist()` in that path.

## Failures and limits

Local capture tests inject lease conflicts, CAS errors, manifest write failures, orphan files, and partition timeouts. Spark execution disables hidden task retries, retries a failed wave once, and reuses validated published files. The Docker gate does not yet kill a worker during a live source query, validate recovery from that exact failure, or test adaptive partition splitting on Spark. It also does not measure SQL session concurrency, benchmark large-table costs, certify real Azure storage, or certify Databricks/Unity Catalog. See [Spark runtime](spark-runtime.md) and [V1 completion](v1-completion.md) for the status of each boundary.
