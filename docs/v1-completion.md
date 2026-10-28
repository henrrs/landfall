# Landfall V1 Completion

## Scope

SQL Server to immutable Arrow IPC Landing, using the independent Ingestion Contract V1. This file records implementation status; a passing unit suite alone does not imply full SQL Server V1 coverage.

## Completion matrix

| Capability | Status | Evidence |
|---|---|---|
| Contract fidelity and separation | DONE | `src/landfall/core/contract.py`, `tests/conformance/test_normative_matrix.py`, `docs/ingestion-contract-integration.md` |
| SQL metadata, keys and indexes | DONE | `src/landfall/adapters/sqlserver.py`, lab profiler assertion, `docs/sqlserver-connector.md` |
| Existing statistics and histogram ingestion | DONE | `src/landfall/adapters/sqlserver.py`, lab seed `UPDATE STATISTICS`, lab profiler assertion, `docs/sqlserver-connector.md` |
| Arrow row calibration | DONE | `src/landfall/adapters/sqlserver.py`, lab profiler assertion, `docs/partition-sizing.md` |
| Numeric/decimal and temporal range | DONE | `src/landfall/core/planner.py`, property tests, SQL reference reader E2E, `docs/adaptive-planner.md` |
| Numeric and temporal quantile | DONE | Planner unit/property tests, SQL histogram profiles and lab reference comparisons (max 34 vs 199 rows), `docs/skew-handling.md` |
| NULL, low-cardinality and hot-value traversal | DONE | Planner tests, hierarchical property test, SQL reference E2E and HotShape selected plan (29 vs 180 rows), `docs/skew-handling.md` |
| Composite key planning | DONE | Key metadata, `COMPOSITE` hierarchical strategy, unit/property tests and CompositeHot reference E2E, `docs/adaptive-planner.md` |
| GUID safe fallback | DONE | `tests/unit/test_planner.py`, `docs/adaptive-planner.md` |
| SQL hash partitioning | DEFERRED | Non-sargable SQL hash scans are not enabled without source-cost evidence |
| Resource-aware sizing and connection cap | DONE | `src/landfall/core/planner.py`, planner tests, `docs/partition-sizing.md` |
| Historical planner feedback | DONE | Manifest partition metrics, `_history`, planner test, `docs/adaptive-planner.md` |
| Conservative physical boundary reuse | DONE | `_reusable_plan`, local compatibility/capacity test, `docs/adaptive-planner.md` |
| Consistency level recording | DONE | `PhysicalReadPlan`, manifest, local and lab assertions, `docs/failure-recovery.md` |
| Shared SQL snapshot across readers | PARTIAL | Database options detected; no shared snapshot mechanism; manifest never claims `SNAPSHOT` |
| Dataset lease and state CAS | DONE | `DatasetLease`, local tests, two-run Floci E2E, `docs/durable-state.md` |
| Resume and streaming orphan recovery | DONE | Local failure injection and E2E, `docs/failure-recovery.md` |
| Broad SQL/contract conformance matrix | PARTIAL | Normative matrix, SQL reference reader fixture and local tests cover representative cases; full matrix remains |
| Wheel-only lab E2E | DONE | `make e2e`, `lab/runner/e2e.py`, `docs/local-lab.md` |
| Distributed Spark runtime / UC resolver | DEFERRED | Explicit non-goal of this phase; `docs/databricks-external-location.md` |

## Completed

The normative contract remains the source of truth. SQL profiling now collects key/index metadata, existing histogram and freshness signals, and bounded Arrow calibration. Physical plans use observed width, resource caps and recent execution metrics. A renewable storage lease serializes writers. Published orphan files are recovered through chunked download and Arrow file validation. A wheel-only lab verifies SQL metadata and compares reference SINGLE with partitioned SQL readers.

## Partially completed

- **Quantile throughput:** The 200-row SQL fixtures prove better row balance, but not production-scale throughput or source cost. Impact: optimal thresholds on large tables remain uncertain. Next: benchmark large indexed skewed numeric/temporal fixtures and compare p95 duration, logical reads and aggregate throughput.
- **Low-cardinality source cost:** The HotShape lab proves plan selection and row balance on 200 rows. Impact: value bucket selection may still be suboptimal on large tables with competing indexes. Next: benchmark selectivity, seeks and logical reads and tune scoring from measured cost.
- **Composite breadth:** The indexed two-column hot-leading-key case uses `COMPOSITE`; other key shapes can safely fall back to SINGLE. Impact: some multi-column workloads may underuse parallel capacity. Next: add lexicographic boundaries or safe composite sampling for cases without a useful leading component.
- **Shared snapshot:** SQL Server options are detected, but independently opened readers cannot share one transaction snapshot in the current adapter. Impact: concurrent source writes may yield a mixed-time full capture or changes within a bounded incremental window. Next: evaluate a database-native frozen view or run-wide snapshot mechanism; add concurrent mutation tests before claiming `SNAPSHOT`.
- **Conformance breadth:** The lab covers sparse/negative numeric, decimal, temporal, NULL, hot-value and composite predicates, GUID/unique/composite PK cases, empty/no-index fallback, wide binary/XML/rowversion types, schema drift, index metadata and incremental filters. It does not yet inject concurrent SQL mutations or process crashes into the Docker runner. Impact: those recovery behaviors are proven only by local failure tests or remain unverified. Next: add controlled concurrent mutation and container-kill fixtures with reference SINGLE comparisons.

## Remaining

- **Plan reuse cost:** Compatible boundaries can be reused, but each run still profiles SQL to validate them. Impact: source metadata/probe cost remains. Next: benchmark and, if useful, add a metadata-only validity check that can skip bounded calibration for unchanged sources.
- **End-to-end performance:** No large-table throughput or p95 balance benchmark. Impact: cost thresholds are provisional. Next: add reproducible benchmark data and record source logical reads, query latency, storage latency and partition throughput.

## Explicitly deferred

SQL hash predicates until an indexed, efficient source mechanism is demonstrated; real Azure compatibility without credentials in this environment; distributed Spark execution and automatic Unity Catalog External Location resolution; new source connectors and downstream Bronze/Silver/Gold.

## Tests / evidence

Local validation on 2026-10-28: `make lint` passed; `make test` passed 122 tests with 0 failures and 0 skips; `make verify-wheel` installed and ran wheel 1.0.0; `make e2e` passed using the installed wheel and emulators; `git diff --check` passed. The separate CI E2E job runs on `main` pushes or manual dispatch. CI status belongs in the final completion report after publication.

## Known risks

SQL statistics can be absent or stale; the planner uses conservative fallback signals. A bounded probe still executes a SQL query and may cost more on an unindexed source. The storage lease relies on reasonably synchronized clocks and renewal; state CAS is the final checkpoint guard. `BOUNDED_POSITION` does not mean transactionally consistent row versions across readers. The IPC file is validated locally before upload and orphan files are validated after download; normal successful uploads rely on SDK transfer integrity and atomic rename rather than downloading every published file again.
