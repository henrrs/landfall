# ADR 0002: Use existing SQL statistics and recent manifest history

Status: accepted.

The profiler reads metadata and existing statistics before bounded data queries. It uses `sys.dm_db_stats_properties` for sample/freshness signals and `sys.dm_db_stats_histogram` for distribution; it never updates source statistics automatically. A bounded Arrow probe measures row width and approximate throughput. Missing statistics fall back to indexed extrema and static width.

The planner consumes normalized `SourceProfile` and `ExecutionHistory`, never SQL catalog rows or storage paths. The most recent committed, contract/schema-compatible manifest supplies duration, retries, splits and throughput within a TTL. Planning remains deterministic for identical inputs. The application reuses physical boundaries only when profile/statistics, contract, policy and capacity fingerprints match and recent performance is acceptable. A reused plan receives a new scope and plan ID.

Consequences: histograms summarize at most 200 steps and may be stale; quantile partitions are approximate. The small lab proves ingestion and predicate equivalence, while large skew and performance thresholds need a reproducible benchmark. See [histogram](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-db-stats-histogram-transact-sql) and [stats properties](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-db-stats-properties-transact-sql).
