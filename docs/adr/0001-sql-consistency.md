# ADR 0001: Report bounded position without claiming a shared snapshot

Status: accepted.

SQL Server `READ_COMMITTED_SNAPSHOT` supplies statement-level snapshots. Independent reader connections do not share a single transaction snapshot merely because `SNAPSHOT ISOLATION` is enabled. Landfall detects these database options but does not change them and does not claim run-wide `SNAPSHOT` consistency.

Full reads record `BEST_EFFORT`. Incremental reads freeze HIGH before execution, reuse it on resume and record `BOUNDED_POSITION`. Each physical query applies the same `(LOW, HIGH]` window. The manifest records the level used. State advances only after durable manifest commit under a dataset lease and CAS.

Consequences: source updates during parallel reads can produce a mixed-time capture. A future `SNAPSHOT` level requires a proven database-native frozen view across all partitions and concurrent mutation conformance tests. See [Microsoft's snapshot isolation documentation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server).
