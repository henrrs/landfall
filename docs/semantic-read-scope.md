# SemanticReadScope

The application combines the validated contract and committed state to create a semantic scope. The scope contains authorized fields, filter AST, read mode, and (for incremental runs) a lower and upper `SourcePosition`. The upper position is captured before execution. Every physical query intersects its partition predicate with the same filter and position window.

The scope fingerprint is recorded in the physical plan and manifest. The planner can change the traversal column, boundaries and concurrency without changing eligibility. A `customer_id` range can split a scope whose checkpoint is `(updated_at, customer_id)`.

For full reads, the current implementation records a run start time but does not hold a database snapshot across independent connections. Incremental runs freeze HIGH before any partition query; each query uses the same `(LOW, HIGH]` window. The manifest records `BEST_EFFORT` for full and `BOUNDED_POSITION` for incremental, never `SNAPSHOT`. See [failure and consistency limits](failure-recovery.md).
