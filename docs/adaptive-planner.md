# Adaptive planner

The planner receives a `SemanticReadScope`, source profile, normalized source capabilities, runtime capacity, contract constraints and platform policy. It computes `max_in_flight` from CPU slots, memory-safe readers, platform safety cap and `maxSourceConnections`. It then estimates Arrow bytes from rows × row width and chooses `SINGLE` for small sources or when no safe bounded candidate exists.

For larger SQL tables the current connector profiles primary-key numeric or temporal bounds and the planner chooses a range candidate based on type, index, uniqueness, null rate and skew signals. If a source profile supplies quantile boundaries and skew exceeds the configured threshold, the planner uses numeric or temporal quantile ranges. It writes reasons and fingerprints to the plan. Outermost ranges are open-ended, so new values outside profiled min/max remain covered. Nullable candidates receive an explicit null bucket. A failed bounded numeric partition can split recursively up to `maxSplitDepth`.

The SQL Server profiler does not yet emit quantile boundaries. Low-cardinality, composite, hash and hot-value strategies are future planner extensions. The conservative `SINGLE` fallback preserves coverage where estimates or safe predicates are unavailable. See [partition sizing](partition-sizing.md) and [skew](skew-handling.md).
