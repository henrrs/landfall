# Skew handling

The planner model can carry null fraction, distinct estimate, quantiles and skew for each column. Candidate scoring penalizes skew and many nulls. When quantiles are supplied and skew exceeds policy, the planner chooses quantile ranges. The SQL Server V1 profiler does not yet collect distributions or quantiles, so its range planning is limited to primary-key bounds and may create uneven work. The executor retries failed partitions and splits bounded numeric ranges; unbounded edge partitions cannot split safely with the current algorithm.

An extension should use bounded samples or source statistics to compare equal-width and quantile boundaries, detect hot values, then use secondary candidates for hierarchical splits. Every resulting predicate must be disjoint and its union must equal the semantic scope. The property tests currently verify numeric range coverage, including values outside profile bounds.
