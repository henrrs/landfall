# Partition sizing

Logical partitions are work units; `max_in_flight` is the maximum number of simultaneous source readers. The planner may create more partitions than connections, executing them in waves.

Estimated Arrow bytes = estimated source rows × contracted row-width estimate. A large workload asks for at least `ceil(bytes / targetPartitionBytes)` partitions and enough for `max_in_flight × desiredWaves`, capped by row count and `floor(bytes / minimumPartitionBytes)`. The small-table threshold is `minimumPartitionBytes` in Platform Config. CPU, available memory, platform connection cap and the contract's hard source cap affect concurrent readers. Memory primarily bounds batch size and concurrent readers; a whole partition is streamed, never loaded as one table.

SQL Server row width currently uses canonical type estimates, not an Arrow calibration sample. Estimates are advisory: even inaccurate estimates must not change semantic coverage.
