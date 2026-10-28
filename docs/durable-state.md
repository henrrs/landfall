# Durable capture state

`state/current.json` records the last committed run, current SourcePosition, recent profile/plan IDs, schema fingerprint and contract version/fingerprint. It is stored in the same Landing storage as run evidence. Local writes use a file lock, fsync and atomic rename; ADLS writes use conditional ETag writes.

Run order is: publish all partition files, validate partition records, write manifest, mark run committed, then compare-and-swap state. A crash before state advancement leaves the previous checkpoint; restart finds a pending run with the same contract and previous position and reuses its saved scope and plan. Already successful partitions are reused.

Concurrent writers cannot silently overwrite state, but the current implementation has no dataset-wide lease. Two simultaneous runs may publish distinct captures before one loses the state compare-and-swap; only the winner advances the checkpoint. Schedule one writer per dataset until leases are implemented.
