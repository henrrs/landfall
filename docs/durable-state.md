# Durable capture state

`state/current.json` records the last committed run, current SourcePosition, recent profile/plan IDs, schema fingerprint and contract version/fingerprint. It is stored in the same Landing storage as run evidence. Local writes use a file lock, fsync and atomic rename; ADLS writes use conditional ETag writes.

Run order is: publish all partition files, validate partition records, write manifest, mark run committed, then compare-and-swap state. A crash before state advancement leaves the previous checkpoint; restart finds a pending run with the same contract and previous position and reuses its saved scope and plan. Already successful partitions are reused.

A renewable dataset lease lives at `_ingestion/state/writer-lease.json`. Conditional storage writes acquire it before state inspection, renew it during the run, and check it before manifest/state commit. Local and ADLS adapters use the same CAS protocol. An abandoned lease expires after 90 seconds; another writer can then resume. State CAS remains the final guard. Plan files now carry `version: 2`; old files without it load as version 1, and existing state files remain readable.
