# Failure recovery and consistency

A partition is published only after a complete Arrow IPC file is validated. `.partial` files are never listed by a committed manifest. Failed runs leave their prior checkpoint untouched. Restart searches run metadata for the same contract fingerprint and previous checkpoint, reuses its HIGH, scope and plan, skips successful partitions, and commits only after all required partition files exist.

State compare-and-swap prevents a stale writer from overwriting a newer checkpoint. The current implementation does not hold a source-wide SQL snapshot across connections. Incremental correctness therefore assumes source position values do not move backward and that eligible rows are not backdated inside an already captured HIGH. Full reads are not transactionally frozen while SQL changes. A production connector should add snapshot isolation or a database-native consistent read mechanism before claiming that guarantee.

The SQL profiler uses source row estimates and may under/over partition; open-ended edge ranges preserve value coverage. Published orphan files are recovered on retry, but very large orphan verification currently loads the file into memory. There is no dataset lease or backfill command in V1.
