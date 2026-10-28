# Execution runtime

The local adapter observes CPU slots and available memory. The planner uses this with source and platform caps to choose `max_in_flight`. A thread pool executes logical partitions with one SQL connection per active task. Each task streams Arrow batches to a temporary IPC file, publishes it and records partition evidence. Retry and bounded numeric split decisions are run-local mechanics, never contract fields.

The execution protocol is intentionally separate from the planner model. A Spark adapter could translate `RuntimeCapacity` from executor resources and run partition tasks without importing Spark in the core. Such an adapter is not included in V1; the same wheel can be installed as a Databricks wheel task, but this repository currently provides the local executor only.
