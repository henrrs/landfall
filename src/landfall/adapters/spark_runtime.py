"""Spark execution adapter using control descriptors and mapInArrow workers."""

from __future__ import annotations

import json
import os
import socket
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from functools import partial
from typing import Any

import pyarrow as pa

from landfall.core.models import Partition, PhysicalReadPlan, RuntimeCapacity

_RESULT_SCHEMA = pa.schema([
    ("partition_id", pa.string()), ("status", pa.string()), ("rows", pa.int64()),
    ("arrow_bytes", pa.int64()), ("ipc_bytes", pa.int64()), ("file", pa.string()),
    ("checksum", pa.string()), ("started_at", pa.string()), ("finished_at", pa.string()),
    ("duration_seconds", pa.float64()), ("source_read_seconds", pa.float64()),
    ("serialization_seconds", pa.float64()), ("storage_seconds", pa.float64()),
    ("retry_count", pa.int32()), ("executor_id", pa.string()),
    ("executor_id_source", pa.string()),
    ("executor_host", pa.string()), ("spark_partition_id", pa.int32()),
    ("task_attempt_id", pa.int64()), ("task_attempt_number", pa.int32()),
    ("spark_stage_id", pa.int32()), ("recovered", pa.bool_()),
])

_CONTROL_FIELDS = (
    ("partition_id", "STRING"), ("partition_kind", "STRING"), ("field", "STRING"),
    ("lower_json", "STRING"), ("upper_json", "STRING"), ("include_null", "BOOLEAN"),
    ("parent", "STRING"), ("depth", "INT"), ("value_json", "STRING"),
    ("secondary_field", "STRING"), ("secondary_lower_json", "STRING"),
    ("secondary_upper_json", "STRING"), ("run_id", "STRING"),
    ("connection_ref", "STRING"),
)


def _control_values_sql(descriptors: list[tuple[Any, ...]]) -> str:
    """Build a tiny typed control DataFrame without Spark's Pandas conversion path."""
    if not descriptors:
        raise ValueError("Spark control wave cannot be empty")

    def literal(value: Any, spark_type: str) -> str:
        if value is None:
            expression = "NULL"
        elif spark_type == "BOOLEAN":
            expression = "TRUE" if value else "FALSE"
        elif spark_type == "INT":
            expression = str(int(value))
        else:
            expression = "'" + str(value).replace("'", "''") + "'"
        return f"CAST({expression} AS {spark_type})"

    selects = []
    for descriptor in descriptors:
        columns = [
            f"{literal(value, spark_type)} AS `{name}`"
            for (name, spark_type), value in zip(_CONTROL_FIELDS, descriptor, strict=True)
        ]
        selects.append("SELECT " + ", ".join(columns))
    return " UNION ALL ".join(selects)


def _timestamp() -> datetime:
    return datetime.now(UTC)


def _executor_identity(driver_host: str | None) -> tuple[str, str, str]:
    host = socket.gethostname()
    executor_id = os.environ.get("SPARK_EXECUTOR_ID")
    if executor_id and executor_id != "driver":
        return executor_id, host, "spark-environment"
    if driver_host and host != driver_host:
        return f"host:{host}", host, "host-fallback"
    return "driver", host, "driver-local"


def _decode(text: str | None) -> Any:
    return json.loads(text) if text is not None else None


def _scope_from_dict(value: dict[str, Any]):
    from landfall.core.models import SemanticReadScope, SourcePosition

    return SemanticReadScope(
        value["identity"], value["mode"], tuple(tuple(item) for item in value["fields"]),
        value.get("filters"), tuple(value["positionFields"]),
        SourcePosition.from_dict(value.get("low")), SourcePosition.from_dict(value.get("high")),
        value["horizon"],
    )


def _partition_from_descriptor(row: dict[str, Any]) -> Partition:
    return Partition(
        row["partition_id"], row["partition_kind"], row["field"],
        _decode(row["lower_json"]), _decode(row["upper_json"]), bool(row["include_null"]),
        row["parent"], int(row["depth"]), _decode(row["value_json"]),
        row["secondary_field"], _decode(row["secondary_lower_json"]),
        _decode(row["secondary_upper_json"]),
    )


def _verified_existing(storage, key: str, schema: pa.Schema) -> tuple[int, int, int, str]:
    import hashlib
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "published.arrow"
        ipc_bytes = storage.download_to(key, path)
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        with pa.ipc.open_file(path) as reader:
            if not reader.schema.equals(schema, check_metadata=False):
                raise ValueError("published partition has incompatible Arrow schema")
            rows = arrow_bytes = 0
            for index in range(reader.num_record_batches):
                batch = reader.get_batch(index)
                rows += batch.num_rows
                arrow_bytes += batch.nbytes
        return rows, arrow_bytes, ipc_bytes, digest


def _execute_one(row: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    import base64
    import hashlib
    import tempfile
    import time
    from pathlib import Path

    import mssql_python  # noqa: F401 - fail early if the worker wheel is incomplete
    import pyarrow as arrow
    from pyspark import TaskContext

    from landfall.adapters.secrets import make_secret_provider
    from landfall.adapters.sqlserver import SqlServerConnector
    from landfall.adapters.storage import make_storage

    started_at = _timestamp()
    tick = time.monotonic()
    context = TaskContext.get()
    if context is None:
        raise RuntimeError("Spark partition function did not run inside a Spark task")
    contract = load_contract_from_document(spec["contract"])
    scope = _scope_from_dict(spec["scope"])
    partition = _partition_from_descriptor(row)
    schema = arrow.ipc.read_schema(arrow.BufferReader(base64.b64decode(spec["schemaIpcBase64"])))
    storage = make_storage({"storage": spec["storage"]}, spec["account"], spec["container"])
    file_key = (f"{spec['base']}/data/run={row['run_id']}/"
                f"part-{partition.id}.arrow")

    if storage.exists(file_key):
        rows, arrow_bytes, ipc_bytes, checksum = _verified_existing(storage, file_key, schema)
        finished_at = _timestamp()
        return _result_row(
            partition.id, rows, arrow_bytes, ipc_bytes, file_key, checksum, started_at, finished_at,
            time.monotonic() - tick, 0.0, 0.0, 0.0, context,
            driver_host=spec.get("driverHost"),
            landfall_retry=int(spec.get("landfallRetry", 0)), recovered=True,
        )

    secrets = make_secret_provider({"secrets": spec["secrets"]})
    secret = secrets.resolve(row["connection_ref"])
    source = SqlServerConnector(secret, spec["sqlserver"]).bind(contract)
    row_width = 0
    for field in schema:
        try:
            row_width += max(1, field.type.bit_width // 8)
        except ValueError:
            row_width += 64
    batch_size = max(1, min(50_000, int(spec["batchTargetBytes"]) // max(1, row_width)))
    source_seconds = serialization_seconds = 0.0
    arrow_bytes = rows = ipc_bytes = 0
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "partition.arrow"
        options = arrow.ipc.IpcWriteOptions(
            compression=None if spec["compression"] == "none" else spec["compression"])
        with arrow.OSFile(str(path), "wb") as sink, arrow.ipc.new_file(sink, schema, options=options) as writer:
            reader = iter(source.read(scope, partition, batch_size))
            try:
                while True:
                    before = time.monotonic()
                    try:
                        batch = next(reader)
                    except StopIteration:
                        source_seconds += time.monotonic() - before
                        break
                    source_seconds += time.monotonic() - before
                    arrow_bytes += batch.nbytes
                    before = time.monotonic()
                    table = arrow.Table.from_batches([batch]).cast(schema, safe=True)
                    for canonical in table.to_batches():
                        writer.write_batch(canonical)
                        rows += canonical.num_rows
                    serialization_seconds += time.monotonic() - before
            finally:
                close = getattr(reader, "close", None)
                if close:
                    close()
        with arrow.ipc.open_file(path) as reader:
            if not reader.schema.equals(schema, check_metadata=False):
                raise ValueError("worker Arrow IPC schema mismatch")
            verified_rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
            if verified_rows != rows:
                raise ValueError("worker Arrow IPC row count mismatch")
        ipc_bytes = path.stat().st_size
        with path.open("rb") as handle:
            checksum = hashlib.file_digest(handle, "sha256").hexdigest()
        storage_tick = time.monotonic()
        try:
            ipc_bytes = storage.publish_file(path, file_key)
        except Exception:
            if not storage.exists(file_key):
                raise
            rows, arrow_bytes, ipc_bytes, checksum = _verified_existing(storage, file_key, schema)
        storage_seconds = time.monotonic() - storage_tick

    finished_at = _timestamp()
    return _result_row(
        partition.id, rows, arrow_bytes, ipc_bytes, file_key, checksum, started_at, finished_at,
        time.monotonic() - tick, source_seconds, serialization_seconds, storage_seconds, context,
        driver_host=spec.get("driverHost"),
        landfall_retry=int(spec.get("landfallRetry", 0)),
    )


def load_contract_from_document(document: dict[str, Any]):
    from landfall.core.contract import IngestionContract

    return IngestionContract.parse(document)


def _result_row(partition_id, rows, arrow_bytes, ipc_bytes, file_key, checksum, started_at,
                finished_at, duration, source_seconds, serialization_seconds, storage_seconds,
                task_context, driver_host=None, landfall_retry=0, recovered=False):
    executor_id, executor_host, executor_id_source = _executor_identity(driver_host)

    return {
        "partition_id": partition_id,
        "status": "SUCCESS",
        "rows": rows,
        "arrow_bytes": arrow_bytes,
        "ipc_bytes": ipc_bytes,
        "file": file_key,
        "checksum": checksum,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration,
        "source_read_seconds": source_seconds,
        "serialization_seconds": serialization_seconds,
        "storage_seconds": storage_seconds,
        "retry_count": task_context.attemptNumber() + landfall_retry,
        "executor_id": executor_id,
        "executor_host": executor_host,
        "executor_id_source": executor_id_source,
        "spark_partition_id": task_context.partitionId(),
        "task_attempt_id": task_context.taskAttemptId(),
        "task_attempt_number": task_context.attemptNumber(),
        "spark_stage_id": task_context.stageId(),
        "recovered": recovered,
    }


def _execute_control_batches(batches: Iterator[pa.RecordBatch], spec_json: str):
    spec = json.loads(spec_json)
    for batch in batches:
        # Only control descriptors are materialized as Python values; source rows remain Arrow.
        descriptors = batch.to_pylist()
        results = [_execute_one(descriptor, spec) for descriptor in descriptors]
        if results:
            yield pa.RecordBatch.from_pylist(results, schema=_RESULT_SCHEMA)


class SparkRuntimeAdapter:
    runtime_type = "spark"

    def __init__(self, options: dict[str, Any], worker_config: dict[str, Any]):
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise RuntimeError("Spark runtime requires optional dependency landfall[spark]") from exc

        options = dict(options)
        mode = options.get("mode", "local")
        master = options.get("master", "local[4]" if mode == "local" else None)
        if not master:
            raise ValueError("Spark mode=cluster requires spark.master")
        self.mode = "single-node" if mode == "local" else "multi-worker"
        settings = dict(options.get("conf", {}))
        if str(settings.get("spark.speculation", "false")).lower() == "true":
            raise ValueError("spark.speculation must be false for SQL Server source protection")
        if str(settings.get("spark.task.maxFailures", "1")) != "1":
            raise ValueError("spark.task.maxFailures must be 1; Landfall owns logical retries")
        settings["spark.speculation"] = "false"
        settings.setdefault("spark.task.maxFailures", "1")
        settings.setdefault("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")
        builder = SparkSession.builder.appName(options.get("appName", "landfall-capture")).master(master)
        for key, value in settings.items():
            builder = builder.config(key, str(value))
        self.spark = builder.getOrCreate()
        if self.spark.conf.get("spark.speculation", "false").lower() == "true":
            self.spark.stop()
            raise ValueError("Spark session has speculation enabled")
        if self.spark.sparkContext.getConf().get("spark.task.maxFailures", "1") != "1":
            self.spark.stop()
            raise ValueError("Spark task retries are enabled; set spark.task.maxFailures=1")
        self.worker_config = worker_config
        self.capacity = SparkRuntimeCapacityAdapter(self.spark, options).capacity

    def execute(self, plan: PhysicalReadPlan,
                partition_handler: Callable[[Partition], list[dict[str, Any]]],
                context: dict[str, Any]) -> dict[str, Any]:
        del partition_handler  # the Spark worker reconstructs adapters from safe references
        connection_limit = context["contract"].get("execution", {}).get("constraints", {}).get(
            "maxSourceConnections")
        if connection_limit is not None and plan.max_in_flight > connection_limit:
            raise ValueError("PhysicalReadPlan exceeds hard maxSourceConnections")
        if plan.max_in_flight > self.capacity.task_slots:
            raise ValueError("PhysicalReadPlan exceeds observed Spark task slots")

        from pyspark.sql import types as st

        result_schema = st.StructType([
            st.StructField("partition_id", st.StringType(), False),
            st.StructField("status", st.StringType(), False),
            st.StructField("rows", st.LongType(), False),
            st.StructField("arrow_bytes", st.LongType(), False),
            st.StructField("ipc_bytes", st.LongType(), False),
            st.StructField("file", st.StringType(), False),
            st.StructField("checksum", st.StringType(), False),
            st.StructField("started_at", st.StringType(), False),
            st.StructField("finished_at", st.StringType(), False),
            st.StructField("duration_seconds", st.DoubleType(), False),
            st.StructField("source_read_seconds", st.DoubleType(), False),
            st.StructField("serialization_seconds", st.DoubleType(), False),
            st.StructField("storage_seconds", st.DoubleType(), False),
            st.StructField("retry_count", st.IntegerType(), False),
            st.StructField("executor_id", st.StringType(), False),
            st.StructField("executor_id_source", st.StringType(), False),
            st.StructField("executor_host", st.StringType(), False),
            st.StructField("spark_partition_id", st.IntegerType(), False),
            st.StructField("task_attempt_id", st.LongType(), False),
            st.StructField("task_attempt_number", st.IntegerType(), False),
            st.StructField("spark_stage_id", st.IntegerType(), False),
            st.StructField("recovered", st.BooleanType(), False),
        ])

        def encode(value):
            return None if value is None else json.dumps(value, default=str, separators=(",", ":"))

        connection_ref = context["sourceConnectionRef"]
        descriptors = []
        for partition in plan.partitions:
            value = partition.to_dict()
            descriptors.append((
                value["id"], value["kind"], value["field"], encode(value["lower"]),
                encode(value["upper"]), value["include_null"], value["parent"], value["depth"],
                encode(value["value"]), value["secondary_field"], encode(value["secondary_lower"]),
                encode(value["secondary_upper"]), context["runId"], connection_ref,
            ))

        worker_spec = {**context, **self.worker_config, "driverHost": socket.gethostname()}
        records: list[dict[str, Any]] = []
        partition_by_id = {part.id: part for part in plan.partitions}
        waves = 0
        for start in range(0, len(descriptors), plan.max_in_flight):
            waves += 1
            wave = descriptors[start:start + plan.max_in_flight]
            dataframe = self.spark.sql(_control_values_sql(wave))
            tasks = min(plan.max_in_flight, len(wave))
            output = None
            for retry in range(2):
                attempt_spec = {**worker_spec, "landfallRetry": retry}
                attempt_json = json.dumps(attempt_spec, sort_keys=True, separators=(",", ":"),
                                          default=str)
                result = dataframe.repartition(tasks).mapInArrow(
                    partial(_execute_control_batches, spec_json=attempt_json), schema=result_schema)
                try:
                    # collect() contains only small partition-result metadata.
                    output = result.collect()
                    break
                except Exception:
                    if retry == 1:
                        raise
            if output is None:
                raise RuntimeError("Spark wave completed without partition evidence")
            records.extend(_partition_result(row.asDict(recursive=True), partition_by_id)
                           for row in output)
        records.sort(key=lambda record: record["partition"]["id"])
        peak = _peak_concurrency(records)
        return {
            "partitions": records,
            "evidence": {
                "type": self.runtime_type,
                "mode": self.mode,
                "sparkVersion": self.spark.version,
                "master": self.spark.sparkContext.master,
                "logicalPartitions": len(plan.partitions),
                "maxInFlight": plan.max_in_flight,
                "waves": waves,
                "tasks": len(records),
                "distinctExecutors": len({record["executorId"] for record in records}),
                "executorIdSources": sorted({record["executorIdSource"] for record in records}),
                "distinctHosts": len({record["executorHost"] for record in records}),
                "tasksByExecutor": _tasks_by_executor(records),
                "peakConcurrentTasks": peak,
                "configuredMaxSourceConnections": connection_limit,
                "observedPeakSqlConnections": None,
                "taskMaxFailures": 1,
                "speculation": False,
            },
        }

    def close(self) -> None:
        self.spark.stop()


class SparkRuntimeCapacityAdapter:
    """Conservative Spark capacity translated into technology-neutral planner inputs."""

    def __init__(self, spark, options: dict[str, Any], reader_memory_bytes: int = 128 * 1024**2):
        context = spark.sparkContext
        configured_cores = options.get("cores")
        task_slots = max(1, int(configured_cores or context.defaultParallelism))
        executor_memory = _memory_bytes(
            context.getConf().get("spark.executor.memory", "1g"))
        executor_count = max(1, int(options.get("executors", 1)))
        if context.master.startswith("local["):
            executor_count = 1
            executor_memory = _memory_bytes(context.getConf().get("spark.driver.memory", "1g"))
        available_memory = executor_memory * executor_count
        memory_safe = max(1, available_memory // reader_memory_bytes)
        self.capacity = RuntimeCapacity(task_slots, int(memory_safe), int(available_memory))


def _memory_bytes(text: str) -> int:
    text = str(text).strip().lower()
    multiplier = {"k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    if text[-1:] in multiplier:
        return max(1, int(float(text[:-1]) * multiplier[text[-1]]))
    return max(1, int(text))


def _partition_result(row: dict[str, Any], partition_by_id: dict[str, Partition]) -> dict[str, Any]:
    partition = partition_by_id[row["partition_id"]]
    return {
        "partition": partition.to_dict(),
        "file": row["file"],
        "rows": row["rows"],
        "bytes": row["ipc_bytes"],
        "ipcBytes": row["ipc_bytes"],
        "arrowBytes": row["arrow_bytes"],
        "checksum": row["checksum"],
        "status": row["status"],
        "attempts": row["retry_count"] + 1,
        "durationSeconds": row["duration_seconds"],
        "sourceReadSeconds": row["source_read_seconds"],
        "serializationSeconds": row["serialization_seconds"],
        "storageSeconds": row["storage_seconds"],
        "runtimeType": "spark",
        "runtimeMode": "single-node" if row["executor_host"] == socket.gethostname() else "multi-worker",
        "startedAt": row["started_at"],
        "finishedAt": row["finished_at"],
        "executorId": row["executor_id"],
        "executorIdSource": row["executor_id_source"],
        "executorHost": row["executor_host"],
        "sparkStageId": row["spark_stage_id"],
        "sparkPartitionId": row["spark_partition_id"],
        "taskAttemptId": row["task_attempt_id"],
        "taskAttemptNumber": row["task_attempt_number"],
        "retries": row["retry_count"],
        "timeouts": 0,
        "recovered": row["recovered"],
    }


def _tasks_by_executor(records: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for record in records:
        executor = record["executorId"]
        result[executor] = result.get(executor, 0) + 1
    return result


def _peak_concurrency(records: list[dict[str, Any]]) -> int:
    events = []
    for record in records:
        start = datetime.fromisoformat(record["startedAt"]).timestamp()
        end = datetime.fromisoformat(record["finishedAt"]).timestamp()
        events.extend(((start, 1), (end, -1)))
    active = peak = 0
    for _, delta in sorted(events, key=lambda event: (event[0], event[1])):
        active += delta
        peak = max(peak, active)
    return peak
