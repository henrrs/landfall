"""Contract → scope → profile → plan → Arrow IPC → manifest → durable state."""

from __future__ import annotations

import json
import statistics
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from landfall.core.contract import ContractError, IngestionContract, fingerprint
from landfall.core.models import (
    ExecutionHistory,
    Partition,
    PhysicalReadPlan,
    PlannerPolicy,
    SemanticReadScope,
    SourcePosition,
    ordered_position_key,
    semantic_scope,
)
from landfall.core.planner import plan, split_partition
from landfall.ports import SourcePort, StoragePort, TelemetryPort


class DatasetBusy(RuntimeError):
    pass


class DatasetLease:
    """Renewable storage CAS lease; no process-local coordination is required."""

    def __init__(self, storage: StoragePort, key: str, seconds: int = 90):
        self.storage, self.key, self.seconds = storage, key, seconds
        self.owner = uuid.uuid4().hex
        self.stop = threading.Event()
        self.guard = threading.Lock()
        self.error: Exception | None = None
        self.etag = ""
        self.thread: threading.Thread | None = None

    def _payload(self, expires: float) -> bytes:
        return _json({"owner": self.owner, "expiresAt": expires})

    def __enter__(self):
        current = self.storage.etag(self.key) or ""
        if current:
            record = json.loads(self.storage.read(self.key))
            if float(record.get("expiresAt", 0)) > time.time():
                raise DatasetBusy("dataset already has an active writer")
        self.etag = self.storage.write(self.key, self._payload(time.time() + self.seconds),
                                       expected_etag=current)
        self.thread = threading.Thread(target=self._renew, daemon=True)
        self.thread.start()
        return self

    def _renew(self):
        while not self.stop.wait(self.seconds / 3):
            try:
                with self.guard:
                    self.etag = self.storage.write(
                        self.key, self._payload(time.time() + self.seconds),
                        expected_etag=self.etag)
            except Exception as exc:  # noqa: BLE001 - the main thread checks lease ownership
                self.error = exc
                return

    def check(self):
        with self.guard:
            if self.error:
                raise DatasetBusy("dataset lease renewal failed") from self.error
            record = json.loads(self.storage.read(self.key))
            if record.get("owner") != self.owner or float(record.get("expiresAt", 0)) <= time.time():
                raise DatasetBusy("dataset lease lost")

    def __exit__(self, *_):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=5)
        with self.guard:
            try:
                record = json.loads(self.storage.read(self.key))
                if record.get("owner") == self.owner:
                    self.storage.write(self.key, self._payload(0), expected_etag=self.etag)
            except Exception:  # noqa: BLE001 - expiration still releases a crashed writer
                import logging

                logging.getLogger(__name__).warning("dataset lease release failed")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, indent=2, default=str).encode()


def _arrow_type(text: str) -> pa.DataType:
    scalar = {"boolean": pa.bool_(), "int8": pa.int8(), "int16": pa.int16(),
              "int32": pa.int32(), "int64": pa.int64(), "float32": pa.float32(),
              "float64": pa.float64(), "string": pa.string(), "binary": pa.binary(),
              "date": pa.date32(), "timestamp": pa.timestamp("us")}
    if text in scalar:
        return scalar[text]
    if text.startswith("decimal("):
        precision, scale = map(int, text[8:-1].split(","))
        return pa.decimal128(precision, scale) if precision <= 38 else pa.decimal256(precision, scale)
    if text.startswith("array<"):
        return pa.list_(_arrow_type(text[6:-1]))
    if text.startswith("map<"):
        from landfall.core.contract import _split_generic

        key, value = _split_generic(text[4:-1])
        return pa.map_(_arrow_type(key), _arrow_type(value))
    if text.startswith("struct<"):
        from landfall.core.contract import _split_generic

        fields = []
        for member in _split_generic(text[7:-1]):
            name, typ = member.split(":", 1)
            fields.append(pa.field(name.strip(), _arrow_type(typ.strip())))
        return pa.struct(fields)
    raise ContractError(f"cannot create Arrow type for {text}")


def arrow_schema(contract: IngestionContract) -> pa.Schema:
    return pa.schema([pa.field(name, _arrow_type(typ)) for name, typ in contract.fields])


class JsonTelemetry:
    def event(self, name: str, **fields: Any) -> None:
        import logging

        logging.getLogger("landfall").info(json.dumps({"event": name, "at": _now(), **fields}, default=str))


class CaptureEngine:
    def __init__(self, contract: IngestionContract, source: SourcePort, storage: StoragePort,
                 capacity, policy: PlannerPolicy, compression: str = "lz4",
                 telemetry: TelemetryPort | None = None):
        self.contract = contract
        self.source = source
        self.storage = storage
        self.capacity = capacity
        self.policy = policy
        self.compression = compression.lower()
        self.telemetry = telemetry or JsonTelemetry()
        self.base = f"{contract.document['metadata']['sourceSystem']}/{contract.document['target']['asset']}"
        self.meta = self.base + "/_ingestion"
        self.schema = arrow_schema(contract)

    def _read_json(self, key: str) -> dict[str, Any]:
        return json.loads(self.storage.read(key))

    def _state(self) -> tuple[dict[str, Any], str]:
        key = self.meta + "/state/current.json"
        etag = self.storage.etag(key)
        return (self._read_json(key) if etag else {}, etag or "")

    def _resume(self, previous: SourcePosition | None, last_run: str | None) -> dict | None:
        prefix = self.meta + "/runs"
        expected = previous.to_dict() if previous else None
        candidates = []
        for key in self.storage.list(prefix):
            if not key.endswith("/run.json"):
                continue
            record = self._read_json(key)
            if record.get("status") in {"RUNNING", "COMMITTED"} and record.get("runId") != last_run and record.get("contractFingerprint") == self.contract.hash and record.get("previousPosition") == expected:
                candidates.append(record)
        return max(candidates, key=lambda r: r["startedAt"]) if candidates else None

    def prepare(self) -> tuple[SemanticReadScope, Any, PhysicalReadPlan]:
        self.source.validate(self.contract)
        self.telemetry.event("contract_validated", identity=self.contract.identity)
        state, _ = self._state()
        low = SourcePosition.from_dict(state.get("position"))
        read = self.contract.document["read"]
        high = self.source.high_position(self.contract) if read["mode"] == "incremental" else None
        if low and (high is None or ordered_position_key(self.contract, high) <
                    ordered_position_key(self.contract, low)):
            high = low
        if low is None and read["mode"] == "incremental" and read["initialPosition"] == "current":
            low = high
        scope = semantic_scope(self.contract, low, high, _now())
        self.telemetry.event("scope_resolved", identity=self.contract.identity, scope=scope.hash)
        profile = self.source.profile(self.contract)
        self.telemetry.event("source_profiled", profile=profile.profile_id, rows=profile.estimated_rows)
        history = self._history(state, profile.schema_fingerprint)
        consistency = "BOUNDED_POSITION" if scope.mode == "incremental" else "BEST_EFFORT"
        physical = self._reusable_plan(state, scope, profile, history, consistency)
        if physical is None:
            physical = plan(self.contract, scope, profile, self.source.capabilities,
                            self.capacity, self.policy, history, consistency)
        self.telemetry.event("plan_created", plan=physical.plan_id, strategy=physical.strategy,
                             partitions=physical.logical_partitions, maxInFlight=physical.max_in_flight)
        return scope, profile, physical

    def _reusable_plan(self, state, scope, profile, history, consistency):
        if not history or history.retries or history.timeouts or history.splits or history.failures:
            return None
        if (history.average_partition_seconds is not None and
                history.average_partition_seconds < self.policy.target_partition_seconds / 10):
            return None
        if (history.p95_partition_seconds is not None and
                history.p95_partition_seconds > self.policy.target_partition_seconds):
            return None
        run_id = state.get("lastCommittedRun")
        if not run_id:
            return None
        manifest = self._read_json(f"{self.meta}/runs/{run_id}/manifest.json")
        if (manifest.get("profileReuseSignature") != profile.reuse_signature or
                manifest.get("policyFingerprint") != fingerprint(asdict(self.policy)) or
                manifest.get("capacityFingerprint") != fingerprint(asdict(self.capacity))):
            return None
        old = PhysicalReadPlan.from_dict(self._read_json(f"{self.meta}/plans/{manifest['planId']}.json"))
        if old.version != 2 or old.contract_fingerprint != self.contract.hash:
            return None
        new_id = fingerprint({"contract": self.contract.hash, "scope": scope.hash,
                              "profile": profile.hash, "strategy": old.strategy,
                              "partitions": [p.to_dict() for p in old.partitions],
                              "concurrency": old.max_in_flight, "policy": asdict(self.policy),
                              "consistency": consistency})[:20]
        return replace(old, plan_id=new_id, scope_fingerprint=scope.hash,
                       profile_fingerprint=profile.hash, consistency_level=consistency,
                       reasons=old.reasons + ("reused compatible physical boundaries",))

    def _history(self, state: dict[str, Any], schema_fingerprint: str) -> ExecutionHistory | None:
        run_id = state.get("lastCommittedRun")
        if not run_id:
            return None
        key = f"{self.meta}/runs/{run_id}/manifest.json"
        if not self.storage.exists(key):
            return None
        manifest = self._read_json(key)
        if (manifest.get("contractFingerprint") != self.contract.hash or
                manifest.get("observedSchemaFingerprint") != schema_fingerprint):
            return None
        ended = datetime.fromisoformat(manifest["endedAt"])
        if (datetime.now(UTC) - ended).total_seconds() > self.policy.history_ttl_seconds:
            return None
        records = manifest.get("partitions", [])
        durations = sorted(float(r.get("durationSeconds", 0)) for r in records if r.get("durationSeconds"))
        rates = [r["bytes"] / r["durationSeconds"] for r in records
                 if r.get("durationSeconds") and r.get("bytes")]
        def mean_metric(key):
            values = [float(r[key]) for r in records if r.get(key) is not None]
            return statistics.mean(values) if values else None
        return ExecutionHistory(
            runs=int(manifest.get("historyRuns", 0)) + 1,
            last_strategy=manifest.get("strategy"), last_candidate=manifest.get("candidate"),
            last_partitions=len(records),
            average_partition_seconds=statistics.mean(durations) if durations else None,
            p95_partition_seconds=durations[min(len(durations) - 1, int(len(durations) * .95))] if durations else None,
            bytes_per_second_per_reader=statistics.mean(rates) if rates else None,
            retries=sum(max(0, int(r.get("attempts", 1)) - 1) for r in records),
            splits=sum(bool(r.get("partition", {}).get("parent")) for r in records),
            timeouts=sum(int(r.get("timeouts", 0)) for r in records),
            average_query_latency_seconds=mean_metric("queryLatencySeconds"),
            average_serialization_seconds=mean_metric("serializationSeconds"),
            average_storage_seconds=mean_metric("storageSeconds"),
            schema_fingerprint=schema_fingerprint, observed_at=manifest["endedAt"])

    def _partition(self, run_id: str, scope: SemanticReadScope, partition: Partition) -> list[dict[str, Any]]:
        manifest_key = f"{self.meta}/runs/{run_id}/partitions/{partition.id}.json"
        if self.storage.exists(manifest_key):
            cached = self._read_json(manifest_key)
            if cached.get("status") == "SUCCESS" and self.storage.exists(cached["file"]):
                return [cached]
        data_key = f"{self.base}/data/run={run_id}/part-{partition.id}.arrow"
        last_error = None
        errors: list[str] = []
        timeouts = 0
        for attempt in range(1, 3):
            if self.storage.exists(data_key):
                try:
                    return self._recover_partition(data_key, manifest_key, partition)
                except Exception as exc:  # noqa: BLE001 - retry transient storage failures
                    last_error = exc
                    errors.append(type(exc).__name__)
                    time.sleep(min(attempt, 2))
                    continue
            started = time.monotonic()
            self.telemetry.event("partition_started", run=run_id, partition=partition.id, attempt=attempt)
            try:
                with tempfile.TemporaryDirectory() as directory:
                    output = Path(directory) / "part.arrow"
                    rows = 0
                    options = pa.ipc.IpcWriteOptions(compression=None if self.compression == "none" else self.compression)
                    source_seconds = serialization_seconds = 0.0
                    query_latency = None
                    with pa.OSFile(str(output), "wb") as sink, pa.ipc.new_file(sink, self.schema, options=options) as writer:
                        reader = iter(self.source.read(
                            scope, partition,
                            max(1, min(50000, self.policy.batch_target_bytes // max(1, self._row_width())))))
                        try:
                            while True:
                                tick = time.monotonic()
                                try:
                                    batch = next(reader)
                                except StopIteration:
                                    source_seconds += time.monotonic() - tick
                                    break
                                elapsed = time.monotonic() - tick
                                source_seconds += elapsed
                                if query_latency is None:
                                    query_latency = elapsed
                                tick = time.monotonic()
                                table = pa.Table.from_batches([batch]).cast(self.schema, safe=True)
                                for canonical in table.to_batches():
                                    writer.write_batch(canonical)
                                    rows += canonical.num_rows
                                serialization_seconds += time.monotonic() - tick
                        finally:
                            close = getattr(reader, "close", None)
                            if close:
                                close()
                    with pa.ipc.open_file(str(output)) as reader:
                        if not reader.schema.equals(self.schema, check_metadata=False):
                            raise ValueError("Arrow IPC schema mismatch")
                        verified_rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                        if verified_rows != rows:
                            raise ValueError("Arrow IPC row count mismatch")
                    tick = time.monotonic()
                    byte_count = self.storage.publish_file(output, data_key)
                    storage_seconds = time.monotonic() - tick
                duration = max(0.000001, time.monotonic() - started)
                record = {"partition": partition.to_dict(), "file": data_key, "rows": rows,
                          "bytes": byte_count, "status": "SUCCESS", "attempts": attempt,
                          "durationSeconds": duration,
                          "sourceReadSeconds": source_seconds,
                          "queryLatencySeconds": query_latency,
                          "serializationSeconds": serialization_seconds,
                          "storageSeconds": storage_seconds,
                          "rowsPerSecond": rows / duration,
                          "bytesPerSecond": byte_count / duration,
                          "timeouts": timeouts, "sourceErrors": errors}
                self.storage.write(manifest_key, _json(record))
                self.telemetry.event("partition_completed", run=run_id, partition=partition.id,
                                     rows=rows, bytes=byte_count, durationSeconds=duration,
                                     sourceReadSeconds=source_seconds,
                                     serializationSeconds=serialization_seconds,
                                     storageSeconds=storage_seconds)
                return [record]
            except Exception as exc:  # noqa: BLE001 - source/storage failures are retryable here
                last_error = exc
                errors.append(type(exc).__name__)
                if isinstance(exc, TimeoutError) or "timeout" in str(exc).lower():
                    timeouts += 1
                self.telemetry.event("partition_retry", run=run_id, partition=partition.id,
                                     attempt=attempt, error=type(exc).__name__)
                time.sleep(min(attempt, 2))
        if self.storage.exists(data_key):
            raise RuntimeError(f"published partition {partition.id} could not be recovered") from last_error
        children = split_partition(partition, self.policy.max_split_depth)
        if children:
            self.telemetry.event("partition_split", run=run_id, parent=partition.id)
            return self._partition(run_id, scope, children[0]) + self._partition(run_id, scope, children[1])
        raise RuntimeError(f"partition {partition.id} failed after retries") from last_error

    def _recover_partition(self, data_key: str, manifest_key: str,
                           partition: Partition) -> list[dict[str, Any]]:
        with tempfile.TemporaryDirectory() as directory:
            local = Path(directory) / "orphan.arrow"
            byte_count = self.storage.download_to(data_key, local)
            with pa.ipc.open_file(str(local)) as reader:
                rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                if not reader.schema.equals(self.schema, check_metadata=False):
                    raise ValueError("published Arrow file has incompatible schema")
        cached = {"partition": partition.to_dict(), "file": data_key, "rows": rows,
                  "bytes": byte_count, "status": "SUCCESS", "recovered": True}
        self.storage.write(manifest_key, _json(cached))
        return [cached]

    def _row_width(self) -> int:
        fixed = {"boolean": 1, "int8": 1, "int16": 2, "int32": 4, "int64": 8,
                 "float32": 4, "float64": 8, "date": 4, "timestamp": 8}
        return sum(fixed.get(typ, 64) for _, typ in self.contract.fields)

    def run(self) -> dict[str, Any]:
        with DatasetLease(self.storage, self.meta + "/state/writer-lease.json") as lease:
            return self._run_locked(lease)

    def _run_locked(self, lease: DatasetLease) -> dict[str, Any]:
        state, expected_etag = self._state()
        previous = SourcePosition.from_dict(state.get("position"))
        self.source.validate(self.contract)
        pending = self._resume(previous, state.get("lastCommittedRun"))
        resumed = pending is not None
        if pending:
            run_id = pending["runId"]
            scope_dict = pending["scope"]
            scope = SemanticReadScope(scope_dict["identity"], scope_dict["mode"],
                                      tuple(tuple(item) for item in scope_dict["fields"]),
                                      scope_dict["filters"], tuple(scope_dict["positionFields"]),
                                      SourcePosition.from_dict(scope_dict["low"]),
                                      SourcePosition.from_dict(scope_dict["high"]), scope_dict["horizon"])
            physical = PhysicalReadPlan.from_dict(self._read_json(f"{self.meta}/plans/{pending['planId']}.json"))
            profile_id = pending["profileId"]
        else:
            scope, profile, physical = self.prepare()
            run_id = uuid.uuid4().hex
            profile_id = profile.profile_id
            dataset_key = self.meta + "/dataset.json"
            if not self.storage.exists(dataset_key):
                self.storage.write(dataset_key, _json({"identity": self.contract.identity,
                    "domain": self.contract.document["metadata"]["domain"],
                    "sourceSystem": self.contract.document["metadata"]["sourceSystem"],
                    "asset": self.contract.document["target"]["asset"],
                    "connector": self.contract.document["source"]["connector"],
                    "resource": self.contract.document["source"]["resource"], "createdAt": _now()}))
            self.storage.write(f"{self.meta}/profiles/{profile_id}.json",
                               _json({**asdict(profile), "reuse_signature": profile.reuse_signature}))
            if state.get("observedSchemaFingerprint") and state["observedSchemaFingerprint"] != profile.schema_fingerprint:
                self.telemetry.event("schema_drift_observed", identity=self.contract.identity,
                                     previous=state["observedSchemaFingerprint"], current=profile.schema_fingerprint)
            self.storage.write(f"{self.meta}/plans/{physical.plan_id}.json", _json(physical.to_dict()))
            pending = {"runId": run_id, "status": "RUNNING", "identity": self.contract.identity,
                       "contractVersion": self.contract.version, "contractFingerprint": self.contract.hash,
                       "targetWrite": self.contract.document["target"]["write"],
                       "previousPosition": previous.to_dict() if previous else None,
                       "scope": scope.to_dict(), "scopeFingerprint": scope.hash,
                       "profileId": profile_id, "planId": physical.plan_id, "startedAt": _now()}
            self.storage.write(f"{self.meta}/runs/{run_id}/run.json", _json(pending))
        self.telemetry.event("run_started", run=run_id, resumed=resumed)
        partition_records = []
        with ThreadPoolExecutor(max_workers=physical.max_in_flight) as executor:
            futures = [executor.submit(self._partition, run_id, scope, part) for part in physical.partitions]
            for future in as_completed(futures):
                partition_records.extend(future.result())
        partition_records.sort(key=lambda record: record["partition"]["id"])
        if not partition_records or any(r["status"] != "SUCCESS" or not self.storage.exists(r["file"]) for r in partition_records):
            raise RuntimeError("capture validation failed")
        lease.check()
        profile_record = self._read_json(f"{self.meta}/profiles/{profile_id}.json")
        prior_history = self._history(state, profile_record["schema_fingerprint"])
        manifest = {"runId": run_id, "identity": self.contract.identity,
                    "status": "COMMITTED", "contractVersion": self.contract.version,
                    "contractFingerprint": self.contract.hash, "targetWrite": self.contract.document["target"]["write"],
                    "profileId": profile_id, "planId": physical.plan_id, "scopeFingerprint": scope.hash,
                    "strategy": physical.strategy, "candidate": physical.candidate,
                    "historyRuns": prior_history.runs if prior_history else 0,
                    "profileReuseSignature": profile_record.get("reuse_signature"),
                    "policyFingerprint": fingerprint(asdict(self.policy)),
                    "capacityFingerprint": fingerprint(asdict(self.capacity)),
                    "schemaFingerprint": fingerprint(str(self.schema)), "schema": str(self.schema),
                    "observedSchemaFingerprint": profile_record["schema_fingerprint"],
                    "sourceHorizon": scope.high.to_dict() if scope.high else scope.horizon,
                    "previousPosition": previous.to_dict() if previous else None,
                    "committedPosition": scope.high.to_dict() if scope.high else None,
                    "startedAt": pending["startedAt"], "endedAt": _now(),
                    "files": [r["file"] for r in partition_records],
                    "rows": sum(r["rows"] for r in partition_records),
                    "bytes": sum(r["bytes"] for r in partition_records),
                    "compression": self.compression, "partitions": partition_records}
        manifest["consistencyLevel"] = physical.consistency_level
        manifest_key = f"{self.meta}/runs/{run_id}/manifest.json"
        if not self.storage.exists(manifest_key):
            self.storage.write(manifest_key, _json(manifest))
        else:
            manifest = self._read_json(manifest_key)
        pending["status"] = "COMMITTED"
        self.storage.write(f"{self.meta}/runs/{run_id}/run.json", _json(pending))
        lease.check()
        new_position = scope.high if scope.mode == "incremental" else previous
        self.storage.write(self.meta + "/state/current.json", _json({
            "lastCommittedRun": run_id, "position": new_position.to_dict() if new_position else None,
            "lastProfile": profile_id, "lastPlan": physical.plan_id,
            "schemaFingerprint": manifest["schemaFingerprint"],
            "observedSchemaFingerprint": manifest["observedSchemaFingerprint"],
            "contractVersion": self.contract.version, "contractFingerprint": self.contract.hash,
            "updatedAt": _now()}), expected_etag=expected_etag)
        self.telemetry.event("run_committed", run=run_id, rows=manifest["rows"],
                             bytes=manifest["bytes"], consistencyLevel=physical.consistency_level)
        self.telemetry.event("checkpoint_advanced", run=run_id,
                             position=new_position.to_dict() if new_position else None)
        return manifest
