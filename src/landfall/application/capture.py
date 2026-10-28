"""Contract → scope → profile → plan → Arrow IPC → manifest → durable state."""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from landfall.core.contract import ContractError, IngestionContract, fingerprint
from landfall.core.models import (
    Partition,
    PhysicalReadPlan,
    PlannerPolicy,
    SemanticReadScope,
    SourcePosition,
    semantic_scope,
)
from landfall.core.planner import plan, split_partition
from landfall.ports import SourcePort, StoragePort, TelemetryPort


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
        if low and (high is None or high.values < low.values):
            high = low
        if low is None and read["mode"] == "incremental" and read["initialPosition"] == "current":
            low = high
        scope = semantic_scope(self.contract, low, high, _now())
        self.telemetry.event("scope_resolved", identity=self.contract.identity, scope=scope.hash)
        profile = self.source.profile(self.contract)
        self.telemetry.event("source_profiled", profile=profile.profile_id, rows=profile.estimated_rows)
        physical = plan(self.contract, scope, profile, self.source.capabilities, self.capacity, self.policy)
        self.telemetry.event("plan_created", plan=physical.plan_id, strategy=physical.strategy,
                             partitions=physical.logical_partitions, maxInFlight=physical.max_in_flight)
        return scope, profile, physical

    def _partition(self, run_id: str, scope: SemanticReadScope, partition: Partition) -> list[dict[str, Any]]:
        manifest_key = f"{self.meta}/runs/{run_id}/partitions/{partition.id}.json"
        if self.storage.exists(manifest_key):
            cached = self._read_json(manifest_key)
            if cached.get("status") == "SUCCESS" and self.storage.exists(cached["file"]):
                return [cached]
        data_key = f"{self.base}/data/run={run_id}/part-{partition.id}.arrow"
        if self.storage.exists(data_key):
            with pa.ipc.open_file(pa.BufferReader(self.storage.read(data_key))) as reader:
                rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                if not reader.schema.equals(self.schema, check_metadata=False):
                    raise ValueError("published Arrow file has incompatible schema")
            cached = {"partition": partition.to_dict(), "file": data_key, "rows": rows,
                      "bytes": len(self.storage.read(data_key)), "status": "SUCCESS", "recovered": True}
            self.storage.write(manifest_key, _json(cached))
            return [cached]
        last_error = None
        for attempt in range(1, 3):
            self.telemetry.event("partition_started", run=run_id, partition=partition.id, attempt=attempt)
            try:
                with tempfile.TemporaryDirectory() as directory:
                    output = Path(directory) / "part.arrow"
                    rows = 0
                    options = pa.ipc.IpcWriteOptions(compression=None if self.compression == "none" else self.compression)
                    with pa.OSFile(str(output), "wb") as sink, pa.ipc.new_file(sink, self.schema, options=options) as writer:
                        for batch in self.source.read(scope, partition, max(1, min(50000, self.policy.batch_target_bytes // max(1, self._row_width())))):
                            table = pa.Table.from_batches([batch]).cast(self.schema, safe=True)
                            for canonical in table.to_batches():
                                writer.write_batch(canonical)
                                rows += canonical.num_rows
                    with pa.ipc.open_file(str(output)) as reader:
                        if not reader.schema.equals(self.schema, check_metadata=False):
                            raise ValueError("Arrow IPC schema mismatch")
                        verified_rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
                        if verified_rows != rows:
                            raise ValueError("Arrow IPC row count mismatch")
                    byte_count = self.storage.publish_file(output, data_key)
                record = {"partition": partition.to_dict(), "file": data_key, "rows": rows,
                          "bytes": byte_count, "status": "SUCCESS", "attempts": attempt}
                self.storage.write(manifest_key, _json(record))
                self.telemetry.event("partition_completed", run=run_id, partition=partition.id, rows=rows)
                return [record]
            except Exception as exc:  # noqa: BLE001 - source/storage failures are retryable here
                last_error = exc
                self.telemetry.event("partition_retry", run=run_id, partition=partition.id,
                                     attempt=attempt, error=type(exc).__name__)
                time.sleep(min(attempt, 2))
        children = split_partition(partition, self.policy.max_split_depth)
        if children:
            self.telemetry.event("partition_split", run=run_id, parent=partition.id)
            return self._partition(run_id, scope, children[0]) + self._partition(run_id, scope, children[1])
        raise RuntimeError(f"partition {partition.id} failed after retries") from last_error

    def _row_width(self) -> int:
        fixed = {"boolean": 1, "int8": 1, "int16": 2, "int32": 4, "int64": 8,
                 "float32": 4, "float64": 8, "date": 4, "timestamp": 8}
        return sum(fixed.get(typ, 64) for _, typ in self.contract.fields)

    def run(self) -> dict[str, Any]:
        state, expected_etag = self._state()
        previous = SourcePosition.from_dict(state.get("position"))
        self.source.validate(self.contract)
        pending = self._resume(previous, state.get("lastCommittedRun"))
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
            self.storage.write(f"{self.meta}/profiles/{profile_id}.json", _json(asdict(profile)))
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
        self.telemetry.event("run_started", run=run_id, resumed=bool(pending.get("resumed", False)))
        partition_records = []
        with ThreadPoolExecutor(max_workers=physical.max_in_flight) as executor:
            futures = [executor.submit(self._partition, run_id, scope, part) for part in physical.partitions]
            for future in as_completed(futures):
                partition_records.extend(future.result())
        partition_records.sort(key=lambda record: record["partition"]["id"])
        if not partition_records or any(r["status"] != "SUCCESS" or not self.storage.exists(r["file"]) for r in partition_records):
            raise RuntimeError("capture validation failed")
        manifest = {"runId": run_id, "identity": self.contract.identity,
                    "status": "COMMITTED", "contractVersion": self.contract.version,
                    "contractFingerprint": self.contract.hash, "targetWrite": self.contract.document["target"]["write"],
                    "profileId": profile_id, "planId": physical.plan_id, "scopeFingerprint": scope.hash,
                    "schemaFingerprint": fingerprint(str(self.schema)), "schema": str(self.schema),
                    "observedSchemaFingerprint": self._read_json(f"{self.meta}/profiles/{profile_id}.json")["schema_fingerprint"],
                    "sourceHorizon": scope.high.to_dict() if scope.high else scope.horizon,
                    "previousPosition": previous.to_dict() if previous else None,
                    "committedPosition": scope.high.to_dict() if scope.high else None,
                    "startedAt": pending["startedAt"], "endedAt": _now(),
                    "files": [r["file"] for r in partition_records],
                    "rows": sum(r["rows"] for r in partition_records),
                    "bytes": sum(r["bytes"] for r in partition_records),
                    "compression": self.compression, "partitions": partition_records}
        manifest_key = f"{self.meta}/runs/{run_id}/manifest.json"
        if not self.storage.exists(manifest_key):
            self.storage.write(manifest_key, _json(manifest))
        else:
            manifest = self._read_json(manifest_key)
        pending["status"] = "COMMITTED"
        self.storage.write(f"{self.meta}/runs/{run_id}/run.json", _json(pending))
        new_position = scope.high if scope.mode == "incremental" else previous
        self.storage.write(self.meta + "/state/current.json", _json({
            "lastCommittedRun": run_id, "position": new_position.to_dict() if new_position else None,
            "lastProfile": profile_id, "lastPlan": physical.plan_id,
            "schemaFingerprint": manifest["schemaFingerprint"],
            "observedSchemaFingerprint": manifest["observedSchemaFingerprint"],
            "contractVersion": self.contract.version, "contractFingerprint": self.contract.hash,
            "updatedAt": _now()}), expected_etag=expected_etag)
        self.telemetry.event("run_committed", run=run_id, rows=manifest["rows"])
        self.telemetry.event("checkpoint_advanced", run=run_id,
                             position=new_position.to_dict() if new_position else None)
        return manifest
