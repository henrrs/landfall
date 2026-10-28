import json

import pyarrow as pa
import pytest

from landfall.adapters.storage import ConcurrentUpdate, LocalStorage
from landfall.application.capture import CaptureEngine
from landfall.core.contract import IngestionContract, fingerprint
from landfall.core.models import (
    ColumnProfile,
    PlannerPolicy,
    RuntimeCapacity,
    SourceCapabilities,
    SourcePosition,
    SourceProfile,
)


class FakeSource:
    capabilities = SourceCapabilities()

    def __init__(self):
        self.rows = [(1, "A", "2026-01-01T00:00:00", "ACTIVE"),
                     (2, "B", "2026-01-01T00:00:00", "ACTIVE"),
                     (3, "C", "2026-01-02T00:00:00", "INACTIVE")]

    def validate(self, contract):
        pass

    def high_position(self, contract):
        eligible = [r for r in self.rows if r[3] == "ACTIVE"]
        if not eligible:
            return None
        row = max(eligible, key=lambda r: (r[2], r[0]))
        return SourcePosition("ordered_fields", (row[2], row[0]))

    def profile(self, contract):
        return SourceProfile("p", len(self.rows), 100, fingerprint(contract.fields),
                             (ColumnProfile("id", "int64", False, 1, max(r[0] for r in self.rows),
                                            indexed=True, unique=True),))

    def read(self, scope, partition, batch_size):
        selected = []
        for row in self.rows:
            position = (row[2], row[0])
            if row[3] != "ACTIVE" or (scope.low and position <= scope.low.values) or (scope.high and position > scope.high.values):
                continue
            if partition.kind == "RANGE" and not ((partition.lower is None or row[0] >= partition.lower) and
                                                   (partition.upper is None or row[0] < partition.upper)):
                continue
            selected.append((row[0], row[1], row[2]))
        if selected:
            yield pa.record_batch([pa.array([r[0] for r in selected], type=pa.int64()),
                                   pa.array([r[1] for r in selected]),
                                   pa.array([r[2] for r in selected], type=pa.string())],
                                  names=["id", "name", "updated_at"])


def contract(initial="beginning"):
    return IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "data"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local", "resource": {}},
        "fields": [{"name": "id", "type": "int64"}, {"name": "name", "type": "string"},
                   {"name": "updated_at", "type": "timestamp"}],
        "read": {"mode": "incremental", "initialPosition": initial,
                 "position": {"strategy": "ordered_fields", "fields": ["updated_at", "id"]},
                 "filters": {"field": "status", "operator": "eq", "value": "ACTIVE"}},
        "target": {"asset": "customer", "write": {"mode": "append"}},
    })


def _engine(tmp_path, source, initial="beginning"):
    return CaptureEngine(contract(initial), source, LocalStorage(str(tmp_path), "account", "landing"),
                         RuntimeCapacity(2, 2, 1024**3), PlannerPolicy(minimum_partition_bytes=1,
                         target_partition_bytes=100, desired_waves=2), compression="none")


def test_capture_incremental_commit_and_allowlist(tmp_path):
    source = FakeSource()
    engine = _engine(tmp_path, source)
    first = engine.run()
    assert first["rows"] == 2
    assert first["committedPosition"]["values"] == ["2026-01-01T00:00:00", 2]
    assert "status" not in first["schema"]
    for key in first["files"]:
        with pa.ipc.open_file(pa.BufferReader(engine.storage.read(key))) as reader:
            assert reader.schema.names == ["id", "name", "updated_at"]
    source.rows.append((4, "D", "2026-01-03T00:00:00", "ACTIVE"))
    second = engine.run()
    assert second["rows"] == 1
    assert second["previousPosition"] == first["committedPosition"]
    assert all("run=" + first["runId"] not in key for key in second["files"])
    state = json.loads(engine.storage.read(engine.meta + "/state/current.json"))
    assert state["lastCommittedRun"] == second["runId"]


def test_initial_current_skips_existing_rows(tmp_path):
    source = FakeSource()
    manifest = _engine(tmp_path, source, "current").run()
    assert manifest["rows"] == 0
    assert manifest["committedPosition"]["values"] == ["2026-01-01T00:00:00", 2]


def test_state_compare_and_swap(tmp_path):
    storage = LocalStorage(str(tmp_path), "account", "landing")
    storage.write("state.json", b"one", expected_etag="")
    old = storage.etag("state.json")
    storage.write("state.json", b"two", expected_etag=old)
    with pytest.raises(ConcurrentUpdate):
        storage.write("state.json", b"three", expected_etag=old)


def test_restart_after_manifest_reuses_run(tmp_path):
    class FailOnceState(LocalStorage):
        fail = True

        def write(self, key, data, expected_etag=None):
            if key.endswith("/state/current.json") and self.fail:
                self.fail = False
                raise OSError("injected failure after manifest")
            return super().write(key, data, expected_etag)

    source = FakeSource()
    storage = FailOnceState(str(tmp_path), "account", "landing")
    engine = CaptureEngine(contract(), source, storage, RuntimeCapacity(2, 2, 1024**3),
                           PlannerPolicy(minimum_partition_bytes=1, target_partition_bytes=100,
                                         desired_waves=2), compression="none")
    with pytest.raises(OSError):
        engine.run()
    manifests = [key for key in storage.list(engine.meta + "/runs") if key.endswith("manifest.json")]
    assert len(manifests) == 1
    recovered = engine.run()
    assert recovered["rows"] == 2
    assert len([key for key in storage.list(engine.meta + "/runs") if key.endswith("manifest.json")]) == 1
    assert json.loads(storage.read(engine.meta + "/state/current.json"))["lastCommittedRun"] == recovered["runId"]
