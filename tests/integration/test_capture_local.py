import json

import pyarrow as pa
import pytest

from landfall.adapters.storage import ConcurrentUpdate, LocalStorage
from landfall.application.capture import CaptureEngine, DatasetBusy, DatasetLease
from landfall.core.contract import IngestionContract, fingerprint
from landfall.core.models import (
    ColumnProfile,
    Partition,
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


def test_dataset_lease_rejects_concurrent_writer_and_releases(tmp_path):
    storage = LocalStorage(str(tmp_path), "account", "landing")
    key = "crm/customer/_ingestion/state/writer-lease.json"
    with DatasetLease(storage, key), pytest.raises(DatasetBusy), DatasetLease(storage, key):
        pass
    with DatasetLease(storage, key):
        pass


def test_history_is_used_on_next_run(tmp_path):
    source = FakeSource()
    engine = _engine(tmp_path, source)
    first = engine.run()
    scope, _, physical = engine.prepare()
    assert scope.low.values == tuple(first["committedPosition"]["values"])
    assert any("history: 1 runs" in reason for reason in physical.reasons)
    assert physical.consistency_level == "BOUNDED_POSITION"


def test_orphan_recovery_streams_from_storage(tmp_path):
    class OrphanOnce(LocalStorage):
        failed = False

        def write(self, key, data, expected_etag=None):
            if "/partitions/" in key and not self.failed:
                self.failed = True
                raise OSError("injected partition record failure")
            return super().write(key, data, expected_etag)

        def read(self, key):
            if key.endswith(".arrow"):
                raise AssertionError("Arrow data must be downloaded to a file")
            return super().read(key)

    source = FakeSource()
    storage = OrphanOnce(str(tmp_path), "account", "landing")
    engine = CaptureEngine(contract(), source, storage, RuntimeCapacity(2, 2, 1024**3),
                           PlannerPolicy(minimum_partition_bytes=1, target_partition_bytes=100,
                                         desired_waves=2), compression="none")
    manifest = engine.run()
    assert manifest["rows"] == 2
    assert any(p.get("recovered") for p in manifest["partitions"])


def test_manifest_failure_resumes_with_same_high(tmp_path):
    class FailManifestOnce(LocalStorage):
        failed = False

        def write(self, key, data, expected_etag=None):
            if key.endswith("/manifest.json") and not self.failed:
                self.failed = True
                raise OSError("injected manifest failure")
            return super().write(key, data, expected_etag)

    source = FakeSource()
    storage = FailManifestOnce(str(tmp_path), "account", "landing")
    engine = CaptureEngine(contract(), source, storage, RuntimeCapacity(2, 2, 1024**3),
                           PlannerPolicy(minimum_partition_bytes=1, target_partition_bytes=100),
                           compression="none")
    with pytest.raises(OSError):
        engine.run()
    assert not storage.exists(engine.meta + "/state/current.json")
    source.rows.append((4, "D", "2026-01-03T00:00:00", "ACTIVE"))
    recovered = engine.run()
    assert recovered["rows"] == 2
    assert recovered["committedPosition"]["values"] == ["2026-01-01T00:00:00", 2]
    assert recovered["consistencyLevel"] == "BOUNDED_POSITION"


def test_failed_bounded_partition_splits_without_changing_scope(tmp_path):
    class ParentFails(FakeSource):
        def read(self, scope, partition, batch_size):
            if partition.depth == 0:
                raise TimeoutError("injected query timeout")
            yield from super().read(scope, partition, batch_size)

    engine = _engine(tmp_path, ParentFails())
    scope, _, _ = engine.prepare()
    records = engine._partition("split-test", scope, Partition("00000", "RANGE", "id", 0, 10))
    assert len(records) == 2
    assert all(r["partition"]["parent"] == "00000" for r in records)
    assert sum(r["rows"] for r in records) == 2


def test_expired_lease_can_be_reclaimed(tmp_path):
    storage = LocalStorage(str(tmp_path), "account", "landing")
    key = "dataset/_ingestion/state/writer-lease.json"
    storage.write(key, b'{"owner":"dead","expiresAt":0}', expected_etag="")
    with DatasetLease(storage, key) as lease:
        lease.check()


def test_compatible_physical_plan_boundaries_are_reused(tmp_path):
    engine = _engine(tmp_path, FakeSource())
    first = engine.run()
    key = f"{engine.meta}/runs/{first['runId']}/manifest.json"
    manifest = json.loads(engine.storage.read(key))
    for record in manifest["partitions"]:
        record["durationSeconds"] = 40.0
    engine.storage.write(key, json.dumps(manifest).encode())
    _, _, reused = engine.prepare()
    assert reused.partitions == tuple(Partition(**r) for r in
                                      json.loads(engine.storage.read(
                                          f"{engine.meta}/plans/{first['planId']}.json"))["partitions"])
    assert "reused compatible physical boundaries" in reused.reasons
    assert reused.scope_fingerprint != first["scopeFingerprint"]
    different_capacity = CaptureEngine(engine.contract, engine.source, engine.storage,
                                       RuntimeCapacity(1, 1, 1024**3), engine.policy,
                                       compression="none")
    assert "reused compatible physical boundaries" not in different_capacity.prepare()[2].reasons


def test_external_state_change_blocks_checkpoint_commit(tmp_path):
    storage = LocalStorage(str(tmp_path), "account", "landing")

    class ConflictingSource(FakeSource):
        injected = False

        def read(self, scope, partition, batch_size):
            if not self.injected:
                self.injected = True
                storage.write("crm/customer/_ingestion/state/current.json",
                              b'{"position":null,"lastCommittedRun":"external"}',
                              expected_etag="")
            yield from super().read(scope, partition, batch_size)

    engine = CaptureEngine(contract(), ConflictingSource(), storage,
                           RuntimeCapacity(1, 1, 1024**3),
                           PlannerPolicy(minimum_partition_bytes=1, target_partition_bytes=100),
                           compression="none")
    with pytest.raises(ConcurrentUpdate):
        engine.run()
    state = json.loads(storage.read(engine.meta + "/state/current.json"))
    assert state["lastCommittedRun"] == "external"


def test_corrupt_published_orphan_is_rejected(tmp_path):
    engine = _engine(tmp_path, FakeSource())
    key = "crm/customer/data/run=failed/part-00000.arrow"
    engine.storage.write(key, b"partial Arrow file")
    with pytest.raises(pa.ArrowInvalid):
        engine._recover_partition(key, engine.meta + "/runs/failed/partitions/00000.json",
                                  Partition("00000"))
    assert not engine.storage.exists(engine.meta + "/runs/failed/partitions/00000.json")
