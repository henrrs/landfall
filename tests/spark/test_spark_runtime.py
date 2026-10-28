import base64
import json
import re
from datetime import UTC, datetime, timedelta
from functools import partial

import pyarrow as pa
import pytest

pytest.importorskip("pyspark")

from landfall.adapters.spark_runtime import _RESULT_SCHEMA, SparkRuntimeAdapter
from landfall.core.contract import IngestionContract
from landfall.core.models import (
    ColumnProfile,
    Partition,
    PhysicalReadPlan,
    PlannerPolicy,
    RuntimeCapacity,
    SourceCapabilities,
    SourceProfile,
    semantic_scope,
)
from landfall.core.planner import plan


class FakeRow:
    def __init__(self, value):
        self.value = value

    def asDict(self, recursive=False):
        return self.value


class FakeDataFrame:
    def __init__(self, session, rows):
        self.session = session
        self.rows = rows

    def repartition(self, count):
        self.session.repartition_counts.append(count)
        return self

    def mapInArrow(self, function, schema):
        self.session.map_in_arrow += 1
        self.session.functions.append(function)
        self.session.output_schema = schema
        return FakeResult(self.rows)


class FakeResult:
    def __init__(self, descriptors):
        self.descriptors = descriptors

    def collect(self):
        started = datetime.now(UTC)
        finished = started + timedelta(milliseconds=50)
        results = []
        for descriptor in self.descriptors:
            partition_id = descriptor[0]
            results.append(FakeRow({
                "partition_id": partition_id,
                "status": "SUCCESS",
                "rows": 10,
                "arrow_bytes": 100,
                "ipc_bytes": 120,
                "file": f"crm/customer/data/run=run/part-{partition_id}.arrow",
                "checksum": "abc",
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "duration_seconds": 0.05,
                "source_read_seconds": 0.02,
                "serialization_seconds": 0.01,
                "storage_seconds": 0.01,
                "retry_count": 0,
                "executor_id": str(len(results) + 1),
                "executor_id_source": "spark-environment",
                "executor_host": f"worker-{len(results) + 1}",
                "spark_partition_id": len(results),
                "task_attempt_id": 100 + len(results),
                "task_attempt_number": 0,
                "spark_stage_id": 3,
                "recovered": False,
            }))
        return results


class FakeSpark:
    version = "4.0.1"

    def __init__(self):
        self.map_in_arrow = 0
        self.functions = []
        self.repartition_counts = []
        self.sparkContext = type("Context", (), {"master": "local[4]"})()

    def sql(self, query):
        self.control_sql = query
        ids = re.findall(r"SELECT CAST\('((?:''|[^'])*)' AS STRING\) AS `partition_id`", query)
        return FakeDataFrame(self, [(value.replace("''", "'"),) for value in ids])


def _fixture():
    adapter = SparkRuntimeAdapter.__new__(SparkRuntimeAdapter)
    adapter.spark = FakeSpark()
    adapter.mode = "single-node"
    adapter.capacity = RuntimeCapacity(4, 4, 1024**3)
    adapter.worker_config = {
        "secrets": {"provider": "keyvault", "references": {"crm-ref": "vault-name"}},
        "storage": {"type": "local", "root": ".landfall"},
        "sqlserver": {"queryTimeoutSeconds": 30},
        "account": "account",
        "container": "landing",
    }
    schema = pa.schema([("id", pa.int64())])
    context = {
        "contract": {
            "apiVersion": "ingestion.contract/v1alpha1",
            "kind": "IngestionContract",
            "metadata": {"version": "1", "domain": "customer", "sourceSystem": "crm",
                         "owner": "tests"},
            "source": {"connector": "sqlserver", "connectionRef": "crm-ref",
                       "resource": {"database": "CRM", "schema": "dbo", "table": "Rows"}},
            "fields": [{"name": "id", "type": "int64"}],
            "read": {"mode": "full"},
            "target": {"asset": "rows", "write": {"mode": "replace"}},
        },
        "scope": {"identity": "customer.crm.rows", "mode": "full", "fields": [["id", "int64"]],
                  "filters": None, "positionFields": [], "low": None, "high": None, "horizon": "x"},
        "runId": "run",
        "sourceConnectionRef": "crm-ref",
        "base": "crm/customer",
        "meta": "crm/customer/_ingestion",
        "schemaIpcBase64": base64.b64encode(schema.serialize().to_pybytes()).decode(),
        "compression": "lz4",
        "batchTargetBytes": 1024,
    }
    plan = PhysicalReadPlan(
        "plan", "contract", "scope", "profile", "SINGLE", None,
        (Partition("00000"), Partition("00001")), 2, 1024, 512,
    )
    return adapter, plan, context


def test_spark_runtime_uses_map_in_arrow_and_collects_only_small_results():
    adapter, plan, context = _fixture()

    result = adapter.execute(plan, lambda _: pytest.fail("driver callback must not be serialized"), context)

    assert adapter.spark.map_in_arrow == 1
    assert adapter.spark.repartition_counts == [2]
    assert adapter.spark.output_schema.fieldNames() == list(_RESULT_SCHEMA.names)
    assert len(result["partitions"]) == 2
    assert result["evidence"]["peakConcurrentTasks"] == 2
    assert result["evidence"]["distinctExecutors"] == 2
    assert result["evidence"]["waves"] == 1
    callback = adapter.spark.functions[0]
    assert isinstance(callback, partial)
    assert callback.func.__name__ == "_execute_control_batches"


def test_plan_descriptors_worker_closure_and_result_never_contain_credentials():
    adapter, plan, context = _fixture()
    secret = "TOP_SECRET_PASSWORD_VALUE"

    def driver_handler(_):
        return [{"password": secret}]

    result = adapter.execute(plan, driver_handler, context)

    plan_json = json.dumps(plan.to_dict(), default=str)
    descriptors_json = adapter.spark.control_sql
    callback = adapter.spark.functions[0]
    work_spec = callback.keywords["spec_json"]
    results_json = json.dumps(result, default=str)
    from pyspark import cloudpickle

    callback_serialized = cloudpickle.dumps(callback).decode("latin1")
    for serialized in (plan_json, descriptors_json, work_spec, results_json, callback_serialized):
        assert secret not in serialized
        assert "password" not in serialized.lower()
        assert "connectionstring" not in serialized.lower()
        assert "token" not in serialized.lower()
    assert "crm-ref" in descriptors_json
    assert "vault-name" in work_spec


def test_map_in_arrow_control_batch_contains_descriptors_only():
    adapter, plan, context = _fixture()
    adapter.execute(plan, lambda _: pytest.fail("driver callback must not be serialized"), context)

    control_sql = adapter.spark.control_sql
    names = [
        "partition_id", "partition_kind", "field", "lower_json", "upper_json",
        "include_null", "parent", "depth", "value_json", "secondary_field",
        "secondary_lower_json", "secondary_upper_json", "run_id", "connection_ref",
    ]
    assert all(f"AS `{name}`" in control_sql for name in names)
    descriptor_ids = re.findall(
        r"SELECT CAST\('((?:''|[^'])*)' AS STRING\) AS `partition_id`", control_sql)
    assert {value.replace("''", "'") for value in descriptor_ids} == {
        part.id for part in plan.partitions
    }
    assert "CAST('run' AS STRING) AS `run_id`" in control_sql
    assert "CAST('crm-ref' AS STRING) AS `connection_ref`" in control_sql


def test_worker_executor_identity_uses_host_only_when_spark_id_is_missing(monkeypatch):
    import socket

    from landfall.adapters.spark_runtime import _executor_identity

    monkeypatch.setattr(socket, "gethostname", lambda: "spark-worker-2")
    monkeypatch.setenv("SPARK_EXECUTOR_ID", "driver")

    assert _executor_identity("spark-driver") == (
        "host:spark-worker-2", "spark-worker-2", "host-fallback")

    monkeypatch.setenv("SPARK_EXECUTOR_ID", "7")
    assert _executor_identity("spark-driver") == (
        "7", "spark-worker-2", "spark-environment")


def test_spark_capacity_changes_plan_parallelism_without_changing_contract():
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1", "domain": "customer", "sourceSystem": "crm",
                     "owner": "tests"},
        "source": {"connector": "sqlserver", "connectionRef": "crm",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "Rows"}},
        "fields": [{"name": "id", "type": "int64"}],
        "read": {"mode": "full"},
        "target": {"asset": "rows", "write": {"mode": "replace"}},
    })
    profile = SourceProfile("profile", 1000, 8, "schema",
                            (ColumnProfile("id", "int64", False, 1, 1000, indexed=True),))
    scope = semantic_scope(contract, None, None, "capacity-test")
    policy = PlannerPolicy(target_partition_bytes=1_000_000, minimum_partition_bytes=1,
                           desired_waves=2, safety_max_connections=8)
    single = plan(contract, scope, profile, SourceCapabilities(), RuntimeCapacity(1, 8, 1024), policy)
    cluster = plan(contract, scope, profile, SourceCapabilities(), RuntimeCapacity(4, 8, 1024), policy)

    assert single.max_in_flight == 1 and cluster.max_in_flight == 4
    assert single.logical_partitions == 2 and cluster.logical_partitions == 8
    assert contract.document["read"] == {"mode": "full"}
