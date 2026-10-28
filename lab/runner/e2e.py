"""Wheel-only local lab seed, capture, and assertions."""

import copy
import json
import hashlib
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import mssql_python
import pyarrow as pa
import yaml

from landfall.adapters.secrets import KeyVaultSecrets
from landfall.adapters.sqlserver import SqlServerConnector
from landfall.adapters.storage import make_storage
from landfall.core.contract import ContractError, IngestionContract
from landfall.core.models import Partition, PlannerPolicy, RuntimeCapacity, semantic_scope
from landfall.core.planner import plan


PASSWORD = os.environ["LANDFALL_LAB_SQL_PASSWORD"]
CONTRACT = "/app/contracts/examples/customer.incremental.yaml"
PLATFORM = "/app/lab/platform-config/e2e.yaml"
BASE = "crm/customer"
META = BASE + "/_ingestion"


def connection(database="master"):
    return mssql_python.connect(
        f"Server=sqlserver,1433;Database={database};UID=sa;PWD={PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=yes", autocommit=True)


def wait_sql():
    last = None
    for _ in range(90):
        try:
            con = connection()
            con.close()
            return
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError("SQL Server did not become ready") from last


def vault():
    last = None
    for _ in range(60):
        try:
            client = KeyVaultSecrets("https://keyvault:4997", {"crm-local": "crm-local"}, True).client
            list(client.list_properties_of_secrets())
            return client
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError("Key Vault emulator did not become ready") from last


def seed():
    wait_sql()
    sql = Path("/app/lab/sqlserver/init/seed.sql").read_text()
    con = connection()
    try:
        cur = con.cursor()
        for batch in sql.split("\nGO\n"):
            if batch.strip():
                cur.execute(batch)
                con.commit()
    finally:
        con.close()
    secret = {"server": "sqlserver", "port": 1433, "database": "CRM", "user": "sa",
              "password": PASSWORD, "encrypt": "yes", "trustServerCertificate": "yes"}
    vault().set_secret("crm-local", json.dumps(secret))
    print("lab seeded")


def run():
    subprocess.run(["landfall", "run", "--contract", CONTRACT, "--platform", PLATFORM], check=True)


def storage():
    return make_storage({"storage": {"type": "adls", "connectionStringEnv": "LANDFALL_ADLS_CONNECTION_STRING",
                                      "dfsHostHeader": "devstoreaccount1.dfs.core.windows.net"}},
                        "devstoreaccount1", "landing-zone")


def manifest():
    store = storage()
    state = json.loads(store.read(META + "/state/current.json"))
    run_id = state["lastCommittedRun"]
    return store, state, json.loads(store.read(f"{META}/runs/{run_id}/manifest.json"))


def verify(expected_ids):
    store, state, run_manifest = manifest()
    assert run_manifest["status"] == "COMMITTED"
    assert run_manifest["consistencyLevel"] == "BOUNDED_POSITION"
    assert run_manifest["contractVersion"] == "1.0.0"
    assert state["position"] == run_manifest["committedPosition"]
    ids = []
    for key in run_manifest["files"]:
        with pa.ipc.open_file(pa.BufferReader(store.read(key))) as reader:
            assert reader.schema.names == ["customer_id", "name", "status", "updated_at"]
            for i in range(reader.num_record_batches):
                batch = reader.get_batch(i)
                ids.extend(batch.column(0).to_pylist())
                assert all(v == "ACTIVE" for v in batch.column(2).to_pylist())
    assert sorted(ids) == expected_ids, (ids, expected_ids)
    assert run_manifest["rows"] == len(ids)
    print(f"verified run {run_manifest['runId']}: {len(ids)} rows, checkpoint {state['position']}")
    return run_manifest


def append_new_row():
    con = connection("CRM")
    try:
        cur = con.cursor()
        cur.execute("IF COL_LENGTH('dbo.Customer','new_source_field') IS NULL "
                    "ALTER TABLE dbo.Customer ADD new_source_field NVARCHAR(20) NULL")
        cur.execute("IF NOT EXISTS (SELECT 1 FROM dbo.Customer WHERE customer_id=4) "
                    "INSERT INTO dbo.Customer(customer_id,name,status,updated_at,internal_note) "
                    "VALUES(4,N'Dora','ACTIVE','2026-01-03T10:00:00',N'new source field stays excluded')")
        con.commit()
    finally:
        con.close()


def reference_reader_conformance(secret):
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "Shape"}},
        "fields": [{"name": "id", "type": "int64"}, {"name": "region", "type": "string"},
                   {"name": "amount", "type": "decimal(18,2)"},
                   {"name": "event_time", "type": "timestamp"},
                   {"name": "payload", "type": "string"}],
        "read": {"mode": "full"}, "target": {"asset": "shape", "write": {"mode": "replace"}},
    })
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    scope = semantic_scope(contract, None, None, "lab")

    def hashes(partitions):
        result = []
        for partition in partitions:
            for batch in source.read(scope, partition, 32):
                for row in batch.to_pylist():
                    result.append(hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest())
        return sorted(result)

    expected = hashes((Partition("single"),))
    assert len(expected) == 8
    profile = source.profile(contract)
    assert ("amount",) in profile.index_included_columns
    assert next(c for c in profile.columns if c.name == "event_time").histogram
    physical = plan(contract, scope, profile, source.capabilities, RuntimeCapacity(2, 2, 1024**3),
                    PlannerPolicy(target_partition_bytes=100, minimum_partition_bytes=1))
    assert physical.strategy in {"NUMERIC_RANGE", "NUMERIC_QUANTILE"}
    assert hashes(physical.partitions) == expected
    cases = (
        (Partition("a", "RANGE", "amount", upper="0"),
         Partition("b", "RANGE", "amount", lower="0"), Partition("c", "NULL", "amount")),
        (Partition("a", "RANGE", "event_time", upper="2026-01-03"),
         Partition("b", "RANGE", "event_time", lower="2026-01-03"),
         Partition("c", "NULL", "event_time")),
        (Partition("a", "VALUE_RANGE", "region", value="A", secondary_field="id", secondary_upper=2),
         Partition("b", "VALUE_RANGE", "region", value="A", secondary_field="id", secondary_lower=2),
         Partition("c", "VALUE", "region", value="B"),
         Partition("d", "OTHER", "region", value=("A", "B"))),
    )
    for partitions in cases:
        assert hashes(partitions) == expected
    print("SQL reference SINGLE equals adaptive numeric, decimal, temporal and hot-value readers")


def additional_sql_shapes(secret):
    cases = (
        ("CompositeShape", "composite_shape", (("group_id", "int32"), ("item_id", "int32"),
                                                ("payload", "string"))),
        ("GuidShape", "guid_shape", (("id", "string"), ("payload", "string"))),
        ("EmptyShape", "empty_shape", (("id", "int64"),)),
        ("NoKeyShape", "no_key_shape", (("n", "int32"), ("payload", "string"))),
    )
    for table, asset, fields in cases:
        contract = IngestionContract.parse({
            "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
            "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
            "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                       "resource": {"database": "CRM", "schema": "dbo", "table": table}},
            "fields": [{"name": name, "type": typ} for name, typ in fields],
            "read": {"mode": "full"}, "target": {"asset": asset, "write": {"mode": "replace"}},
        })
        source = SqlServerConnector(secret).bind(contract)
        source.validate(contract)
        profile = source.profile(contract)
        scope = semantic_scope(contract, None, None, "lab")
        physical = plan(contract, scope, profile, source.capabilities,
                        RuntimeCapacity(2, 2, 1024**3),
                        PlannerPolicy(target_partition_bytes=1, minimum_partition_bytes=1))
        if table == "CompositeShape":
            assert ("group_id", "item_id") in profile.keys
            assert physical.strategy == "NUMERIC_RANGE"
            reference = sorted(row["payload"] for batch in source.read(scope, Partition("all"), 32)
                               for row in batch.to_pylist())
            adaptive = sorted(row["payload"] for part in physical.partitions
                              for batch in source.read(scope, part, 32) for row in batch.to_pylist())
            assert adaptive == reference == ["a", "b", "c"]
        else:
            assert physical.strategy == "SINGLE"
            if table == "EmptyShape":
                assert profile.estimated_rows == 0
            if table == "GuidShape":
                assert ("id",) in profile.keys
            if table == "NoKeyShape":
                assert profile.keys == ()
    print("SQL composite, GUID, empty and unindexed fallback shapes verified")


def quantile_conformance(secret):
    for table, asset, fields, expected_strategy in (
        ("SkewNumeric", "skew_numeric", (("id", "int64"), ("payload", "string")),
         "NUMERIC_QUANTILE"),
        ("SkewTemporal", "skew_temporal", (("id", "int32"), ("event_time", "timestamp")),
         "TEMPORAL_QUANTILE"),
    ):
        contract = IngestionContract.parse({
            "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
            "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
            "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                       "resource": {"database": "CRM", "schema": "dbo", "table": table}},
            "fields": [{"name": name, "type": typ} for name, typ in fields],
            "read": {"mode": "full"}, "target": {"asset": asset, "write": {"mode": "replace"}},
        })
        source = SqlServerConnector(secret).bind(contract)
        source.validate(contract)
        profile = source.profile(contract)
        scope = semantic_scope(contract, None, None, "lab")
        policy = PlannerPolicy(target_partition_bytes=1000, minimum_partition_bytes=1)
        capacity = RuntimeCapacity(2, 2, 1024**3)
        quantile = plan(contract, scope, profile, source.capabilities, capacity, policy)
        assert quantile.strategy == expected_strategy
        uniform_profile = replace(profile, columns=tuple(replace(c, skew=1.0, histogram=(), quantiles=())
                                                         for c in profile.columns))
        uniform = plan(contract, scope, uniform_profile, source.capabilities, capacity, policy)
        assert uniform.strategy.endswith("_RANGE")

        def read_ids(partitions):
            result = []
            counts = []
            for part in partitions:
                rows = [row["id"] for batch in source.read(scope, part, 64)
                        for row in batch.to_pylist()]
                result.extend(rows)
                counts.append(len(rows))
            return sorted(result), counts

        reference, _ = read_ids((Partition("single"),))
        quantile_rows, balanced = read_ids(quantile.partitions)
        range_rows, uneven = read_ids(uniform.partitions)
        assert reference == quantile_rows == range_rows
        assert len(reference) == 200
        assert max(balanced) < max(uneven), (balanced, uneven)
        print(f"{table}: quantile max partition {max(balanced)} vs equal-width {max(uneven)}")


def hot_value_plan_conformance(secret):
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "HotShape"}},
        "fields": [{"name": "region", "type": "string"}, {"name": "id", "type": "int64"},
                   {"name": "payload", "type": "string"}],
        "read": {"mode": "full"}, "target": {"asset": "hot_shape", "write": {"mode": "replace"}},
    })
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    profile = source.profile(contract)
    scope = semantic_scope(contract, None, None, "lab")
    physical = plan(contract, scope, profile, source.capabilities,
                    RuntimeCapacity(2, 2, 1024**3),
                    PlannerPolicy(target_partition_bytes=1000, minimum_partition_bytes=1))
    assert physical.strategy == "HOT_VALUE_HIERARCHICAL"
    assert any(p.kind == "VALUE_RANGE" and p.value == "A" for p in physical.partitions)
    reference = sorted(row["id"] for batch in source.read(scope, Partition("single"), 64)
                       for row in batch.to_pylist())
    counts = []
    adaptive = []
    for part in physical.partitions:
        rows = [row["id"] for batch in source.read(scope, part, 64)
                for row in batch.to_pylist()]
        adaptive.extend(rows)
        counts.append(len(rows))
    assert sorted(adaptive) == reference and len(reference) == 200
    assert max(counts) < 180
    print(f"HotShape: hierarchical max partition {max(counts)} vs hot bucket 180")


def wide_type_conformance(secret):
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "WideShape"}},
        "fields": [{"name": "id", "type": "int64"}, {"name": "narrow", "type": "string"},
                   {"name": "wide", "type": "string"}, {"name": "blob", "type": "binary"},
                   {"name": "document", "type": "string"}, {"name": "rv", "type": "binary"}],
        "read": {"mode": "full"}, "target": {"asset": "wide_shape", "write": {"mode": "replace"}},
    })
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    profile = source.profile(contract)
    scope = semantic_scope(contract, None, None, "lab")
    rows = [row for batch in source.read(scope, Partition("single"), 32)
            for row in batch.to_pylist()]
    assert len(rows) == 1
    row = rows[0]
    assert len(row["narrow"]) == 4000 and len(row["wide"]) == 4000
    assert row["blob"] == b"\x01\x02\x03\x04"
    assert b"<root>" in row["document"].encode()
    assert len(row["rv"]) == 8
    assert profile.observed_arrow_row_bytes and profile.observed_arrow_row_bytes > 8000
    print("SQL wide varchar/nvarchar/varbinary/XML/rowversion and Arrow width verified")


def unique_position_conformance(secret):
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "UniqueNoPk"}},
        "fields": [{"name": "id", "type": "int64"},
                   {"name": "updated_at", "type": "timestamp"},
                   {"name": "payload", "type": "string"}],
        "read": {"mode": "incremental", "initialPosition": "beginning",
                 "position": {"strategy": "ordered_fields", "fields": ["updated_at", "id"]}},
        "target": {"asset": "unique_position", "write": {"mode": "append"}},
    })
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    high = source.high_position(contract)
    assert high.values[1] == 2
    scope = semantic_scope(contract, None, high, "lab")
    ids = sorted(row["id"] for batch in source.read(scope, Partition("single"), 32)
                 for row in batch.to_pylist())
    assert ids == [1, 2]
    print("SQL unique-index ordered position without primary key verified")


def composite_hot_conformance(secret):
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "CompositeHot"}},
        "fields": [{"name": "group_id", "type": "int32"}, {"name": "item_id", "type": "int64"}],
        "read": {"mode": "full"},
        "target": {"asset": "composite_hot", "write": {"mode": "replace"}},
    })
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    profile = source.profile(contract)
    scope = semantic_scope(contract, None, None, "lab")
    physical = plan(contract, scope, profile, source.capabilities,
                    RuntimeCapacity(2, 2, 1024**3),
                    PlannerPolicy(target_partition_bytes=1000, minimum_partition_bytes=1))
    assert physical.strategy == "COMPOSITE", (physical.strategy, profile.keys,
                                               [(c.name, c.skew, c.distinct_estimate,
                                                 c.minimum, c.maximum, c.index_quality,
                                                 c.statistics_confidence) for c in profile.columns])
    assert any(p.kind == "VALUE_RANGE" and p.value == 1 for p in physical.partitions)
    reference = sorted(row["item_id"] for batch in source.read(scope, Partition("single"), 64)
                       for row in batch.to_pylist())
    counts = []
    adaptive = []
    for part in physical.partitions:
        rows = [row["item_id"] for batch in source.read(scope, part, 64)
                for row in batch.to_pylist()]
        adaptive.extend(rows)
        counts.append(len(rows))
    assert sorted(adaptive) == reference and len(reference) == 200
    assert max(counts) < 180
    print(f"CompositeHot: max partition {max(counts)} vs hot leading key 180")


def sql_contract_conformance(secret):
    document = {
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm", "owner": "lab"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "Shape"}},
        "fields": [{"name": "id", "type": "int64"}, {"name": "region", "type": "string"}],
        "read": {"mode": "full", "filters": {"field": "region", "operator": "eq", "value": "A"}},
        "target": {"asset": "shape_filter", "write": {"mode": "replace"}},
    }
    contract = IngestionContract.parse(document)
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    scope = semantic_scope(contract, None, None, "lab")
    ids = sorted(row["id"] for batch in source.read(scope, Partition("single"), 32)
                 for row in batch.to_pylist())
    assert ids == [-100, -1, 1, 2, 3, 1_000_000]
    for field in ({"name": "missing", "type": "string"},
                  {"name": "id", "type": "string"}):
        invalid = copy.deepcopy(document)
        invalid["fields"][0] = field
        try:
            bad_contract = IngestionContract.parse(invalid)
            SqlServerConnector(secret).bind(bad_contract).validate(bad_contract)
        except ContractError:
            pass
        else:
            raise AssertionError("invalid contracted SQL field was accepted")
    print("SQL full+filter, missing-field and type-mismatch contract cases verified")


def e2e():
    seed()
    contract = IngestionContract.parse(yaml.safe_load(Path(CONTRACT).read_text()))
    secret = json.loads(vault().get_secret("crm-local").value)
    profile = SqlServerConnector(secret).bind(contract).profile(contract)
    assert profile.estimated_rows == 3
    assert profile.observed_arrow_row_bytes is not None and profile.probe_rows > 0
    assert ("customer_id",) in profile.indexes
    assert next(c for c in profile.columns if c.name == "customer_id").histogram
    print("SQL profiler metadata, histogram and Arrow calibration verified")
    reference_reader_conformance(secret)
    additional_sql_shapes(secret)
    quantile_conformance(secret)
    hot_value_plan_conformance(secret)
    wide_type_conformance(secret)
    unique_position_conformance(secret)
    composite_hot_conformance(secret)
    sql_contract_conformance(secret)
    run()
    first = verify([1, 2])
    append_new_row()
    run()
    second = verify([4])
    assert first["runId"] != second["runId"]
    assert second["previousPosition"] == first["committedPosition"]
    assert first["observedSchemaFingerprint"] != second["observedSchemaFingerprint"]
    print("wheel E2E passed")


if __name__ == "__main__":
    {"seed": seed, "run": run, "verify": lambda: verify([1, 2]), "e2e": e2e}[sys.argv[1]]()
