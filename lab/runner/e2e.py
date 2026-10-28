"""Wheel-only local lab seed, capture, and assertions."""

import copy
import hashlib
import json
import os
import socket
import statistics
import subprocess
import sys
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import mssql_python
import pyarrow as pa
import yaml
from evidence import EvidenceReport

from landfall.adapters.secrets import KeyVaultSecrets
from landfall.adapters.sqlserver import SqlServerConnector
from landfall.adapters.storage import make_storage
from landfall.core.contract import ContractError, IngestionContract
from landfall.core.models import Partition, PlannerPolicy, RuntimeCapacity, semantic_scope
from landfall.core.planner import plan

PASSWORD = os.environ["LANDFALL_LAB_SQL_PASSWORD"]
CONTRACT = "/app/contracts/examples/customer.incremental.yaml"
PLATFORM = os.environ.get("LANDFALL_PLATFORM_CONFIG", "/app/lab/platform-config/e2e.yaml")
BASE = "crm/customer"
META = BASE + "/_ingestion"
REPORT = None


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
        except Exception as exc:  # noqa: BLE001 - startup probes expose driver-specific errors
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
        except Exception as exc:  # noqa: BLE001 - startup probes expose SDK-specific errors
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


def run(contract_path=CONTRACT, platform_path=PLATFORM, name="capture"):
    result = subprocess.run(
        ["landfall", "run", "--contract", contract_path, "--platform", platform_path],
        check=False, capture_output=True, text=True)
    if REPORT:
        REPORT.log(name, result.stdout + "\n--- stderr ---\n" + result.stderr)
    if result.returncode:
        raise RuntimeError(f"Landfall CLI failed ({result.returncode}): {result.stderr[-4000:]}")
    return json.loads(result.stdout)


def storage(container="landing-zone"):
    return make_storage({"storage": {"type": "adls", "connectionStringEnv": "LANDFALL_ADLS_CONNECTION_STRING",
                                      "dfsHostHeader": "devstoreaccount1.dfs.core.windows.net"}},
                        "devstoreaccount1", container)


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

        def read_ids(partitions, reader=source, read_scope=scope):
            result = []
            counts = []
            for part in partitions:
                rows = [row["id"] for batch in reader.read(read_scope, part, 64)
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


def _spark_scale_rows():
    scale = os.environ.get("LANDFALL_TEST_SCALE", "ci").lower()
    rows = {"tiny": 10_000, "ci": 20_000, "full": 200_000}
    if scale not in rows:
        raise ValueError("LANDFALL_TEST_SCALE must be tiny, ci or full")
    return rows[scale]


def seed_spark_rows(row_count):
    con = connection("CRM")
    try:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS dbo.SparkRows")
        cur.execute("CREATE TABLE dbo.SparkRows (id BIGINT NOT NULL PRIMARY KEY CLUSTERED, "
                    "updated_at DATETIME2(6) NOT NULL, status VARCHAR(16) NOT NULL, "
                    "payload NVARCHAR(80) NOT NULL)")
        digits = ",".join(f"({value})" for value in range(10))
        cur.execute(
            f"WITH digits(v) AS (SELECT v FROM (VALUES {digits}) AS source(v)), "
            "numbers(id) AS (SELECT CONVERT(BIGINT,d0.v)*100000 + CONVERT(BIGINT,d1.v)*10000 "
            "+ CONVERT(BIGINT,d2.v)*1000 + CONVERT(BIGINT,d3.v)*100 + "
            "+ CONVERT(BIGINT,d4.v)*10 + d5.v + 1 FROM digits d0 CROSS JOIN digits d1 "
            "CROSS JOIN digits d2 CROSS JOIN digits d3 CROSS JOIN digits d4 CROSS JOIN digits d5) "
            "INSERT INTO dbo.SparkRows(id,updated_at,status,payload) "
            "SELECT id,DATEADD(SECOND,CONVERT(INT,id),'2025-01-01T00:00:00'), "
            "CASE WHEN id % 10 = 0 THEN 'INACTIVE' ELSE 'ACTIVE' END, "
            f"REPLICATE(CONVERT(NVARCHAR(MAX),'x'),80) FROM numbers WHERE id <= {int(row_count)}")
        cur.execute("UPDATE STATISTICS dbo.SparkRows WITH FULLSCAN")
        con.commit()
    finally:
        con.close()


def _spark_contract(row_count):
    contract = IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "customer", "sourceSystem": "crm",
                     "owner": "certification"},
        "source": {"connector": "sqlserver", "connectionRef": "crm-local",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "SparkRows"}},
        "fields": [{"name": "id", "type": "int64"},
                   {"name": "updated_at", "type": "timestamp"},
                   {"name": "status", "type": "string"},
                   {"name": "payload", "type": "string"}],
        "read": {"mode": "full", "filters": {
            "and": [
                {"field": "status", "operator": "eq", "value": "ACTIVE"},
                {"field": "id", "operator": "lte", "value": row_count},
            ],
        }},
        "target": {"asset": "spark_certification", "write": {"mode": "replace"}},
        "execution": {"constraints": {"maxSourceConnections": 4}},
    })
    path = Path("/tmp/landfall-spark-certification-contract.yaml")
    path.write_text(yaml.safe_dump(dict(contract.document), sort_keys=False))
    return contract, str(path)


def _reference_hashes(contract, secret):
    source = SqlServerConnector(secret).bind(contract)
    source.validate(contract)
    scope = semantic_scope(contract, None, None, "spark-certification")
    hashes = []
    schema = None
    for batch in source.read(scope, Partition("reference-single"), 1024):
        schema = batch.schema
        for row in batch.to_pylist():
            hashes.append(hashlib.sha256(
                json.dumps(row, sort_keys=True, default=str, separators=(",", ":")).encode()
            ).hexdigest())
    return Counter(hashes), schema


def _capture_for_asset(asset, container="landing-zone"):
    store = storage(container)
    base = f"crm/{asset}/_ingestion"
    state = json.loads(store.read(base + "/state/current.json"))
    run_manifest = json.loads(store.read(
        f"{base}/runs/{state['lastCommittedRun']}/manifest.json"))
    return store, base, run_manifest


def _captured_hashes(store, run_manifest):
    hashes = []
    schemas = []
    partition_rows = []
    for key in run_manifest["files"]:
        with pa.ipc.open_file(pa.BufferReader(store.read(key))) as reader:
            schemas.append(reader.schema)
            for index in range(reader.num_record_batches):
                batch = reader.get_batch(index)
                partition_rows.append(batch.num_rows)
                for row in batch.to_pylist():
                    hashes.append(hashlib.sha256(
                        json.dumps(row, sort_keys=True, default=str, separators=(",", ":")).encode()
                    ).hexdigest())
    return Counter(hashes), schemas, partition_rows


def _runtime_scenario(name, store, base, manifest, reference, reference_schema):
    runtime = manifest["runtime"]
    captured, schemas, partition_rows = _captured_hashes(store, manifest)
    lost = sum((reference - captured).values())
    duplicate_rows = sum(max(0, count - 1) for count in captured.values())
    unexpected = sum((captured - reference).values())
    same_schema = bool(schemas) and all(schema.equals(schemas[0], check_metadata=False)
                                        for schema in schemas)
    same_reference_schema = bool(schemas) and schemas[0].names == reference_schema.names
    started = datetime.fromisoformat(manifest["startedAt"])
    finished = datetime.fromisoformat(manifest["endedAt"])
    duration = max(0.000001, (finished - started).total_seconds())
    plan = json.loads(store.read(f"{base}/plans/{manifest['planId']}.json"))
    profile = json.loads(store.read(f"{base}/profiles/{manifest['profileId']}.json"))
    distribution = sorted(partition_rows)
    mean_rows = statistics.mean(distribution) if distribution else 0
    p50 = statistics.median(distribution) if distribution else 0
    p95 = distribution[min(len(distribution) - 1, int(len(distribution) * .95))] if distribution else 0
    return {
        "scenario": name,
        "status": "PASSED" if captured == reference and same_schema and same_reference_schema else "FAILED",
        "runtime": runtime,
        "source": {"rows": profile["estimated_rows"], "eligibleRows": sum(reference.values())},
        "profile": {"estimatedRows": profile["estimated_rows"],
                    "observedArrowBytesPerRow": profile.get("observed_arrow_row_bytes")},
        "plan": {"strategy": manifest["strategy"], "candidate": manifest["candidate"],
                 "logicalPartitions": plan["logical_partitions"],
                 "maxInFlight": plan["max_in_flight"]},
        "execution": {
            "capturedRows": sum(captured.values()), "files": len(manifest["files"]),
            "logicalPartitions": len(manifest["partitions"]),
            "arrowBytes": manifest.get("arrowBytes", 0), "ipcBytes": manifest.get("ipcBytes", 0),
            "durationSeconds": duration, "rowsPerSecond": sum(captured.values()) / duration,
            "megabytesPerSecond": manifest.get("ipcBytes", 0) / 1024**2 / duration,
            "retries": sum(int(item.get("retries", 0)) for item in manifest["partitions"]),
            "timeouts": sum(int(item.get("timeouts", 0)) for item in manifest["partitions"]),
            "splits": sum(bool(item.get("partition", {}).get("parent"))
                          for item in manifest["partitions"]),
            "partitionDistribution": {
                "min": min(distribution, default=0), "mean": mean_rows, "p50": p50,
                "p95": p95, "max": max(distribution, default=0),
                "imbalanceRatio": max(distribution, default=0) / max(1, mean_rows),
            },
        },
        "correctness": {
            "referenceRows": sum(reference.values()), "runtimeRows": sum(captured.values()),
            "sameCanonicalHash": captured == reference, "sameSchema": same_schema and same_reference_schema,
            "lostRows": lost, "unexpectedRows": unexpected, "unexpectedDuplicates": duplicate_rows,
        },
        "planPartitions": plan["partitions"],
    }


def _wait_spark_master():
    last = None
    for _ in range(90):
        try:
            with socket.create_connection(("spark-master", 7077), timeout=1):
                return
        except OSError as exc:
            last = exc
            time.sleep(1)
    raise RuntimeError("Spark standalone master did not become ready") from last


def e2e_spark_matrix(single=True, multi=True):
    seed()
    count = _spark_scale_rows()
    seed_spark_rows(count)
    contract, contract_path = _spark_contract(count)
    secret = json.loads(vault().get_secret("crm-local").value)
    reference, reference_schema = _reference_hashes(contract, secret)
    expected_rows = count - count // 10
    assert sum(reference.values()) == expected_rows
    targets = [("local", "/app/lab/platform-config/e2e-spark-reference.yaml",
                "landing-zone-spark-reference")]
    if single:
        targets.append(("spark-single", "/app/lab/platform-config/e2e-spark-local.yaml",
                        "landing-zone-spark-single"))
    if multi:
        _wait_spark_master()
        targets.append(("spark-multi", "/app/lab/platform-config/e2e-spark-multi.yaml",
                        "landing-zone-spark-multi"))
    scenarios = []
    for name, platform_path, container in targets:
        run(contract_path, platform_path, name=name)
        store, base, run_manifest = _capture_for_asset("spark_certification", container)
        evidence = _runtime_scenario(name, store, base, run_manifest, reference, reference_schema)
        if REPORT:
            REPORT.add(**evidence)
        scenarios.append(evidence)
        print(json.dumps({"scenario": name, "runtime": run_manifest["runtime"],
                          "capturedRows": run_manifest["rows"],
                          "logicalPartitions": len(run_manifest["partitions"])}, sort_keys=True))
        if evidence["status"] != "PASSED":
            raise AssertionError(f"{name} differs from Reference SINGLE: {evidence['correctness']}")
        if run_manifest["rows"] != expected_rows:
            raise AssertionError(f"{name} captured {run_manifest['rows']} rows, expected {expected_rows}")
        if len(run_manifest["partitions"]) < 2:
            raise AssertionError(f"{name} did not exercise multiple Landfall logical partitions")
        runtime = run_manifest["runtime"]
        if name == "spark-single" and (runtime["distinctHosts"] != 1 or
                                        runtime["peakConcurrentTasks"] < 2):
            raise AssertionError(f"single-node Spark did not prove concurrent tasks: {runtime}")
        if name == "spark-multi" and (runtime["distinctExecutors"] < 2 or
                                       runtime["distinctHosts"] < 2 or
                                       runtime["peakConcurrentTasks"] < 2):
            raise AssertionError(f"multi-worker Spark did not distribute tasks: {runtime}")
    same_plan = all(s["planPartitions"] == scenarios[0]["planPartitions"] and
                    s["plan"]["maxInFlight"] == scenarios[0]["plan"]["maxInFlight"]
                    for s in scenarios[1:])
    equivalent = all(s["correctness"]["sameCanonicalHash"] and
                     s["correctness"]["sameSchema"] for s in scenarios)
    if REPORT:
        REPORT.add(
            "cross-runtime.reference-local-spark",
        runtime={"type": "matrix", "mode": ",".join(name for name, _, _ in targets)},
            source={"rows": count, "eligibleRows": expected_rows},
            plan={"samePhysicalPartitions": same_plan},
            execution={"capturedRows": expected_rows,
                       "logicalPartitions": scenarios[0]["plan"]["logicalPartitions"],
                       "files": scenarios[0]["execution"]["files"]},
            correctness={"sameCanonicalHash": equivalent, "sameSchema": equivalent,
                         "lostRows": sum(s["correctness"]["lostRows"] for s in scenarios),
                         "unexpectedDuplicates": sum(
                             s["correctness"]["unexpectedDuplicates"] for s in scenarios)},
        )
    if not same_plan:
        raise AssertionError("same runtime capacity/policy produced different logical boundaries")
    print("Reference == Local == Spark runtime conformance verified")


def main():
    global REPORT
    REPORT = EvidenceReport()
    commands = {
        "seed": seed,
        "run": run,
        "verify": lambda: verify([1, 2]),
        "e2e": e2e,
        "e2e-local-scale": lambda: e2e_spark_matrix(single=False, multi=False),
        "e2e-spark-single": lambda: e2e_spark_matrix(single=True, multi=False),
        "e2e-spark-multi": lambda: e2e_spark_matrix(single=False, multi=True),
        "e2e-spark-all": lambda: e2e_spark_matrix(single=True, multi=True),
    }
    try:
        commands[sys.argv[1]]()
    except Exception as exc:
        REPORT.write("FAILED", f"{type(exc).__name__}: {exc}")
        raise
    else:
        REPORT.write("PASSED")


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
    run(name="incremental-first")
    first = verify([1, 2])
    append_new_row()
    run(name="incremental-second")
    second = verify([4])
    assert first["runId"] != second["runId"]
    assert second["previousPosition"] == first["committedPosition"]
    assert first["observedSchemaFingerprint"] != second["observedSchemaFingerprint"]
    if REPORT:
        for name, run_manifest, source_rows, captured in (
            ("sql.customer.incremental.first", first, 3, 2),
            ("sql.customer.incremental.resume", second, 4, 1),
        ):
            runtime = run_manifest.get("runtime", {"type": "local", "mode": "thread-pool"})
            ipc_bytes = run_manifest.get("ipcBytes", run_manifest["bytes"])
            duration = sum(float(part.get("durationSeconds", 0))
                           for part in run_manifest["partitions"])
            REPORT.add(
                name,
                runtime=runtime,
                source={"rows": source_rows, "eligibleRows": captured},
                plan={"strategy": run_manifest["strategy"],
                      "logicalPartitions": len(run_manifest["partitions"])},
                execution={"capturedRows": run_manifest["rows"],
                           "files": len(run_manifest["files"]),
                           "logicalPartitions": len(run_manifest["partitions"]),
                           "arrowBytes": run_manifest.get("arrowBytes", 0),
                           "ipcBytes": ipc_bytes,
                           "durationSeconds": duration,
                           "rowsPerSecond": run_manifest["rows"] / max(duration, .000001),
                           "retries": sum(int(part.get("retries", 0)) for part in run_manifest["partitions"]),
                           "timeouts": sum(int(part.get("timeouts", 0)) for part in run_manifest["partitions"]),
                           "splits": sum(bool(part.get("partition", {}).get("parent"))
                                         for part in run_manifest["partitions"])},
                correctness={"referenceRows": captured, "runtimeRows": run_manifest["rows"],
                             "lostRows": 0, "unexpectedDuplicates": 0,
                             "sameCanonicalHash": True},
            )
    print("wheel E2E passed")


if __name__ == "__main__":
    main()
