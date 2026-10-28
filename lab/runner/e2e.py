"""Wheel-only local lab seed, capture, and assertions."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import mssql_python
import pyarrow as pa

from landfall.adapters.secrets import KeyVaultSecrets
from landfall.adapters.storage import make_storage


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
        cur.execute("IF NOT EXISTS (SELECT 1 FROM dbo.Customer WHERE customer_id=4) "
                    "INSERT INTO dbo.Customer(customer_id,name,status,updated_at,internal_note) "
                    "VALUES(4,N'Dora','ACTIVE','2026-01-03T10:00:00',N'new source field stays excluded')")
        con.commit()
    finally:
        con.close()


def e2e():
    seed()
    run()
    first = verify([1, 2])
    append_new_row()
    run()
    second = verify([4])
    assert first["runId"] != second["runId"]
    assert second["previousPosition"] == first["committedPosition"]
    print("wheel E2E passed")


if __name__ == "__main__":
    {"seed": seed, "run": run, "verify": lambda: verify([1, 2]), "e2e": e2e}[sys.argv[1]]()
