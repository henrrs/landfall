import inspect

import pyarrow as pa
import pytest

from landfall.adapters.sqlserver import (
    SqlServerConnector,
    _lexicographic,
    compile_filter,
    distribution_skew,
    quoted,
    sql_type_to_canonical,
)
from landfall.core.contract import ContractError, IngestionContract
from landfall.core.models import Partition, semantic_scope


@pytest.mark.parametrize("sql_type,expected", [
    ("tinyint", "int16"), ("smallint", "int16"), ("int", "int32"),
    ("bigint", "int64"), ("float", "float64"), ("real", "float32"),
    ("bit", "boolean"), ("date", "date"), ("datetime2", "timestamp"),
    ("datetimeoffset", "timestamp"), ("varchar", "string"),
    ("nvarchar", "string"), ("uniqueidentifier", "string"),
    ("xml", "string"), ("timestamp", "binary"), ("rowversion", "binary"),
    ("varbinary", "binary")])
def test_type_mapping(sql_type, expected):
    assert sql_type_to_canonical(sql_type) == expected


def test_decimal_type_mapping():
    assert sql_type_to_canonical("decimal", 18, 2) == "decimal(18,2)"


def test_unmapped_type_fails():
    with pytest.raises(ContractError):
        sql_type_to_canonical("time")


def test_filter_compilation_nested_and_parameterized():
    node = {"and": [{"field": "status", "operator": "in", "values": ["A", "B"]},
                    {"or": [{"field": "id", "operator": "gt", "value": 2},
                            {"field": "id", "operator": "is_null"}]}]}
    sql, params = compile_filter(node)
    assert "[status] IN (?, ?)" in sql
    assert "[id] IS NULL" in sql
    assert params == ["A", "B", 2]


def test_lexicographic_window():
    low, params = _lexicographic(("updated_at", "customer_id"), ("2026-01-01", 7), False)
    high, upper_params = _lexicographic(("updated_at", "customer_id"), ("2026-02-01", 9), True)
    assert "[updated_at] > ?" in low and "[customer_id] > ?" in low
    assert "[customer_id] = ?" in high
    assert params == ["2026-01-01", "2026-01-01", 7]
    assert upper_params[-2:] == ["2026-02-01", 9]


def test_identifier_injection_rejected():
    with pytest.raises(ContractError):
        quoted("x]; DROP TABLE T")


def test_histogram_detects_sparse_numeric_and_temporal_distribution():
    numeric = tuple((i, 1) for i in range(1, 200)) + ((1_000_000_000, 1),)
    temporal = tuple((f"2026-01-01T00:{i // 60:02d}:{i % 60:02d}", 1)
                     for i in range(199)) + (("2027-01-01T00:00:00", 1),)
    assert distribution_skew(numeric, "int64") > 8
    assert distribution_skew(temporal, "timestamp") > 8


def test_continuous_is_contract_language_but_sql_server_capability_rejects_it():
    document = {
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1", "domain": "customer", "sourceSystem": "crm",
                     "owner": "tests"},
        "source": {"connector": "sqlserver", "connectionRef": "crm",
                   "resource": {"database": "CRM", "schema": "dbo", "table": "Customer"}},
        "fields": [{"name": "id", "type": "int64"}],
        "read": {"mode": "continuous", "initialPosition": "current",
                 "position": {"strategy": "source_native"}},
        "target": {"asset": "customer", "write": {"mode": "append"}},
    }
    contract = IngestionContract.parse(document)
    with pytest.raises(ContractError, match="does not support continuous"):
        SqlServerConnector({"server": "localhost", "database": "CRM", "user": "tester",
                            "password": "not-used"}).validate(contract)


def test_sql_server_data_path_uses_native_arrow_reader_without_python_rows():
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
    expected = pa.record_batch([pa.array([1, 2], type=pa.int64())], names=["id"])

    class Cursor:
        def execute(self, query, params):
            self.query = query

        def fetchall(self):
            raise AssertionError("source rows must not use fetchall")

        def arrow_reader(self, batch_size):
            assert batch_size == 16
            yield expected

    class Connection:
        def __init__(self):
            self.value = Cursor()

        def cursor(self):
            return self.value

        def close(self):
            pass

    source = SqlServerConnector({"server": "localhost", "database": "CRM", "user": "tester",
                                 "password": "not-used"}).bind(contract)
    source._connect = Connection
    scope = semantic_scope(contract, None, None, "test")
    batches = list(source.read(scope, Partition("single"), 16))
    assert len(batches) == 1 and batches[0].equals(expected)
    source_body = inspect.getsource(SqlServerConnector.read)
    assert "arrow_reader(" in source_body
    assert "fetchall(" not in source_body
    assert "to_pylist(" not in source_body
