from landfall.core.contract import IngestionContract
from landfall.core.models import SourcePosition, ordered_position_key


def contract(position_type):
    return IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1", "domain": "customer", "sourceSystem": "crm", "owner": "data"},
        "source": {"connector": "sqlserver", "connectionRef": "crm", "resource": {}},
        "fields": [{"name": "position", "type": position_type}, {"name": "id", "type": "int64"}],
        "read": {"mode": "incremental", "initialPosition": "beginning",
                 "position": {"strategy": "ordered_fields", "fields": ["position", "id"]}},
        "target": {"asset": "customer", "write": {"mode": "append"}},
    })


def test_decimal_position_uses_numeric_order():
    c = contract("decimal(18,2)")
    low = SourcePosition("ordered_fields", ("2.00", 3))
    high = SourcePosition("ordered_fields", ("10.00", 1))
    assert ordered_position_key(c, low) < ordered_position_key(c, high)


def test_timestamp_position_uses_instant_order_across_offsets():
    c = contract("timestamp")
    low = SourcePosition("ordered_fields", ("2026-01-01T10:00:00+02:00", 1))
    high = SourcePosition("ordered_fields", ("2026-01-01T09:00:00+00:00", 1))
    assert ordered_position_key(c, low) < ordered_position_key(c, high)
