from landfall.core.contract import IngestionContract
from landfall.core.models import (
    ColumnProfile,
    PlannerPolicy,
    RuntimeCapacity,
    SourceCapabilities,
    SourceProfile,
    semantic_scope,
)
from landfall.core.planner import plan


def contract():
    return IngestionContract.parse({
        "apiVersion": "ingestion.contract/v1alpha1", "kind": "IngestionContract",
        "metadata": {"version": "1.0.0", "domain": "x", "sourceSystem": "crm", "owner": "data"},
        "source": {"connector": "sqlserver", "connectionRef": "x", "resource": {}},
        "fields": [{"name": "id", "type": "int64"}], "read": {"mode": "full"},
        "target": {"asset": "item", "write": {"mode": "replace"}},
        "execution": {"constraints": {"maxSourceConnections": 2}},
    })


def test_small_table_uses_single():
    c = contract()
    profile = SourceProfile("p", 2, 8, "s", (ColumnProfile("id", "int64", False, 1, 2, indexed=True),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(8, 8, 1024), PlannerPolicy(minimum_partition_bytes=100))
    assert result.strategy == "SINGLE"
    assert result.logical_partitions == 1


def test_partition_coverage_and_connection_limit():
    c = contract()
    profile = SourceProfile("p", 100, 8, "s", (ColumnProfile("id", "int64", True, 1, 100, indexed=True),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(8, 8, 1024), PlannerPolicy(target_partition_bytes=100,
                                                               minimum_partition_bytes=1, desired_waves=3))
    assert result.logical_partitions > result.max_in_flight
    assert result.max_in_flight == 2
    ranges = [p for p in result.partitions if p.kind == "RANGE"]
    assert ranges[0].lower is None and ranges[-1].upper is None
    assert len([p for p in result.partitions if p.kind == "NULL"]) == 1
    for value in [None, -100, *range(1, 101), 1000000]:
        matches = [p for p in result.partitions if (p.kind == "NULL" and value is None) or
                   (p.kind == "RANGE" and value is not None and
                    (p.lower is None or value >= p.lower) and (p.upper is None or value < p.upper))]
        assert len(matches) == 1


def test_skew_prefers_quantiles_when_profile_supplies_boundaries():
    c = contract()
    profile = SourceProfile("p", 1000, 8, "s", (ColumnProfile(
        "id", "int64", False, 1, 100, quantiles=(10, 10, 20, 90), indexed=True,
        unique=True, skew=3.0),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(8, 8, 1024), PlannerPolicy(target_partition_bytes=100,
                                                               minimum_partition_bytes=1))
    assert result.strategy == "NUMERIC_QUANTILE"
    assert [(p.lower, p.upper) for p in result.partitions] == [(None, 10), (10, 20), (20, 90), (90, None)]


def test_minimum_partition_size_caps_waves():
    c = contract()
    profile = SourceProfile("p", 10000, 1000, "s", (ColumnProfile(
        "id", "int64", False, 1, 10000, indexed=True),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(8, 8, 1024**3), PlannerPolicy(
                      target_partition_bytes=20_000_000, minimum_partition_bytes=2_000_000,
                      desired_waves=5))
    assert result.logical_partitions <= 5
