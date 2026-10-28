import copy

from landfall.core.contract import IngestionContract
from landfall.core.models import (
    ColumnProfile,
    ExecutionHistory,
    PhysicalReadPlan,
    PlannerPolicy,
    RuntimeCapacity,
    SourceCapabilities,
    SourceProfile,
    semantic_scope,
)
from landfall.core.planner import plan, skew_band


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


def test_histogram_quantiles_handle_sparse_negative_keys():
    c = contract()
    column = ColumnProfile("id", "int64", False, -100, 900_000_000,
                           histogram=((-100, 50), (2, 50), (3, 50),
                                      (1_000_000, 50), (900_000_000, 1)),
                           indexed=True, skew=3.0)
    profile = SourceProfile("p", 201, 8, "s", (column,))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(2, 2, 1024**3), PlannerPolicy(target_partition_bytes=400,
                                                                 minimum_partition_bytes=1))
    assert result.strategy == "NUMERIC_QUANTILE"
    assert result.partitions[0].lower is None
    assert result.partitions[-1].upper is None
    assert any(p.upper == 2 for p in result.partitions)


def test_low_cardinality_hot_value_uses_secondary_range():
    c = contract()
    status = ColumnProfile("status", "string", False, histogram=(("A", 900), ("B", 100)),
                           hot_values=(("A", 900), ("B", 100)), distinct_estimate=2,
                           indexed=True, statistics_confidence=.9, index_quality=1)
    secondary = ColumnProfile("sequence", "int64", False, 1, 1000, indexed=True)
    profile = SourceProfile("p", 1000, 100, "s", (status, secondary))
    result = plan(c, semantic_scope(c, None, None, "t"), profile,
                  SourceCapabilities(low_cardinality=True), RuntimeCapacity(2, 2, 1024**3),
                  PlannerPolicy(target_partition_bytes=10000, minimum_partition_bytes=1))
    assert result.strategy == "HOT_VALUE_HIERARCHICAL"
    assert sum(p.kind == "VALUE_RANGE" and p.value == "A" for p in result.partitions) > 1
    assert result.partitions[-1].kind == "OTHER"


def test_low_cardinality_buckets_keep_other_and_null_complements():
    c = contract()
    status = ColumnProfile("status", "string", True,
                           histogram=(("A", 50), ("B", 50)),
                           hot_values=(("A", 50), ("B", 50)),
                           distinct_estimate=2, indexed=True)
    profile = SourceProfile("p", 100, 100, "s", (status,))
    result = plan(c, semantic_scope(c, None, None, "t"), profile,
                  SourceCapabilities(low_cardinality=True), RuntimeCapacity(2, 2, 1024**3),
                  PlannerPolicy(target_partition_bytes=1000, minimum_partition_bytes=1))
    assert result.strategy == "LOW_CARDINALITY_BUCKETS"
    assert [part.kind for part in result.partitions] == ["VALUE", "VALUE", "OTHER", "NULL"]


def test_skewed_numeric_composite_key_splits_hot_leading_value():
    c = contract()
    group = ColumnProfile("group_id", "int32", False, 1, 3,
                          histogram=((1, 180), (2, 10), (3, 10)),
                          hot_values=((1, 180), (2, 10), (3, 10)),
                          distinct_estimate=3, indexed=True, skew=1.8,
                          index_quality=1.5)
    item = ColumnProfile("item_id", "int64", False, 1, 200, indexed=True,
                         key_position=2, index_quality=.25)
    profile = SourceProfile("p", 200, 40, "s", (group, item), keys=(("group_id", "item_id"),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile,
                  SourceCapabilities(low_cardinality=True), RuntimeCapacity(2, 2, 1024**3),
                  PlannerPolicy(target_partition_bytes=1000, minimum_partition_bytes=1))
    assert result.strategy == "COMPOSITE"
    assert sum(p.kind == "VALUE_RANGE" and p.value == 1 for p in result.partitions) > 1


def test_history_changes_sizing_and_plan_is_deterministic():
    c = contract()
    profile = SourceProfile("random-profile-id", 1000, 100, "s",
                            (ColumnProfile("id", "int64", False, 1, 1000, indexed=True),))
    scope = semantic_scope(c, None, None, "t")
    policy = PlannerPolicy(target_partition_bytes=20000, minimum_partition_bytes=1,
                           target_partition_seconds=100)
    capacity = RuntimeCapacity(2, 2, 1024**3)
    baseline = plan(c, scope, profile, SourceCapabilities(), capacity, policy)
    history = ExecutionHistory(runs=1, last_partitions=baseline.logical_partitions,
                               p95_partition_seconds=1000, retries=2)
    updated = plan(c, scope, profile, SourceCapabilities(), capacity, policy, history)
    assert updated.logical_partitions > baseline.logical_partitions
    assert updated.plan_id == plan(c, scope, profile, SourceCapabilities(), capacity, policy, history).plan_id
    assert updated.version == 2


def test_guid_falls_back_to_single_without_safe_hash():
    c = contract()
    profile = SourceProfile("p", 100000, 100, "s",
                            (ColumnProfile("id", "string", False, indexed=True, unique=True),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(8, 8, 1024**3), PlannerPolicy(minimum_partition_bytes=1))
    assert result.strategy == "SINGLE"


def test_temporal_histogram_selects_quantiles_and_null_bucket():
    c = contract()
    column = ColumnProfile("created_at", "timestamp", True,
                           "2026-01-01T00:00:00", "2026-04-01T00:00:00",
                           histogram=(("2026-01-01T00:00:00", 10),
                                      ("2026-03-31T00:00:00", 900),
                                      ("2026-04-01T00:00:00", 10)), indexed=True,
                           skew=3.0)
    profile = SourceProfile("p", 920, 32, "s", (column,))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(2, 2, 1024**3), PlannerPolicy(minimum_partition_bytes=1,
                                                                 target_partition_bytes=1000))
    assert result.strategy == "TEMPORAL_QUANTILE"
    assert result.partitions[-1].kind == "NULL"
    assert result.partitions[0].lower is None


def test_v1_plan_file_can_be_loaded_after_v2_upgrade():
    c = contract()
    profile = SourceProfile("p", 2, 8, "s", ())
    current = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                   RuntimeCapacity(1, 1, 1024), PlannerPolicy())
    old = current.to_dict()
    old.pop("version")
    old.pop("consistency_level")
    loaded = PhysicalReadPlan.from_dict(old)
    assert loaded.version == 1
    assert loaded.consistency_level == "BEST_EFFORT"


def test_skew_classification_is_policy_driven():
    policy = PlannerPolicy(skew_moderate_threshold=1.5,
                           skew_high_threshold=3, skew_extreme_threshold=8)
    assert [skew_band(value, policy) for value in (1, 2, 4, 10)] == [
        "LOW", "MODERATE", "HIGH", "EXTREME"]


def test_slo_explanation_does_not_violate_connection_limit():
    document = copy.deepcopy(contract().document)
    document["serviceLevel"] = {"targetDuration": "PT30M"}
    c = IngestionContract.parse(document)
    profile = SourceProfile("p", 1000, 100, "s",
                            (ColumnProfile("id", "int64", False, 1, 1000, indexed=True),))
    result = plan(c, semantic_scope(c, None, None, "t"), profile, SourceCapabilities(),
                  RuntimeCapacity(16, 16, 1024**3), PlannerPolicy(minimum_partition_bytes=1),
                  ExecutionHistory(runs=1, bytes_per_second_per_reader=1.0))
    assert result.max_in_flight == 2
    assert any("target likely unreachable" in reason for reason in result.reasons)
