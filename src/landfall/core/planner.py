"""Technology-independent physical partition decisions."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from itertools import pairwise

from landfall.core.contract import IngestionContract, fingerprint
from landfall.core.models import (
    ColumnProfile,
    Partition,
    PhysicalReadPlan,
    PlannerPolicy,
    RuntimeCapacity,
    SemanticReadScope,
    SourceCapabilities,
    SourceProfile,
)


def _score(column: ColumnProfile, capabilities: SourceCapabilities) -> tuple[float, list[str]]:
    typ = column.logical_type
    numeric = typ.startswith("int") and capabilities.numeric_range
    temporal = typ in {"date", "timestamp"} and capabilities.temporal_range
    if not numeric and not temporal:
        return (-1.0, ["no safe range predicate"])
    if column.minimum is None or column.maximum is None or column.minimum == column.maximum:
        return (-1.0, ["no usable bounds"])
    score = 2.0 if numeric else 1.5
    reasons = ["numeric range" if numeric else "temporal range"]
    if column.indexed:
        score += 2.0
        reasons.append("indexed")
    if column.unique:
        score += 1.0
        reasons.append("unique")
    score -= min(2.0, column.null_fraction * 2)
    score -= min(2.0, max(0, column.skew - 1) * 0.2)
    return score, reasons


def _numeric_partitions(column: ColumnProfile, count: int) -> tuple[Partition, ...]:
    low, high = int(column.minimum), int(column.maximum)
    width = max(1, math.ceil((high - low + 1) / count))
    bounds = list(range(low, high + 1, width)) + [high + 1]
    return tuple(Partition(f"{i:05d}", "RANGE", column.name,
                           None if i == 0 else a, None if i == len(bounds) - 2 else b)
                 for i, (a, b) in enumerate(pairwise(bounds)))


def _temporal_partitions(column: ColumnProfile, count: int) -> tuple[Partition, ...]:
    as_date = column.logical_type == "date"
    low = date.fromisoformat(str(column.minimum)) if as_date else datetime.fromisoformat(str(column.minimum))
    high = date.fromisoformat(str(column.maximum)) if as_date else datetime.fromisoformat(str(column.maximum))
    span = (high - low) / count
    if span < (timedelta(days=1) if as_date else timedelta(microseconds=1)):
        return (Partition("00000"),)
    bounds = [low + span * i for i in range(count)] + [high + (timedelta(days=1) if as_date else timedelta(microseconds=1))]
    return tuple(Partition(f"{i:05d}", "RANGE", column.name,
                           None if i == 0 else str(a), None if i == len(bounds) - 2 else str(b))
                 for i, (a, b) in enumerate(pairwise(bounds)))


def _quantile_partitions(column: ColumnProfile) -> tuple[Partition, ...]:
    interior = sorted({value for value in column.quantiles
                       if column.minimum < value < column.maximum})
    bounds = [None, *interior, None]
    return tuple(Partition(f"{i:05d}", "RANGE", column.name, lower, upper)
                 for i, (lower, upper) in enumerate(pairwise(bounds)))


def plan(contract: IngestionContract, scope: SemanticReadScope, profile: SourceProfile,
         capabilities: SourceCapabilities, capacity: RuntimeCapacity,
         policy: PlannerPolicy) -> PhysicalReadPlan:
    limit = contract.document.get("execution", {}).get("constraints", {}).get("maxSourceConnections")
    max_in_flight = max(1, min(capacity.task_slots, capacity.memory_safe_readers,
                               policy.safety_max_connections, limit or policy.safety_max_connections))
    reasons = [f"estimated {profile.estimated_rows} rows, {profile.estimated_bytes} Arrow bytes",
               f"reader budget {max_in_flight}"]
    chosen = None
    partitions = (Partition("00000"),)
    strategy = "SINGLE"
    if profile.estimated_bytes >= policy.minimum_partition_bytes and profile.estimated_rows > 1:
        candidates = sorted(((_score(c, capabilities)[0], c) for c in profile.columns),
                            key=lambda item: item[0], reverse=True)
        if candidates and candidates[0][0] > 0:
            chosen = candidates[0][1]
            size_count = max(1, math.ceil(profile.estimated_bytes / policy.target_partition_bytes))
            wave_count = max_in_flight * policy.desired_waves
            min_size_limit = max(1, profile.estimated_bytes // policy.minimum_partition_bytes)
            count = min(profile.estimated_rows, min_size_limit, max(size_count, wave_count))
            is_numeric = chosen.logical_type.startswith("int")
            if chosen.skew >= policy.skew_quantile_threshold and chosen.quantiles and capabilities.quantiles:
                partitions = _quantile_partitions(chosen)
                strategy = "NUMERIC_QUANTILE" if is_numeric else "TEMPORAL_QUANTILE"
                reasons.append("skew detected; source quantile boundaries selected")
            elif is_numeric:
                partitions = _numeric_partitions(chosen, count)
                strategy = "NUMERIC_RANGE"
            else:
                partitions = _temporal_partitions(chosen, count)
                strategy = "TEMPORAL_RANGE"
            if chosen.nullable:
                partitions += (Partition(f"{len(partitions):05d}", "NULL", chosen.name),)
            reasons += _score(chosen, capabilities)[1]
            if chosen.skew >= policy.skew_quantile_threshold and not chosen.quantiles:
                reasons.append("skew observed but no quantiles; range may be uneven")
        else:
            reasons.append("no safe candidate")
    else:
        reasons.append("small source")
    if len(partitions) == 1 and partitions[0].kind == "ALL":
        chosen = None
        strategy = "SINGLE"
    plan_id = fingerprint({"contract": contract.hash, "scope": scope.hash,
                           "profile": profile.hash, "strategy": strategy,
                           "partitions": [p.to_dict() for p in partitions],
                           "concurrency": max_in_flight})[:20]
    return PhysicalReadPlan(plan_id, contract.hash, scope.hash, profile.hash, strategy,
                            chosen.name if chosen else None, partitions, max_in_flight,
                            policy.target_partition_bytes, policy.batch_target_bytes,
                            tuple(reasons))


def split_partition(partition: Partition, max_depth: int) -> tuple[Partition, Partition] | None:
    if partition.kind != "RANGE" or partition.depth >= max_depth:
        return None
    try:
        low, high = int(partition.lower), int(partition.upper)
        mid = low + (high - low) // 2
        if mid <= low or mid >= high:
            return None
    except (ValueError, TypeError):
        return None
    return (Partition(partition.id + ".1", "RANGE", partition.field, low, mid,
                      parent=partition.id, depth=partition.depth + 1),
            Partition(partition.id + ".2", "RANGE", partition.field, mid, high,
                      parent=partition.id, depth=partition.depth + 1))
