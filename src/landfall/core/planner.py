"""Technology-independent physical partition decisions."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise

from landfall.core.contract import IngestionContract, fingerprint
from landfall.core.models import (
    ColumnProfile,
    ExecutionHistory,
    Partition,
    PhysicalReadPlan,
    PlannerPolicy,
    RuntimeCapacity,
    SemanticReadScope,
    SourceCapabilities,
    SourceProfile,
)


def skew_band(score: float, policy: PlannerPolicy) -> str:
    if score >= policy.skew_extreme_threshold:
        return "EXTREME"
    if score >= policy.skew_high_threshold:
        return "HIGH"
    if score >= policy.skew_moderate_threshold:
        return "MODERATE"
    return "LOW"


def _score(column: ColumnProfile, capabilities: SourceCapabilities) -> tuple[float, list[str]]:
    typ = column.logical_type
    numeric = typ.startswith(("int", "decimal(")) and capabilities.numeric_range
    temporal = typ in {"date", "timestamp"} and capabilities.temporal_range
    bucket = (capabilities.low_cardinality and column.indexed and not column.unique and column.hot_values
              and column.distinct_estimate is not None and column.distinct_estimate <= 32)
    if not numeric and not temporal and not bucket:
        return (-1.0, ["no safe range predicate"])
    if not bucket and (column.minimum is None or column.maximum is None or column.minimum == column.maximum):
        return (-1.0, ["no usable bounds"])
    score = 2.0 if numeric else 1.5
    reasons = ["numeric range" if numeric else "temporal range" if temporal else "indexed value buckets"]
    if column.indexed:
        score += 2.0
        reasons.append("indexed")
    else:
        score -= 3.5
        reasons.append("unindexed predicate cost")
    if column.unique:
        score += 1.0
        reasons.append("unique")
    score -= min(2.0, column.null_fraction * 2)
    score -= min(2.0, max(0, column.skew - 1) * 0.2)
    score += column.statistics_confidence
    score += column.index_quality
    return score, reasons


def _numeric_partitions(column: ColumnProfile, count: int) -> tuple[Partition, ...]:
    if column.logical_type.startswith("decimal("):
        low, high = Decimal(str(column.minimum)), Decimal(str(column.maximum))
        scale = int(column.logical_type.split(",", 1)[1][:-1])
        unit = Decimal(1).scaleb(-scale)
        total = int((high - low) / unit) + 1
        width = max(1, math.ceil(total / count))
        bounds = [low + unit * width * i for i in range(math.ceil(total / width))]
        bounds.append(high + unit)
        return tuple(Partition(f"{i:05d}", "RANGE", column.name,
                               None if i == 0 else str(a),
                               None if i == len(bounds) - 2 else str(b))
                     for i, (a, b) in enumerate(pairwise(bounds)))
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
                           None if i == 0 else a.isoformat(),
                           None if i == len(bounds) - 2 else b.isoformat())
                 for i, (a, b) in enumerate(pairwise(bounds)))


def _quantile_partitions(column: ColumnProfile, count: int | None = None) -> tuple[Partition, ...]:
    if column.logical_type.startswith("decimal("):
        convert, serialize = lambda value: Decimal(str(value)), str
    elif column.logical_type == "date":
        convert, serialize = lambda value: date.fromisoformat(str(value)), lambda value: value.isoformat()
    elif column.logical_type == "timestamp":
        convert, serialize = lambda value: datetime.fromisoformat(str(value)), lambda value: value.isoformat()
    else:
        convert, serialize = lambda value: value, lambda value: value
    low, high = convert(column.minimum), convert(column.maximum)
    if column.histogram and count:
        total = sum(max(0, rows) for _, rows in column.histogram)
        seen = 0
        boundaries = []
        target = 1
        for value, rows in column.histogram:
            seen += max(0, rows)
            while target < count and total and seen >= total * target / count:
                boundaries.append(value)
                target += 1
        interior = sorted({convert(value) for value in boundaries
                           if low < convert(value) < high})
    else:
        interior = sorted({convert(value) for value in column.quantiles
                           if low < convert(value) < high})
    bounds = [None, *(serialize(value) for value in interior), None]
    return tuple(Partition(f"{i:05d}", "RANGE", column.name, lower, upper)
                 for i, (lower, upper) in enumerate(pairwise(bounds)))


def _value_partitions(column: ColumnProfile, secondary: ColumnProfile | None,
                      target_rows: int) -> tuple[Partition, ...]:
    values = tuple(value for value, _ in column.hot_values)
    partitions = []
    for value, rows in column.hot_values:
        if (secondary and rows > 2 * target_rows and secondary.minimum is not None
                and secondary.maximum is not None):
            children = _numeric_partitions(secondary, max(2, math.ceil(rows / target_rows))) \
                if secondary.logical_type.startswith(("int", "decimal(")) else \
                _temporal_partitions(secondary, max(2, math.ceil(rows / target_rows)))
            partitions.extend(Partition(f"{len(partitions):05d}", "VALUE_RANGE", column.name,
                                        value=value, secondary_field=secondary.name,
                                        secondary_lower=child.lower,
                                        secondary_upper=child.upper) for child in children)
        else:
            partitions.append(Partition(f"{len(partitions):05d}", "VALUE", column.name, value=value))
    partitions.append(Partition(f"{len(partitions):05d}", "OTHER", column.name, value=values))
    return tuple(partitions)


def _duration_seconds(text: str) -> int:
    import re

    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", text)
    if not match:
        raise ValueError("invalid target duration")
    hours, minutes, seconds = (int(part or 0) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def plan(contract: IngestionContract, scope: SemanticReadScope, profile: SourceProfile,
         capabilities: SourceCapabilities, capacity: RuntimeCapacity,
         policy: PlannerPolicy, history: ExecutionHistory | None = None,
         consistency_level: str = "BEST_EFFORT") -> PhysicalReadPlan:
    limit = contract.document.get("execution", {}).get("constraints", {}).get("maxSourceConnections")
    max_in_flight = max(1, min(capacity.task_slots, capacity.memory_safe_readers,
                               policy.safety_max_connections, limit or policy.safety_max_connections))
    reasons = [f"estimated {profile.estimated_rows} rows, {profile.estimated_bytes} Arrow bytes",
               f"reader budget {max_in_flight}"]
    if profile.observed_arrow_row_bytes:
        reasons.append(f"observed Arrow width {profile.observed_arrow_row_bytes:.1f} bytes/row")
    if history and history.runs:
        reasons.append(f"history: {history.runs} runs, {history.retries} retries, "
                       f"{history.timeouts} timeouts, {history.splits} splits")
    chosen = None
    partitions = (Partition("00000"),)
    strategy = "SINGLE"
    if profile.estimated_bytes >= policy.minimum_partition_bytes and profile.estimated_rows > 1:
        candidates = sorted(((_score(c, capabilities)[0], c) for c in profile.columns),
                            key=lambda item: (-item[0], item[1].name))
        if candidates and candidates[0][0] > 0:
            chosen = candidates[0][1]
            reasons.append(f"candidate {chosen.name} skew {skew_band(chosen.skew, policy)} "
                           f"({chosen.skew:.2f})")
            size_count = max(1, math.ceil(profile.estimated_bytes / policy.target_partition_bytes),
                             math.ceil(profile.estimated_bytes / policy.maximum_partition_bytes))
            wave_count = max_in_flight * policy.desired_waves
            min_size_limit = max(1, profile.estimated_bytes // policy.minimum_partition_bytes)
            count = min(profile.estimated_rows, min_size_limit, policy.max_logical_partitions,
                        max(size_count, wave_count))
            if history and history.runs:
                if history.retries or history.timeouts or history.splits or (history.p95_partition_seconds and
                    history.p95_partition_seconds > policy.target_partition_seconds):
                    count = min(policy.max_logical_partitions, min_size_limit, max(count, history.last_partitions * 2))
                    reasons.append("history increased partition count after slow or failed partitions")
                elif (history.average_partition_seconds is not None and
                      history.average_partition_seconds < policy.target_partition_seconds / 10 and
                      history.last_partitions > max_in_flight):
                    count = max(1, min(count, math.ceil(history.last_partitions / 2)))
                    reasons.append("history reduced partition count after short partitions")
            is_numeric = chosen.logical_type.startswith(("int", "decimal("))
            is_bucket = (chosen.hot_values and capabilities.low_cardinality and not chosen.unique and
                         chosen.distinct_estimate is not None and chosen.distinct_estimate <= 32)
            composite_secondary = next((c for key in profile.keys if len(key) > 1 and key[0] == chosen.name
                                        for c in profile.columns if c.name == key[1] and not c.nullable and
                                        c.minimum is not None and c.maximum is not None and
                                        c.logical_type.startswith(("int", "decimal("))), None)
            hot_fraction = (max((rows for _, rows in chosen.hot_values), default=0) /
                            max(1, profile.estimated_rows))
            if (is_bucket and is_numeric and composite_secondary and
                    max((rows for _, rows in chosen.hot_values), default=0) >=
                    policy.minimum_hot_value_rows and
                    (chosen.skew >= policy.skew_quantile_threshold or
                     hot_fraction >= policy.hot_value_fraction_threshold)):
                target_rows = max(1, math.ceil(profile.estimated_rows / max(1, count)))
                partitions = _value_partitions(chosen, composite_secondary, target_rows)
                strategy = "COMPOSITE"
                reasons.append("skewed leading composite key split by second key")
            elif is_bucket and not (is_numeric or chosen.logical_type in {"date", "timestamp"}):
                secondary = next((c for _, c in candidates if c.name != chosen.name and
                                  not c.nullable and
                                  _score(c, capabilities)[0] > 0 and
                                  (c.logical_type.startswith(("int", "decimal(")) or
                                   c.logical_type in {"date", "timestamp"})), None)
                target_rows = max(1, math.ceil(profile.estimated_rows / max(1, count)))
                partitions = _value_partitions(chosen, secondary, target_rows)
                strategy = "HOT_VALUE_HIERARCHICAL" if any(p.kind == "VALUE_RANGE" for p in partitions) else "LOW_CARDINALITY_BUCKETS"
                reasons.append("indexed low-cardinality distribution selected")
            elif chosen.skew >= policy.skew_quantile_threshold and (chosen.quantiles or chosen.histogram) and capabilities.quantiles:
                partitions = _quantile_partitions(chosen, count)
                strategy = "NUMERIC_QUANTILE" if is_numeric else "TEMPORAL_QUANTILE"
                reasons.append(f"skew {chosen.skew:.2f}; source distribution boundaries selected")
            elif is_numeric:
                partitions = _numeric_partitions(chosen, count)
                strategy = "NUMERIC_RANGE"
            else:
                partitions = _temporal_partitions(chosen, count)
                strategy = "TEMPORAL_RANGE"
            if chosen.nullable:
                partitions += (Partition(f"{len(partitions):05d}", "NULL", chosen.name),)
            reasons += _score(chosen, capabilities)[1]
            if chosen.skew >= policy.skew_quantile_threshold and not (chosen.quantiles or chosen.histogram):
                reasons.append("skew observed but no quantiles; range may be uneven")
            if len(partitions) > policy.max_logical_partitions:
                partitions = (Partition("00000"),)
                reasons.append("candidate exceeds maximum logical partitions; safe SINGLE fallback")
        else:
            reasons.append("no safe candidate")
    else:
        reasons.append("small source")
    if len(partitions) == 1 and partitions[0].kind == "ALL":
        chosen = None
        strategy = "SINGLE"
    target = contract.document.get("serviceLevel", {}).get("targetDuration")
    rate = (history.bytes_per_second_per_reader if history and history.bytes_per_second_per_reader
            else profile.probe_bytes_per_second)
    if target and rate:
        estimate = profile.estimated_bytes / (rate * max_in_flight)
        goal = _duration_seconds(target)
        reasons.append(f"estimated runtime {estimate:.0f}s vs target {goal}s")
        if estimate > goal:
            reasons.append("target likely unreachable under source connection budget")
    plan_id = fingerprint({"contract": contract.hash, "scope": scope.hash,
                           "profile": profile.hash, "strategy": strategy,
                           "partitions": [p.to_dict() for p in partitions],
                           "concurrency": max_in_flight, "history": history,
                           "policy": policy, "consistency": consistency_level})[:20]
    return PhysicalReadPlan(plan_id, contract.hash, scope.hash, profile.hash, strategy,
                            chosen.name if chosen else None, partitions, max_in_flight,
                            policy.target_partition_bytes, policy.batch_target_bytes,
                            tuple(reasons), consistency_level=consistency_level)


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
