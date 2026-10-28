"""Semantic scope, source observations, physical plans, and durable positions."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from landfall.core.contract import IngestionContract, fingerprint


@dataclass(frozen=True)
class SourcePosition:
    strategy: str
    values: tuple[Any, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"strategy": self.strategy, "values": list(self.values)}

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> SourcePosition | None:
        return None if value is None else cls(value["strategy"], tuple(value["values"]))


@dataclass(frozen=True)
class SemanticReadScope:
    identity: str
    mode: str
    fields: tuple[tuple[str, str], ...]
    filters: dict[str, Any] | None
    position_fields: tuple[str, ...]
    low: SourcePosition | None
    high: SourcePosition | None
    horizon: str

    @property
    def hash(self) -> str:
        return fingerprint(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {"identity": self.identity, "mode": self.mode, "fields": self.fields,
                "filters": self.filters, "positionFields": self.position_fields,
                "low": self.low.to_dict() if self.low else None,
                "high": self.high.to_dict() if self.high else None, "horizon": self.horizon}


def semantic_scope(contract: IngestionContract, low: SourcePosition | None,
                   high: SourcePosition | None, horizon: str) -> SemanticReadScope:
    read = contract.document["read"]
    position_fields = tuple(read["position"].get("fields", [])) if read["mode"] in {"incremental", "continuous"} else ()
    return SemanticReadScope(contract.identity, read["mode"], contract.fields,
                             read.get("filters"), position_fields, low, high, horizon)


def ordered_position_key(contract: IngestionContract, position: SourcePosition) -> tuple[Any, ...]:
    """Compare durable JSON position values by contracted type, not string spelling."""
    types = dict(contract.fields)
    fields = contract.document["read"]["position"]["fields"]
    converted = []
    for field_name, value in zip(fields, position.values, strict=True):
        typ = types[field_name]
        if typ.startswith("decimal("):
            converted.append(Decimal(str(value)))
        elif typ.startswith("int"):
            converted.append(int(value))
        elif typ in {"float32", "float64"}:
            converted.append(float(value))
        elif typ == "timestamp":
            converted.append(datetime.fromisoformat(str(value)))
        elif typ == "date":
            converted.append(date.fromisoformat(str(value)))
        else:
            converted.append(value)
    return tuple(converted)


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    logical_type: str
    nullable: bool
    minimum: Any = None
    maximum: Any = None
    quantiles: tuple[Any, ...] = ()
    distinct_estimate: int | None = None
    indexed: bool = False
    unique: bool = False
    null_fraction: float = 0.0
    skew: float = 1.0
    histogram: tuple[tuple[Any, int], ...] = ()
    hot_values: tuple[tuple[Any, int], ...] = ()
    statistics_confidence: float = 0.0
    statistics_updated_at: str | None = None
    key_position: int | None = None
    index_quality: float = 0.0


@dataclass(frozen=True)
class SourceProfile:
    profile_id: str
    estimated_rows: int
    arrow_row_bytes: int
    schema_fingerprint: str
    columns: tuple[ColumnProfile, ...]
    keys: tuple[tuple[str, ...], ...] = ()
    confidence: float = 0.5
    observed_fields: tuple[str, ...] = ()
    observed_arrow_row_bytes: float | None = None
    probe_rows: int = 0
    probe_seconds: float | None = None
    probe_bytes_per_second: float | None = None
    indexes: tuple[tuple[str, ...], ...] = ()
    index_included_columns: tuple[tuple[str, ...], ...] = ()
    consistency_capabilities: tuple[str, ...] = ()

    @property
    def estimated_bytes(self) -> int:
        return int(self.estimated_rows * (self.observed_arrow_row_bytes or self.arrow_row_bytes))

    @property
    def hash(self) -> str:
        value = asdict(self)
        value.pop("profile_id")
        return fingerprint(value)

    @property
    def reuse_signature(self) -> str:
        value = asdict(self)
        for key in ("profile_id", "probe_seconds", "probe_bytes_per_second"):
            value.pop(key)
        return fingerprint(value)


@dataclass(frozen=True)
class ExecutionHistory:
    runs: int = 0
    last_strategy: str | None = None
    last_candidate: str | None = None
    last_partitions: int = 0
    average_partition_seconds: float | None = None
    p95_partition_seconds: float | None = None
    bytes_per_second_per_reader: float | None = None
    retries: int = 0
    splits: int = 0
    failures: int = 0
    timeouts: int = 0
    average_query_latency_seconds: float | None = None
    average_serialization_seconds: float | None = None
    average_storage_seconds: float | None = None
    schema_fingerprint: str | None = None
    observed_at: str | None = None


@dataclass(frozen=True)
class SourceCapabilities:
    numeric_range: bool = True
    temporal_range: bool = True
    ordered_fields: bool = True
    quantiles: bool = True
    low_cardinality: bool = False
    hash_predicate: bool = False
    source_native_position: bool = False


@dataclass(frozen=True)
class RuntimeCapacity:
    task_slots: int
    memory_safe_readers: int
    available_memory: int


@dataclass(frozen=True)
class PlannerPolicy:
    target_partition_bytes: int = 2 * 1024**3
    minimum_partition_bytes: int = 128 * 1024**2
    desired_waves: int = 3
    safety_max_connections: int = 4
    batch_target_bytes: int = 16 * 1024**2
    skew_quantile_threshold: float = 2.0
    max_split_depth: int = 2
    maximum_partition_bytes: int = 8 * 1024**3
    target_partition_seconds: float = 300.0
    skew_moderate_threshold: float = 1.5
    skew_high_threshold: float = 3.0
    skew_extreme_threshold: float = 8.0
    max_logical_partitions: int = 4096
    history_ttl_seconds: int = 7 * 86400
    hot_value_fraction_threshold: float = 0.5
    minimum_hot_value_rows: int = 64


@dataclass(frozen=True)
class Partition:
    id: str
    kind: str = "ALL"
    field: str | None = None
    lower: Any = None
    upper: Any = None
    include_null: bool = False
    parent: str | None = None
    depth: int = 0
    value: Any = None
    secondary_field: str | None = None
    secondary_lower: Any = None
    secondary_upper: Any = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PhysicalReadPlan:
    plan_id: str
    contract_fingerprint: str
    scope_fingerprint: str
    profile_fingerprint: str
    strategy: str
    candidate: str | None
    partitions: tuple[Partition, ...]
    max_in_flight: int
    target_partition_bytes: int
    batch_target_bytes: int
    reasons: tuple[str, ...] = field(default_factory=tuple)
    version: int = 2
    consistency_level: str = "BEST_EFFORT"

    @property
    def logical_partitions(self) -> int:
        return len(self.partitions)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["logical_partitions"] = self.logical_partitions
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PhysicalReadPlan:
        return cls(value["plan_id"], value["contract_fingerprint"], value["scope_fingerprint"],
                   value["profile_fingerprint"], value["strategy"], value["candidate"],
                   tuple(Partition(**p) for p in value["partitions"]), value["max_in_flight"],
                   value["target_partition_bytes"], value["batch_target_bytes"],
                   tuple(value["reasons"]), value.get("version", 1),
                   value.get("consistency_level", "BEST_EFFORT"))
