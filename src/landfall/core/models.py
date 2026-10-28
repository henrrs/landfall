"""Semantic scope, source observations, physical plans, and durable positions."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
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

    @property
    def estimated_bytes(self) -> int:
        return self.estimated_rows * self.arrow_row_bytes

    @property
    def hash(self) -> str:
        return fingerprint(asdict(self))


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
                   tuple(value["reasons"]))
