"""Dependency inversion boundaries used by the application layer."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa

from landfall.core.contract import IngestionContract
from landfall.core.models import (
    Partition,
    PhysicalReadPlan,
    RuntimeCapacity,
    SemanticReadScope,
    SourceCapabilities,
    SourcePosition,
    SourceProfile,
)


class SecretProviderPort(Protocol):
    def resolve(self, connection_ref: str) -> dict[str, Any]: ...


class SourcePort(Protocol):
    capabilities: SourceCapabilities

    def validate(self, contract: IngestionContract) -> None: ...
    def profile(self, contract: IngestionContract) -> SourceProfile: ...
    def high_position(self, contract: IngestionContract) -> SourcePosition | None: ...
    def read(self, scope: SemanticReadScope, partition: Partition,
             batch_size: int) -> Iterator[pa.RecordBatch]: ...


class StoragePort(Protocol):
    def exists(self, key: str) -> bool: ...
    def read(self, key: str) -> bytes: ...
    def write(self, key: str, data: bytes, expected_etag: str | None = None) -> str: ...
    def publish_file(self, file: Path, key: str) -> int: ...
    def download_to(self, key: str, file: Path) -> int: ...
    def list(self, prefix: str) -> list[str]: ...
    def etag(self, key: str) -> str | None: ...


class TelemetryPort(Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class ExecutionRuntimePort(Protocol):
    """Executes logical partitions while leaving run state with the application."""

    runtime_type: str
    mode: str
    capacity: RuntimeCapacity

    def execute(self, plan: PhysicalReadPlan,
                partition_handler: Callable[[Partition], list[dict[str, Any]]],
                context: dict[str, Any]) -> dict[str, Any]: ...

    def close(self) -> None: ...
