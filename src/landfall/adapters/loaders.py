"""YAML/environment contract and separate platform configuration loading."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml

from landfall.core.contract import ContractError, IngestionContract
from landfall.core.models import PlannerPolicy


def load_contract(path: str | None = None) -> IngestionContract:
    raw = os.environ.get("LANDFALL_CONTRACT_JSON")
    if raw:
        return IngestionContract.parse(json.loads(raw))
    path = path or os.environ.get("LANDFALL_CONTRACT_PATH")
    if not path:
        raise ContractError("set --contract, LANDFALL_CONTRACT_PATH or LANDFALL_CONTRACT_JSON")
    return IngestionContract.parse(yaml.safe_load(Path(path).read_text()))


def load_platform(path: str | None = None) -> dict[str, Any]:
    path = path or os.environ.get("LANDFALL_PLATFORM_CONFIG")
    if not path:
        raise ValueError("set --platform or LANDFALL_PLATFORM_CONFIG")
    value = yaml.safe_load(Path(path).read_text())
    if not isinstance(value, dict):
        raise TypeError("Platform Config must be an object")
    return value


def resolve_platform(config: dict[str, Any], domain: str,
                     source_system: str | None = None) -> tuple[str, str, PlannerPolicy]:
    try:
        topology = config["storageTopology"]["domains"][domain]
        account = topology["storageAccount"]
        container = topology["containers"]["landing"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"domain {domain!r} has no landing storage topology") from exc
    if not isinstance(account, str) or not account or not isinstance(container, str) or not container:
        raise ValueError("invalid landing storage account/container")
    registry = config.get("registries", {}).get("sourceSystems")
    if registry is not None and source_system not in registry:
        raise ValueError(f"sourceSystem {source_system!r} is not in the platform registry")
    p = config.get("planner", {})
    policy = PlannerPolicy(
        target_partition_bytes=int(p.get("targetPartitionBytes", 2 * 1024**3)),
        minimum_partition_bytes=int(p.get("minimumPartitionBytes", 128 * 1024**2)),
        desired_waves=int(p.get("desiredWaves", 3)),
        safety_max_connections=int(p.get("safetyMaxConnections", 4)),
        batch_target_bytes=int(p.get("batchTargetBytes", 16 * 1024**2)),
        max_split_depth=int(p.get("maxSplitDepth", 2)),
    )
    if min(policy.target_partition_bytes, policy.minimum_partition_bytes,
           policy.desired_waves, policy.safety_max_connections, policy.batch_target_bytes) < 1:
        raise ValueError("planner settings must be positive")
    compression = config.get("landing", {}).get("compression", "lz4").lower()
    if compression not in {"none", "lz4", "zstd"}:
        raise ValueError("landing.compression must be none, lz4 or zstd")
    return account, container, policy
