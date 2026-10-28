"""Local runtime capacity observation."""

from __future__ import annotations

import os
import socket
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

from landfall.core.models import Partition, PhysicalReadPlan, RuntimeCapacity


def local_capacity(reader_memory_bytes: int = 128 * 1024**2) -> RuntimeCapacity:
    slots = max(1, os.cpu_count() or 1)
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_AVPHYS_PAGES")
    except (OSError, ValueError):
        memory = reader_memory_bytes
    return RuntimeCapacity(slots, max(1, memory // reader_memory_bytes), memory)


class LocalRuntimeAdapter:
    runtime_type = "local"
    mode = "thread-pool"

    def __init__(self, capacity: RuntimeCapacity | None = None):
        self.capacity = capacity or local_capacity()

    def execute(self, plan: PhysicalReadPlan,
                partition_handler: Callable[[Partition], list[dict[str, Any]]],
                context: dict[str, Any] | None = None) -> dict[str, Any]:
        guard = threading.Lock()
        active = peak = 0

        def run(partition: Partition) -> list[dict[str, Any]]:
            nonlocal active, peak
            started = datetime.now(UTC)
            tick = time.monotonic()
            with guard:
                active += 1
                peak = max(peak, active)
            try:
                records = partition_handler(partition)
                finished = datetime.now(UTC)
                elapsed = time.monotonic() - tick
                for record in records:
                    record.setdefault("runtimeType", self.runtime_type)
                    record.setdefault("runtimeMode", self.mode)
                    record.setdefault("startedAt", started.isoformat())
                    record.setdefault("finishedAt", finished.isoformat())
                    record.setdefault("runtimeDurationSeconds", elapsed)
                    record.setdefault("executorId", "local")
                    record.setdefault("executorHost", socket.gethostname())
                    record.setdefault("sparkStageId", None)
                    record.setdefault("sparkPartitionId", None)
                    record.setdefault("taskAttemptId", None)
                return records
            finally:
                with guard:
                    active -= 1

        records = []
        with ThreadPoolExecutor(max_workers=plan.max_in_flight) as executor:
            futures = [executor.submit(run, part) for part in plan.partitions]
            for future in as_completed(futures):
                records.extend(future.result())
        records.sort(key=lambda record: record["partition"]["id"])
        return {
            "partitions": records,
            "evidence": {
                "type": self.runtime_type,
                "mode": self.mode,
                "logicalPartitions": len(plan.partitions),
                "maxInFlight": plan.max_in_flight,
                "waves": (len(plan.partitions) + plan.max_in_flight - 1) // plan.max_in_flight,
                "tasks": len(records),
                "distinctExecutors": len({record["executorId"] for record in records}),
                "distinctHosts": len({record["executorHost"] for record in records}),
                "peakConcurrentTasks": peak,
                "observedPeakSqlConnections": None,
            },
        }

    def close(self) -> None:
        return None


def make_runtime(platform: dict[str, Any], domain: str | None = None):
    runtime = platform.get("runtime", {"type": "local"})
    runtime_type = runtime.get("type", "local")
    if runtime_type == "local":
        return LocalRuntimeAdapter()
    if runtime_type == "spark":
        from landfall.adapters.spark_runtime import SparkRuntimeAdapter

        options = runtime.get("spark", {})
        if not domain:
            raise ValueError("Spark runtime requires the contract domain for worker storage resolution")
        try:
            topology = platform["storageTopology"]["domains"][domain]
            account = topology["storageAccount"]
            container = topology["containers"]["landing"]
        except (KeyError, TypeError) as exc:
            raise ValueError(f"domain {domain!r} has no worker landing storage topology") from exc
        worker_config = {
            "secrets": platform.get("secrets", {"provider": "env"}),
            "storage": platform.get("storage", {"type": "local", "root": ".landfall"}),
            "sqlserver": platform.get("sqlserver", {}),
            "account": account,
            "container": container,
        }
        return SparkRuntimeAdapter(options, worker_config)
    raise ValueError(f"unknown runtime type: {runtime_type}")
