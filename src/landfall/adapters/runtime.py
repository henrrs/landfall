"""Local runtime capacity observation."""

from __future__ import annotations

import os

from landfall.core.models import RuntimeCapacity


def local_capacity(reader_memory_bytes: int = 128 * 1024**2) -> RuntimeCapacity:
    slots = max(1, os.cpu_count() or 1)
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_AVPHYS_PAGES")
    except (OSError, ValueError):
        memory = reader_memory_bytes
    return RuntimeCapacity(slots, max(1, memory // reader_memory_bytes), memory)
