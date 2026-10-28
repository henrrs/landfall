from hypothesis import given
from hypothesis import strategies as st

from landfall.core.models import ColumnProfile
from landfall.core.planner import _numeric_partitions


@given(st.integers(min_value=-1_000_000, max_value=1_000_000),
       st.integers(min_value=1, max_value=10000), st.integers(min_value=1, max_value=50))
def test_numeric_partitions_cover_all_values(low, width, count):
    parts = _numeric_partitions(ColumnProfile("id", "int64", False, low, low + width), count)
    for value in [low - 100, low, low + width // 2, low + width, low + width + 100]:
        matches = [p for p in parts if (p.lower is None or value >= p.lower) and
                   (p.upper is None or value < p.upper)]
        assert len(matches) == 1
