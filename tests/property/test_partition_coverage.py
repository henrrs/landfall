from datetime import date, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from landfall.core.models import ColumnProfile, Partition
from landfall.core.planner import _numeric_partitions, _quantile_partitions, _temporal_partitions


@given(st.integers(min_value=-1_000_000, max_value=1_000_000),
       st.integers(min_value=1, max_value=10000), st.integers(min_value=1, max_value=50))
def test_numeric_partitions_cover_all_values(low, width, count):
    parts = _numeric_partitions(ColumnProfile("id", "int64", False, low, low + width), count)
    for value in [low - 100, low, low + width // 2, low + width, low + width + 100]:
        matches = [p for p in parts if (p.lower is None or value >= p.lower) and
                   (p.upper is None or value < p.upper)]
        assert len(matches) == 1


@given(st.integers(min_value=-1000, max_value=1000),
       st.integers(min_value=2, max_value=100), st.integers(min_value=2, max_value=20))
def test_quantile_boundaries_are_disjoint(low, width, count):
    quantiles = tuple(low + (width * i // count) for i in range(1, count))
    column = ColumnProfile("id", "int64", False, low, low + width, quantiles=quantiles)
    parts = _quantile_partitions(column)
    for value in (low - 1, low, *quantiles, low + width, low + width + 1):
        assert sum((p.lower is None or value >= p.lower) and
                   (p.upper is None or value < p.upper) for p in parts) == 1


@given(st.integers(min_value=1, max_value=1000), st.integers(min_value=2, max_value=20))
def test_temporal_range_coverage(days, count):
    low = date(2000, 1, 1)
    high = low + timedelta(days=days)
    parts = _temporal_partitions(ColumnProfile("created", "date", True,
                                              low.isoformat(), high.isoformat()), count)
    if parts[0].kind == "ALL":
        return
    for value in (low - timedelta(days=1), low, high, high + timedelta(days=1)):
        assert sum((p.lower is None or value.isoformat() >= p.lower) and
                   (p.upper is None or value.isoformat() < p.upper) for p in parts) == 1


def test_decimal_range_and_split_boundaries():
    column = ColumnProfile("amount", "decimal(18,2)", False, "-10.25", "3.75")
    parts = _numeric_partitions(column, 7)
    for value in (Decimal(-100), Decimal("-10.25"), Decimal(0), Decimal("3.75"), Decimal(100)):
        assert sum((p.lower is None or value >= Decimal(str(p.lower))) and
                   (p.upper is None or value < Decimal(str(p.upper))) for p in parts) == 1


def test_hierarchical_value_ranges_match_reference_single():
    parts = (Partition("0", "VALUE_RANGE", "status", value="A", secondary_field="id",
                       secondary_upper=5),
             Partition("1", "VALUE_RANGE", "status", value="A", secondary_field="id",
                       secondary_lower=5),
             Partition("2", "VALUE", "status", value="B"),
             Partition("3", "OTHER", "status", value=("A", "B")),
             Partition("4", "NULL", "status"))
    for status in (None, "A", "B", "C"):
        for identifier in (-1, 4, 5, 100):
            matches = []
            for part in parts:
                if part.kind == "NULL":
                    match = status is None
                elif part.kind == "OTHER":
                    match = status is not None and status not in part.value
                else:
                    match = status == part.value
                    if part.kind == "VALUE_RANGE":
                        match &= ((part.secondary_lower is None or identifier >= part.secondary_lower) and
                                  (part.secondary_upper is None or identifier < part.secondary_upper))
                if match:
                    matches.append(part.id)
            assert len(matches) == 1
