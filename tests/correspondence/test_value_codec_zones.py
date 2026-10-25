"""Tests that the value codec stores a time-zone-aware value whatever library supplied its zone.

pandas accepts a zone from several libraries: pytz, the standard library's `zoneinfo`, dateutil, and
fixed offsets from pytz or `datetime`. The comparator's exact licence tells a value whose zone came
from one library apart from the same instants under another library's zone, so a codec that rebuilds
every zone through pandas' default zone library reads back a different value for some of them.
These tests build one value per zone library and ask for an exact round trip.
"""

from __future__ import annotations

import zoneinfo

import pandas as pd
import pytest

from tests.correspondence import compare, value_codec

pytestmark = pytest.mark.state_check


def _zoneinfo_zone():
    """New York's zone as the standard library's `zoneinfo` supplies it."""
    return zoneinfo.ZoneInfo("America/New_York")


def _dateutil_zone():
    """New York's zone as dateutil supplies it, through pandas' `dateutil/` prefix."""
    return pd.Timestamp("2020-01-01", tz="dateutil/America/New_York").tz


def _pytz_fixed_offset():
    """A fixed offset of five hours west, as pytz supplies it; skipped where pytz is not installed."""
    pytz = pytest.importorskip("pytz")
    return pytz.FixedOffset(-300)


ZONE_SOURCES = {
    "zoneinfo": _zoneinfo_zone,
    "dateutil": _dateutil_zone,
    "pytz fixed offset": _pytz_fixed_offset,
}


@pytest.mark.parametrize("source", list(ZONE_SOURCES))
def test_a_zone_aware_value_round_trips_whatever_library_supplied_its_zone(source):
    """Property: `decode(encode(v))` is admitted by `compare.Exact` against `v` when `v` holds zone-aware dates whose zone comes from `source`.

    Class: every pandas value holding a time-zone-aware datetime index or column, quantified over
    the zone libraries pandas accepts. Each parameter puts the zone on both an index and a column of
    one `DataFrame`, so one value exercises both places the codec writes such dates.
    Why required: the codec's own docstring lists time-zone-aware datetime among the dtypes it
    stores, and `encode` refuses any value that does not read back exactly. A codec that rebuilds
    the zone by its name alone hands back a zone from another library, which `compare.Exact`
    refuses, so `encode` raises `CodecError` and a generator returning such a value cannot write
    its fixture at all.
    Killed by: rebuilding every zone with `tz_convert` on the zone's name, which is what makes the
    zone's library change on the way back; a codec that records which library supplied the zone
    and rebuilds it with that library passes.
    A second correct implementation: storing pandas' own zone identifier together with the library
    it names, or storing a fixed offset as its number of minutes and a named zone as its key, each
    rebuilt into the original library's zone object.
    """
    zone = ZONE_SOURCES[source]()
    instants = pd.date_range("2020-03-07", periods=3, freq="h", tz=zone)
    value = pd.DataFrame({"when": instants, "depth": [0.5, 1.5, 2.5]}, index=instants)
    assert compare.Exact().admits(value, value.copy())  # the licence itself accepts the value against a copy
    back = value_codec.decode(value_codec.encode(value))
    assert compare.Exact().admits(value, back)
