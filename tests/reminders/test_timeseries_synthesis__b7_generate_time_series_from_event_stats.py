"""Refactor reminders for the units of _b7_generate_time_series_from_event_stats.py scoped in to timeseries-synthesis.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(
    unit="_b7_generate_time_series_from_event_stats.py:88-380", system="timeseries-synthesis"
)
def test_reminder_88_380():
    pytest.fail(
        "refactor reminder: legacy unit _b7_generate_time_series_from_event_stats.py:88-380 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(
    unit="_b7_generate_time_series_from_event_stats.py:381-890", system="timeseries-synthesis"
)
def test_reminder_381_890():
    pytest.fail(
        "refactor reminder: legacy unit _b7_generate_time_series_from_event_stats.py:381-890 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )
