"""Refactor reminders for the units of _b7_generate_time_series_from_event_stats.py scoped in to boundary-condition-output.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(
    unit="_b7_generate_time_series_from_event_stats.py:891-914", system="boundary-condition-output"
)
def test_reminder_891_914():
    pytest.fail(
        "refactor reminder: legacy unit _b7_generate_time_series_from_event_stats.py:891-914 is scoped in to boundary-condition-output and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(
    unit="_b7_generate_time_series_from_event_stats.py:915-969", system="boundary-condition-output"
)
def test_reminder_915_969():
    pytest.fail(
        "refactor reminder: legacy unit _b7_generate_time_series_from_event_stats.py:915-969 is scoped in to boundary-condition-output and has no equivalence test yet"
    )
