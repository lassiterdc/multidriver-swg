"""Refactor reminders for the units of _b3_event_analysis.py scoped in to event-occurrence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:98-134", system="event-occurrence-modelling")
def test_reminder_98_134():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:98-134 is scoped in to event-occurrence-modelling and has no equivalence test yet"
    )
