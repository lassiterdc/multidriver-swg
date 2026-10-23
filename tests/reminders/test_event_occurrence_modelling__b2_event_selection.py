"""Refactor reminders for the units of _b2_event_selection.py scoped in to event-occurrence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:95-108", system="event-occurrence-modelling")
def test_reminder_95_108():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:95-108 is scoped in to event-occurrence-modelling and has no equivalence test yet"
    )
