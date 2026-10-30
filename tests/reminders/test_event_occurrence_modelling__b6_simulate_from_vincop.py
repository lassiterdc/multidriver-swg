"""Refactor reminders for the units of _b6_simulate_from_vincop.py scoped in to event-occurrence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:393-398", system="event-occurrence-modelling")
def test_reminder_393_398():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:393-398 is scoped in to event-occurrence-modelling and has no equivalence test yet"
    )
