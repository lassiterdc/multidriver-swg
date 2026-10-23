"""Refactor reminders for the units of _utils.py scoped in to event-occurrence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_utils.py:467-573", system="event-occurrence-modelling")
def test_reminder_467_573():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:467-573 is scoped in to event-occurrence-modelling and has no equivalence test yet"
    )
