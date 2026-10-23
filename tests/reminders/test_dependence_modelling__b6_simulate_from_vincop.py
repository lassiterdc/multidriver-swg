"""Refactor reminders for the units of _b6_simulate_from_vincop.py scoped in to dependence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:489-596", system="dependence-modelling")
def test_reminder_489_596():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:489-596 is scoped in to dependence-modelling and has no equivalence test yet"
    )
