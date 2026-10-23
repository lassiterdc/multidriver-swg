"""Refactor reminders for the units of _utils.py scoped in to dependence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_utils.py:421-434", system="dependence-modelling")
def test_reminder_421_434():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:421-434 is scoped in to dependence-modelling and has no equivalence test yet"
    )
