"""Refactor reminders for the units of _b6_simulate_from_vincop.py scoped in to generator-validation.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:597-626", system="generator-validation")
def test_reminder_597_626():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:597-626 is scoped in to generator-validation and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:746-770", system="generator-validation")
def test_reminder_746_770():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:746-770 is scoped in to generator-validation and has no equivalence test yet"
    )
