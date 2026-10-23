"""Refactor reminders for the units of _utils.py scoped in to marginal-distribution-fitting.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_utils.py:410-420", system="marginal-distribution-fitting")
def test_reminder_410_420():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:410-420 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:1198-1238", system="marginal-distribution-fitting")
def test_reminder_1198_1238():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:1198-1238 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )
