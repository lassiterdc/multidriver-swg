"""Refactor reminders for the units of __ref_ams_functions.py scoped in to marginal-distribution-fitting.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="__ref_ams_functions.py:39-43", system="marginal-distribution-fitting")
def test_reminder_39_43():
    pytest.fail(
        "refactor reminder: legacy unit __ref_ams_functions.py:39-43 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="__ref_ams_functions.py:44-47", system="marginal-distribution-fitting")
def test_reminder_44_47():
    pytest.fail(
        "refactor reminder: legacy unit __ref_ams_functions.py:44-47 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )
