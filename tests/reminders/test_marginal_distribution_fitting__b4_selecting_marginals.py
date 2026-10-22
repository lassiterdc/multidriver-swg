"""Refactor reminders for the units of _b4_selecting_marginals.py scoped in to marginal-distribution-fitting.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b4_selecting_marginals.py:23-65", system="marginal-distribution-fitting")
def test_reminder_23_65():
    pytest.fail(
        "refactor reminder: legacy unit _b4_selecting_marginals.py:23-65 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b4_selecting_marginals.py:66-181", system="marginal-distribution-fitting")
def test_reminder_66_181():
    pytest.fail(
        "refactor reminder: legacy unit _b4_selecting_marginals.py:66-181 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b4_selecting_marginals.py:268-316", system="marginal-distribution-fitting")
def test_reminder_268_316():
    pytest.fail(
        "refactor reminder: legacy unit _b4_selecting_marginals.py:268-316 is scoped in to marginal-distribution-fitting and has no equivalence test yet"
    )
