"""Refactor reminders for the units of _b5_fit_vine_copulas.py scoped in to interactive-decision-support.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b5_fit_vine_copulas.py:241-252", system="interactive-decision-support")
def test_reminder_241_252():
    pytest.fail(
        "refactor reminder: legacy unit _b5_fit_vine_copulas.py:241-252 is scoped in to interactive-decision-support and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b5_fit_vine_copulas.py:253-291", system="interactive-decision-support")
def test_reminder_253_291():
    pytest.fail(
        "refactor reminder: legacy unit _b5_fit_vine_copulas.py:253-291 is scoped in to interactive-decision-support and has no equivalence test yet"
    )
