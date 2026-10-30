"""Refactor reminders for the units of _b5_fit_vine_copulas.py scoped in to dependence-modelling.

Each test fails until an equivalence test carrying the same unit key replaces it. The seven units of this file whose
legacy code ran have equivalence tests; the one that never ran keeps its reminder.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b5_fit_vine_copulas.py:38-57", system="dependence-modelling")
def test_reminder_38_57():
    pytest.fail(
        "refactor reminder: legacy unit _b5_fit_vine_copulas.py:38-57 is scoped in to dependence-modelling and has no equivalence test yet"
    )
