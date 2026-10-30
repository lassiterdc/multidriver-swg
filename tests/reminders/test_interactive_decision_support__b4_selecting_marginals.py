"""Refactor reminders for the units of _b4_selecting_marginals.py scoped in to interactive-decision-support.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b4_selecting_marginals.py:250-267", system="interactive-decision-support")
def test_reminder_250_267():
    pytest.fail(
        "refactor reminder: legacy unit _b4_selecting_marginals.py:250-267 is scoped in to interactive-decision-support and has no equivalence test yet"
    )
