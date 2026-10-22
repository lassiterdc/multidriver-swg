"""Refactor reminders for the units of __ref_ams_functions.py scoped in to interactive-decision-support.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="__ref_ams_functions.py:215-324", system="interactive-decision-support")
def test_reminder_215_324():
    pytest.fail(
        "refactor reminder: legacy unit __ref_ams_functions.py:215-324 is scoped in to interactive-decision-support and has no equivalence test yet"
    )
