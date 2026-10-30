"""Refactor reminders for the units of _b3_event_analysis.py scoped in to interactive-decision-support.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:544-621", system="interactive-decision-support")
def test_reminder_544_621():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:544-621 is scoped in to interactive-decision-support and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:658-668", system="interactive-decision-support")
def test_reminder_658_668():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:658-668 is scoped in to interactive-decision-support and has no equivalence test yet"
    )
