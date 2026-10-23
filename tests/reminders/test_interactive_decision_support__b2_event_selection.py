"""Refactor reminders for the units of _b2_event_selection.py scoped in to interactive-decision-support.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:109-153", system="interactive-decision-support")
def test_reminder_109_153():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:109-153 is scoped in to interactive-decision-support and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:154-268", system="interactive-decision-support")
def test_reminder_154_268():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:154-268 is scoped in to interactive-decision-support and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:325-347", system="interactive-decision-support")
def test_reminder_325_347():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:325-347 is scoped in to interactive-decision-support and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:412-552", system="interactive-decision-support")
def test_reminder_412_552():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:412-552 is scoped in to interactive-decision-support and has no equivalence test yet"
    )
