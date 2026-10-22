"""Refactor reminders for the units of _b2_event_selection.py scoped in to combined-event-identification.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:273-324", system="combined-event-identification")
def test_reminder_273_324():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:273-324 is scoped in to combined-event-identification and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:553-568", system="combined-event-identification")
def test_reminder_553_568():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:553-568 is scoped in to combined-event-identification and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:569-587", system="combined-event-identification")
def test_reminder_569_587():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:569-587 is scoped in to combined-event-identification and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:588-617", system="combined-event-identification")
def test_reminder_588_617():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:588-617 is scoped in to combined-event-identification and has no equivalence test yet"
    )
