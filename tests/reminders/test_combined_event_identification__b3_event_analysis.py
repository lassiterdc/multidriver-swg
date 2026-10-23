"""Refactor reminders for the units of _b3_event_analysis.py scoped in to combined-event-identification.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:24-41", system="combined-event-identification")
def test_reminder_24_41():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:24-41 is scoped in to combined-event-identification and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:62-97", system="combined-event-identification")
def test_reminder_62_97():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:62-97 is scoped in to combined-event-identification and has no equivalence test yet"
    )
