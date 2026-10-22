"""Refactor reminders for the units of _inputs.py scoped in to combined-event-identification.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_inputs.py:37-199", system="combined-event-identification")
def test_reminder_37_199():
    pytest.fail(
        "refactor reminder: legacy unit _inputs.py:37-199 is scoped in to combined-event-identification and has no equivalence test yet"
    )
