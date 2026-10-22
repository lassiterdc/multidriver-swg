"""Refactor reminders for the units of _b3_event_analysis.py scoped in to observed-record-assembly.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:42-51", system="observed-record-assembly")
def test_reminder_42_51():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:42-51 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:52-61", system="observed-record-assembly")
def test_reminder_52_61():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:52-61 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:135-220", system="observed-record-assembly")
def test_reminder_135_220():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:135-220 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b3_event_analysis.py:228-261,270-296", system="observed-record-assembly")
def test_reminder_228_261_270_296():
    pytest.fail(
        "refactor reminder: legacy unit _b3_event_analysis.py:228-261,270-296 is scoped in to observed-record-assembly and has no equivalence test yet"
    )
