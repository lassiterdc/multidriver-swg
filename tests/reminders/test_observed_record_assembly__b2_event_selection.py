"""Refactor reminders for the units of _b2_event_selection.py scoped in to observed-record-assembly.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:25-87", system="observed-record-assembly")
def test_reminder_25_87():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:25-87 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:88-94", system="observed-record-assembly")
def test_reminder_88_94():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:88-94 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:348-385", system="observed-record-assembly")
def test_reminder_348_385():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:348-385 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:386-392,406-411", system="observed-record-assembly")
def test_reminder_386_392_406_411():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:386-392,406-411 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b2_event_selection.py:393-405", system="observed-record-assembly")
def test_reminder_393_405():
    pytest.fail(
        "refactor reminder: legacy unit _b2_event_selection.py:393-405 is scoped in to observed-record-assembly and has no equivalence test yet"
    )
