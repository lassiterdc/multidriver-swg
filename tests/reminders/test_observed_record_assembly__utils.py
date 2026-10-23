"""Refactor reminders for the units of _utils.py scoped in to observed-record-assembly.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_utils.py:922-990", system="observed-record-assembly")
def test_reminder_922_990():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:922-990 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:991-1055", system="observed-record-assembly")
def test_reminder_991_1055():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:991-1055 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:1990-2016", system="observed-record-assembly")
def test_reminder_1990_2016():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:1990-2016 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:2017-2025", system="observed-record-assembly")
def test_reminder_2017_2025():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:2017-2025 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:2026-2059", system="observed-record-assembly")
def test_reminder_2026_2059():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:2026-2059 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:2075-2153", system="observed-record-assembly")
def test_reminder_2075_2153():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:2075-2153 is scoped in to observed-record-assembly and has no equivalence test yet"
    )
