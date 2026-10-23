"""Refactor reminders for the units of _b6_simulate_from_vincop.py scoped in to observed-record-assembly.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:1-99", system="observed-record-assembly")
def test_reminder_1_99():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:1-99 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:356-376", system="observed-record-assembly")
def test_reminder_356_376():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:356-376 is scoped in to observed-record-assembly and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_b6_simulate_from_vincop.py:377-383", system="observed-record-assembly")
def test_reminder_377_383():
    pytest.fail(
        "refactor reminder: legacy unit _b6_simulate_from_vincop.py:377-383 is scoped in to observed-record-assembly and has no equivalence test yet"
    )
