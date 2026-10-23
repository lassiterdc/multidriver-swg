"""Refactor reminders for the units of _utils.py scoped in to generator-validation.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_utils.py:597-604", system="generator-validation")
def test_reminder_597_604():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:597-604 is scoped in to generator-validation and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:605-660", system="generator-validation")
def test_reminder_605_660():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:605-660 is scoped in to generator-validation and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:661-717", system="generator-validation")
def test_reminder_661_717():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:661-717 is scoped in to generator-validation and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:718-746", system="generator-validation")
def test_reminder_718_746():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:718-746 is scoped in to generator-validation and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:747-856", system="generator-validation")
def test_reminder_747_856():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:747-856 is scoped in to generator-validation and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:857-921", system="generator-validation")
def test_reminder_857_921():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:857-921 is scoped in to generator-validation and has no equivalence test yet"
    )
