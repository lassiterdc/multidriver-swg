"""Refactor reminders for the units of _utils.py scoped in to timeseries-synthesis.

Each test fails until an equivalence test carrying the same unit key replaces it.
"""

import pytest


@pytest.mark.refactor_reminder(unit="_utils.py:29-95", system="timeseries-synthesis")
def test_reminder_29_95():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:29-95 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:96-103", system="timeseries-synthesis")
def test_reminder_96_103():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:96-103 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:104-171", system="timeseries-synthesis")
def test_reminder_104_171():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:104-171 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:172-212", system="timeseries-synthesis")
def test_reminder_172_212():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:172-212 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:213-227", system="timeseries-synthesis")
def test_reminder_213_227():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:213-227 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:236-270", system="timeseries-synthesis")
def test_reminder_236_270():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:236-270 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:271-334", system="timeseries-synthesis")
def test_reminder_271_334():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:271-334 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )


@pytest.mark.refactor_reminder(unit="_utils.py:335-398", system="timeseries-synthesis")
def test_reminder_335_398():
    pytest.fail(
        "refactor reminder: legacy unit _utils.py:335-398 is scoped in to timeseries-synthesis and has no equivalence test yet"
    )
