"""Tests of the unit-key grammar the ledger shares with its readers, and of the ceded-file unit count.

A unit key names a legacy file and its line ranges, and every reader takes its pattern from
`ledger.KEY_RE`, so a key the pattern wrongly accepts is accepted everywhere at once. These tests
pin the two ways a key could slip through (a trailing newline, and whitespace inside the file
part) and the count the ledger prints for the ceded files, which skips the preamble and
nothing else.
"""

import pytest

from tests.correspondence import extent, ledger

pytestmark = pytest.mark.state_check


def test_a_key_followed_by_a_newline_is_not_canonical(tmp_path):
    """Property: one format finding, naming the key, for a row keyed with a trailing newline.

    Class: any otherwise valid key with a trailing newline, given to the per-row format check.
    Why: `$` matches before a final newline, so a key read from a hand-edited file with a stray
    newline would pass as canonical and never match its unit. Killed by: ending the key pattern in
    `$` again. Second correct implementation: calling `fullmatch` at every reader instead of
    anchoring the pattern.
    """
    key = "x.py:1-18\n"
    row = {"key": key, "disposition": "discard", "reason": "r", "commented_out_runs": []}
    findings = ledger._row_format_findings(row, "x.py", tmp_path)
    assert len(findings) == 1, findings
    assert f"row {key!r}: key is not canonical" in findings[0], findings
    assert not ledger._row_format_findings(dict(row, key="x.py:1-18"), "x.py", tmp_path)


def test_a_key_whose_file_part_holds_whitespace_is_not_canonical():
    """Property: a key with a newline or a space anywhere in its file part is not canonical.

    Class: keys whose file part carries whitespace at its start, in its middle, or as a space.
    Why: no legacy file name holds whitespace, so such a key names no file and must be refused
    rather than silently carried. Killed by: letting the file part be any run of non-colons
    again. Second correct implementation: checking the file part against the retained file names.
    """
    for key in ("_ut\nils.py:1-18", "\n_utils.py:1-18", "_ut ils.py:1-18"):
        assert not ledger._canonical(key), repr(key)
    assert ledger._canonical("_utils.py:1-18")


def test_the_ceded_file_count_leaves_out_only_the_preamble():
    """Property: the count skips the nameless preamble unit and keeps every definition, wherever it sits.

    Class: files with a definition above their first cell marker, and files with no cell marker.
    Why: the ledger prints this count as the units the ceded-file records stand for, so a
    definition left out of it is a unit no record accounts for. Killed by: leaving out every unit
    at cell index -1 rather than only the nameless preamble. Second correct implementation:
    counting the definitions and the non-preamble residual units separately and adding them.
    """
    marker = "#" + "%" * 2
    above = f"import os\n\n\ndef above():\n    pass\n\n\n{marker}\ndef inside():\n    pass\n\n\nx = 1\n"
    no_marker = "def first():\n    pass\n\n\ndef second():\n    pass\n"
    assert extent.unit_count_without_preamble(above) == 3
    assert extent.unit_count_without_preamble(no_marker) == 2
