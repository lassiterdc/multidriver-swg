"""Tests of the commented-out run detector: its count on the real tree under the published predicate, and the one-unit property."""

import pytest

from tests.correspondence import commented_runs

pytestmark = pytest.mark.state_check

# A run that starts inside f's span and ends among the comment lines above g: the two lines
# inside f are the tail of a triple-quoted string, and the closing line `# b = 2 #'''` still
# meets the predicate, because `b = 2 #'''` parses (the quotes sit in a comment).
STRADDLE = "#%%\ndef f():\n    s = '''\n# a = 1\n# b = 2 #'''\n# c = 3\n# d = 4\ndef g():\n    pass\n"


def test_the_real_tree_has_87_runs_in_418_lines_none_spanning_two_units():
    """Property: under the published predicate the retained files hold 87 runs in 418 lines, each inside one unit (satisfying arm for row 6).

    Class: the real retained files. Why: the ledger's check 3 acknowledges these runs row by
    row. Killed by: any change of the per-line condition (another predicate counts 94 or 97).
    Second correct implementation: a scanner that collects runs per file with a regular
    expression for the comment prefix still returns 87 and 418.
    """
    findings, totals = commented_runs.check()
    assert not findings, "\n".join(findings)
    assert (totals["runs"], totals["lines"]) == (87, 418)


def test_a_run_spanning_two_units_is_named_with_both(tmp_path):
    """Property: a run lying in two units' extents is reported, naming the run and both units (arm row 6).

    Class: any run whose lines fall in more than one unit. Why: one ledger row cannot
    acknowledge a run another row's unit also holds. Killed by: deleting the straddle
    assertion. Second correct implementation: comparing each run's first and last line's
    owner instead of every line's.
    """
    findings = commented_runs.straddles("synthetic.py", STRADDLE)
    assert findings, "no finding"
    assert "synthetic.py:4-7" in findings[0]
    assert "`synthetic.py:1-5`" in findings[0] and "`synthetic.py:6-9`" in findings[0], findings[0]
