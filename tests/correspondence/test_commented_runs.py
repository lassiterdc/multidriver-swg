"""Tests of the commented-out run detector: its count on the real tree under the published predicate (`commented_runs.PREDICATE`), and the one-unit property."""

import pytest

from tests.correspondence import commented_runs

pytestmark = pytest.mark.state_check

# A run that starts inside f's span and ends among the comment lines above g: the two lines
# inside f are the tail of a triple-quoted string, and the closing line `# b = 2 #'''` still
# meets the predicate, because `b = 2 #'''` parses (the quotes sit in a comment).
STRADDLE = "#%%\ndef f():\n    s = '''\n# a = 1\n# b = 2 #'''\n# c = 3\n# d = 4\ndef g():\n    pass\n"


def test_the_real_tree_has_87_runs_in_418_lines_none_spanning_two_units():
    """Property: under the published predicate the retained files hold 87 runs in 418 lines, each inside one unit.

    This is the passing half of the one-unit property on the real tree; the next test shows,
    on a synthetic file, that a run spanning two units would be reported.

    Class: the real retained files. Why: the ledger's check 3 acknowledges these runs row by
    row. Killed by: a change to the per-line condition in `_is_code_comment` that moves the count
    off 87, as each single-condition removal tried does (dropping the single-identifier
    exclusion gives 93 runs; dropping the parses-as-Python condition gives 117).
    Second correct implementation: a scanner that collects runs per file with a regular
    expression for the comment prefix still returns 87 and 418.
    """
    findings, totals = commented_runs.check()
    assert not findings, "\n".join(findings)
    assert (totals["runs"], totals["lines"]) == (87, 418)


def test_a_run_spanning_two_units_is_named_with_both(tmp_path):
    """Property: a run lying in two units' extents is reported, naming the run and both units.

    This is the failing half of the one-unit property: the real-tree test above holds only if
    this detection works, since a detector that never reported a straddle would pass it too.

    Class: any run whose lines fall in more than one unit. Why: one ledger row cannot
    acknowledge a run another row's unit also holds. Killed by: deleting the straddle
    assertion. Second correct implementation: comparing each run's first and last line's
    owner instead of every line's.
    """
    findings = commented_runs.straddles("synthetic.py", STRADDLE)
    assert findings, "no finding"
    assert "synthetic.py:4-7" in findings[0]
    assert "`synthetic.py:1-5`" in findings[0] and "`synthetic.py:6-9`" in findings[0], findings[0]
