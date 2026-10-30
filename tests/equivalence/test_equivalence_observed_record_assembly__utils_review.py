"""Check that the draw-law test of `_utils.py:991-1055` certifies the law the unit's two resampling streams specify.

The unit draws twice per resample, once from each of two series (lines 1015 and 1016, `sample(..., replace=True,
random_state=None)`), so the law it specifies is two **independent** streams of uniform picks with replacement. The
statistic set the shape carries for this unit (open item O-2, closed; the four tests per draw-holding unit) is that
law's four tests, the fourth of which is the independence of the two streams, whose defect class is one draw shared by
both. A licence that certifies only one stream's picks admits a new side that draws one array and uses it for both,
because the one array is itself a correct stream.
"""

import ast
import importlib
import inspect

import numpy as np
import pytest

from tests.correspondence import fixtures, lift
from tests.equivalence import _support

pytestmark = pytest.mark.state_check

REPO_ROOT = lift.REPO_ROOT
MODULE = "tests.equivalence.test_equivalence_observed_record_assembly__utils"
UNIT = "_utils.py:991-1055"
FIRST, SECOND = "ou_two_samples_a", "ou_two_samples_b"


def _draw_law_licence():
    """The `DrawLaw` the draw-law test of `UNIT` builds, taken from the module or from its own source."""
    module = importlib.import_module(MODULE)
    found = [value for value in vars(module).values() if isinstance(value, _support.DrawLaw)]
    if found:
        return found[0]
    tree = ast.parse(inspect.getsource(module))
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            if not any(
                keyword.arg == "part" and getattr(keyword.value, "value", None) == _support.DRAW_LAW
                for keyword in decorator.keywords
            ):
                continue
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and getattr(call.func, "attr", None) == "check" and len(call.args) >= 3:
                    expression = ast.Expression(call.args[2])
                    ast.fix_missing_locations(expression)
                    value = eval(compile(expression, "<licence>", "eval"), module.__dict__)
                    if isinstance(value, _support.DrawLaw):
                        return value
    raise AssertionError(f"no DrawLaw licence found for {UNIT}'s draw-law test in {MODULE}")


def _declaration():
    """The unit's `part="draw-law"` lift declaration."""
    for declaration in lift.discover(REPO_ROOT):
        if declaration.unit == UNIT and declaration.part == _support.DRAW_LAW:
            return declaration
    raise AssertionError(f"no draw-law lift declaration for {UNIT}")


def test_the_draw_law_licence_admits_two_independent_streams_and_refuses_one_draw_shared_by_both():
    """Property: the licence the draw-law test builds admits two independent streams over the fixtures' rows, and refuses the same array used for both.

    Class: the two streams the unit draws from `ou_two_samples_a` and `ou_two_samples_b` (24 rows each), 10,000
    resamples of 10 picks each; and the shared case, one array passed as both streams. Why it is required: the unit
    specifies two independent streams, and only a licence carrying the cross-stream independence test refuses a new
    side that draws once and uses the result twice. Killed by: a single-stream licence, whose precondition refuses a
    pair of arrays entirely, so it fails the admitting arm whatever the new side does. Second correct implementation:
    any licence built from the four tests O-2 names for this unit passes both arms, and naming
    `_support.two_stream_law()` directly is the shortest form.
    """
    reference = lift.run_lift(_declaration())
    licence = _draw_law_licence()
    first = fixtures.load("observed_record_assembly", FIRST, root=REPO_ROOT)
    second = fixtures.load("observed_record_assembly", SECOND, root=REPO_ROOT)
    rows = len(first)
    assert rows == len(second), (rows, len(second))
    generator = np.random.default_rng(11)
    one = generator.integers(0, rows, (10_000, 10))
    two = generator.integers(0, rows, (10_000, 10))
    assert licence.admits(reference, (one, two)), f"licence {type(licence).__name__} refuses two independent streams"
    assert not licence.admits(reference, (one, one))
