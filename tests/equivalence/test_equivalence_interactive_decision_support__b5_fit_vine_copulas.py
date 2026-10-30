"""Equivalence tests of the legacy units of `_b5_fit_vine_copulas.py` owned by interactive-decision-support.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (labels,
texts, counts) exact.

Two units ran, so two tests. Lines 241-252 keep the vine-copula settings whose p-values all lie between 0.05 and 0.95
and save them: the test compares the settings kept, as the cell holds them and as it saves them. Lines 253-291 draw the
goodness-of-fit figures of one short-listed setting of each event type (`[0]`, `[7]`, `[1]`; seven figures each) at
sample sizes of 50: its lift runs an adapted copy of the cell, which changes six assigned values of the cell, because the
unchanged cell (ten settings, sizes of 1000 and 500) takes about 750 s a run. The test is marked `slow` so it can be
found and triaged.

The vine library's `cdf` and `simulate` are seeded by the lift from one committed fixture, `seed`, which the
declaration lists and reads, so the test's `inputs` carry the same value as `inputs["seed"]` (no literal in a test or a
declaration, and no `seed=` to `check`, which refuses one for a licence that is not a draw law). A `cdf` call takes the
seed itself. A `simulate` call whose legacy call names no seeds takes a seed drawn from one generator built from it,
`numpy.random.default_rng(seed)`, one `integers(1, 2**31 - 1)` per call, in the order the legacy text calls; the
registered callable builds the same generator from `inputs["seed"]` and passes a seed from it to each `simulate` call in
the same order. The figures' observed lines, simulated panels and histograms therefore compare exactly. The cell passes a
fitted copula to every test, so the pandas bootstrap of `_utils.py:733` is not reached and nothing else is drawn.

The titles that state a test's result and rate are set apart by the lift (`titles_tests` of each figure) and dropped by
name, because the licensed stop markers at lines 48 and 78 of this file change the result they state; the titles of the
observed and simulated panels, the paths, labels, patches and texts are compared. Not established: that the histogram
patches and texts do not themselves depend on a result the licensed stop at line 78 changes; the lift's two-run
comparison showed them repeatable, not independent of the licence.

`RULINGS` records the register entries these units reach through the functions they call, each under the register's own
key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_b5_fit_vine_copulas.py:38-57": {48: "LICENSED"},
    "_b5_fit_vine_copulas.py:76-96": {78: "LICENSED"},
    "_utils.py:421-434": {431: "LICENSED"},
    "_utils.py:597-604": {598: "OBLIGED"},
    "_utils.py:718-746": {723: "OBLIGED", 733: "LICENSED"},
    "_utils.py:857-921": {902: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
SAVED_FIGURES = 21  # three settings (one per event type) of seven figures each
TEST_TITLES = [f"fig{number:03d}__titles_tests" for number in range(SAVED_FIGURES)]


@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:241-252", system="interactive-decision-support")
def test_the_settings_kept_and_saved_are_the_same(new_side, request):
    """Equivalence of the vine-copula settings whose p-values all lie between 0.05 and 0.95, as held and as saved."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.slow
@pytest.mark.equivalence(unit="_b5_fit_vine_copulas.py:253-291", system="interactive-decision-support")
def test_the_short_listed_settings_draw_the_same_figures(new_side, request):
    """Equivalence of the twenty-one goodness-of-fit figures of the three short-listed settings, seeded exactly; the test-result titles are dropped by name."""
    _support.check(request, new_side, _support.Rescoped(DEFAULT, TEST_TITLES))
