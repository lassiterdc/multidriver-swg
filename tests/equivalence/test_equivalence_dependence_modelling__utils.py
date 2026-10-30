"""Equivalence tests of the copula-sampling function of `_utils.py` (lines 421-434), owned by dependence-modelling.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The registered callable is called as
`callable(part, inputs)`.

The function samples a fitted vine copula and returns the draws as a table with the observations' columns. The value
test calls it four ways, in this order: twice with no seeds, with a list of seeds, and with one seed. The legacy
unseeded call draws afresh; under U-16 the lift draws each unseeded call's seed from one generator built from the
committed fixture `seed` (`numpy.random.default_rng(seed)`, one `integers(1, 2**31 - 1)` per call, in call order),
and a call that names seeds keeps them. The registered callable receives the same fixture as `inputs["seed"]`, builds
the same generator and does the same, so every table is compared exactly, under the default licence of one part in a
billion; the unit's one register entry (line 431, `LICENSED`: the new code always samples with the run's seeded
generator) is covered by that seeding, so nothing is dropped. The declaration is `seeded` and its value is stored.

The second test, `part="draw-law"`, tests the new routine's 100,000 draws against the law the function specifies: a
sample of the fitted vine copula, through its Rosenblatt transform (four tests at 0.001 each, a seed written here that
is the one `derived_seed` gives for the unit). Its lift lists no `seed` fixture, which `check` refuses beside the
`inputs["seed"]` it adds to a draw-law test; its reference is the copula's json text and the column names.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {"_utils.py:421-434": {431: "LICENSED"}}


@pytest.mark.equivalence(unit="_utils.py:421-434", system="dependence-modelling")
def test_the_copula_sampling_function_returns_the_same_seeded_tables(new_side, request):
    """Equivalence of the tables sampled by two unseeded calls, a call with a list of seeds and a call with one seed."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_utils.py:421-434", system="dependence-modelling", part="draw-law")
def test_the_copula_sampling_follows_the_fitted_vine_copula(new_side, request):
    """Draw-law test: 100,000 draws of the new sampling routine follow the fitted vine copula."""
    _support.check(request, new_side, _support.vine_copula_law(), seed=210230778)
