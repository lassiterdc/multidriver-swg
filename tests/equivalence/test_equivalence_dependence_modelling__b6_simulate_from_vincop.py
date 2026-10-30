"""Equivalence tests of the simulation cell of `_b6_simulate_from_vincop.py` (lines 489-596), owned by dependence-modelling.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test. The registered callable is called as `callable(part, inputs)`.

The cell simulates each event type from its fitted vine copula, transforms the simulated probabilities back to real
space through the fitted marginals, classifies the events against the observed thresholds, and keeps a random sample
of the valid events of each type. The lift is the adapted form (the cell's star-import of the legacy functions module
is replaced by a comment, and the three functions it supplies are bound to their stored legacy text by the declaration).

The value test compares everything that does not depend on the pandas `sample` calls (they draw from numpy's global
state, which is not seeded, so the unit declares `unseeded` and its value is not stored): the two thresholds, the
event counts, the fraction invalid and the whole over-simulation of each event type, the number, source and columns of
the events returned for each type, and the four counts checks. The vine copula's `simulate` is seeded: the legacy
call names its seeds (`seeds = random_seed`), so it keeps them (U-16), and the lift binds that name to the committed
fixture `seed`, which the test hands to the registered callable as `inputs["seed"]`; the callable passes it to each
of the three `simulate` calls. The licence is the default of one part in a billion for floating-point data and exact
for the rest, less two components, by name: the checks `chk2` and `chk3`, which count the sampled surge-only and
rain-only events and which the licensed correction of the filters at lines 564 and 567 changes (the legacy filter
`(a == True) & b == False` reads as `((a == True) & b) == False`, so it keeps every row that is not both).

The second test, `part="draw-law"`, tests the new sampling routine against the law the legacy call specifies: a
uniform sample, without replacement, of `k` of the `pool` valid compound events, in uniformly random order (four tests
at 0.001 each, 100,000 draws, a seed written here that is the one `derived_seed` gives for the unit). Its lift runs the
cell over the compound event type alone, whose filter has no defect and whose fitted marginals make every simulated
row a valid compound event, so the pool is twice the count; the reference is that pool and that count. That lift lists no `seed` fixture, which
`check` refuses beside the `inputs["seed"]` it adds to a draw-law test, and binds the legacy script's own constant for the
name the cell reads, because the pool does not depend on it.

`RULINGS` records the register entries the unit reaches: the five in its own lines and the unseeded copula sampling of
`_utils.py:421-434`, which the cell calls with seeds named, each under the register's own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_b6_simulate_from_vincop.py:489-596": {
        564: "LICENSED",
        565: "LICENSED",
        567: "LICENSED",
        568: "LICENSED",
        571: "LICENSED",
    },
    "_utils.py:421-434": {431: "LICENSED"},
}


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:489-596", system="dependence-modelling")
def test_the_simulation_cell_leaves_the_same_over_simulation_and_counts(new_side, request):
    """Equivalence of the over-simulated events of each type, the fraction invalid, the returned tables' shape and the checks; the sampled-event checks dropped by name."""
    licence = _support.Rescoped(compare.Tolerance(rtol=1e-9, atol=0.0), ["chk2", "chk3"])
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:489-596", system="dependence-modelling", part="draw-law")
def test_the_event_sampling_follows_a_uniform_subset_of_the_valid_events(new_side, request):
    """Draw-law test: 100,000 draws of the new sampling routine are uniform samples without replacement, in random order, of the valid compound events."""
    _support.check(request, new_side, _support.subset_law(), seed=2839212386)
