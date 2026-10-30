"""Equivalence tests of the simulation script's preamble and its rejected-period cells (`_b6_simulate_from_vincop.py`) owned by observed-record-assembly.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (time stamps,
counts, labels, texts) exact. The registered callable is called as `callable(part, inputs)`; the fixtures the lifts read
(`orb_*`, `ora_*`) are in `inputs`.

Three units ran, so three tests, one per unit, all `draws="none"` and stored. The preamble (lines 1-99) takes ADR-6's
adapted form, because its two star-imports of the legacy inputs and utilities modules cannot run in the runner; the
eleven names they supplied are bound by the declaration, and the cell's file reads are fed from a scratch directory the
declaration writes the fixture tables and the stored copula text into. It builds the legacy generator
(`np.random.RandomState(234)`) and draws nothing from it. The two rejected-period cells (356-376 and 377-383) chart the
valid dates of the periods with many missing days: the first compares the dates with `datetime.date` values, which this
environment's pandas refuses, so its lift binds the index to a stand-in that reads a `date` as midnight and delegates the
comparison; both are compared as the data their figures draw.

No unit of this file reads `lst_sim_to_skip` (the script's skip-to-cache shortcut at line 512, in another package's
unit), and none of their texts holds a register stop marker or reaches a callee.

`RULINGS` is empty: the three units reach no register site.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS: dict[str, dict[int, str]] = {}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:1-99", system="observed-record-assembly")
def test_the_preamble_loads_the_same_tables_series_dates_and_copulas(new_side, request):
    """Equivalence of the event tables, the rainfall and water-level series, the date series and indexes, the resampled dataset and the three copulas."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:356-376", system="observed-record-assembly")
def test_the_rejected_periods_cell_collects_the_same_series_and_figures(new_side, request):
    """Equivalence of the per-period valid-date series and the figures the cell saves for the years with many missing days."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:377-383", system="observed-record-assembly")
def test_the_rejected_periods_chart_draws_the_same_data(new_side, request):
    """Equivalence of the path, save keywords, title, bar heights and tick labels of the combined rejected-periods figure."""
    _support.check(request, new_side, DEFAULT)
