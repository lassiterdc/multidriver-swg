"""Equivalence tests of the legacy units of `_b6_simulate_from_vincop.py` owned by event-occurrence-modelling.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks the
owning system's registered callable for the same output, and compares the two. No callable is registered yet, so each
test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each test:
floating-point data compares at a relative tolerance of one part in a billion, with everything else (dates, counts,
labels, texts) exact.

Each unit yields several tables, rates or charts, so each test compares them packed as the named components of one
dataset (a chart is compared as the data it draws). The registered callable is called as `callable(part, inputs)`.
The unit `479-488` holds one random-draw routine, a seeded bootstrap of rows with replacement: its value test compares
the seeded draw exactly, and a second test, `part="draw-law"`, tests the new routine's draws against the law the legacy
call specifies (uniform picks over the rows, four tests at 0.001 each, 100,000 draws, a seed written here that is the
one `derived_seed` gives for the unit). The unit `627-745` reaches the legacy two-sample test function, whose register
site is recorded in `RULINGS`. No unit of this file reaches a register stop marker.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {"_utils.py:857-921": {902: "OBLIGED"}}


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:100-216", system="event-occurrence-modelling")
def test_the_occurrence_count_function_returns_the_same_yearly_tables(new_side, request):
    """Equivalence of the yearly event-count tables the function returns for a calendar-year and a mid-year start."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:217-233", system="event-occurrence-modelling")
def test_the_observed_count_cell_leaves_the_same_tables(new_side, request):
    """Equivalence of the yearly counts, the counts kept for fitting, the occurrence table and the valid dates the cell leaves."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:384-392", system="event-occurrence-modelling")
def test_the_poisson_rate_function_returns_the_same_rates(new_side, request):
    """Equivalence of the Poisson rate the function estimates for each event type."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:399-455", system="event-occurrence-modelling")
def test_the_poisson_rates_cell_leaves_the_same_rates(new_side, request):
    """Equivalence of the three Poisson rates the cell leaves."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:456-478", system="event-occurrence-modelling")
def test_the_monthly_event_chart_draws_the_same_data(new_side, request):
    """Equivalence of the data the monthly event-count chart draws."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:479-488", system="event-occurrence-modelling")
def test_the_bootstrap_cell_gives_the_same_sampled_counts_and_chart(new_side, request):
    """Equivalence of the seeded sampled counts, the counts written and the chart the callee draws."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(unit="_b6_simulate_from_vincop.py:627-745", system="event-occurrence-modelling")
def test_the_event_distribution_cell_leaves_the_same_tables_and_charts(new_side, request):
    """Equivalence of the simulated tables, the event counter and the comparison charts the cell leaves."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))


@pytest.mark.equivalence(
    unit="_b6_simulate_from_vincop.py:479-488", system="event-occurrence-modelling", part="draw-law"
)
def test_the_bootstrap_draws_follow_uniform_picks_over_the_rows(new_side, request):
    """Draw-law test: 100,000 draws of the new bootstrap routine are uniform picks with replacement over the observed rows."""
    _support.check(request, new_side, _support.bootstrap_law(), seed=3012265033)
