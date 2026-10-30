"""Equivalence tests of the legacy units of `_b3_event_analysis.py` owned by interactive-decision-support.

Each test runs the unit's lift (the stored legacy text with declared bindings, on a committed synthetic fixture), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (labels,
texts, counts) exact.

Five units ran, so five tests; the unit of lines 544-621 and the unit of lines 658-668 never ran and keep their
reminders. None draws at random and no register stop marker lies in the texts they run.

- lines 297-331 split the combined event summaries by type and list each type's quantile correlations: the test
  compares the three subsets, the three correlation tables and the rain statistic the later cells analyse. The
  correlation function's register entry (line 2200, `OBLIGED`, differs from the manuscript) is inside its text.
- lines 332-394, 395-473 and 474-543 pick the strongest rain-surge relationships by name for the surge-only, rain-only
  and compound events: each test compares the selected table sorted by absolute correlation and the last listing.
  Each cell reads the correlation tables the first cell builds, so its lift rebuilds them by running that cell's
  stored text.
- lines 622-657 plot the key relationships of each event type and write the selected tables: the test compares every
  saved figure (path, keywords, captions, titles, labels, limits, scatter points) and the three csv files the cell
  writes, which the lift reads back from a folder of the run's own.

`RULINGS` records the register entry these units reach through the functions they call, under the register's own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:2198-2283": {2200: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:297-331", system="interactive-decision-support")
def test_the_quantile_correlations_of_each_event_type_are_the_same(new_side, request):
    """Equivalence of the three event subsets, their quantile correlation tables and the rain statistic analysed later."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:332-394", system="interactive-decision-support")
def test_the_surge_event_relationships_selected_are_the_same(new_side, request):
    """Equivalence of the selected rain-surge relationships of the surge-only events, sorted by absolute correlation."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:395-473", system="interactive-decision-support")
def test_the_rain_event_relationships_selected_are_the_same(new_side, request):
    """Equivalence of the selected rain-surge relationships of the rain-only events, sorted by absolute correlation."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:474-543", system="interactive-decision-support")
def test_the_compound_event_relationships_selected_are_the_same(new_side, request):
    """Equivalence of the selected rain-surge relationships of the compound events, sorted by absolute correlation."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b3_event_analysis.py:622-657", system="interactive-decision-support")
def test_the_relationship_plots_and_tables_written_are_the_same(new_side, request):
    """Equivalence of every saved relationship figure and of the three selected-relationship tables the cell writes."""
    _support.check(request, new_side, DEFAULT)
