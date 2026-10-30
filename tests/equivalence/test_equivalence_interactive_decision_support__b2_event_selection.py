"""Equivalence tests of the legacy units of `_b2_event_selection.py` owned by interactive-decision-support.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures),
asks the owning system's registered callable for the same output, and compares the two. No callable is registered
yet, so each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal
in each test: floating-point data compares at a relative tolerance of one part in a billion, with everything else
(labels, texts, counts) exact.

All four units that ran are tested here. The fourth (lines 412-552) is an ADAPTED lift: its two calls of the
event-statistics function (lines 496 and 498) do not name the event-id and time levels of the surge event table the
selection function returns, and the function as committed removes every level it is not told, so the stored text
raises on any input. The adapted copy adds `idx_name_time="date_time", idx_event_id="surge_event_id"` to those two
calls, as the next cell's call has it, and nothing else; its diff is stored beside it. The reference is the cell
computed as those calls computed it when they were written.

- lines 109-153 sweep five rain thresholds over three rainfall sources and collect, for each, the event summaries,
  the event time series, the threshold and the return rate. The selection function stops at its first statement with a
  `sys.exit` the register rules a stop marker (line 1274, `OBLIGED`) and the lift's recorder passes it, so the
  reference is the run that went on, which the preserved method is asked to reproduce.
- lines 154-268 compare the sources at each threshold: rolling event statistics are added to the summaries, events
  are counted in every 365-day window, and the sources are compared by two-sample tests with three bootstrap
  resamples. The bootstrap draws through pandas `sample` with no seed, so the lift declares `unseeded`, and the
  figures of the bootstrap p-values (their panel titles, bars and texts) are components named with `draw`, which the
  licence drops by name; the panel names, the tables, the window counts and the histograms are compared.
- lines 412-552 compare an hourly with a finer water level record over five surge thresholds: surge events are
  selected in each, rolling statistics joined, events counted in every 365-day window, and the records compared by
  two-sample test figures without bootstrap (every call passes `do_bootstrapping=False`), so nothing is drawn at
  random and the whole value is compared. The reference runs three register sites, each `OBLIGED`: the surge
  selection's line 1477, the statistics function's stop marker at 1563 (passed by the lift's recorder) and the
  two-sample battery's line 902.
- lines 325-347 compare radar with gridded rainfall where both have events and draw two scatter plots; the test
  compares the hourly table, the event pairs and the points, labels and limits of both plots.

`RULINGS` records the register entries these units reach through the functions they call, each under the register's
own key.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_utils.py:857-921": {902: "OBLIGED"},
    "_utils.py:991-1055": {1015: "LICENSED", 1016: "LICENSED"},
    "_utils.py:1269-1422": {1274: "OBLIGED", 1390: "OBLIGED"},
    "_utils.py:1423-1539": {1477: "OBLIGED"},
    "_utils.py:1540-1577": {1563: "OBLIGED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
# six `compare_2_distributions` calls per threshold save a bootstrap p-value figure and then a histogram, so the even-numbered
# figures of the sixty saved are the bootstrap ones, each with seven test panels
PANEL_COMPONENTS = ("title", "labels", "lims", "patch_meta", "patch_data", "lines", "scatter", "texts", "legend")
THRESHOLD_DRAW_COMPONENTS = [
    f"fig{number:02d}__draw__{name}"
    for number in range(0, 60, 2)
    for name in (
        "suptitle",
        "size",
        "n_axes",
        *(f"ax{panel}__{component}" for panel in range(7) for component in PANEL_COMPONENTS),
    )
]


@pytest.mark.equivalence(unit="_b2_event_selection.py:109-153", system="interactive-decision-support")
def test_the_rain_threshold_sweep_collects_the_same_events(new_side, request):
    """Equivalence of the event summaries, event series, thresholds and return rates the sweep collects for three sources at five thresholds."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b2_event_selection.py:154-268", system="interactive-decision-support")
def test_the_rain_threshold_comparison_builds_the_same_tables_and_figures(new_side, request):
    """Equivalence of the joined summaries, the window counts and the figures saved at every threshold; the bootstrap p-value figures' draw-dependent panels are dropped by name."""
    licence = _support.Rescoped(DEFAULT, THRESHOLD_DRAW_COMPONENTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b2_event_selection.py:325-347", system="interactive-decision-support")
def test_the_radar_against_gridded_comparison_draws_the_same_points(new_side, request):
    """Equivalence of the hourly comparison table, the event pairs and the two scatter plots' points, labels and limits."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b2_event_selection.py:412-552", system="interactive-decision-support")
def test_the_surge_threshold_experiment_builds_the_same_tables_and_figures(new_side, request):
    """Equivalence of the joined surge event summaries, the window counts, the yearly series and every saved comparison figure, for two water level records at five thresholds."""
    _support.check(request, new_side, DEFAULT)
