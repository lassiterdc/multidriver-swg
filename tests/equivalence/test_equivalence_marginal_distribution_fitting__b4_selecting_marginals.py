"""Equivalence tests of the legacy marginal-selection cells of `_b4_selecting_marginals.py` owned by marginal-distribution-fitting.

Each test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks
the owning system's registered callable for the same output, and compares the two. No callable is registered yet, so
each test fails at its comparison against a placeholder lying outside its licence. The licence is a literal in each
test: floating-point data compares at a relative tolerance of one part in a billion, with everything else (time stamps,
counts, labels, texts, flags) exact. The registered callable is called as `callable(part, inputs)`; the fixtures the
lifts read (`msel_*`) are in `inputs`.

Three units ran, so three tests, one per unit, all `draws="none"` and stored. The loading cell (lines 23-65) is compared
in full. The all-fits cell (lines 66-181) begins with an unconditional stop that the register rules `OBLIGED` (line 67):
the lift passes it, as the run behind the kept outputs did, and the registered callable is the owning system's preserved
method there. It calls `fit_dist`, whose registered departures change the AIC and the Kolmogorov-Smirnov p-value of a
fit; the test drops those two columns of each type's table of fits by name (`__ref_ams_functions.py:111-214`, lines 158,
164 and 199). The same cell records, in a table of failed fits, each fit that returned nothing or raised; its fixtures
take the fixed-location gamma and lognormal fits, whose data sit on the open bound and raise, as developer ruling U-20
requires, and no register entry is made for them. The test compares which fits failed and the kind of each failure
(returned nothing, or raised); the library's error wording is dropped by name (`failed_*__problem_text`), because the
new code is asked to classify distributions instead of reading the error (below) and need not reproduce a message. The
final-selection cell (lines 268-316) refits the twelve named fits with a plot and writes two tables per event type: the
test drops the AIC and Kolmogorov-Smirnov columns of the fit tables and each figure's super title, which states both.

`RULINGS` records the register entries the units reach, under the register's keys: line 67 (stop marker, `OBLIGED`) and
lines 158, 164 and 199 (`LICENSED`) of the fitting function.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support

RULINGS = {
    "_b4_selecting_marginals.py:66-181": {67: "OBLIGED"},
    "__ref_ams_functions.py:111-214": {158: "LICENSED", 164: "LICENSED", 199: "LICENSED"},
}

DEFAULT = compare.Tolerance(rtol=1e-9, atol=0.0)
KEYS = ("compound_events", "rain_events", "surge_events")
FIT_SCORES = [f"fits_{key}__{score}" for key in KEYS for score in ("aic", "ks_pval")]
ERROR_TEXTS = [f"failed_{key}__problem_text" for key in KEYS]
FINAL_FIT_SCORES = [
    f"fits_{key}__{score}" for key in ("surge_events", "rain_events", "compound_events") for score in ("aic", "ks_pval")
]
FIGURE_TITLES = [f"fig{number:02d}__suptitle" for number in range(12)]


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:23-65", system="marginal-distribution-fitting")
def test_the_loading_cell_leaves_the_same_event_tables_and_statistics(new_side, request):
    """Equivalence of the event tables by type, the thresholds, the statistics each type kept, the folder asked for and the file reported."""
    _support.check(request, new_side, DEFAULT)


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:66-181", system="marginal-distribution-fitting")
def test_the_all_fits_cell_writes_the_same_tables_of_fits_and_failed_fits(new_side, request):
    """Equivalence of each type's table of fits and table of failed fits; the AIC, the Kolmogorov-Smirnov p-value and the error wording are dropped by name."""
    # The developer's preference for the code that owns this test, quoted verbatim, on 2026-10-28. It changes no comparison,
    # fixture or licence of this test; the new code of marginal-distribution-fitting is where families are to be pre-classified.
    #
    # my thinking when i wrote the code was that if fixing a low bound failed on a distribution, the distribution
    # itself must not have natural support for a lower bound. using errors to make that distinction is probably a weak
    # approach, so when we get around to building the code that owns these tests, i would be interested in better ways
    # of pre-classifying distributoins as having or as not having a setttable lower bound to ensure that the
    # distribution is viable for parametric lower bounds.
    licence = _support.Rescoped(DEFAULT, FIT_SCORES + ERROR_TEXTS)
    _support.check(request, new_side, licence)


@pytest.mark.equivalence(unit="_b4_selecting_marginals.py:268-316", system="marginal-distribution-fitting")
def test_the_final_selection_cell_writes_the_same_tables_and_figures(new_side, request):
    """Equivalence of the fit and empirical-versus-fitted tables and the twelve figures; the AIC, the Kolmogorov-Smirnov p-value and the figures' super titles are dropped by name."""
    licence = _support.Rescoped(DEFAULT, FINAL_FIT_SCORES + FIGURE_TITLES)
    _support.check(request, new_side, licence)
