"""Equivalence test of the legacy unit of `__ref_ams_functions.py` owned by interactive-decision-support.

The test runs the unit's lift (the stored legacy text with declared bindings, on committed synthetic fixtures), asks the
owning system's registered callable for the same output, and compares the two. No callable is registered yet, so the
test fails at its comparison against a placeholder lying outside its licence. The licence is a literal: floating-point
data compares at a relative tolerance of one part in a billion, with everything else (labels, texts, counts) exact.

The unit draws the four-panel figure of one fitted distribution, so its test compares the data three calls draw (the
histograms' bars, the density, fitted and probability-plot lines, the axis labels and limits, the legend, and the
figure title with the fit's scores, support and transformations, including the back-transformed bounds) and the
paths and keyword arguments of the two saves. The registered callable is called as `callable(part, inputs)` and
returns the dataset. The unit calls `backtransform_data`, whose own unit belongs to marginal-distribution-fitting; the
lift runs it as its stored legacy text. The unit holds no register entry, reaches none through its callees, and
draws nothing, so no ruling is recorded.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(unit="__ref_ams_functions.py:215-324", system="interactive-decision-support")
def test_the_fitted_distribution_figure_draws_the_same_data(new_side, request):
    """Equivalence of the data the fitted-distribution figure draws for a plain, a fully transformed and a shifted fit."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
