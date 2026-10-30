"""Equivalence test of the legacy unit of `_inputs.py` owned by combined-event-identification.

The test runs the unit's lift (the stored legacy text with declared bindings), asks the owning system's registered
callable for the same output, and compares the two. No callable is registered yet, so the test fails at its comparison
against a placeholder lying outside its licence. The licence is a literal: floating-point data compares at a relative
tolerance of one part in a billion, with everything else (integers, time spans) exact.

The unit sets the legacy steps' folder and file names, which are not ported, and the event-selection and tide settings,
which are. The test compares the ported settings, packed as the named components of one dataset. The cell reads
nothing, so it has no fixtures. The registered callable is called as `callable(part, inputs)` with an empty `inputs`
and returns the dataset. The unit has no callee and reaches no register stop marker, and no register entry lies in its
ranges, so no ruling is recorded.
"""

import pytest

from tests.correspondence import compare
from tests.equivalence import _support


@pytest.mark.equivalence(unit="_inputs.py:37-199", system="combined-event-identification")
def test_the_event_selection_and_tide_settings_are_the_same(new_side, request):
    """Equivalence of the summary durations, events per year, minimum gap, longest event, surge level and the two tide settings."""
    _support.check(request, new_side, compare.Tolerance(rtol=1e-9, atol=0.0))
