"""Tests of the shared equivalence support: the test body, the stored-value helper, the re-scoped licence and the draw-law licences.

Most tests build the small git repository of `test_reference_values.py` in `tmp_path` (a one-section corpus record,
one retained file whose unit is one cell, a ledger, the generated stored extractions, one lift declaration and one
generated fixture), and call the support against it. The licence tests need no repository. Every test is a NEW
CAPABILITY: no support existed, so a failure is shown by the wrong implementation a docstring names.
"""

import hashlib
import importlib.util

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.correspondence import binding, compare, lift
from tests.correspondence.test_reference_values import _build, _declaration
from tests.equivalence import _support

pytestmark = pytest.mark.state_check

HAS_SCIPY = importlib.util.find_spec("scipy") is not None
HAS_VINE = HAS_SCIPY and importlib.util.find_spec("pyvinecopulib") is not None
needs_scipy = pytest.mark.skipif(
    not HAS_SCIPY, reason="the statistical tests need scipy, which the push job does not install"
)
needs_vine = pytest.mark.skipif(not HAS_VINE, reason="the vine-copula law needs pyvinecopulib and scipy")


class _Request:
    """The part of pytest's `request` that `check` reads: a node holding an equivalence marker."""

    def __init__(self, **kwargs):
        """Hold a marker with `kwargs` as its keyword arguments."""
        marker = pytest.mark.equivalence(**kwargs).mark
        self.node = type("Node", (), {"nodeid": "probe", "get_closest_marker": lambda self, name: marker})()


def _bound(function):
    """A `new_side` that calls `function(part, inputs)`, as the fixture's `bound` calls a registered callable."""
    return lambda part, inputs, *, reference, licence: function(part, inputs)


def test_check_passes_when_the_new_side_returns_the_lifts_value(tmp_path, monkeypatch):
    """Property: with a callable that returns the reference, `check` returns the stored-value report and raises nothing.

    Class: the one-lift tree, `Exact`, a callable returning `12` (the lift's value). Why it is required: it is the
    satisfying arm of the body. Killed by: a body that always fails. Second correct implementation: comparing the
    values with `==` and no licence passes this input.
    """
    root = _build(tmp_path, monkeypatch)
    request = _Request(unit="mod.py:1-3", system="sys-a")
    report = _support.check(request, _bound(lambda part, inputs: np.int64(12)), compare.Exact(), root=root)
    assert report == "ABSENT"


def test_check_fails_at_the_comparisons_assertion_when_the_new_side_differs(tmp_path, monkeypatch):
    """Property: a callable returning another value fails with an `AssertionError` whose last frame is `compare.py`.

    Class: the same tree with a callable returning `13`. Why it is required: the proof recorder credits a red as an
    authoring red only when the last frame is in the test's file or in the comparison helper; an assertion raised in
    the support module would be recorded as not an assertion. Killed by: a body that compares with its own `assert`.
    Second correct implementation: none other.
    """
    root = _build(tmp_path, monkeypatch)
    request = _Request(unit="mod.py:1-3", system="sys-a")
    with pytest.raises(AssertionError) as raised:
        _support.check(request, _bound(lambda part, inputs: np.int64(13)), compare.Exact(), root=root)
    assert raised.traceback[-1].path.name == "compare.py"


def test_the_callable_receives_the_part_and_the_loaded_fixtures_keyed_by_name(tmp_path, monkeypatch):
    """Property: the callable is called as `callable(part, inputs)` with `inputs` the dict of loaded fixtures.

    Class: a callable that records its arguments. Why it is required: it is the one call form every owning system's
    callable answers. Killed by: passing the fixtures positionally, or the declaration. Second correct
    implementation: none other.
    """
    root = _build(tmp_path, monkeypatch)
    seen = {}

    def callable_(part, inputs):
        """Record the arguments and return the lift's value."""
        seen.update(part=part, names=sorted(inputs), total=int(inputs["numbers"].sum()))
        return np.int64(12)

    _support.check(_Request(unit="mod.py:1-3", system="sys-a"), _bound(callable_), compare.Exact(), root=root)
    assert seen == {"part": None, "names": ["numbers"], "total": 6}


def test_a_unit_with_no_lift_or_a_lift_in_another_systems_file_is_a_lookup_error(tmp_path, monkeypatch):
    """Property: an undeclared unit and a system that does not own the lift's file are `LookupError`, not a failed comparison.

    Class: the unit `mod.py:1-3` asked as `sys-b`, and an unknown unit. Why it is required: neither is the new code
    being wrong, and the proof recorder must not credit either as an authoring red. Killed by: ignoring the system,
    or returning None for a missing lift. Second correct implementation: comparing `declaration.system` with the
    marker's system after hyphen-to-underscore passes.
    """
    root = _build(tmp_path, monkeypatch)
    ok = _bound(lambda part, inputs: np.int64(12))
    with pytest.raises(LookupError, match="owned by 'sys-b'"):
        _support.check(_Request(unit="mod.py:1-3", system="sys-b"), ok, compare.Exact(), root=root)
    with pytest.raises(LookupError, match="no lift declares"):
        _support.check(_Request(unit="mod.py:5-9", system="sys-a"), ok, compare.Exact(), root=root)


def _law():
    """A draw-law licence whose four tests always pass and whose precondition accepts any value."""

    def passes(reference, value):
        """A test that always gives p = 0.5."""
        return 0.5

    return _support.DrawLaw([passes] * 4, precondition=lambda reference, value: True)


@pytest.mark.parametrize("part", [None, "draw-law"])
def test_a_draw_law_licence_needs_a_seed_gets_it_with_the_draw_count_and_part_reaches_the_callable_unchanged(
    tmp_path, monkeypatch, part
):
    """Property: a test whose licence is a `DrawLaw` passes a derived seed, receives `draws` and `seed`, and the callable gets `part` as marked.

    Class: a draw-law licence over a lift declared with no part and over one declared `part="draw-law"`. Why it is
    required: a draw-law test that is its unit's only test carries no `part`, and keying the mode on `part` made
    that test unwritable; the callable must also see the part it was marked with. Killed by: keying draw-law mode
    on `part == "draw-law"`, or replacing `part` with a constant. Second correct implementation: keying on a seed
    argument being passed, with the licence checked against it, passes the first half.
    """
    root = _build(tmp_path, monkeypatch)
    if part:
        source = (root / lift.LIFTS / "sys_a.py").read_text()
        (root / lift.LIFTS / "sys_a.py").write_text(
            source.replace('fixtures=["numbers"]', 'part="draw-law", fixtures=["numbers"]'), encoding="utf-8"
        )
    seen = {}

    def callable_(got_part, inputs):
        """Record the part and the extra inputs."""
        seen.update(part=got_part, **{k: inputs[k] for k in ("draws", "seed")})
        return np.int64(12)

    kwargs = {"unit": "mod.py:1-3", "system": "sys-a", **({"part": part} if part else {})}
    request = _Request(**kwargs)
    with pytest.raises(ValueError, match="passes a seed"):
        _support.check(request, _bound(callable_), _law(), root=root)
    seed = _support.derived_seed("mod.py:1-3")
    _support.check(request, _bound(callable_), _law(), seed=seed, root=root)
    assert seen == {"part": part, "draws": _support.DRAWS, "seed": seed}
    with pytest.raises(ValueError, match="not derived_seed"):
        _support.check(request, _bound(callable_), _law(), seed=5, root=root)
    _support.check(request, _bound(callable_), _law(), seed=5, ruling="a developer ruling", root=root)


def test_a_value_licence_refuses_a_seed_and_a_draw_law_part_under_it_is_refused_before_the_lift_runs(
    tmp_path, monkeypatch
):
    """Property: `Exact` with a seed is refused; `part="draw-law"` under `Exact` is refused before any lift is looked up.

    Class: the one-lift tree (declaring no draw-law part) and a draw-law marker under `compare.Exact`. Why it is
    required: keyed on the licence alone, a `part="draw-law"` test under another licence runs as a value test with
    no seed, takes its authoring red from that licence's placeholder and is counted as a draw-law test that tests
    no law. The refusal must precede the lift lookup, which would otherwise raise `LookupError` here because no
    such part is declared. Killed by: no refusal (a `LookupError` or a pass), or one placed after the lift runs.
    Second correct implementation: refusing in the `new_side` fixture is not possible from here, so none other.
    """
    root = _build(tmp_path, monkeypatch)
    ok = _bound(lambda part, inputs: np.int64(12))
    with pytest.raises(ValueError, match="passes none"):
        _support.check(_Request(unit="mod.py:1-3", system="sys-a"), ok, compare.Exact(), seed=5, root=root)
    request = _Request(unit="mod.py:1-3", system="sys-a", part="draw-law")
    with pytest.raises(ValueError, match="not a DrawLaw"):
        _support.check(request, ok, compare.Exact(), root=root)


def test_store_repeatable_stores_a_repeatable_value_and_refuses_an_unseeded_lift(tmp_path, monkeypatch):
    """Property: a repeatable lift is stored and reads back `CHARACTERIZED DIVERGENCE: agrees`; an unseeded lift is refused.

    Class: the one-lift tree, then the same tree declared `unseeded`. Why it is required: it is the one sanctioned
    way a reference value is written. Killed by: storing without running twice, or storing an unseeded lift. Second
    correct implementation: calling `reference_values.store` after an explicit equality check of the two values.
    """
    root = _build(tmp_path, monkeypatch)
    descriptor = _support.store_repeatable(_declaration(root))
    assert descriptor.is_file()
    (tmp_path / "u").mkdir()
    unseeded = _build(tmp_path / "u", monkeypatch, extra=', draws="unseeded"')
    with pytest.raises(ValueError, match="never stored"):
        _support.store_repeatable(_declaration(unseeded))


def test_store_repeatable_refuses_a_lift_whose_two_runs_differ(tmp_path, monkeypatch):
    """Property: a lift that returns a different value on its second run raises `NotRepeatableError` and stores nothing.

    Class: a declaration whose reduction returns a counter that advances on each call. Why it is required: a value
    that differs between two runs of unchanged code was never a reference value. Killed by: storing the first value
    without a second run. Second correct implementation: comparing the encoded bytes of the two values passes.
    """
    root = _build(tmp_path, monkeypatch)
    path = root / lift.LIFTS / "sys_a.py"
    path.write_text(
        path.read_text()
        .replace("def declare(fixtures):", "def declare(fixtures, _seen=[]):")
        .replace(
            'return {"scale": int(fixtures["numbers"].sum())}, lambda ns: ns["y"]',
            '_seen.append(1)\n    return {"scale": 3}, lambda ns: len(_seen)',
        ),
        encoding="utf-8",
    )
    declaration = _declaration(root)
    with pytest.raises(_support.NotRepeatableError):
        _support.store_repeatable(declaration)
    assert not (root / "tests/correspondence/reference_values").exists()


def _dataset(a=(1.0, 2.0), b=(10.0, 20.0)):
    """A Dataset of two data variables."""
    return xr.Dataset({"a": ("x", list(a)), "b": ("x", list(b))})


def test_rescoped_ignores_the_dropped_component_and_compares_the_rest():
    """Property: a Dataset differing only in the dropped variable is admitted; differing in a kept one is not.

    Class: two values against one reference, and a value that lacks the dropped variable. Why it is required: it is
    the licence of a licensed departure. Killed by: dropping nothing, or dropping every variable. Second correct
    implementation: masking the dropped variable on both sides before the comparison passes.
    """
    licence = _support.Rescoped(compare.Exact(), ["b"])
    reference = _dataset()
    assert licence.admits(reference, _dataset(b=(0.0, 0.0)))
    assert licence.admits(reference, _dataset().drop_vars("b"))
    assert not licence.admits(reference, _dataset(a=(1.0, 3.0)))


def test_rescoped_works_on_a_dataframe_and_its_placeholder_is_refused_when_nothing_remains():
    """Property: a DataFrame column can be dropped, `outside` is rejected by `admits`, and an all-dropped reference is refused.

    Class: a two-column frame; a licence dropping both columns. Why it is required: a placeholder the licence admits
    would make the test pass with no new code, and a licence with nothing left to compare cannot decide. Killed by:
    an `outside` that returns the reference. Second correct implementation: none other.
    """
    reference = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
    licence = _support.Rescoped(compare.Tolerance(rtol=1e-9, atol=0.0), ["b"])
    placeholder = licence.outside(reference)
    assert not licence.admits(reference, placeholder)
    assert licence.admits(reference, reference.assign(b=[0.0, 0.0]))
    everything = _support.Rescoped(compare.Exact(), ["a", "b"])
    with pytest.raises(binding.RegistrationError):
        binding._refuse_admitted(reference, everything, everything.outside(reference))


def test_rescoped_names_a_missing_component_and_a_reference_without_components():
    """Property: a dropped name absent from the reference is `ValueError`; a reference of another kind is `TypeError`.

    Class: a misspelt name, and a bare number. Why it is required: a misspelt component would otherwise leave the
    departing output compared and the test failing for a reason it never states. Killed by: ignoring absent names.
    Second correct implementation: validating the names in the constructor against a schema is not possible here, so
    none other.
    """
    with pytest.raises(ValueError, match="'c'"):
        _support.Rescoped(compare.Exact(), ["c"]).admits(_dataset(), _dataset())
    with pytest.raises(TypeError, match="named components"):
        _support.Rescoped(compare.Exact(), ["a"]).outside(np.float64(1.0))
    with pytest.raises(ValueError):
        _support.Rescoped(compare.Exact(), [])


# ---- the draw-law licence and the seven laws ----

R = _support.DRAWS


def _rng(unit: str, which: int = 0):
    """A generator seeded from the unit's derived seed (or a confirmation seed), as the registered callables use."""
    seeds = (_support.derived_seed(unit), *_support.confirmation_seeds(unit))
    return np.random.default_rng(seeds[which])


def test_the_seed_rule_is_the_first_eight_hex_digits_of_the_sha256_of_the_unit_key():
    """Property: `derived_seed` and `confirmation_seeds` follow the stated rule and the four seeds differ.

    Class: the unit key `_utils.py:172-212`, recomputed here with `hashlib` independently. Why it is required: a
    a person recomputes a literal seed from the rule, and a rule that drifted would make seed-shopping invisible.
    Killed by: hashing the key without its suffix for the confirmation seeds, or taking the last eight digits. Second
    correct implementation: `int(digest[:8], 16)` written inline passes.
    """
    key = "_utils.py:172-212"
    assert _support.derived_seed(key) == int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)
    confirmations = _support.confirmation_seeds(key)
    assert confirmations[1] == int(hashlib.sha256((key + ":c2").encode()).hexdigest()[:8], 16)
    assert len({_support.derived_seed(key), *confirmations}) == 4


def test_a_draw_law_licence_needs_exactly_four_tests_and_names_its_four_p_values():
    """Property: three or five tests are refused, and `p_values` returns one figure per test or raises on a bad sample.

    Class: constant tests, a plain finite sample and a short one. Why it is required: "four tests" is the accepted
    count, and a routed red carries the four figures. Killed by: accepting any number of tests. Second correct
    implementation: validating in `admits` instead of the constructor passes the second half only.
    """

    def one(reference, value):
        """A test that always gives p = 0.5."""
        return 0.5

    with pytest.raises(ValueError, match="exactly 4"):
        _support.DrawLaw([one] * 3)
    with pytest.raises(ValueError, match="exactly 4"):
        _support.DrawLaw([one] * 5)
    licence = _support.DrawLaw([one] * 4, count=10)
    assert licence.p_values(None, np.arange(10.0)) == [0.5] * 4
    assert licence.admits(None, np.arange(10.0)) and not licence.admits(None, np.full(10, np.nan))
    assert not _support.DrawLaw([one, one, one, lambda r, v: float("nan")], count=10).admits(None, np.arange(10.0))
    with pytest.raises(ValueError, match="preconditions"):
        licence.p_values(None, np.arange(3.0))


def test_a_contingency_table_with_an_empty_margin_leaves_it_out_and_reports_nan_below_two_by_two():
    """Property: empty rows and columns are dropped before the independence test, and a table left under two by two gives NaN.

    Class: a 3 x 3 label pair with one empty row and column (a real test), and one whose labels are all equal.
    Why it is required: `p_values` must return a figure for the samples that produce reds. Killed by: handing the
    table to `chi2_contingency` as built, which raises. Second correct implementation: adding a pseudo-count to
    every cell would return a figure but change the statistic, so none other.
    """
    pytest.importorskip("scipy")
    first, second = np.array([0, 0, 1, 1, 0, 1]), np.array([0, 1, 0, 1, 1, 0])
    assert 0.0 < _support._contingency(first, second, 3, 3) <= 1.0
    assert np.isnan(_support._contingency(np.zeros(6, dtype=int), second, 3, 3))


def _every_placeholder_is_refused(licence, reference):
    """The placeholder of a law fails the licence and the binding's own refusal does not fire."""
    placeholder = licence.outside(reference)
    assert not licence.admits(reference, placeholder)
    binding._refuse_admitted(reference, licence, placeholder)


@needs_scipy
def test_the_subset_law_admits_a_uniform_subset_and_refuses_a_sorted_one_and_one_missing_a_row():
    """Property: uniform ordered k-subsets of a pool pass; rows returned in sorted order, or never including one pool row, are refused.

    Class: 100,000 subsets of 20 of 40 positions drawn by ranking random keys (not the legacy code), the same sorted
    within each row, and subsets of the first 39 positions only. Why it is required: S2 sees ordering and S1 sees
    an excluded row, and each is a defect the other cannot see. Killed by: an unscaled inclusion chi-square (it
    rejects the correct sample), or dropping S2. Second correct implementation: a full-permutation chi-square is not
    possible at these sizes, so none other.
    """
    unit = "_b6_simulate_from_vincop.py:489-596"
    licence, reference = _support.subset_law(), pd.Series({"pool": 40, "k": 20})
    correct = np.argsort(_rng(unit).random((R, 40)), axis=1)[:, :20]
    assert licence.admits(reference, correct), licence.p_values(reference, correct)
    assert not licence.admits(reference, np.sort(correct, axis=1))
    short = np.argsort(_rng(unit).random((R, 39)), axis=1)[:, :20]
    assert not licence.admits(reference, short)
    assert not licence.admits(reference, np.where(correct == 3, 4, correct))
    _every_placeholder_is_refused(licence, reference)


@needs_scipy
def test_the_subset_laws_scaled_inclusion_test_is_calibrated_under_the_law():
    """Property: on correct samples from a pool of 4 the inclusion test rejects at 0.05 about 5% of the time, not 12%.

    Class: 400 correct samples of 2000 two-of-four subsets. Why it is required: the scale factor P/(P-1) is what
    makes the statistic's mean equal its degrees of freedom; without it the test over-rejects by a factor that grows
    as the pool shrinks, 33% in the statistic at P = 4 (it is 2.6% at the pool of 40 the units use, which no run of
    this size can see). Killed by: an unscaled chi-square, which rejects about 12% at this size. Second correct
    implementation: `scipy.stats.chisquare` with the expected counts scaled to the same variance passes.
    """
    licence = _support.subset_law(count=2000)
    reference = pd.Series({"pool": 4, "k": 2})
    p = np.array(
        [
            licence._tests[0](reference, np.argsort(np.random.default_rng(i).random((2000, 4)), axis=1)[:, :2])
            for i in range(400)
        ]
    )
    assert 0.02 <= float((p < 0.05).mean()) <= 0.09


@needs_vine
def test_the_vine_copula_law_admits_a_sample_and_refuses_swapped_columns_and_a_flipped_margin():
    """Property: a sample of the stated vine passes; two swapped columns, or one margin flipped to 1 - u, are refused.

    Class: the three-dimensional D-vine with Clayton 2, Gumbel 1.5 and Frank 3 pair copulas, 100,000 draws from
    pyvinecopulib at a fixed seed, and the two named wrong outputs. Why it is required: the Rosenblatt coordinates
    carry both defects (C1 is blind to the flip by design). Killed by: testing the margins only. Second correct
    implementation: a pairwise Cramer-von Mises test on the Rosenblatt coordinates passes the first half.
    """
    import pyvinecopulib as pv

    pair = pv.Bicop
    copula = pv.Vinecop.from_structure(
        structure=pv.DVineStructure([1, 2, 3]),
        pair_copulas=[
            [
                pair(pv.BicopFamily.clayton, parameters=np.array([[2.0]])),
                pair(pv.BicopFamily.gumbel, parameters=np.array([[1.5]])),
            ],
            [pair(pv.BicopFamily.frank, parameters=np.array([[3.0]]))],
        ],
    )
    columns = ["a", "b", "c"]
    reference = pd.Series({"vine_json": copula.to_json(), "columns": columns})
    licence = _support.vine_copula_law()
    draws = copula.sample(R, seeds=[int(s) for s in _rng("_utils.py:421-434").integers(1, 10**6, 3)])
    correct = pd.DataFrame(draws, columns=columns)
    assert licence.admits(reference, correct), licence.p_values(reference, correct)
    swapped = correct[["b", "a", "c"]].set_axis(columns, axis=1)
    assert not licence.admits(reference, swapped)
    flipped = correct.assign(b=1 - correct["b"])
    assert not licence.admits(reference, flipped)
    assert licence._tests[0](reference, flipped) > 0.001  # the margins test is blind to the flip, as it must be
    _every_placeholder_is_refused(licence, reference)
    assert not licence.admits(reference, correct.rename(columns={"a": "z"}))


@needs_scipy
def test_the_bootstrap_law_admits_iid_picks_and_refuses_sorted_picks_and_picks_without_replacement():
    """Property: iid uniform picks pass in one and two dimensions; sorted picks and per-resample picks without replacement are refused.

    Class: 100,000 picks over 20 rows as one array and as 10,000 resamples of 10, a sorted copy, and resamples of 10
    distinct positions. Why it is required: sorting is a serial defect (W4, W2) and a `replace=` slip is the repeat
    rate's (W3). Killed by: dropping W3. Second correct implementation: a runs test in place of Spearman passes the
    sorted half.
    """
    unit = "_b6_simulate_from_vincop.py:479-488"
    licence, reference = _support.bootstrap_law(), pd.Series({"rows": 20})
    flat = _rng(unit).integers(0, 20, R)
    assert licence.admits(reference, flat), licence.p_values(reference, flat)
    resamples = _rng(unit, 1).integers(0, 20, (10_000, 10))
    assert licence.admits(reference, resamples), licence.p_values(reference, resamples)
    assert not licence.admits(reference, np.sort(flat))
    distinct = np.argsort(_rng(unit, 2).random((10_000, 20)), axis=1)[:, :10]
    assert not licence.admits(reference, distinct)
    assert licence._tests[2](reference, distinct) < 0.001
    _every_placeholder_is_refused(licence, reference)
    assert not licence.admits(reference, flat + 20)


@needs_scipy
def test_the_two_stream_law_admits_independent_streams_and_refuses_one_draw_shared_by_both():
    """Property: two independent resample streams pass; the same array used for both streams is refused by the cross test.

    Class: two (10,000, 10) arrays over 20 rows each, from different generators, and one array twice. Why it is
    required: sharing one draw between two streams is the defect B4 exists to see, and B1 to B3 cannot. Killed by:
    testing each stream alone. Second correct implementation: a mutual-information permutation test passes.
    """
    licence, reference = _support.two_stream_law(), pd.Series({"rows1": 20, "rows2": 20})
    one = _rng("_utils.py:991-1055").integers(0, 20, (10_000, 10))
    two = _rng("_utils.py:991-1055", 1).integers(0, 20, (10_000, 10))
    assert licence.admits(reference, (one, two)), licence.p_values(reference, (one, two))
    assert not licence.admits(reference, (one, one))
    assert licence._tests[3](reference, (one, one)) < 0.001
    _every_placeholder_is_refused(licence, reference)
    with pytest.raises(ValueError, match="equal row counts"):
        _support.two_stream_law().p_values(pd.Series({"rows1": 20, "rows2": 19}), (one, two % 19))


@needs_scipy
def test_the_plackett_luce_law_admits_a_weighted_permutation_and_refuses_uniform_and_reversed_weights():
    """Property: permutations from successive weighted sampling with weights 1/r pass; uniform permutations and ones weighted by r are refused.

    Class: 100,000 rows made by sorting exponential keys divided by the weights (an exact Plackett-Luce sampler,
    not pandas), then uniform permutations, then the same sampler with the weights reversed. Why it is required: the
    four tests are the exact first-slot, first-two-slot, last-slot and slot-of-rank-1 laws found by enumeration.
    Killed by: testing against the unweighted law. Second correct implementation: a likelihood-ratio test of the
    ordered pairs against the exact law passes.
    """
    unit = "_utils.py:172-212"
    weights = 1 / np.arange(1, 8)
    licence, reference = _support.plackett_luce_law(), pd.Series(weights)

    def sample(w, which=0):
        """Rows made by sorting exponential keys divided by the weights."""
        keys = _rng(unit, which).exponential(size=(R, 7)) / w
        return np.argsort(keys, axis=1) + 1

    correct = sample(weights)
    assert licence.admits(reference, correct), licence.p_values(reference, correct)
    uniform = np.argsort(_rng(unit, 1).random((R, 7)), axis=1) + 1
    assert not licence.admits(reference, uniform)
    assert not licence.admits(reference, sample(weights[::-1], 2))
    _every_placeholder_is_refused(licence, reference)
    assert not licence.admits(reference, correct - 1)
    first, pairs, last, slot = _support._plackett_luce(weights)
    assert abs(first[0] - 0.3857) < 5e-4 and abs(last[6] - 0.3161) < 5e-4 and abs(slot[0] - 0.3857) < 5e-4
    assert abs(pairs.sum() - 1) < 1e-12


def _tide_sample(which: int, shift_rule=np.rint, tied_to_shift: bool = False):
    """100,000 (shift, year) rows: a uniform shift on [-168, 168] h rounded by `shift_rule`, a year uniform over three."""
    rng = _rng("_utils.py:236-270", which)
    shift = shift_rule(rng.uniform(-168, 168, R)).astype(int)
    years = np.array([2019, 2020, 2022])
    year = years[(shift + 168) * 3 // 337] if tied_to_shift else rng.choice(years, R)
    return np.column_stack([shift, year])


@needs_scipy
def test_the_tide_law_admits_a_rounded_shift_and_uniform_year_and_refuses_floor_and_a_year_tied_to_the_shift():
    """Property: a rounded uniform shift with an independent uniform year passes; flooring the shift, or tying the year to the shift, is refused.

    Class: 100,000 rows of (shift in whole hours, year) over a 336 h window, tstep 1 h and three valid years, and the
    two named wrong samples. Why it is required: floor-for-round halves the end points (T1) and a year tied to the
    shift is what T3 sees. Killed by: a uniform shift law without the half-weight end points, which rejects the
    correct sample. Second correct implementation: a KS test on the continuous shift is not possible, the shift being
    reported on the lattice.
    """
    licence = _support.tide_law()
    reference = pd.Series({"window_hours": 336, "tstep_hours": 1, "valid_years": [2019, 2020, 2022]})
    correct = _tide_sample(0)
    assert licence.admits(reference, correct), licence.p_values(reference, correct)
    assert not licence.admits(reference, _tide_sample(1, shift_rule=np.floor))
    assert not licence.admits(reference, _tide_sample(2, tied_to_shift=True))
    _every_placeholder_is_refused(licence, reference)
    outside = correct.copy()
    outside[0, 1] = 2021
    assert not licence.admits(reference, outside)
