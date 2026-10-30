"""The body every equivalence test shares, the two licences that `compare.py` does not provide, and the stored-value helper.

An equivalence test of a legacy unit does the same five things whichever unit it covers: find the unit's lift,
run it, report how the stored value stands, ask the owning system's registered callable for the new side's value,
and compare the two under the test's licence. Written out in every test the five steps would drift apart,
and one of them (which frame raises the failed assertion) decides whether the proof recorder counts a red as an
authoring red at all. So they live here once, and a test is its decorator, its licence and one call to `check`.

`check` reads the unit, the part and the owning system from the test's own family marker rather than taking them
again as arguments, so the decorator the inventory and the collection-time refusals read cannot disagree with
what the test runs. The comparison itself is `compare.assert_equivalent`, called from `check` and not copied: the
proof recorder credits a failure as an assertion when the last frame of its traceback is in the test's file or in
`compare.py`, and a comparison written in this file would put that frame somewhere neither.

Two licences are defined here because the comparison helper stops at `Exact` and `Tolerance`:

- `Rescoped` wraps one of them and drops, by name, the components of a Dataset or DataFrame that a registered
  defect's licensed departure changes. The reference stays the lift's value; only the compared part narrows.
- `DrawLaw` admits draws when exact preconditions hold and exactly four statistical tests each pass at the false-alarm
  rate. Six factories build the seven laws the legacy code draws from (`subset_law`, `vine_copula_law`,
  `bootstrap_law`, `two_stream_law`, `plackett_luce_law`, `tide_law`), each with its four named tests, so a system's
  draw-law test names its licence and edits nothing here. The reference of such a test is the law's parameters, a
  pandas Series the lift's reduction returns; the value is whatever the registered callable returns, in the layout
  each factory documents. `derived_seed` and `confirmation_seeds` give the seed rule: an integer literal derived from
  the unit key before any run, never changed after a red without a ruling.

`store_repeatable` is the one way a reference value is stored. A value is written only after the lift has been run
twice in the process and the two values have the same codec digest, because a value that differs between two runs
of unchanged code was never a reference value.

Not established: that the fixtures a draw-law test uses (a pool of 40, three columns, twenty rows) expose a defect that
appears only at the legacy run's scale, or that `DRAWS` and `FALSE_ALARM` are the right size and rate; they are the
accepted comparison's numbers and this module only carries them. The tests certify the law and not the stream.
"""

from __future__ import annotations

import hashlib
import itertools
import math
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from tests.correspondence import compare, fixtures, lift, reference_values, value_codec

REPO_ROOT = lift.REPO_ROOT
DRAW_LAW = "draw-law"  # the `part=` value of a draw-law test
DRAWS = 100_000  # how many draws a draw-law test asks the registered callable for
FALSE_ALARM = 0.001  # the false-alarm rate of each statistical test of a draw-law licence
EXTRA_INPUTS = ("draws", "seed")  # the two keys a draw-law test adds to the dict the callable receives


class NotRepeatableError(Exception):
    """A lift whose two runs in one process gave values with different codec digests, so no value can be stored."""


# ---- the body of every equivalence test ----


def find_declaration(unit: str, part: str | None = None, root: Path = REPO_ROOT) -> lift.LiftDeclaration:
    """The lift declaration of (`unit`, `part`) under `root`, or `LookupError` naming the declarations that do exist.

    `lift.discover` already refuses two declarations of one key, so zero is the only other count.
    """
    declarations = lift.discover(root)
    for declaration in declarations:
        if declaration.unit == unit and declaration.part == part:
            return declaration
    declared = sorted((d.unit, d.part) for d in declarations if d.unit == unit)
    raise LookupError(f"no lift declares unit {unit!r} with part {part!r}; this unit's declared parts: {declared}")


def load_inputs(declaration: lift.LiftDeclaration) -> dict[str, Any]:
    """The declaration's fixtures, loaded afresh and keyed by fixture name, as the lift's declaration receives them.

    They are loaded again for the new side instead of being shared with the lift run, so that a registered
    callable which edits what it is given cannot change the inputs of the legacy side, or of its own next call.
    """
    return {name: fixtures.load(declaration.system, name, root=declaration.root) for name in declaration.fixtures}


def check(
    request, new_side, licence, *, seed: int | None = None, ruling: str | None = None, root: Path = REPO_ROOT
) -> str:
    """Run one equivalence test body and return the stored-value report; fail at `compare.assert_equivalent` on a mismatch.

    `request` is pytest's `request` fixture, from which the unit, the part and the owning system are read off the
    test's own `equivalence` marker. The steps run in this order: the lift is found and run; the stored value is
    reported (a report, never a verdict); `new_side(part, inputs, reference=..., licence=...)` is called with
    `inputs` the dict of the lift's loaded fixtures; and the value is compared with the reference under `licence`.

    A draw-law test is a test whose licence is a `DrawLaw`, whatever its `part`: it must pass `seed`, an integer
    literal written in the test, and its `inputs` also carry `"draws"` and `"seed"`, so that the registered callable
    draws from a generator built from that seed and the verdict is fixed for a given commit. `part` reaches the
    registered callable unchanged, so a draw-law test that is its unit's only test carries none. A test whose `part`
    is `"draw-law"` under any other licence is refused before the lift runs: it would run as a value test with no
    seed and be counted as a draw-law test that tests no law. The seed must equal `derived_seed(unit)`; any other seed is refused
    unless `ruling` names the developer's ruling that allowed it, because a seed changed after a red voids the test.
    Any other test must not pass a seed.

    Raises `LookupError` when the marker is missing, no lift declares the unit and part, or the lift is filed
    under another owning system than the test names; none of those is a failed comparison, and none is credited
    as an assertion by the proof recorder.
    """
    marker = request.node.get_closest_marker("equivalence")
    if marker is None or "unit" not in marker.kwargs or "system" not in marker.kwargs:
        raise LookupError(f"{request.node.nodeid}: check() needs an equivalence marker naming unit and system")
    unit, part, system = marker.kwargs["unit"], marker.kwargs.get("part"), marker.kwargs["system"]
    law = isinstance(licence, DrawLaw)
    if part == DRAW_LAW and not law:
        raise ValueError(
            f"{unit}: part={DRAW_LAW!r} but the licence is {type(licence).__name__}, not a DrawLaw; such a test would "
            "run as a value test with no seed and be counted as a draw-law test that tests no law"
        )
    declaration = find_declaration(unit, part, root)
    if declaration.system != system.replace("-", "_"):
        raise LookupError(
            f"{unit} part {part!r} is owned by {system!r} but its lift is declared in tests/lifts/{declaration.system}.py"
        )
    if law != (seed is not None):
        raise ValueError(
            f"{unit}: a draw-law licence passes a seed and every other licence passes none (part={part!r})"
        )
    if law and seed != derived_seed(unit) and not ruling:
        raise ValueError(
            f"{unit}: seed {seed} is not derived_seed({unit!r}) = {derived_seed(unit)} and no ruling is named"
        )
    reference = lift.run_lift(declaration)
    report = reference_values.compare_stored(declaration, reference)
    inputs = load_inputs(declaration)
    if law:
        clash = [name for name in EXTRA_INPUTS if name in inputs]
        if clash:
            raise LookupError(f"{unit}: the lift's fixtures {clash} collide with the keys a draw-law test adds")
        inputs = {**inputs, "draws": DRAWS, "seed": seed}
    value = new_side(part, inputs, reference=reference, licence=licence)
    compare.assert_equivalent(value, reference, licence)
    return report


# ---- the stored value ----


def store_repeatable(declaration: lift.LiftDeclaration) -> Path:
    """Store the declared lift's value after two runs in this process agree on its codec digest; return the descriptor.

    Refuses a lift declared `unseeded`, whose value is never stored, and raises `NotRepeatableError` when the two
    digests differ: a lift whose unchanged text gives a different value on a second run contradicts its own
    declared draws, and storing either value would record a number nobody can reproduce.
    """
    if declaration.draws == "unseeded":
        raise ValueError(f"{declaration.unit}: a lift declared unseeded is never stored")
    first, second = lift.run_lift(declaration), lift.run_lift(declaration)
    digests = [value_codec.label(value_codec.encode(value)) for value in (first, second)]
    if digests[0] != digests[1]:
        raise NotRepeatableError(
            f"{declaration.unit} part {declaration.part!r}: two runs of the lift gave {digests[0]} and {digests[1]}"
        )
    return reference_values.store(declaration, first)


# ---- the re-scoped licence ----


class Rescoped:
    """A licence that compares everything of a Dataset or DataFrame reference except the components it names.

    Construction takes the licence to apply to the rest, `compare.Exact()` or `compare.Tolerance(...)`, and the names
    of the data variables (Dataset) or columns (DataFrame) to leave out. The names must exist in the reference: a
    name that does not is `ValueError`, because a misspelt component would otherwise leave the departing output
    compared and the test failing for a reason the test never states. The value may carry the dropped components or
    omit them; either way they are not compared. Any other label of the value, a coordinate or an index, is still
    compared by the licence underneath.

    When every component of the reference is dropped nothing is left to compare, `outside` hands back the empty
    remainder, and the binding refuses the placeholder: such a test cannot pass or fail and has to be ruled on.
    """

    def __init__(self, base, drop: Sequence[str]):
        """Hold the licence for the compared remainder and the non-empty, duplicate-free names to drop."""
        names = tuple(drop)
        if not names or len(set(names)) != len(names):
            raise ValueError(f"a re-scoped licence drops one or more distinct components, not {names!r}")
        self._base, self._drop = base, names

    def _without(self, obj: Any, *, reference: bool) -> Any:
        """`obj` less the dropped components; a reference must hold every one of them and have named components."""
        if isinstance(obj, xr.Dataset):
            present = [name for name in self._drop if name in obj.data_vars]
            held = obj.data_vars
        elif isinstance(obj, pd.DataFrame):
            present = [name for name in self._drop if name in obj.columns]
            held = obj.columns
        elif reference:
            raise TypeError(
                f"a re-scoped licence needs a reference with named components (an xarray Dataset or a pandas "
                f"DataFrame), not {type(obj).__name__}"
            )
        else:
            return obj  # a value of another kind: the licence underneath rejects it by kind
        if reference and len(present) != len(self._drop):
            raise ValueError(
                f"components {sorted(set(self._drop) - set(present))} are not in the reference {list(held)}"
            )
        if isinstance(obj, xr.Dataset):
            return obj.drop_vars(present)
        return obj.drop(columns=present)

    def admits(self, reference: Any, value: Any) -> bool:
        """Whether the licence underneath admits `value` against `reference`, both less the dropped components."""
        return self._base.admits(self._without(reference, reference=True), self._without(value, reference=False))

    def outside(self, reference: Any) -> Any:
        """A stand-in for the reference's remaining components that the licence underneath turns down."""
        return self._base.outside(self._without(reference, reference=True))


# ---- the draw-law licence and the seven laws ----

TESTS_PER_LAW = 4  # every draw-law licence carries exactly this many statistical tests
TIDE_SHIFT_BINS = 12  # the shift bins of the tide law's independence tests


def derived_seed(unit: str, suffix: str = "") -> int:
    """The seed a draw-law test writes as a literal: the first 8 hex digits of the SHA-256 of the unit key, as an integer.

    `suffix` is `":c1"`, `":c2"` or `":c3"` for the three confirmation seeds. The seed is fixed by a rule before any
    run is made, so it cannot have been chosen to pass, and anyone can recompute it; a seed changed after a
    red would no longer match this derivation.
    """
    return int(hashlib.sha256((unit + suffix).encode()).hexdigest()[:8], 16)


def confirmation_seeds(unit: str) -> tuple[int, int, int]:
    """The three confirmation seeds of a unit, derived by the same rule; evidence for a routed red, never part of a verdict."""
    return tuple(derived_seed(unit, f":c{n}") for n in (1, 2, 3))


def _stats():
    """scipy.stats, imported when a statistical test first runs, because the push job installs no scipy."""
    from scipy import stats

    return stats


def _bonferroni(p_values) -> float:
    """The p-value of a test made of several components: `min(1, m * min(p_j))`, which keeps the whole test at its level."""
    return float(min(1.0, len(p_values) * min(p_values)))


def _chisquare(counts, probabilities) -> float:
    """p-value of a chi-square test of observed `counts` against `probabilities` (which sum to 1), df = cells - 1."""
    counts = np.asarray(counts, dtype=float)
    return float(_stats().chisquare(counts, f_exp=np.asarray(probabilities, dtype=float) * counts.sum()).pvalue)


def _contingency(first, second, rows: int, columns: int) -> float:
    """p-value of the independence test (no continuity correction) of two integer label arrays on a rows x columns table.

    Empty rows and columns are left out, because the test is undefined with a zero expected frequency, and a table
    left with fewer than two rows or two columns gives NaN, which fails a licence like any p-value below its level.
    A sample that empties a margin is the kind a red is routed with, so its four figures must still be returned.
    """
    table = np.zeros((rows, columns))
    np.add.at(table, (np.asarray(first), np.asarray(second)), 1)
    table = table[table.sum(axis=1) > 0][:, table.sum(axis=0) > 0]
    if min(table.shape) < 2:
        return float("nan")
    return float(_stats().chi2_contingency(table, correction=False).pvalue)


def _is_ints(value: Any) -> bool:
    """Whether `value` is a plain numpy array of integers."""
    return type(value) is np.ndarray and value.dtype.kind in "iu"


class DrawLaw:
    """A licence that admits draws when exact preconditions hold and exactly four statistical tests all pass.

    Each test is a callable `test(reference, value) -> p_value`, where `reference` is the law's parameters as the
    lift's reduction returned them (a pandas Series with named entries) and `value` is what the registered callable
    returned. The draws are admitted only when `precondition(reference, value)` is true (exact checks that spend no
    false-alarm budget: shape, support, distinctness) and every p-value is at least `alpha`; a NaN p-value fails,
    because "not below alpha" is not "passed". Exactly `TESTS_PER_LAW` tests are required: the number is the accepted
    comparison's, and a licence with fewer would be a weaker test under the same name.

    `outside` returns `null(reference)`, the law's single most probable outcome repeated `count` times, which the
    first test of each law rejects with p near 0, so the placeholder of an unregistered draw-law test fails at the
    comparison's own assertion. The default precondition and null are a one-dimensional finite numeric array of
    `count` draws and an array of NaN.
    """

    def __init__(
        self,
        tests: Sequence[Callable[[Any, Any], float]],
        *,
        precondition: Callable[[Any, Any], bool] | None = None,
        null: Callable[[Any], Any] | None = None,
        alpha: float = FALSE_ALARM,
        count: int = DRAWS,
    ):
        """Hold the four tests, the precondition, the placeholder, the per-test rate and the draw count."""
        if len(tests) != TESTS_PER_LAW:
            raise ValueError(f"a draw-law licence carries exactly {TESTS_PER_LAW} statistical tests, not {len(tests)}")
        self._tests, self.alpha, self.count = tuple(tests), alpha, count
        self._precondition = precondition or self._plain_draws
        self._null = null or (lambda reference: np.full(self.count, np.nan))

    def _plain_draws(self, reference: Any, value: Any) -> bool:
        """The default precondition: a one-dimensional numpy array of exactly `count` finite numbers."""
        return (
            type(value) is np.ndarray
            and value.ndim == 1
            and value.size == self.count
            and value.dtype.kind in "iuf"
            and bool(np.all(np.isfinite(value)))
        )

    def p_values(self, reference: Any, value: Any) -> list[float]:
        """The four p-values, for a caller routing a red with the figures; raises if the precondition fails."""
        if not self._precondition(reference, value):
            raise ValueError("the draws fail the law's exact preconditions")
        return [float(test(reference, value)) for test in self._tests]

    def admits(self, reference: Any, value: Any) -> bool:
        """Whether `value` meets the precondition and every test passes at `alpha`."""
        if not self._precondition(reference, value):
            return False
        return all(bool(float(test(reference, value)) >= self.alpha) for test in self._tests)

    def outside(self, reference: Any) -> Any:
        """The law's most probable outcome repeated `count` times, which the licence rejects."""
        return self._null(reference)


# ---- unit 1: a uniform k-subset of a pool, in uniformly random order ----


def subset_law(count: int = DRAWS) -> DrawLaw:
    """The licence of a without-replacement uniform sample of `k` of `pool` positions, `count` realizations of it.

    The reference is a Series with `pool` (P) and `k`. Value: an integer array of shape (count, k), row r the pool
    positions placed in output slots 0..k-1 of realization r. Preconditions: shape, positions in 0..P-1, distinct
    within a row. S1 scaled inclusion chi-square (P-1 df), S2 rank of slot 0 within its row (k-1 df), S3 minimum
    position (cells with expected count under 5 pooled), S4 slot-0 buckets of consecutive row pairs (10 x 10).
    """

    def parameters(reference):
        """The pool size and the sample size from the reference."""
        return int(reference["pool"]), int(reference["k"])

    def precondition(reference, value):
        """Exact checks: shape, positions inside the pool, distinct positions within each row."""
        pool, k = parameters(reference)
        if not _is_ints(value) or value.shape != (count, k) or value.min() < 0 or value.max() >= pool:
            return False
        return bool(np.all(np.diff(np.sort(value, axis=1), axis=1) > 0))

    def inclusion(reference, value):
        """S1: scaled chi-square of how often each pool position is included."""
        pool, k = parameters(reference)
        counts = np.bincount(value.ravel(), minlength=pool)
        pi = k / pool
        statistic = ((counts - count * pi) ** 2).sum() / (count * pi * (1 - pi) * pool / (pool - 1))
        return float(_stats().chi2.sf(statistic, pool - 1))

    def order(reference, value):
        """S2: chi-square of the rank of slot 0's position within its own row."""
        _, k = parameters(reference)
        ranks = (value < value[:, [0]]).sum(axis=1)
        return _chisquare(np.bincount(ranks, minlength=k), np.full(k, 1 / k))

    def minimum(reference, value):
        """S3: chi-square of each row's minimum position against its exact law, small cells pooled."""
        pool, k = parameters(reference)
        cells = np.arange(pool - k + 1)
        probabilities = np.array([math.comb(pool - 1 - j, k - 1) for j in cells]) / math.comb(pool, k)
        observed = np.bincount(value.min(axis=1), minlength=cells.size).astype(float)
        small = probabilities * count < 5
        if small.any():
            observed = np.append(observed[~small], observed[small].sum())
            probabilities = np.append(probabilities[~small], probabilities[small].sum())
        return _chisquare(observed, probabilities)

    def pairs(reference, value):
        """S4: independence of the slot-0 buckets of consecutive realizations."""
        pool, _ = parameters(reference)
        buckets = value[:, 0] * 10 // pool
        return _contingency(buckets[0::2], buckets[1::2], 10, 10)

    def null(reference):
        """The most probable outcome repeated: the same k positions in every row."""
        _, k = parameters(reference)
        return np.tile(np.arange(k), (count, 1))

    return DrawLaw((inclusion, order, minimum, pairs), precondition=precondition, null=null, count=count)


# ---- unit 2: draws from a vine copula ----


def vine_copula_law(count: int = DRAWS) -> DrawLaw:
    """The licence of `count` samples of a vine copula, tested through its Rosenblatt transform.

    The reference is a Series with `vine_json` (the copula's `to_json()` text) and `columns` (the column names, in order).
    Value: a DataFrame of shape (count, d) with those columns and values strictly inside (0, 1). The forward
    Rosenblatt transform of a correct sample is iid uniform on the unit cube. C1 margins uniform (KS, Bonferroni
    over d), C2 Rosenblatt coordinates uniform (KS, Bonferroni over d), C3 Rosenblatt coordinates pairwise
    independent (Kendall tau, Bonferroni over d(d-1)/2), C4 lag-1 serial independence of the first coordinate.
    Needs pyvinecopulib and scipy, imported when a test first runs.
    """

    def rosenblatt(reference, value):
        """The forward Rosenblatt transform of the sample under the reference's copula."""
        import pyvinecopulib as pv

        return pv.Vinecop.from_json(str(reference["vine_json"])).rosenblatt(value.to_numpy(dtype=float))

    def uniform_p(columns):
        """The Bonferroni-combined Kolmogorov-Smirnov p-value of uniformity over the columns."""
        stats = _stats()
        return _bonferroni([float(stats.kstest(columns[:, j], "uniform").pvalue) for j in range(columns.shape[1])])

    def precondition(reference, value):
        """Exact checks: a DataFrame of the reference's columns, `count` rows, every value inside (0, 1)."""
        columns = list(reference["columns"])
        if not isinstance(value, pd.DataFrame) or value.shape != (count, len(columns)):
            return False
        if list(value.columns) != columns or not np.all(np.isfinite(value.to_numpy(dtype=float))):
            return False
        return bool(((value.to_numpy(dtype=float) > 0) & (value.to_numpy(dtype=float) < 1)).all())

    def margins(reference, value):
        """C1: the margins are uniform."""
        return uniform_p(value.to_numpy(dtype=float))

    def coordinates(reference, value):
        """C2: the Rosenblatt coordinates are uniform."""
        return uniform_p(rosenblatt(reference, value))

    def independence(reference, value):
        """C3: the Rosenblatt coordinates are pairwise independent."""
        v = rosenblatt(reference, value)
        pairs = list(itertools.combinations(range(v.shape[1]), 2))
        return _bonferroni([float(_stats().kendalltau(v[:, a], v[:, b]).pvalue) for a, b in pairs])

    def serial(reference, value):
        """C4: lag-1 independence of the first Rosenblatt coordinate."""
        v = rosenblatt(reference, value)
        return float(_stats().kendalltau(v[:-1, 0], v[1:, 0]).pvalue)

    def null(reference):
        """A constant sample at the centre of the cube."""
        return pd.DataFrame(np.full((count, len(list(reference["columns"]))), 0.5), columns=list(reference["columns"]))

    return DrawLaw((margins, coordinates, independence, serial), precondition=precondition, null=null, count=count)


# ---- units 3 and 4: iid uniform picks over M rows, with replacement ----


def _pair_columns(positions: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """The first and second member of each non-overlapping consecutive pair, within a resample when the array is 2-D."""
    if positions.ndim == 1:
        usable = positions[: positions.size // 2 * 2].reshape(-1, 2)
    else:
        usable = positions[:, : positions.shape[1] // 2 * 2].reshape(-1, 2)
    return usable[:, 0], usable[:, 1]


def _pick_tests(rows: int, positions: np.ndarray) -> tuple[float, float, float, float]:
    """The four tests of iid uniform picks over `rows`: frequency, pair cells, repeat rate, lag-1 Spearman rho."""
    stats = _stats()
    first, second = _pair_columns(positions)
    frequency = _chisquare(np.bincount(positions.ravel(), minlength=rows), np.full(rows, 1 / rows))
    cells = _chisquare(np.bincount(first * rows + second, minlength=rows * rows), np.full(rows * rows, 1 / rows**2))
    repeats = float(stats.binomtest(int((first == second).sum()), first.size, 1 / rows).pvalue)
    flat = positions.ravel()
    serial = float(stats.spearmanr(flat[:-1], flat[1:]).pvalue)
    return frequency, cells, repeats, serial


def bootstrap_law(count: int = DRAWS) -> DrawLaw:
    """The licence of `count` iid uniform picks, with replacement, over the `rows` rows of a series.

    The reference is a Series with `rows`, the row count. Value: an integer array of positions below the row count with `count` entries, either
    one-dimensional (pick order) or two-dimensional (resamples x picks, pairs then taken within each resample).
    Preconditions: size and support. W1 frequency chi-square, W2 pair-cell chi-square (M^2 cells), W3 repeat-rate
    binomial test, W4 lag-1 Spearman. Row integrity (an output row equals its source row) is not testable from
    positions and is the registered callable's own reduction.
    """

    def rows_of(reference):
        """The row count M from the reference."""
        return int(reference["rows"])

    def precondition(reference, value):
        """Exact checks: integer positions below the row count, `count` of them, in one or two dimensions."""
        return (
            _is_ints(value)
            and value.ndim in (1, 2)
            and value.size == count
            and value.min() >= 0
            and value.max() < rows_of(reference)
        )

    tests = tuple(lambda reference, value, n=n: _pick_tests(rows_of(reference), value)[n] for n in range(4))
    return DrawLaw(tests, precondition=precondition, null=lambda reference: np.zeros(count, dtype=int), count=count)


def two_stream_law(count: int = DRAWS) -> DrawLaw:
    """The licence of two independent streams of iid uniform picks, each with `count` entries.

    The reference is a Series with `rows1` and `rows2`, equal. Value: a pair (tuple or list) of two integer arrays of the
    same shape, one-dimensional or resamples x picks. B1 and B2 are each stream's frequency chi-square, B3 the
    within-stream pair-cell chi-square over both streams pooled, B4 the independence of stream 1's pick and stream 2's
    pick at the same slot.
    """

    def sizes(reference):
        """The two row counts from the reference."""
        return int(reference["rows1"]), int(reference["rows2"])

    def precondition(reference, value):
        """Exact checks: two integer arrays of one shape, `count` entries each, positions inside their rows."""
        if not isinstance(value, tuple | list) or len(value) != 2:
            return False
        one, two = value
        m1, m2 = sizes(reference)
        return (
            _is_ints(one)
            and _is_ints(two)
            and one.shape == two.shape
            and one.ndim in (1, 2)
            and one.size == count
            and one.min() >= 0
            and one.max() < m1
            and two.min() >= 0
            and two.max() < m2
        )

    def frequency(which):
        """Build the frequency test of one stream."""

        def test(reference, value):
            """Chi-square of one stream's pick frequencies against uniform."""
            rows = sizes(reference)[which]
            return _chisquare(np.bincount(value[which].ravel(), minlength=rows), np.full(rows, 1 / rows))

        return test

    def pooled_pairs(reference, value):
        """B3: chi-square of within-stream consecutive pairs, both streams pooled."""
        m1, m2 = sizes(reference)
        if m1 != m2:
            raise ValueError(f"the pooled pair test needs equal row counts, got {m1} and {m2}")
        firsts, seconds = zip(*(_pair_columns(stream) for stream in value), strict=True)
        first, second = np.concatenate(firsts), np.concatenate(seconds)
        return _chisquare(np.bincount(first * m1 + second, minlength=m1 * m1), np.full(m1 * m1, 1 / m1**2))

    def cross(reference, value):
        """B4: independence of stream 1's pick and stream 2's pick at the same slot."""
        m1, m2 = sizes(reference)
        return _contingency(value[0].ravel(), value[1].ravel(), m1, m2)

    def null(reference):
        """Two streams of the same single position."""
        return (np.zeros(count, dtype=int), np.zeros(count, dtype=int))

    return DrawLaw((frequency(0), frequency(1), pooled_pairs, cross), precondition=precondition, null=null, count=count)


# ---- unit 5: a rank-weighted permutation (successive weighted sampling without replacement) ----


def _plackett_luce(weights: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Exact first-slot, ordered first-two-slots, last-slot and slot-of-item-0 laws by enumerating the permutations."""
    n = weights.size
    if n > 9:
        raise ValueError(f"enumerating the permutations of {n} items is not supported; the law has at most 9")
    first, last, slot_of_first = np.zeros(n), np.zeros(n), np.zeros(n)
    pairs = np.zeros((n, n))
    for perm in itertools.permutations(range(n)):
        remaining, p = weights.sum(), 1.0
        for item in perm:
            p *= weights[item] / remaining
            remaining -= weights[item]
        first[perm[0]] += p
        last[perm[-1]] += p
        slot_of_first[perm.index(0)] += p
        pairs[perm[0], perm[1]] += p
    return first, pairs, last, slot_of_first


def plackett_luce_law(count: int = DRAWS) -> DrawLaw:
    """The licence of `count` permutations drawn by successive weighted sampling without replacement.

    The reference is a Series of the n weights, entry i belonging to rank i+1 (unnormalised is fine). Value: an integer
    array of shape (count, n), each row the ranks 1..n in output-slot order. Precondition: every row is a
    permutation of 1..n. P1 first-slot law, P2 ordered first-two-slots law, P3 last-slot law, P4 slot of rank 1; each
    is a chi-square against the exact law found by enumerating all n! permutations.
    """

    def weights_of(reference):
        """The weights from the reference, as floats."""
        return np.asarray(reference, dtype=float)

    def precondition(reference, value):
        """Exact checks: shape, and every row a permutation of the ranks 1..n."""
        n = weights_of(reference).size
        return (
            _is_ints(value)
            and value.shape == (count, n)
            and bool(np.all(np.sort(value, axis=1) == np.arange(1, n + 1)))
        )

    def test(which):
        """Build the p-th of the four exact-law tests."""

        def run(reference, value):
            """Chi-square of one slot statistic against its exact law."""
            n = weights_of(reference).size
            first, pairs, last, slot = _plackett_luce(weights_of(reference))
            if which == 0:
                return _chisquare(np.bincount(value[:, 0] - 1, minlength=n), first)
            if which == 1:
                cells = np.bincount((value[:, 0] - 1) * n + (value[:, 1] - 1), minlength=n * n).reshape(n, n)
                keep = ~np.eye(n, dtype=bool)
                return _chisquare(cells[keep], pairs[keep])
            if which == 2:
                return _chisquare(np.bincount(value[:, -1] - 1, minlength=n), last)
            return _chisquare(np.bincount((value == 1).argmax(axis=1), minlength=n), slot)

        return run

    def null(reference):
        """The identity permutation in every row."""
        n = weights_of(reference).size
        return np.tile(np.arange(1, n + 1), (count, 1))

    return DrawLaw(tuple(test(n) for n in range(4)), precondition=precondition, null=null, count=count)


# ---- unit 6: a uniform tide shift on a lattice and a year uniform over the valid years ----


def tide_law(count: int = DRAWS) -> DrawLaw:
    """The licence of `count` (shift, year) draws: a shift uniform on [-w/2, w/2] rounded to the tstep lattice, a year uniform over the valid years.

    The reference is a Series with `window_hours` (w), `tstep_hours` and `valid_years` (a list, the same for every shift).
    Value: an integer array of shape (count, 2), column 0 the shift in whole multiples of `tstep_hours` hours counted
    in hours (so tstep 1 h gives whole hours), column 1 the year. Preconditions: shape, shifts on the lattice inside the
    window, years in the valid set. T1 shift lattice chi-square (end points half weight), T2 year chi-square, T3 year
    against twelve shift bins, T4 twelve-bin shift of consecutive row pairs.
    """

    def parameters(reference):
        """The window, the lattice step and the sorted valid years from the reference."""
        return int(reference["window_hours"]), int(reference["tstep_hours"]), sorted(reference["valid_years"])

    def precondition(reference, value):
        """Exact checks: shape, shifts on the lattice inside the window, years in the valid set."""
        window, step, years = parameters(reference)
        if not _is_ints(value) or value.shape != (count, 2):
            return False
        shift = value[:, 0]
        return bool(
            np.all(np.abs(shift) <= window // 2) and np.all(shift % step == 0) and np.all(np.isin(value[:, 1], years))
        )

    def bins(reference, shift):
        """The shift bin (one of twelve equal bins of the window) of each shift."""
        window, _, _ = parameters(reference)
        return np.minimum((shift + window // 2) * TIDE_SHIFT_BINS // window, TIDE_SHIFT_BINS - 1)

    def lattice(reference, value):
        """T1: chi-square of the shift over the lattice, end points at half weight."""
        window, step, _ = parameters(reference)
        points = np.arange(-(window // 2), window // 2 + 1, step)
        probabilities = np.full(points.size, step / window)
        probabilities[[0, -1]] /= 2
        index = (value[:, 0] + window // 2) // step
        return _chisquare(np.bincount(index, minlength=points.size), probabilities)

    def year(reference, value):
        """T2: chi-square of the year against uniform over the valid years."""
        _, _, years = parameters(reference)
        index = np.searchsorted(years, value[:, 1])
        return _chisquare(np.bincount(index, minlength=len(years)), np.full(len(years), 1 / len(years)))

    def year_by_shift(reference, value):
        """T3: independence of the year and the shift bin."""
        _, _, years = parameters(reference)
        return _contingency(
            np.searchsorted(years, value[:, 1]), bins(reference, value[:, 0]), len(years), TIDE_SHIFT_BINS
        )

    def serial(reference, value):
        """T4: independence of the shift bins of consecutive rows."""
        b = bins(reference, value[:, 0])
        usable = b[: b.size // 2 * 2]
        return _contingency(usable[0::2], usable[1::2], TIDE_SHIFT_BINS, TIDE_SHIFT_BINS)

    def null(reference):
        """Shift 0 and the first valid year in every row."""
        _, _, years = parameters(reference)
        return np.column_stack([np.zeros(count, dtype=int), np.full(count, years[0], dtype=int)])

    return DrawLaw((lattice, year, year_by_shift, serial), precondition=precondition, null=null, count=count)
