"""Independent checks of the five dependence equation tests: can a correct implementation pass them, and does each
wrong one fail them?

A conformance test is only evidence about the code when a red means "the code departs from the manuscript". Two
things break that without anyone noticing. A test can be red for a reason no code change removes (it renders its
own reference differently from the value it compares), so the divergence it reports can never be fixed. And a test
can stay green on a wrong implementation (its literal input does not separate the right form from the wrong one),
so a later change that breaks the code goes unseen. Neither shows up by running the tests against the legacy code,
because the legacy code is wrong for the four rows that are red and the one green row agrees with the manuscript.

So these tests replace the code under test with stand-ins written here from the manuscript's formulas. Each
equation test in `test_equations_dependence.py` obtains its observed value by calling `implementation(unit, part,
reduce)` and handing the finished namespace to its own `reduce`; the stand-in `implementation` below ignores the
unit and hands that `reduce` a namespace of plain functions, one set that computes the printed formula and
several that each compute one wrong form. The module's real `check` still runs, reading the equation id from the
test function's own marker, so a refusal arrives exactly as it would in the family: as a `ConformanceDivergence`.

What is covered, by class:

- every equation test passes against a manuscript-faithful stand-in (E-11 in six listing layouts);
- every wrong form named below fails with a `ConformanceDivergence` and not with another exception;
- the two rows that draw random numbers seed numpy's global stream before the draw, whatever the ambient state;
- every equation test hands numpy's global stream back as it found it;
- the five tests keep the structure the family relies on: one marker on the function that names its ledger row,
  no skip and no expected failure, no handler that could turn a refusal into a pass, one final `check` call, and
  no reference computed through the comparison module.

Not established: that the stand-ins' readings are the manuscript's. They share the module under review's readings
of three things the manuscript does not state: that the Spearman coefficient is taken over the rows complete in
every column, that the printed summation bound `p <= q` is read as printed, and that the empirical distribution
function divides by the sample size. A different ruling on any of the three moves a stand-in here and the
expected value there together. Nor do these tests check that a register-recorded divergence is the only way the
legacy code differs from the manuscript.

These carry no family marker, so they enter no inventory count.
"""

from __future__ import annotations

import ast
import inspect
import itertools
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from tests.conformance import bindings, equation_check
from tests.conformance import test_equations_dependence as module_under_review

SYSTEM = "publication-math-conformance"
EQUATION_IDS = ("E-11", "E-13", "E-15", "E-16", "E-30")
SOURCE = inspect.getsource(module_under_review)
TREE = ast.parse(SOURCE)


def _equation_functions() -> dict[str, object]:
    """Every test function of the module under review that carries a `conformance` marker, keyed by name."""
    found = {}
    for name, member in vars(module_under_review).items():
        marks = getattr(member, "pytestmark", [])
        if name.startswith("test_") and any(mark.name == "conformance" for mark in marks):
            found[name] = member
    return found


def _function_for(equation_id: str):
    """The one equation test whose marker names `equation_id`."""
    matches = [
        function
        for function in _equation_functions().values()
        if next(mark for mark in function.pytestmark if mark.name == "conformance").kwargs["unit"] == equation_id
    ]
    assert len(matches) == 1, f"{equation_id}: expected exactly one marked test, found {len(matches)}"
    return matches[0]


def _request_for(function) -> object:
    """A stand-in for pytest's request whose node answers `get_closest_marker` from the function's own marks."""

    def get_closest_marker(name):
        """Return the function's own mark of that name, or None."""
        return next((mark for mark in function.pytestmark if mark.name == name), None)

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


def _run(monkeypatch, equation_id: str, namespace: dict) -> Exception | None:
    """Run the equation test for `equation_id` with `namespace` as the finished namespace its reduction reads.

    Returns the exception the test raised, or None when it passed.
    """

    def standin(unit, part, reduce, overrides=None):
        """Hand the test's own reduction the stand-in namespace, whatever unit it asked for."""
        return reduce(namespace)

    monkeypatch.setattr(module_under_review, "implementation", standin)
    monkeypatch.setattr(equation_check, "implementation", standin)
    function = _function_for(equation_id)
    try:
        function(_request_for(function))
    except Exception as raised:
        return raised
    return None


# --- the stand-ins -------------------------------------------------------------------------------------------


def _is_omitted_pair(first: str, second: str) -> bool:
    """The manuscript's rule: two rainfall-intensity measures, or two maximum-surge measures, are not paired."""
    return ("intensity" in first and "intensity" in second) or ("surge" in first and "surge" in second)


def _listing(frame: pd.DataFrame, *, pairs, coefficients, orientation="column", reverse_rows=False) -> pd.DataFrame:
    """The `var1`, `var2`, `corr` table the code under test returns, laid out as `orientation` and `reverse_rows` say."""
    rows = []
    for first, second in pairs:
        one, two = {
            "column": (first, second),
            "alphabetical": tuple(sorted((first, second))),
            "reversed": (second, first),
        }[orientation]
        rows.append((one, two, float(coefficients.loc[first, second])))
    if reverse_rows:
        rows.reverse()
    return pd.DataFrame(rows, columns=["var1", "var2", "corr"])


def _manuscript_pairs(frame: pd.DataFrame) -> list[tuple[str, str]]:
    """Every pair of columns except the two the manuscript's rule omits."""
    return [pair for pair in itertools.combinations(frame.columns, 2) if not _is_omitted_pair(*pair)]


def _quantile_correlations(orientation="column", reverse_rows=False, form="manuscript"):
    """A `calculate_quantile_correlations` stand-in: the manuscript's form, or one recorded wrong form.

    `manuscript`: Spearman over the rows complete in every column, every pair but the omitted two.
    `one_surge_pairs`: the code's pair rule (a pair carries exactly one surge name), right coefficients.
    `own_sample_ranks`: right pairs, each column ranked over its own non-missing values and then correlated on the
    rows complete in every column, which is the code's order of operations.
    """

    def calculate(frame: pd.DataFrame) -> pd.DataFrame:
        """The listing for `frame`."""
        spearman = frame.dropna().rank().corr()
        if form == "manuscript":
            return _listing(
                frame,
                pairs=_manuscript_pairs(frame),
                coefficients=spearman,
                orientation=orientation,
                reverse_rows=reverse_rows,
            )
        if form == "one_surge_pairs":
            kept = [
                (first, second)
                for first, second in itertools.combinations(frame.columns, 2)
                if ("surge" in first) != ("surge" in second)
            ]
            return _listing(frame, pairs=kept, coefficients=spearman, orientation=orientation)
        if form == "own_sample_ranks":
            pearson = frame.rank().dropna().corr()
            return _listing(frame, pairs=_manuscript_pairs(frame), coefficients=pearson, orientation=orientation)
        raise ValueError(form)

    return calculate


def _empirical_cdf(denominator_extra: int = 0):
    """An `empirical_multivariate_cdf` stand-in: the share of sample rows at or below each row, over n plus `extra`."""

    def empirical(sample: pd.DataFrame) -> pd.Series:
        """The empirical joint probability at each row of `sample`."""
        counts = [(sample <= sample.loc[row]).all(axis=1).sum() for row in sample.index]
        return pd.Series(counts, index=sample.index, dtype=float) / (len(sample) + denominator_extra)

    return empirical


def _cvm(form: str = "sum_of_squares"):
    """A `compute_cvm_criterion` stand-in: the printed sum of squares, or one wrong form."""

    def criterion(empirical: pd.Series, fitted: pd.Series) -> float:
        """The distance between the two series."""
        difference = empirical - fitted
        if form == "sum_of_squares":
            return float((difference**2).sum())
        if form == "mean_of_squares":
            return float((difference**2).mean())
        if form == "sum_of_absolute":
            return float(difference.abs().sum())
        raise ValueError(form)

    return criterion


def _printed_tau(first, second) -> float:
    """The manuscript's statistic: `2 / (n (n - 1))` times the sum over `1 <= p <= q <= n` of +1 or -1."""
    first, second = np.asarray(first, dtype=float), np.asarray(second, dtype=float)
    count = len(first)
    score = sum(
        1 if (first[p] - first[q]) * (second[p] - second[q]) > 0 else -1 for p in range(count) for q in range(p, count)
    )
    return 2.0 / (count * (count - 1)) * score


def _tau_test(*, statistic="printed", p_value="cdf_at_observed", decision="printed", draws_short=0, size="observed"):
    """A `monte_carlo_tau_test` stand-in: the manuscript's form, or one recorded wrong form.

    `statistic`: `printed` is the manuscript's tau; `tie_corrected` is `scipy.stats.kendalltau`, the code's.
    `p_value`: `cdf_at_observed` is the null distribution evaluated at the observed statistic; `nearest_rank` is the
    percentile rank of the simulated value nearest to it, the code's estimator.
    `decision`: `printed` is the two-sided region on the printed thresholds; `flipped` is its opposite.
    `draws_short`: how many fewer bootstrap samples than asked for it draws.
    `size`: `observed` draws the observed sample's row count per bootstrap; `simulated` draws the simulated frame's.
    """

    def tau(first, second):
        """The statistic under test for two columns."""
        if statistic == "tie_corrected":
            return float(stats.kendalltau(first, second)[0])
        return _printed_tau(first, second)

    def test(df_obs, col1_iloc, col2_iloc, df_simulated=None, cop_fitted=None, n_samples=1000, alpha=0.05):
        """The null distribution of tau from `df_simulated`, the p-value and the decision at `alpha`."""
        observed = df_obs.iloc[:, [col1_iloc, col2_iloc]].dropna()
        observed_tau = tau(observed.iloc[:, 0], observed.iloc[:, 1])
        draw_size = len(observed) if size == "observed" else len(df_simulated)
        taus = []
        for _ in range(n_samples - draws_short):
            drawn = df_simulated.sample(n=draw_size, replace=True)
            taus.append(tau(drawn.iloc[:, col1_iloc], drawn.iloc[:, col2_iloc]))
        taus = pd.Series(taus, dtype=float)
        if p_value == "cdf_at_observed":
            pvalue = float((taus <= observed_tau).mean())
        else:
            nearest = taus.iloc[(taus - observed_tau).abs().argmin()]
            pvalue = float(taus.rank(pct=True)[taus == nearest].iloc[0])
        lower = float(np.quantile(taus, alpha / 2, method="inverted_cdf"))
        upper = float(np.quantile(taus, 1 - alpha / 2, method="inverted_cdf"))
        if p_value == "cdf_at_observed":
            rejected = observed_tau < lower or observed_tau > upper
        else:
            rejected = pvalue < alpha / 2 or pvalue > 1 - alpha / 2
        if decision == "flipped":
            rejected = not rejected
        return ("reject" if rejected else "fail to reject"), pvalue, taus, observed_tau

    return test


def _shifted_p_value(inner):
    """Wrap a tau test so its p-value moves by 0.001 and nothing else does."""

    def test(*args, **kwargs):
        """The wrapped test's result with a shifted p-value."""
        decision, pvalue, taus, observed_tau = inner(*args, **kwargs)
        return decision, pvalue + 0.001, taus, observed_tau

    return test


def _namespace(**callables) -> dict:
    """The finished namespace a reduction reads: the names the five tests look up, with `callables` overriding."""
    base = {
        "calculate_quantile_correlations": _quantile_correlations(),
        "empirical_multivariate_cdf": _empirical_cdf(),
        "compute_cvm_criterion": _cvm(),
        "monte_carlo_tau_test": _tau_test(),
    }
    return {**base, **callables}


# --- a correct implementation passes -------------------------------------------------------------------------

FAITHFUL = [
    pytest.param(
        "E-11",
        _namespace(calculate_quantile_correlations=_quantile_correlations(orientation, reverse)),
        id=f"E-11-{orientation}-{'reversed-rows' if reverse else 'natural-rows'}",
    )
    for orientation, reverse in itertools.product(("column", "alphabetical", "reversed"), (False, True))
] + [
    pytest.param("E-13", _namespace(), id="E-13"),
    pytest.param("E-15", _namespace(), id="E-15"),
    pytest.param("E-16", _namespace(), id="E-16"),
    pytest.param("E-30", _namespace(), id="E-30"),
]


@pytest.mark.parametrize(("equation_id", "namespace"), FAITHFUL)
def test_a_manuscript_faithful_implementation_passes_the_equation_test(monkeypatch, equation_id, namespace):
    """Property: with the code replaced by a function that computes the manuscript's formula on the test's own
    literal input, the equation test returns without raising.

    Class: each of the five equation tests against a faithful stand-in, and E-11 against every layout a faithful
    listing can take (which member of a pair comes first, and the row order). REGRESSION: red on the tree at the
    reviewed tip for the six E-11 cases, output in the round file.
    Reachability: the layouts are free choices of any implementation of the listing; the test's reduction sorts
    the observed pairs, so what the layout can reach is only the test's rendering of its own reference.
    Both-states anchor: the oracle is that the observed and expected sides agree when the code is right; it holds
    for the current code's repair and for any other repair of the rendering.
    Why: a red that no implementation can clear is not a divergence the code owes a fix for. The classifier would be
    handed a record whose cause is the test, and a repaired code would stay red forever.
    Kills: rendering the expected pairs in column order while the observed pairs are sorted inside each pair (E-11);
    an off-by-one in the reference sum (E-13, E-15); a threshold read at the wrong end (E-16); a recorded size
    taken from the wrong frame (E-30).
    Second correct implementation: the module under review normalizes both sides to one pair layout instead of
    only the observed side; this test still passes.
    """
    outcome = _run(monkeypatch, equation_id, namespace)
    assert outcome is None, f"{equation_id}: a manuscript-faithful stand-in did not pass: {outcome!r}"


# --- a wrong form fails with a divergence --------------------------------------------------------------------

WRONG = [
    pytest.param(
        "E-11",
        _namespace(calculate_quantile_correlations=_quantile_correlations(form="one_surge_pairs")),
        id="E-11-pairs-with-exactly-one-surge-name",
    ),
    pytest.param(
        "E-11",
        _namespace(calculate_quantile_correlations=_quantile_correlations(form="own_sample_ranks")),
        id="E-11-columns-ranked-over-their-own-samples",
    ),
    pytest.param(
        "E-13",
        _namespace(empirical_multivariate_cdf=_empirical_cdf(denominator_extra=1)),
        id="E-13-denominator-n-plus-one",
    ),
    pytest.param("E-13", _namespace(compute_cvm_criterion=_cvm("mean_of_squares")), id="E-13-mean-not-sum"),
    pytest.param("E-13", _namespace(compute_cvm_criterion=_cvm("sum_of_absolute")), id="E-13-absolute-not-squared"),
    pytest.param(
        "E-15", _namespace(monte_carlo_tau_test=_tau_test(statistic="tie_corrected")), id="E-15-tie-corrected-tau"
    ),
    pytest.param(
        "E-16", _namespace(monte_carlo_tau_test=_tau_test(p_value="nearest_rank")), id="E-16-nearest-value-rank"
    ),
    pytest.param(
        "E-16", _namespace(monte_carlo_tau_test=_tau_test(decision="flipped")), id="E-16-decision-only-flipped"
    ),
    pytest.param(
        "E-16", _namespace(monte_carlo_tau_test=_shifted_p_value(_tau_test())), id="E-16-p-value-only-shifted"
    ),
    pytest.param(
        "E-30",
        _namespace(monte_carlo_tau_test=_tau_test(size="simulated")),
        id="E-30-bootstrap-size-of-the-simulated-frame",
    ),
    pytest.param("E-30", _namespace(monte_carlo_tau_test=_tau_test(draws_short=1)), id="E-30-one-draw-short"),
]


@pytest.mark.parametrize(("equation_id", "namespace"), WRONG)
def test_a_recorded_wrong_form_fails_the_equation_test_with_a_conformance_divergence(
    monkeypatch, equation_id, namespace
):
    """Property: with the code replaced by one wrong form of the equation, the equation test raises
    `ConformanceDivergence` and nothing else.

    Class: the wrong forms the register records for these rows (pair rule and rank order for E-11, the `n + 1`
    denominator for E-13, the tie-corrected tau for E-15, the nearest-value rank for E-16), plus the forms that
    separate the two quantities E-30 reads (`b` and the bootstrap size), and, for E-16, a refusal that rests on
    the decision alone and one that rests on the p-value alone. NEW CAPABILITY: the plausible wrong test compares
    only the quantity the wrong form happens to move; red under it because the stand-in moves the other.
    Reachability: every form is a function the equation test's own reduction can call, with the signature the
    legacy text has.
    Both-states anchor: the oracle is the exception type, a property of the licence's verdict on a value pair and
    not of any wording.
    Why: a diverging row is recorded as a red that stays red, and a red of any other type (a type error from a
    value the licence cannot read, an assertion outside `check`) is a defect of the test, not a divergence.
    For E-11 the two forms refuse for their own reason only once the pair layout is the same on both sides, so
    until then they are green for the layout's reason too; they are held for after that repair.
    Kills: dropping a component from the rendered value (E-11 coefficients, E-16 decision, E-30 `b`); loosening
    the licence on a text or integer component.
    Second correct implementation: a different reference computation of the same formula; the wrong forms still fail.
    """
    outcome = _run(monkeypatch, equation_id, namespace)
    assert isinstance(outcome, equation_check.ConformanceDivergence), (
        f"{equation_id}: a wrong form must be refused as a ConformanceDivergence, got {outcome!r}"
    )


# --- seeding and the global stream ---------------------------------------------------------------------------


def _recording_tau_test(record: list):
    """A faithful tau test that notes numpy's global stream at the moment of its first draw."""
    inner = _tau_test()

    def test(*args, **kwargs):
        """Record the stream, then run the faithful test."""
        record.append(np.random.get_state()[1].copy())
        return inner(*args, **kwargs)

    return test


@pytest.mark.parametrize("equation_id", ["E-16", "E-30"])
def test_a_row_that_draws_seeds_numpys_global_stream_before_the_draw(monkeypatch, equation_id):
    """Property: the stream the code draws from is the same when the test was entered with two different ambient
    streams, and differs from either of them.

    Class: the two rows whose test must seed a generator because the legacy text names none (E-16 and E-30), each under two ambient
    streams. NEW CAPABILITY: the plausible wrong test seeds after the implementation call, or around only the
    reference; red under it because the recorded stream follows the ambient one.
    Reachability: any earlier test or import may leave numpy's stream anywhere.
    Both-states anchor: the oracle is the stream at the moment of the call, a property of when the seed is set,
    not of how.
    Why: the legacy text names no generator, so numpy's global stream is the only way to repeat a draw; a draw the
    ambient state steers is a flaky red in a file nobody touched.
    Kills: removing the `seeded_global_numpy` block around the implementation call.
    Second correct implementation: `np.random.seed(seed)` before the call and a manual restore afterwards.
    """
    saved = np.random.get_state()
    recorded = []
    try:
        for ambient in (1, 2):
            np.random.seed(ambient)
            ambient_stream = np.random.get_state()[1].copy()
            seen = []
            outcome = _run(monkeypatch, equation_id, _namespace(monte_carlo_tau_test=_recording_tau_test(seen)))
            assert outcome is None, f"{equation_id}: {outcome!r}"
            assert seen, f"{equation_id}: the stand-in was never called"
            assert not np.array_equal(seen[0], ambient_stream), f"{equation_id}: the stream was not seeded"
            recorded.append(seen[0])
    finally:
        np.random.set_state(saved)
    assert np.array_equal(recorded[0], recorded[1]), f"{equation_id}: the stream at the draw follows the ambient state"


@pytest.mark.parametrize("equation_id", EQUATION_IDS)
def test_an_equation_test_hands_numpys_global_stream_back_as_it_found_it(monkeypatch, equation_id):
    """Property: after the equation test returns or raises, numpy's global stream is what it was on entry.

    Class: all five equation tests, with the faithful stand-in. HELD: green on the tree at the reviewed tip.
    Reachability: a seed set for one test and left in place changes every later draw in the session.
    Both-states anchor: the oracle compares the stream before and after, true of any way of seeding and restoring.
    Why: a leaked seed turns a later test's input into a function of test order and shows as a flaky red elsewhere.
    Kills: replacing the `seeded_global_numpy` block by a bare `np.random.seed(...)`.
    Second correct implementation: seeding through a local generator that never touches the global stream.
    """
    saved = np.random.get_state()
    try:
        np.random.seed(99)
        before = np.random.get_state()
        _run(monkeypatch, equation_id, _namespace())
        after = np.random.get_state()
    finally:
        np.random.set_state(saved)
    assert before[0] == after[0]
    assert np.array_equal(before[1], after[1]) and before[2:] == after[2:], (
        f"{equation_id}: numpy's global stream was changed"
    )


# --- the structure the family relies on ----------------------------------------------------------------------


def test_each_equation_test_carries_one_function_marker_naming_its_ledger_row():
    """Property: the module's marked functions are exactly the five equations of this package; each carries one
    `conformance` marker on the function, `unit` is its equation id, `part` is its own name, `system` is the
    system slug, and the ledger binds that id as a test of that name. The module sets no module-level marks.

    Class: every `conformance`-marked function of the module, found by marker and not by name. HELD: green.
    Reachability: the inventory the coverage check reads takes a function's own decorator and nothing else.
    Both-states anchor: the ledger and the marker are two homes of one fact; the oracle is their agreement.
    Why: a marker that names the wrong id or part makes the coverage check pass or fail on the wrong function.
    Kills: a module-level `pytestmark`; a `part` that is not the function name; a function bound to another id.
    Second correct implementation: the same five functions in a different order, or with extra helper functions.
    """
    functions = _equation_functions()
    assert not hasattr(module_under_review, "pytestmark")
    ids = []
    for name, function in functions.items():
        marks = [mark for mark in function.pytestmark if mark.name == "conformance"]
        assert len(marks) == 1, f"{name}: {len(marks)} conformance markers"
        kwargs = marks[0].kwargs
        assert kwargs["part"] == name, f"{name}: part is {kwargs['part']!r}"
        assert kwargs["system"] == SYSTEM, f"{name}: system is {kwargs['system']!r}"
        row = bindings.load()[kwargs["unit"]]
        assert row["binding"] == "TEST" and row["test"] == name, f"{name}: ledger row is {row}"
        ids.append(kwargs["unit"])
    assert sorted(ids) == sorted(EQUATION_IDS)


def _equation_function_nodes() -> list[ast.FunctionDef]:
    """The syntax nodes of the module's marked functions."""
    names = set(_equation_functions())
    return [node for node in TREE.body if isinstance(node, ast.FunctionDef) and node.name in names]


def test_no_equation_test_can_pass_by_skipping_or_by_swallowing_its_refusal():
    """Property: none of the five functions is marked `skip`, `skipif` or `xfail`; the module calls no
    `pytest.skip`, `pytest.xfail` or `pytest.importorskip`; and no function contains an exception handler.

    Class: the five marked functions and the module's calls. HELD: green.
    Reachability: a divergence is a red by design; a skip or an expected failure or a handler around `check` turns
    it into a result the family's census counts as green.
    Both-states anchor: a syntactic property of the source, true of any correct version of these tests.
    Why: a diverging row is a red that stays red; a hidden pass defeats the record that the code diverges.
    Kills: adding `@pytest.mark.xfail`, or wrapping the final `check` in `try/except AssertionError`.
    Second correct implementation: a `try/finally` that restores state, which is not a handler and is admitted.
    """
    for name, function in _equation_functions().items():
        assert not [m for m in function.pytestmark if m.name in {"skip", "skipif", "xfail"}], name
    forbidden = {"skip", "xfail", "importorskip"}
    for node in ast.walk(TREE):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in forbidden:
            raise AssertionError(f"line {node.lineno}: a call to {node.func.attr}")
    for function in _equation_function_nodes():
        handlers = [n for n in ast.walk(function) if isinstance(n, ast.ExceptHandler)]
        assert not handlers, f"{function.name}: an exception handler at line {handlers[0].lineno}"


def test_each_equation_test_ends_in_one_check_call_and_computes_no_reference_through_the_comparison_module():
    """Property: each of the five functions makes exactly one call to `check` and it is the function's last
    statement; and the only names the function takes from the `compare` module are the two licences.

    Class: the five marked functions. HELD: green.
    Reachability: a reference computed through `compare` would make the comparison its own oracle, and a second
    `check` call or a statement after it would hide a refusal behind later code.
    Both-states anchor: a syntactic property of the source, true of any correct version of these tests.
    Why: each test computes its reference in its own body and ends in one `check`, so the reference is independent.
    Kills: a second `check` call; `compare.assert_equivalent(...)` in the body; a statement after the `check`.
    Second correct implementation: `equation_check.check(...)` instead of the bare name; admitted.
    """
    for function in _equation_function_nodes():
        calls = [
            n
            for n in ast.walk(function)
            if isinstance(n, ast.Call)
            and (
                (isinstance(n.func, ast.Name) and n.func.id == "check")
                or (isinstance(n.func, ast.Attribute) and n.func.attr == "check")
            )
        ]
        assert len(calls) == 1, f"{function.name}: {len(calls)} check calls"
        last = function.body[-1]
        assert isinstance(last, ast.Expr) and last.value is calls[0], (
            f"{function.name}: check is not the last statement"
        )
        taken = {
            n.attr
            for n in ast.walk(function)
            if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "compare"
        }
        assert taken <= {"Tolerance", "Exact"}, f"{function.name}: takes {sorted(taken)} from compare"
