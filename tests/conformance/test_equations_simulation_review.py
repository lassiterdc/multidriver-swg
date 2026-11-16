"""Independent checks of the two simulation equation tests: can a correct implementation pass them, and does each
wrong one fail them?

A conformance test is only evidence about the code when a red means "the code departs from the manuscript". Two
things break that without anyone noticing. A test can be red for a reason no code change removes (its reference
is rendered differently from the value it compares, or it compares a quantity the equation does not state), so
the divergence it reports can never be fixed. And a test can stay green on a wrong implementation (its literal
input does not separate the right form from the wrong one), so a later change that breaks the code goes unseen.
Neither shows up by running the tests against the legacy code, because the legacy code is wrong for both
equations and a run against it can only report red.

So these tests replace the code under test with stand-ins written here from the manuscript's formulas. Each
equation test in `test_equations_simulation.py` obtains its observed value by calling `implementation(unit,
part, reduce, overrides)` and handing the finished namespace to its own `reduce`; the stand-in `implementation`
below builds that namespace from the overrides the test passes, one set that computes the printed formula and
several that each compute one wrong form. The module's real `check` still runs, reading the equation id from the
test function's own marker, so a refusal arrives exactly as it would in the family: as a `ConformanceDivergence`.

What is covered, by class:

- both equation tests pass against a manuscript-faithful stand-in, in several observably different forms of it;
- every wrong form named below fails with a `ConformanceDivergence` and not with another exception;
- the recorded reflection of E-19 moves the upper-bounded marginal on every draw and no other marginal, so the
  red is attributable to that one departure;
- the E-19 test seeds numpy's global stream before the cell runs and hands it back as it found it;
- the E-18 test leaves no scratch directory behind, and the E-19 test's pandas binding reads one file;
- the variable names of the E-19 fit table are the ones the real cell's probability table carries;
- both tests end in a `ConformanceDivergence` on the stored legacy text, which is the admissible red;
- the two tests keep the structure the family relies on: one marker on the function that names its ledger row,
  no skip and no expected failure, no handler that could turn a refusal into a pass, one final `check` call, no
  reference computed through the comparison module, and the register's span quoted exactly.

Not established: that the stand-ins' readings are the manuscript's. They share the module under review's reading
of one thing the manuscript does not state: that the transform `T` of an upper-bounded marginal is the reflection
`x -> bound - x`, so that `T^{-1}` is the same reflection. A different ruling on it moves a stand-in here and the
expected value there together. Nor do these tests check that a register-recorded divergence is the only way the
legacy code differs from the manuscript.

These carry no family marker, so they enter no inventory count.
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from tests.conformance import bindings, equation_check, register
from tests.conformance import test_equations_simulation as module_under_review

SYSTEM = "publication-math-conformance"
EQUATION_IDS = ("E-18", "E-19")
SOURCE = inspect.getsource(module_under_review)
TREE = ast.parse(SOURCE)


def _equation_functions() -> dict[str, Any]:
    """Every test function of the module under review that carries a `conformance` marker, keyed by name."""
    found = {}
    for name, member in vars(module_under_review).items():
        marks = getattr(member, "pytestmark", [])
        if name.startswith("test_") and any(mark.name == "conformance" for mark in marks):
            found[name] = member
    return found


def _function_for(equation_id: str) -> Any:
    """The one equation test whose marker names `equation_id`."""
    matches = [
        function
        for function in _equation_functions().values()
        if next(mark for mark in function.pytestmark if mark.name == "conformance").kwargs["unit"] == equation_id
    ]
    assert len(matches) == 1, f"{equation_id}: expected exactly one marked test, found {len(matches)}"
    return matches[0]


def _request_for(function: Any) -> object:
    """A stand-in for pytest's request whose node answers `get_closest_marker` from the function's own marks."""

    def get_closest_marker(name: str) -> object | None:
        """Return the function's own mark of that name, or None."""
        return next((mark for mark in function.pytestmark if mark.name == name), None)

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


def _call(function: Any, tmp_path: Path) -> None:
    """Call an equation test as pytest would: with its own request and, when it asks for one, a scratch directory."""
    arguments: dict[str, Any] = {"request": _request_for(function)}
    if "tmp_path" in inspect.signature(function).parameters:
        arguments["tmp_path"] = tmp_path
    function(**arguments)


def _run(
    monkeypatch: pytest.MonkeyPatch, equation_id: str, standin: Callable[..., Any], tmp_path: Path
) -> Exception | None:
    """Run the equation test for `equation_id` with `standin` in place of `implementation`.

    Returns the exception the test raised, or None when it passed.
    """
    monkeypatch.setattr(module_under_review, "implementation", standin)
    monkeypatch.setattr(equation_check, "implementation", standin)
    try:
        _call(_function_for(equation_id), tmp_path)
    except Exception as raised:
        return raised
    return None


# --- E-18: the stand-ins -------------------------------------------------------------------------------------


def _joint_rows(observed: pd.DataFrame, size: int, seed: Any) -> pd.DataFrame:
    """`size` annual count vectors bootstrapped, as whole rows, from the observed table."""
    return observed.sample(n=size, replace=True, random_state=seed).reset_index(drop=True).astype(int)


def _mismatched_rows(observed: pd.DataFrame, size: int) -> pd.DataFrame:
    """`size` count vectors whose three counts come from three different observed years.

    This is the bootstrap that resamples each count on its own, so an inter-type correlation the observed years
    carry is lost. The rows are built, not drawn, so the form is repeatable; the assertion is the control that
    the construction produced a vector no observed year has.
    """
    columns = list(module_under_review.COUNT_COLUMNS)
    frame = pd.DataFrame(
        {
            column: [int(observed[column].iloc[(row + shift) % len(observed)]) for row in range(size)]
            for shift, column in enumerate(columns)
        }
    )
    frame["any"] = frame[columns].sum(axis=1)
    observed_rows = {tuple(int(value) for value in row) for row in observed[columns].itertuples(index=False)}
    drawn_rows = {tuple(int(value) for value in row) for row in frame[columns].itertuples(index=False)}
    assert drawn_rows - observed_rows, "the mismatched rows are all observed rows, so this wrong form is not wrong"
    return frame


def _chained_joint(overrides: dict[str, Any]) -> pd.DataFrame:
    """Whole rows, as many as the preamble's year count the test handed over."""
    return _joint_rows(
        overrides["df_occurences_obs"], overrides["n_years_to_simulate"], overrides.get("random_seed", 0)
    )


def _requested_joint(overrides: dict[str, Any]) -> pd.DataFrame:
    """Whole rows, as many as years were requested, whatever the preamble's year count was."""
    return _joint_rows(
        overrides["df_occurences_obs"], overrides["n_years_of_weather_to_generate"], overrides.get("random_seed", 0)
    )


def _unseeded_chained_joint(overrides: dict[str, Any]) -> pd.DataFrame:
    """Whole rows, the preamble's count, drawn from a generator that ignores the seed the test bound."""
    return _joint_rows(overrides["df_occurences_obs"], overrides["n_years_to_simulate"], np.random.RandomState(0))


def _one_row_per_observed_year(overrides: dict[str, Any]) -> pd.DataFrame:
    """Whole rows, one per observed year, whatever the preamble's year count was."""
    observed = overrides["df_occurences_obs"]
    return _joint_rows(observed, len(observed), overrides.get("random_seed", 0))


def _chained_mismatched(overrides: dict[str, Any]) -> pd.DataFrame:
    """Rows of the right count whose counts are not an observed year's joint vector."""
    return _mismatched_rows(overrides["df_occurences_obs"], overrides["n_years_to_simulate"])


def _occurrences(
    tmp_path: Path, *, preamble_years: Callable[[int], int], bootstrap_rows: Callable[[dict[str, Any]], pd.DataFrame]
) -> Callable[..., Any]:
    """An `implementation` for E-18's two units: `preamble_years(requested)` and `bootstrap_rows(overrides)`.

    The preamble unit hands the reduction the year count and a scratch directory it owns, as the real unit does;
    the bootstrap unit hands it the table of drawn count vectors.
    """

    def standin(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: dict[str, Any] | None = None
    ) -> Any:
        """Build the finished namespace of whichever of the two units was asked for."""
        overrides = dict(overrides or {})
        if unit == module_under_review.PREAMBLE_UNIT:
            scratch = tmp_path / "preamble-scratch"
            (scratch / "sim").mkdir(parents=True, exist_ok=True)
            years = preamble_years(overrides["n_years_of_weather_to_generate"])
            return reduce({"n_years_to_simulate": years, "dir_simulate_from_copula": f"{scratch}/sim/"})
        if unit == module_under_review.BOOTSTRAP_UNIT:
            return reduce({"df_occurences_sim": bootstrap_rows(overrides)})
        raise AssertionError(f"the test asked for a unit this stand-in does not model: {unit!r}")

    return standin


def _identity(requested: int) -> int:
    """The manuscript's year count: as many years as were requested."""
    return requested


def _one_and_a_half_times(requested: int) -> int:
    """The legacy preamble's year count, `ceil(1.5 * requested)`."""
    return int(np.ceil(1.5 * requested))


# --- E-19: the stand-ins -------------------------------------------------------------------------------------


def _inverse_cdf(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """The fitted distribution's inverse CDF at `probabilities`, for the two families the literal table uses."""
    if fit["distribution"] == "norm":
        return np.asarray(stats.norm.ppf(probabilities, loc=fit["loc"], scale=fit["scale"]), dtype=float)
    if fit["distribution"] == "gamma":
        return np.asarray(stats.gamma.ppf(probabilities, fit["shape"], loc=fit["loc"], scale=fit["scale"]), dtype=float)
    raise AssertionError(f"the stand-in models no family named {fit['distribution']!r}")


def _manuscript(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """`T^{-1}(F^{-1}(u))`: the inverse CDF at `u`, then the transform reversed (a reflection about the bound)."""
    value = _inverse_cdf(fit, probabilities)
    return value if np.isnan(fit["upper_bound"]) else fit["upper_bound"] - value


def _reflected(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """The legacy form: the probability of an upper-bounded marginal is flipped before the inverse CDF as well."""
    if np.isnan(fit["upper_bound"]):
        return _inverse_cdf(fit, probabilities)
    return fit["upper_bound"] - _inverse_cdf(fit, 1.0 - probabilities)


def _no_reversal(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """The inverse CDF alone: the transform is never reversed."""
    return _inverse_cdf(fit, probabilities)


def _flipped_without_reversal(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """The inverse CDF at `1 - u` for an upper-bounded marginal, with the reversal omitted."""
    return _inverse_cdf(fit, 1.0 - probabilities if not np.isnan(fit["upper_bound"]) else probabilities)


def _wrong_sign(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """The reversal with the opposite sign: `F^{-1}(u) - bound` for an upper-bounded marginal."""
    value = _inverse_cdf(fit, probabilities)
    return value if np.isnan(fit["upper_bound"]) else value - fit["upper_bound"]


def _perturbed_unbounded(*, first_draw_only: bool) -> Callable[[pd.Series, np.ndarray], np.ndarray]:
    """The manuscript's form with the first unbounded marginal scaled by `1 + 1e-6` on every draw or on the first."""

    def value(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
        """The marginal's value, scaled when it is the first of the unbounded marginals."""
        result = _manuscript(fit, probabilities).copy()
        if fit["data"] == module_under_review.DATA_COLUMNS[0]:
            if first_draw_only:
                result[0] *= 1.0 + 1e-6
            else:
                result *= 1.0 + 1e-6
        return result

    return value


def _location_and_scale_swapped(fit: pd.Series, probabilities: np.ndarray) -> np.ndarray:
    """The manuscript's form with the first marginal's location and scale exchanged."""
    if fit["data"] == module_under_review.DATA_COLUMNS[0]:
        fit = fit.copy()
        fit["loc"], fit["scale"] = fit["scale"], fit["loc"]
    return _manuscript(fit, probabilities)


def _draw(count: int, names: list[str], law: str) -> pd.DataFrame:
    """`count` simulated probabilities for each of `names`, drawn from numpy's global stream or placed on a grid."""
    if law == "grid":
        grid = (np.arange(count) + 0.5) / count
        return pd.DataFrame({name: np.roll(grid, shift) for shift, name in enumerate(names)})
    return pd.DataFrame(np.random.uniform(0.001, 0.999, size=(count, len(names))), columns=names)


def _inverse_transform_cell(
    form: Callable[[pd.Series, np.ndarray], np.ndarray] = _manuscript,
    *,
    law: str = "global",
    decorated: bool = False,
    foreign_probabilities: bool = False,
) -> Callable[..., Any]:
    """An `implementation` for E-19's cell: it reads the fit table the test binds and builds the finished namespace.

    The table is read through the `pd` binding and the directory binding the test passes, from the file the legacy
    text names. `form(fit, probabilities)` is the real-space value of one marginal. `decorated` adds the
    bookkeeping columns the cell adds and reverses the column order; `foreign_probabilities` reports probabilities
    other than the ones the real-space values were computed from.
    """

    def standin(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: dict[str, Any] | None = None
    ) -> Any:
        """Build the finished namespace the cell leaves: its simulated probabilities and its real-space frame."""
        overrides = dict(overrides or {})
        assert unit == module_under_review.INVERSE_TRANSFORM_UNIT, unit
        table = overrides["pd"].read_csv(f"{overrides['dir_fitting_maringals']}surge_events_distributions.csv")
        count = 2 * int(overrides["df_occurences_sim"]["surge"].sum())
        names = sorted(table["data"])
        probabilities = _draw(count, names, law)
        realspace = pd.DataFrame(
            {fit["data"]: form(fit, probabilities[fit["data"]].to_numpy(dtype=float)) for _, fit in table.iterrows()}
        )
        if decorated:
            realspace = realspace[list(reversed(realspace.columns))]
            realspace["surge_event"] = True
            realspace["copula_source"] = "surge_events"
        reported = _draw(count, names, "grid") if foreign_probabilities else probabilities
        return reduce({"df_simulated_cdf": reported, "dic_full_over_sim": {"surge_events": realspace}})

    return standin


# --- a correct implementation passes -------------------------------------------------------------------------

FAITHFUL = [
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=_identity, bootstrap_rows=_chained_joint),
        id="E-18-years-requested-drawn-as-whole-rows",
    ),
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=_one_and_a_half_times, bootstrap_rows=_requested_joint),
        id="E-18-over-count-in-the-preamble-and-the-requested-count-drawn",
    ),
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=_identity, bootstrap_rows=_unseeded_chained_joint),
        id="E-18-bootstrap-ignoring-the-bound-seed",
    ),
    pytest.param("E-19", lambda tmp_path: _inverse_transform_cell(), id="E-19-drawn-probabilities"),
    pytest.param("E-19", lambda tmp_path: _inverse_transform_cell(law="grid"), id="E-19-probabilities-on-a-grid"),
    pytest.param(
        "E-19", lambda tmp_path: _inverse_transform_cell(decorated=True), id="E-19-extra-columns-in-reversed-order"
    ),
]


@pytest.mark.parametrize(("equation_id", "build"), FAITHFUL)
def test_a_manuscript_faithful_implementation_passes_the_equation_test(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, equation_id: str, build: Callable[[Path], Callable[..., Any]]
) -> None:
    """Property: with the code replaced by a function that computes the manuscript's formula, the equation test
    returns without raising.

    Class: each of the two equation tests against faithful stand-ins that differ in what the test cannot be
    allowed to depend on: for E-18 whether the preamble's own year count reaches the bootstrap, and which seed the
    bootstrap draws with; for E-19 which probabilities the cell simulated, and whether its frame carries more
    columns than the three marginals. REGRESSION: green on the tree at the reviewed tip.
    Reachability: every form is something a correct repair of the code may do; the test sees only the finished
    namespace, so what it can reach is its own rendering of the reference.
    Both-states anchor: the oracle is that the observed and expected sides agree when the code is right; it holds
    for the current code's repair and for any other repair.
    Why: a red that no implementation can clear is not a divergence the code owes a fix for.
    Kills: an E-19 reference taken over a probability grid the test chose instead of the cell's own; an E-18
    value that fails when the over-count is made in the preamble and corrected before the draw; a column order or
    extra column that moves the E-19 rendering.
    Second correct implementation: a stand-in that draws its probabilities from a different law; still passes.
    """
    outcome = _run(monkeypatch, equation_id, build(tmp_path), tmp_path)
    assert outcome is None, f"{equation_id}: a manuscript-faithful stand-in did not pass: {outcome!r}"


# --- a wrong form fails with a divergence --------------------------------------------------------------------

WRONG = [
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=_one_and_a_half_times, bootstrap_rows=_chained_joint),
        id="E-18-one-and-a-half-times-the-years",
    ),
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=lambda years: years + 1, bootstrap_rows=_chained_joint),
        id="E-18-one-year-too-many",
    ),
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=lambda years: years - 1, bootstrap_rows=_chained_joint),
        id="E-18-one-year-too-few",
    ),
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=_identity, bootstrap_rows=_one_row_per_observed_year),
        id="E-18-one-row-per-observed-year",
    ),
    pytest.param(
        "E-18",
        lambda tmp_path: _occurrences(tmp_path, preamble_years=_identity, bootstrap_rows=_chained_mismatched),
        id="E-18-each-count-resampled-on-its-own",
    ),
    pytest.param(
        "E-19",
        lambda tmp_path: _inverse_transform_cell(_reflected),
        id="E-19-probability-flipped-before-the-inverse-cdf",
    ),
    pytest.param("E-19", lambda tmp_path: _inverse_transform_cell(_no_reversal), id="E-19-transform-never-reversed"),
    pytest.param(
        "E-19",
        lambda tmp_path: _inverse_transform_cell(_flipped_without_reversal),
        id="E-19-flipped-and-not-reversed",
    ),
    pytest.param("E-19", lambda tmp_path: _inverse_transform_cell(_wrong_sign), id="E-19-reversal-with-the-wrong-sign"),
    pytest.param(
        "E-19",
        lambda tmp_path: _inverse_transform_cell(_perturbed_unbounded(first_draw_only=False)),
        id="E-19-an-unbounded-marginal-scaled-by-a-millionth",
    ),
    pytest.param(
        "E-19",
        lambda tmp_path: _inverse_transform_cell(_perturbed_unbounded(first_draw_only=True)),
        id="E-19-first-draw-of-an-unbounded-marginal-scaled",
    ),
    pytest.param(
        "E-19",
        lambda tmp_path: _inverse_transform_cell(_location_and_scale_swapped),
        id="E-19-location-and-scale-exchanged",
    ),
    pytest.param(
        "E-19",
        lambda tmp_path: _inverse_transform_cell(foreign_probabilities=True),
        id="E-19-values-from-other-probabilities-than-the-ones-reported",
    ),
]


@pytest.mark.parametrize(("equation_id", "build"), WRONG)
def test_a_recorded_wrong_form_fails_the_equation_test_with_a_conformance_divergence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, equation_id: str, build: Callable[[Path], Callable[..., Any]]
) -> None:
    """Property: with the code replaced by one wrong form of the equation, the equation test raises
    `ConformanceDivergence` and nothing else.

    Class: the wrong forms the register records (the over-count of years for E-18, the flipped probability for
    E-19), the neighbours of each (a count one off, the transform never reversed or reversed with the wrong
    sign), and the forms that separate the two quantities E-18 compares (the count, and the joint-row
    containment) and the three marginals and every draw E-19 compares. NEW CAPABILITY: the plausible wrong test
    compares only the quantity the wrong form happens to move, or only the first draws, or only the bounded
    marginal; each is red under it because the stand-in moves the other.
    Reachability: every form is a function the equation test's own reduction can read from the namespace it
    gets, with the names the legacy text binds.
    Both-states anchor: the oracle is the exception type, a property of the licence's verdict on a value pair and
    not of any wording.
    Why: a diverging equation is recorded as a red that stays red, and a red of any other type (a type error from
    a value the licence cannot read, an assertion outside `check`) is a defect of the test, not a divergence.
    Kills: dropping a component from the rendered E-18 value; comparing a prefix or the mean of the E-19 draws;
    loosening the tolerance on E-19; an expected value computed from the returned values.
    Second correct implementation: a different reference computation of the same formula; the wrong forms still
    fail.
    """
    outcome = _run(monkeypatch, equation_id, build(tmp_path), tmp_path)
    assert isinstance(outcome, equation_check.ConformanceDivergence), (
        f"{equation_id}: a wrong form must be refused as a ConformanceDivergence, got {outcome!r}"
    )
    assert outcome.failure.equation_id == equation_id
    assert outcome.failure.diverged is True


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 7, 2**31 - 1])
def test_the_verdict_on_the_year_count_does_not_depend_on_the_bootstrap_seed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seed: int
) -> None:
    """Property: for any seed the test binds to the bootstrap, a faithful stand-in passes E-18 and the
    over-counting stand-in fails it with a divergence.

    Class: seven seeds, including the largest a 32-bit generator takes. NEW CAPABILITY: the plausible wrong test
    compares which rows were drawn, so its verdict follows the seed; both verdicts here are the same for every seed.
    Reachability: the seed is a module constant the test binds into the cell; any other value is as good.
    Both-states anchor: the oracle is the equality of the verdict across seeds, true of any correct version.
    Why: a count verdict that turns on the seed is a flaky red in a file nobody touched.
    Kills: an expected value built from the rows a given seed happens to draw.
    Second correct implementation: binding a different seed constant; still the same verdict.
    """
    monkeypatch.setattr(module_under_review, "BOOTSTRAP_SEED", seed)
    faithful = _occurrences(tmp_path, preamble_years=_identity, bootstrap_rows=_chained_joint)
    assert _run(monkeypatch, "E-18", faithful, tmp_path) is None
    over_counting = _occurrences(tmp_path, preamble_years=_one_and_a_half_times, bootstrap_rows=_chained_joint)
    assert isinstance(_run(monkeypatch, "E-18", over_counting, tmp_path), equation_check.ConformanceDivergence)


def test_the_recorded_reflection_moves_the_bounded_marginal_on_every_draw_and_no_other_marginal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: between the legacy form (probability flipped, then reversed) and the manuscript's, the values
    differ on every draw of the one upper-bounded marginal by more than a millionth, and agree to the licence's
    tolerance on every other marginal.

    Class: the values the E-19 test hands to `check`, captured with the legacy-form stand-in. NEW CAPABILITY: the
    plausible wrong test uses literal parameters or a seed on which the two forms agree for most draws, or on
    which a different marginal also moves; red under it because the difference is not on every draw of one column.
    Reachability: the literal fit table and the seed are the test's own; the probabilities are drawn after it
    seeds numpy's stream.
    Both-states anchor: the oracle is a property of the two formulas on the test's literal inputs, true whatever
    code is under test.
    Why: the docstring of the test says the two unbounded marginals are its control; a red that also moved them
    would carry a divergence the register does not record.
    Kills: a fit table with no upper-bounded marginal, with two of them, or with an unbounded marginal that has
    a transform; a seed that draws every probability at 0.5.
    Second correct implementation: other literal parameters with the same one bounded marginal.
    """
    seen: dict[str, Any] = {}

    def record(request: object, observed: Any, expected: Any, licence: object) -> None:
        """Keep the two values the test compared."""
        seen["observed"], seen["expected"] = observed, expected

    monkeypatch.setattr(module_under_review, "check", record)
    outcome = _run(monkeypatch, "E-19", _inverse_transform_cell(_reflected), tmp_path)
    assert outcome is None, f"E-19: the legacy-form stand-in did not reach the comparison: {outcome!r}"
    fits = module_under_review.MARGINAL_FITS
    bounded = [name for name, bound in zip(fits["data"], fits["upper_bound"], strict=True) if not np.isnan(bound)]
    assert len(bounded) == 1, f"E-19: the literal fit table carries {len(bounded)} upper-bounded marginals, not one"
    assert set(module_under_review.DATA_COLUMNS) == set(fits["data"])
    for column in module_under_review.DATA_COLUMNS:
        observed = np.asarray(seen["observed"][column].to_numpy(), dtype=float)
        expected = np.asarray(seen["expected"][column].to_numpy(), dtype=float)
        assert observed.shape == expected.shape and observed.size > 0
        if column in bounded:
            assert (np.abs(observed - expected) > 1e-6).all(), f"E-19: {column} agrees on some draw"
        else:
            assert np.allclose(observed, expected, rtol=1e-9, atol=0.0), f"E-19: {column} moved"


def test_the_pandas_the_test_binds_reads_the_one_literal_fit_table_and_refuses_any_other_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: the `pd` the E-19 test binds returns the literal fit table for the file the legacy text names
    under the bound directory, raises `FileNotFoundError` for another file that exists, and is pandas for
    everything else.

    Class: the bindings the E-19 test passes, captured. NEW CAPABILITY: the plausible wrong binding answers any
    file name with the table, so a cell that reads a second table reads the first one's values unseen; red under
    it because the other file is refused. The control is that the other file exists, so pandas itself would read it.
    Reachability: the cell reads `{dir}{event type}_distributions.csv` and the declaration's own pandas stand-in
    answers by file name; the binding the test passes replaces it.
    Both-states anchor: the oracle is the behaviour of the binding on three calls, true of any correct version.
    Why: a literal input the cell does not read is a vacuous green; a literal read where the cell reads another
    file is a red for a reason the code does not own.
    Kills: a binding that returns the table for every path; a binding that is not pandas for other attributes.
    Second correct implementation: a binding that serves the table from memory and not from disk.
    """
    captured: dict[str, Any] = {}
    inner = _inverse_transform_cell()

    def capturing(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: dict[str, Any] | None = None
    ) -> Any:
        """Keep the overrides, then behave as the faithful stand-in."""
        captured.update(overrides or {})
        return inner(unit, part, reduce, overrides)

    assert _run(monkeypatch, "E-19", capturing, tmp_path) is None
    binding = captured["pd"]
    other = tmp_path / "other_events_distributions.csv"
    pd.DataFrame({"data": ["unrelated"]}).to_csv(other, index=False)
    assert pd.read_csv(other)["data"].tolist() == ["unrelated"], "the control file is not readable"
    with pytest.raises(FileNotFoundError):
        binding.read_csv(str(other))
    assert binding.DataFrame is pd.DataFrame
    table = binding.read_csv(f"{captured['dir_fitting_maringals']}surge_events_distributions.csv")
    assert table["data"].tolist() == module_under_review.MARGINAL_FITS["data"].tolist()


def test_the_literal_fit_table_names_the_variables_the_real_cells_probability_table_carries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: the variable names of the E-19 test's literal fit table are exactly the column names of the
    probability table the real stored cell simulates.

    Class: the one real run of the E-19 cell. The cell names its probability columns from a table the lift
    declaration supplies and the test does not control, and the test looks each fit up by name in that table.
    HELD: green on the tree at the reviewed tip. NEW CAPABILITY: the plausible wrong change renames a variable
    in the declaration's table or in the literal fit table; red under it, with the two name sets in the message when
    the cell completes and a stop message when it does not, where the equation test alone would end on a `KeyError`
    that is not a divergence.
    Reachability: the equation test indexes the cell's probabilities by each fit's `data` name.
    Both-states anchor: the oracle is equality of two name sets, true whichever side a repair renames.
    Why: a red of any other kind than `ConformanceDivergence` is a defect of the run, and this coupling is the
    one place an unrelated edit of a shared fixture could produce one.
    Kills: renaming one variable in either the literal fit table or the declaration's probability table.
    Second correct implementation: a test that builds its fit table from the names the cell reports.
    """
    seen: dict[str, list[str]] = {}
    real = equation_check.implementation

    def recording(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: dict[str, Any] | None = None
    ) -> Any:
        """The real helper, noting the column names of the cell's probability table before the test's reduction."""

        def noting(finished: Any) -> Any:
            """Note the names, then reduce as the test does."""
            seen["columns"] = list(finished["df_simulated_cdf"].columns)
            return reduce(finished)

        return real(unit, part, noting, overrides)

    monkeypatch.setattr(module_under_review, "check", lambda *arguments, **keywords: None)
    monkeypatch.setattr(module_under_review, "implementation", recording)
    with contextlib.suppress(KeyError):
        _call(_function_for("E-19"), tmp_path)
    assert "columns" in seen, "the real cell stopped before it left its probability table, on the literal fit table"
    assert sorted(seen["columns"]) == sorted(module_under_review.MARGINAL_FITS["data"]), (
        f"the cell's probability columns {sorted(seen['columns'])} are not the fit table's variables "
        f"{sorted(module_under_review.MARGINAL_FITS['data'])}"
    )


# --- seeding and the global stream ---------------------------------------------------------------------------


def test_the_e19_test_seeds_numpys_global_stream_before_the_cell_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: the stream the cell draws from is the same when the test was entered with two different ambient
    streams, and differs from either of them.

    Class: E-19 under two ambient streams. NEW CAPABILITY: the plausible wrong test seeds after the
    implementation call, or around only the reference; red under it because the recorded stream follows the
    ambient one.
    Reachability: any earlier test or import may leave numpy's stream anywhere, and the legacy cell draws the
    rows it returns through `DataFrame.sample` with no generator, which reads that stream.
    Both-states anchor: the oracle is the stream at the moment of the call, a property of when the seed is set,
    not of how.
    Why: a draw the ambient state steers is a flaky red in a file nobody touched.
    Kills: removing the `seeded_global_numpy` block around the implementation call.
    Second correct implementation: `np.random.seed(seed)` before the call and a manual restore afterwards.
    """
    saved = np.random.get_state()
    recorded = []
    try:
        for ambient in (1, 2):
            np.random.seed(ambient)
            ambient_stream = np.random.get_state()[1].copy()
            seen: list[np.ndarray] = []
            inner = _inverse_transform_cell()

            def recording(
                unit: str,
                part: str | None,
                reduce: Callable[[Any], Any],
                overrides: dict[str, Any] | None = None,
                seen: list[np.ndarray] = seen,
                inner: Callable[..., Any] = inner,
            ) -> Any:
                """Note numpy's stream at the moment of the call, then behave as the faithful stand-in."""
                seen.append(np.random.get_state()[1].copy())
                return inner(unit, part, reduce, overrides)

            outcome = _run(monkeypatch, "E-19", recording, tmp_path)
            assert outcome is None, f"E-19: {outcome!r}"
            assert seen, "E-19: the stand-in was never called"
            assert not np.array_equal(seen[0], ambient_stream), "E-19: the stream was not seeded"
            recorded.append(seen[0])
    finally:
        np.random.set_state(saved)
    assert np.array_equal(recorded[0], recorded[1]), "E-19: the stream at the call follows the ambient state"


@pytest.mark.parametrize(
    ("equation_id", "build"),
    [
        pytest.param(
            "E-18",
            lambda tmp_path: _occurrences(tmp_path, preamble_years=_identity, bootstrap_rows=_chained_joint),
            id="E-18-faithful",
        ),
        pytest.param(
            "E-18",
            lambda tmp_path: _occurrences(
                tmp_path, preamble_years=_one_and_a_half_times, bootstrap_rows=_chained_joint
            ),
            id="E-18-diverging",
        ),
        pytest.param("E-19", lambda tmp_path: _inverse_transform_cell(), id="E-19-faithful"),
        pytest.param("E-19", lambda tmp_path: _inverse_transform_cell(_reflected), id="E-19-diverging"),
    ],
)
def test_an_equation_test_hands_numpys_global_stream_back_as_it_found_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, equation_id: str, build: Callable[[Path], Callable[..., Any]]
) -> None:
    """Property: after the equation test returns or raises, numpy's global stream is what it was on entry.

    Class: both equation tests, each with a faithful stand-in (the test returns) and a diverging one (the test
    raises). The stand-in for E-19 draws from the global stream, so a seed left in place shows in the stream.
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
        _run(monkeypatch, equation_id, build(tmp_path), tmp_path)
        after = np.random.get_state()
    finally:
        np.random.set_state(saved)
    assert before[0] == after[0]
    assert np.array_equal(before[1], after[1]) and before[2:] == after[2:], (
        f"{equation_id}: numpy's global stream was changed"
    )


# --- the cell's scratch directory ----------------------------------------------------------------------------


def test_the_e18_test_leaves_no_scratch_directory_of_the_preamble_run_behind(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Property: running the E-18 test against the real stored units, with the temporary-file directory redirected
    to an empty one, leaves that directory empty.

    Class: the real preamble unit, which makes a scratch directory and removes it in a reduction the test replaces.
    The control runs the same test with a reduction that does not remove it and finds one directory, so an empty
    result cannot be a blind detector. NEW CAPABILITY: the plausible wrong test replaces the declaration's
    reduction and forgets its cleanup; red under it because a directory is left per run.
    Reachability: every run of the family would leave one directory in the system's temporary directory.
    Both-states anchor: the oracle is the count of entries the run left, true of any way of cleaning up.
    Why: a test that leaks a directory per run is invisible in a green suite and fills a disk in a long one.
    Kills: dropping the removal from the E-18 test's preamble reduction.
    Second correct implementation: a reduction that removes the directory by another route.
    """
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(module_under_review, "check", lambda *arguments, **keywords: None)
    real = equation_check.implementation

    def leaking(
        unit: str, part: str | None, reduce: Callable[[Any], Any], overrides: dict[str, Any] | None = None
    ) -> Any:
        """The real helper, with a preamble reduction that removes nothing."""
        if unit == module_under_review.PREAMBLE_UNIT:
            return real(unit, part, lambda finished: int(finished["n_years_to_simulate"]), overrides)
        return real(unit, part, reduce, overrides)

    function = _function_for("E-18")
    monkeypatch.setattr(module_under_review, "implementation", leaking)
    _call(function, tmp_path)
    left = sorted(each for each in tmp_path.iterdir())
    assert len(left) == 1, f"the control run left {len(left)} entries, so this detector cannot see a leak: {left}"
    shutil.rmtree(left[0])
    monkeypatch.setattr(module_under_review, "implementation", real)
    _call(function, tmp_path)
    assert sorted(tmp_path.iterdir()) == [], "the test left a scratch directory behind"


# --- the stored legacy text ----------------------------------------------------------------------------------


@pytest.mark.parametrize("equation_id", EQUATION_IDS)
def test_an_equation_test_is_red_with_a_conformance_divergence_on_the_stored_legacy_text(
    tmp_path: Path, equation_id: str
) -> None:
    """Property: run unpatched, against the stored legacy text, each equation's test raises
    `ConformanceDivergence` for its own equation id, and raises nothing else.

    Class: both equations of this file; each is expected to be red against the stored text, and a red is the
    admissible state. NEW CAPABILITY: the plausible wrong test ends in an unrunnable lift, a `KeyError` from a
    variable name the lift's fixture does not carry, a `TypeError` or a `SystemExit`: a red that is not a
    divergence; or in a reference that equals what the legacy text returns: a green, so a recorded divergence is
    lost. Both are red here, the second because nothing is raised.
    Reachability: the test is called as pytest calls it, with its own marker, through the real discovery and
    the real stored extraction.
    Both-states anchor: the oracle is the exception type and its equation id; the paired tests above show the
    same functions green and red on implementations that follow and depart from the manuscript.
    Why: a red is admitted exactly when it is a `ConformanceDivergence`, and a red of any other kind is a defect
    of the run that a classifier would be handed as a divergence.
    Kills: a literal input that makes the legacy text raise inside itself; a bare `assert` in place of `check`.
    Second correct implementation: any test of the same equation that ends in `equation_check.check` and compares
    a manuscript-derived value with the legacy one, whatever its literals.
    Flip condition: an owning system lands a `src/` implementation that the stored text no longer stands in for;
    this test then asserts a red the flipped equation no longer owes.
    """
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        _call(_function_for(equation_id), tmp_path)
    assert refused.value.failure.equation_id == equation_id
    assert refused.value.failure.diverged is True


# --- the structure the family relies on ----------------------------------------------------------------------


def test_each_equation_test_carries_one_function_marker_naming_its_ledger_row() -> None:
    """Property: the module's marked functions are exactly the two equations of this file; each carries one
    `conformance` marker on the function, `unit` is its equation id, `part` is its own name, `system` is the
    system slug, and the ledger binds that id as a test of that name. The module sets no module-level marks.

    Class: every `conformance`-marked function of the module, found by marker and not by name. HELD: green.
    Reachability: the inventory the coverage check reads takes a function's own decorator and nothing else.
    Both-states anchor: the ledger and the marker are two homes of one fact; the oracle is their agreement.
    Why: a marker that names the wrong id or part makes the coverage check pass or fail on the wrong function.
    Kills: a module-level `pytestmark`; a `part` that is not the function name; a function bound to another id.
    Second correct implementation: the same two functions in a different order, or with extra helper functions.
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
    unmarked = [
        node.name
        for node in TREE.body
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_") and node.name not in functions
    ]
    assert not unmarked, f"test functions with no conformance marker: {unmarked}"


def _equation_function_nodes() -> list[ast.FunctionDef]:
    """The syntax nodes of the module's marked functions."""
    names = set(_equation_functions())
    return [node for node in TREE.body if isinstance(node, ast.FunctionDef) and node.name in names]


def test_no_equation_test_can_pass_by_skipping_or_by_swallowing_its_refusal() -> None:
    """Property: neither function is marked `skip`, `skipif` or `xfail`; the module calls no `pytest.skip`,
    `pytest.xfail` or `pytest.importorskip`; and no function contains an exception handler.

    Class: the two marked functions and the module's calls. HELD: green.
    Reachability: a divergence is a red by design; a skip or an expected failure or a handler around `check`
    turns it into a result the family's census counts as green.
    Both-states anchor: a syntactic property of the source, true of any correct version of these tests.
    Why: a diverging equation is a red that stays red; a hidden pass defeats the record that the code diverges.
    Kills: adding `@pytest.mark.xfail`, or wrapping the final `check` in `try/except AssertionError`.
    Second correct implementation: a `try/finally` that restores state, which is not a handler and is admitted.
    """
    for name, function in _equation_functions().items():
        assert not [mark for mark in function.pytestmark if mark.name in {"skip", "skipif", "xfail"}], name
    forbidden = {"skip", "xfail", "importorskip"}
    for node in ast.walk(TREE):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in forbidden:
            raise AssertionError(f"line {node.lineno}: a call to {node.func.attr}")
    for function in _equation_function_nodes():
        handlers = [node for node in ast.walk(function) if isinstance(node, ast.ExceptHandler)]
        assert not handlers, f"{function.name}: an exception handler at line {handlers[0].lineno}"


def test_each_equation_test_ends_in_one_check_call_and_computes_no_reference_through_the_comparison_module() -> None:
    """Property: each function makes exactly one call to `check` and it is the function's last statement; and the
    only names the function takes from the `compare` module are the two licences.

    Class: the two marked functions. HELD: green.
    Reachability: a reference computed through `compare` would make the comparison its own oracle, and a second
    `check` call or a statement after it would hide a refusal behind later code.
    Both-states anchor: a syntactic property of the source, true of any correct version of these tests.
    Why: each test computes its reference in its own body and ends in one `check`, so the reference is independent.
    Kills: a second `check` call; `compare.assert_equivalent(...)` in the body; a statement after the `check`.
    Second correct implementation: `equation_check.check(...)` instead of the bare name; admitted.
    """
    for function in _equation_function_nodes():
        calls = [
            node
            for node in ast.walk(function)
            if isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Name) and node.func.id == "check")
                or (isinstance(node.func, ast.Attribute) and node.func.attr == "check")
            )
        ]
        assert len(calls) == 1, f"{function.name}: {len(calls)} check calls"
        last = function.body[-1]
        assert isinstance(last, ast.Expr) and last.value is calls[0], (
            f"{function.name}: check is not the last statement"
        )
        taken = {
            node.attr
            for node in ast.walk(function)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "compare"
        }
        assert taken <= {"Tolerance", "Exact"}, f"{function.name}: takes {sorted(taken)} from compare"


@pytest.mark.parametrize("equation_id", EQUATION_IDS)
def test_each_equation_tests_docstring_quotes_the_registers_span_exactly(equation_id: str) -> None:
    """Property: the register's `verbatim_span` for the equation occurs, byte for byte, in the equation test's
    docstring once its common indentation is removed.

    Class: both equation functions. REGRESSION: red on the tree at the reviewed tip for E-18, whose span is
    wrapped at a different place in the docstring than in the register; output in the round file.
    Reachability: the docstring says "the register's span for this equation, verbatim", and a reader searching
    the manuscript or the register for the quoted text finds it only if it is the same bytes.
    Both-states anchor: the oracle is containment of the register's own text, true of any correct docstring.
    Why: a quotation that is re-wrapped is a different string; the family's other equation tests quote their spans
    exactly, and one that does not cannot be found by the search the docstring invites.
    Kills: re-wrapping a long span to the line length; dropping the line break or the indent inside a span.
    Second correct implementation: a docstring that quotes the span exactly and says more around it.
    """
    span = register.entry(equation_id)["verbatim_span"]
    docstring = inspect.getdoc(_function_for(equation_id))
    assert span, f"{equation_id}: the register carries no span"
    assert docstring is not None and span in docstring, f"{equation_id}: the docstring does not quote the span exactly"
