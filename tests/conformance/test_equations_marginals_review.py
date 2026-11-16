"""Review tests for the marginal-fitting equation test E-12 in `test_equations_marginals.py`.

An equation test of this family is red on build by design, and a red alone cannot tell a test that found
the recorded paper/code departure from a test whose own reference or plumbing is wrong: both end in the
same `ConformanceDivergence`, or in some other exception. The tests below separate the cases by giving the
equation test a SECOND implementation to run against, written here from the manuscript and sharing no text
with the legacy unit or with that test's own reference. A faithful second implementation must turn the test
green; each wrong form must turn it red with a `ConformanceDivergence` and with exactly the measure that
form gets wrong disagreeing; the stored legacy text must leave it red with a divergence. They also pin the
file's own shape: which function carries the conformance marker, which units it runs, that its expected
value does not come from the implementation's measures, and that the family list is the manuscript's.

The second implementations enter through `lift.run_lift`, the one engine the equation test's two calls to
`equation_check.implementation` must reach. The declaration lookup, the declared bindings and the test's own
reductions all still run; only the compiled legacy text is replaced by a namespace holding the candidate
list and a fitting function written here.

These carry no family marker, so they enter no inventory count. They are companion tests, and this file
guards `tests/conformance/test_equations_marginals.py` (a module test of it on promotion; no test here pins
an absolute count or a stored blob).

Not established: that the second implementation is the cited chapter's own wording. It takes the two
deviation indices and the Gringorten plotting position from the chapter's equations 2-31 to 2-33 as a
text extraction of that chapter prints them; the extraction drops the absolute-value bars of the first
index, which that index's name supplies, so the absolute form is read and not seen. A reader who holds
the printed page can confirm the bars.
"""

from __future__ import annotations

import ast
import functools
import math
import re
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from tests.conformance import bindings, equation_check, register, test_equations_marginals
from tests.correspondence import lift

SUBJECT = "tests.conformance.test_equations_marginals"
SYSTEM = "publication-math-conformance"
EQUATION_ID = "E-12"
EQUATION_TEST = "test_eq12_marginal_fit_and_metrics"

# The two units the equation test runs, by the name each is bound to in the tested file.
UNIT_NAMES = ("FITTING_UNIT", "CANDIDATE_UNIT")

# The six measures in the order the manuscript's span lists them, which is the column order of the
# value the equation test compares.
CVM, KS, AIC, BIC, MSDI, MADI = range(6)


def _subject() -> ModuleType:
    """The module under review, read from `sys.modules` so a probe that substitutes a variant of it is seen.

    The module is also imported by name at the top of this file, so the selection of tests to run after a
    change to it finds this file by its import and not only by its declared path.
    """
    return sys.modules.get(SUBJECT, test_equations_marginals)


def _manuscript_families() -> tuple[str, ...]:
    """The thirteen family names of the manuscript's list, in scipy's spelling, read from the register's span.

    The list is the bracketed one at the head of the span. A name the manuscript writes with a space is the
    scipy name with an underscore, which is the one rule this file applies to it.
    """
    span = register.entry(EQUATION_ID)["verbatim_span"]
    inside = re.search(r"\[(.*?)\]", span, re.DOTALL)
    assert inside is not None, "the register's span carries no bracketed list of families"
    return tuple("_".join(each.split()) for each in inside.group(1).split(","))


MANUSCRIPT_FAMILIES = _manuscript_families()


def _request_for(test: Callable[..., Any]) -> object:
    """A stand-in for pytest's request whose node answers with the test's own `conformance` marker.

    Reading the marker off the test function, rather than writing the id here, keeps the equation id the
    one the decorator states, which is the id `check` attributes a red to.
    """
    marker = next(each for each in test.pytestmark if each.name == "conformance")  # type: ignore[attr-defined]

    def get_closest_marker(name: str) -> object | None:
        """Return the test's conformance marker for that name and nothing for any other."""
        return marker if name == "conformance" else None

    return SimpleNamespace(node=SimpleNamespace(get_closest_marker=get_closest_marker))


def _candidates(names: tuple[str, ...]) -> list[dict[str, Any]]:
    """The candidate list a code would hold for `names`: each family object and its estimated-argument count.

    The count is the family's shape arguments plus its location and scale, read from scipy rather than
    written beside each name.
    """
    return [{"args": {"fx": getattr(stats, name), "n_params": getattr(stats, name).numargs + 2}} for name in names]


@functools.cache
def _maximum_likelihood(name: str, sample_bytes: bytes) -> tuple[float, ...]:
    """The fitted arguments of family `name` on the sample, one optimizer run per family and sample."""
    return tuple(float(each) for each in getattr(stats, name).fit(np.frombuffer(sample_bytes, dtype=float)))


def _measures(family: Any, parameters: tuple[float, ...], sample: np.ndarray) -> dict[str, float]:
    """The six measures the manuscript lists, for `family` at `parameters` over the sorted `sample`.

    Every one is computed by a route that differs from the equation test's reference: the frozen
    distribution's own `logpdf`, `cdf` and `ppf`, and scipy's plotting-position helper in place of the
    written formula.
    """
    frozen = family(*parameters)
    count = sample.size
    arguments = len(parameters)
    log_likelihood = float(frozen.logpdf(sample).sum())
    positions = stats.mstats.plotting_positions(sample, 0.44, 0.44)
    relative = (sample - frozen.ppf(positions)) / sample
    return {
        "cvm_pval": float(stats.cramervonmises(sample, frozen.cdf).pvalue),
        "ks_pval": float(stats.kstest(sample, frozen.cdf).pvalue),
        "aic": 2.0 * arguments - 2.0 * log_likelihood,
        "bic": arguments * math.log(count) - 2.0 * log_likelihood,
        "msdi": float(np.mean(relative**2)),
        "madi": float(np.mean(np.abs(relative))),
    }


Form = Callable[[dict[str, float], Any, tuple[float, ...], np.ndarray], dict[str, float]]


def _fit_dist(form: Form | None = None) -> Callable[..., tuple[pd.Series, pd.DataFrame]]:
    """A fitting function with the legacy function's signature and record layout, applying `form` to its measures.

    With no `form` the measures are the manuscript's. A `form` receives the correct measures, the family, the
    fitted arguments and the sample, and returns the measures a wrong implementation would report.
    """

    def fit_dist(
        s_data: pd.Series,
        fx: Any,
        n_params: int,
        upper_bound: float,
        fix_loc_to_min: bool,
        scalar_shift: float,
        normalize: bool,
        boxcox: bool,
        plot: bool = False,
        fname_savefig: str | None = None,
    ) -> tuple[pd.Series, pd.DataFrame]:
        """Return the fit record of one family on the sample, in the legacy record's index names."""
        sample = np.sort(s_data.to_numpy(dtype=float))
        parameters = _maximum_likelihood(fx.name, sample.tobytes())
        measures = _measures(fx, parameters, sample)
        if form is not None:
            measures = form(measures, fx, parameters, sample)
        record = pd.Series(
            {
                "data": s_data.name,
                "distribution": fx.name,
                "n_params": n_params,
                "shape": parameters[0] if n_params == 3 else math.nan,
                "loc": parameters[-2],
                "scale": parameters[-1],
                **measures,
            },
            dtype=object,
        )
        return record, pd.DataFrame()

    return fit_dist


def _run_against(
    monkeypatch: pytest.MonkeyPatch,
    fit_dist: Callable[..., Any],
    names: tuple[str, ...] = MANUSCRIPT_FAMILIES,
) -> None:
    """Make every `lift.run_lift` call hand the test's reduction a namespace holding the second implementation.

    The declaration's own bindings are built and discarded, so the helper's unit lookup, its override check
    and the stored texts' integrity checks still run on the real declarations; only the execution of the
    stored legacy text is replaced.
    """
    namespace = {"fxs": _candidates(names), "fit_dist": fit_dist}
    fixtures = {"series_positive": pd.Series([1.0, 2.0, 3.0], name="series_positive")}

    def run_lift(declaration: lift.LiftDeclaration) -> Any:
        """Build the declaration's bindings as the engine would, then reduce over the replacement namespace."""
        _bindings, reduce = declaration.function(fixtures)
        return reduce(namespace)

    monkeypatch.setattr(lift, "run_lift", run_lift)


def _disagreeing_measures(failure: equation_check.ConformanceFailure) -> set[int]:
    """The measure columns on which any family's observed value differs from its expected one at the licence's tolerance."""
    observed = failure.observed["metrics"].to_numpy()
    expected = failure.expected["metrics"].to_numpy()
    same = np.isclose(observed, expected, rtol=1e-9, atol=0.0)
    return {column for column in range(same.shape[1]) if not same[:, column].all()}


def _without_bic(measures: dict[str, float], *_rest: Any) -> dict[str, float]:
    """The legacy record's layout: an AIC and no Bayesian information criterion."""
    return {key: value for key, value in measures.items() if key != "bic"}


def _two_sample_ks(measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray):
    """A KS p-value from two samples (the observations against the fitted quantiles), not from the fitted CDF."""
    positions = stats.mstats.plotting_positions(sample, 0.44, 0.44)
    fitted = family(*parameters).ppf(positions)
    return {**measures, "ks_pval": float(stats.ks_2samp(sample, fitted).pvalue)}


def _cvm_statistic_for_pvalue(
    measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray
):
    """The Cramer-von Mises statistic reported where its p-value belongs."""
    return {**measures, "cvm_pval": float(stats.cramervonmises(sample, family(*parameters).cdf).statistic)}


def _aic_with_wrong_sign(measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray):
    """An AIC that adds twice the log-likelihood instead of subtracting it."""
    log_likelihood = float(family(*parameters).logpdf(sample).sum())
    return {**measures, "aic": 2.0 * len(parameters) + 2.0 * log_likelihood}


def _bic_with_base_ten_log(measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray):
    """A BIC whose penalty takes the base-ten logarithm of the sample size."""
    log_likelihood = float(family(*parameters).logpdf(sample).sum())
    return {**measures, "bic": len(parameters) * math.log10(sample.size) - 2.0 * log_likelihood}


def _msdi_with_unsquared_ratio(
    measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray
):
    """A mean square deviation index that squares the difference and divides by the observation, once."""
    positions = stats.mstats.plotting_positions(sample, 0.44, 0.44)
    fitted = family(*parameters).ppf(positions)
    return {**measures, "msdi": float(np.mean((sample - fitted) ** 2 / sample))}


def _madi_without_absolute_value(
    measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray
):
    """A mean absolute deviation index whose differences keep their sign."""
    positions = stats.mstats.plotting_positions(sample, 0.44, 0.44)
    fitted = family(*parameters).ppf(positions)
    return {**measures, "madi": float(np.mean((sample - fitted) / sample))}


def _indices_at_weibull_positions(
    measures: dict[str, float], family: Any, parameters: tuple[float, ...], sample: np.ndarray
):
    """Both deviation indices at the Weibull plotting position i / (N + 1), not the Gringorten one."""
    positions = np.arange(1, sample.size + 1) / (sample.size + 1)
    relative = (sample - family(*parameters).ppf(positions)) / sample
    return {**measures, "msdi": float(np.mean(relative**2)), "madi": float(np.mean(np.abs(relative)))}


# Each recorded or plausible wrong form, the measure columns that must disagree under it, and nothing else.
WRONG_FORMS: dict[str, tuple[Form, set[int]]] = {
    "no-bic-in-the-record": (_without_bic, {BIC}),
    "two-sample-ks": (_two_sample_ks, {KS}),
    "cvm-statistic-for-p-value": (_cvm_statistic_for_pvalue, {CVM}),
    "aic-with-wrong-sign": (_aic_with_wrong_sign, {AIC}),
    "bic-with-base-ten-log": (_bic_with_base_ten_log, {BIC}),
    "msdi-with-unsquared-ratio": (_msdi_with_unsquared_ratio, {MSDI}),
    "madi-without-absolute-value": (_madi_without_absolute_value, {MADI}),
    "indices-at-weibull-positions": (_indices_at_weibull_positions, {MSDI, MADI}),
}


def test_the_equation_test_passes_against_an_implementation_that_follows_the_manuscript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Property: with an implementation behind the call that lists the manuscript's thirteen families and
    reports the six measures as the manuscript defines them, the equation test returns without raising, so
    its red on the stored text is the text's disagreement and not the test's own error.

    Class: every implementation of the fitting function that follows the manuscript on the literal sample.
    NEW CAPABILITY: the plausible wrong implementation of the tested equation test renders its two sides in
    different layouts, orders the family rows differently from the paper's names, or reads a reference that
    equals a legacy choice; it is red here because the faithful value then differs from the expected one.
    Reachability: the call goes through the real `equation_check.implementation`, so both lift lookups, the
    test's two reductions, the family names and the six columns all run; only the compiled legacy text is
    replaced.
    Both-states anchor: the oracle is that no exception is raised on an implementation that follows the
    manuscript; it turns on the test's reference and reduction, which exist before and after any change to
    the legacy text.
    Why: a test that is red under every implementation reports a divergence whatever the code does, so the
    red carries no information about the code.
    Kills: a reference computed with a different layout; family rows ordered by the code's names and not the
    manuscript's; a tolerance too tight for two honest routes to the same log-likelihood.
    Second correct implementation: any fitting function that reports the six measures by the manuscript's
    definitions, which is what the one here does by a route that shares no text with the reference.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TEST)
    _run_against(monkeypatch, _fit_dist())
    test(_request_for(test))


@pytest.mark.parametrize("form_name", WRONG_FORMS)
def test_the_equation_test_is_red_on_each_wrong_measure_and_only_on_that_measure(
    monkeypatch: pytest.MonkeyPatch, form_name: str
) -> None:
    """Property: with a wrong form of one measure behind the call, the equation test raises
    `ConformanceDivergence` for its own equation id, and the measure columns that disagree are exactly the
    ones that form gets wrong.

    Class: the six measures, one wrong form each, and two wrong forms that move two columns. NEW CAPABILITY:
    the plausible wrong implementation of the tested equation test drops a measure from its rendered value,
    compares two measures against each other's reference, or loosens its tolerance until a different form
    passes; it is red here because the exception is not raised or because a column the form did not touch
    disagrees.
    Reachability: each wrong form differs from the faithful one only in the named measure, so the literal
    sample and the record layout are the whole of what this test needs the tested file to carry.
    Both-states anchor: the oracle is the exception, its equation id and the set of disagreeing columns of
    the failure's two values; none moves with the wording of any docstring, and the paired test above shows
    the same function green on the faithful form.
    Why: an input that cannot separate the manuscript's form of a measure from a recorded or plausible
    wrong one is a vacuous green on that measure once an implementation adopts the wrong form.
    Kills: a sample that makes two forms coincide; a measure left out of the compared value; one column
    compared with another column's reference.
    Second correct implementation: a test whose sample differs from the tested file's in value but still
    separates every form here, which passes the paired test above and this one.
    """
    form, columns = WRONG_FORMS[form_name]
    subject = _subject()
    test = getattr(subject, EQUATION_TEST)
    _run_against(monkeypatch, _fit_dist(form))
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        test(_request_for(test))
    assert refused.value.failure.equation_id == EQUATION_ID
    assert refused.value.failure.diverged is True
    assert _disagreeing_measures(refused.value.failure) == columns


@pytest.mark.parametrize(
    "names",
    [
        pytest.param(MANUSCRIPT_FAMILIES[:-1], id="one-family-fewer"),
        pytest.param((*MANUSCRIPT_FAMILIES, "expon"), id="one-family-more"),
        pytest.param((*MANUSCRIPT_FAMILIES[:-1], "expon"), id="one-family-replaced"),
    ],
)
def test_the_equation_test_is_red_with_a_conformance_divergence_when_the_family_list_differs(
    monkeypatch: pytest.MonkeyPatch, names: tuple[str, ...]
) -> None:
    """Property: with a candidate list that differs from the manuscript's thirteen names in length or in a
    name, and a fitting function that is otherwise faithful, the equation test raises `ConformanceDivergence`
    for its own equation id and raises nothing else.

    Class: candidate lists one family short, one family long and one family replaced. NEW CAPABILITY: the
    plausible wrong implementation of the tested equation test builds its expected family rows from the
    code's own list, so a list of another length cannot be assembled into the expected value and a
    `ValueError` ends the run before the comparison; the row's one structural comparison, the family list,
    can then never be reported as a divergence for a list of another length. It is red here because the
    exception is not a `ConformanceDivergence`.
    Reachability: the list enters through the candidate cell's reduction, as a changed legacy cell would
    give it, and the fitting function is the faithful one, so the list is the only difference.
    Both-states anchor: the oracle is the exception type and the equation id, a property of the comparison
    and not of any wording the test introduces.
    Why: a red of any other kind is a defect of the test that a classifier would be handed as a divergence,
    and the register records the family list as one of the two things this equation states.
    Kills: expected rows built over the code's names; an expected value whose family dimension takes its
    size from the code; a reduction that indexes the paper's list by the code's.
    Second correct implementation: a test that builds the expected rows over the manuscript's own names and
    compares the family variable and the measure array in one value.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TEST)
    _run_against(monkeypatch, _fit_dist(), names)
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        test(_request_for(test))
    assert refused.value.failure.equation_id == EQUATION_ID
    assert refused.value.failure.diverged is True


def test_the_equation_test_is_red_with_a_conformance_divergence_on_the_stored_legacy_text() -> None:
    """Property: run unpatched, against the stored legacy text, the equation test raises
    `ConformanceDivergence` for its own equation id and raises nothing else; the family list the text holds
    agrees with the manuscript's, and the record's Bayesian information criterion is the empty slot.

    Class: the one equation this file owns. NEW CAPABILITY: the plausible wrong implementation is a
    reference that happens to equal what the legacy text returns (a green, so the recorded absence is lost)
    or one whose run ends in an unrunnable lift, a `TypeError` or a `SystemExit` (a red that is not a
    divergence); both are red here, the first because nothing is raised.
    Reachability: the test is called as pytest calls it, with its own marker, through the real discovery and
    the real stored extractions of both units.
    Both-states anchor: the oracle is the exception type, its equation id and the two values the failure
    carries; the paired tests above show the same function green and red on implementations that follow and
    depart from the manuscript.
    Why: a red is admitted exactly when it is a `ConformanceDivergence`, and a red whose cause is the
    plumbing would record a divergence the text does not have.
    Kills: a reference computed from the returned record; an observed value that fills a missing criterion
    with a number; a family list read from the paper on both sides.
    Second correct implementation: any test of the same equation that ends in `equation_check.check` and
    compares a manuscript-derived value with the legacy one, whatever its sample.
    Flip condition: an owning system lands an implementation that the stored text no longer stands in for;
    this test then asserts a red the flipped equation no longer owes.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TEST)
    with pytest.raises(equation_check.ConformanceDivergence) as refused:
        test(_request_for(test))
    failure = refused.value.failure
    assert failure.equation_id == EQUATION_ID
    assert failure.diverged is True
    assert sorted(failure.observed["distribution"].to_numpy().tolist()) == sorted(MANUSCRIPT_FAMILIES)
    assert failure.observed["distribution"].to_numpy().tolist() == failure.expected["distribution"].to_numpy().tolist()
    assert np.isnan(failure.observed["metrics"].to_numpy()[:, BIC]).all()
    assert np.isfinite(failure.expected["metrics"].to_numpy()).all()


def test_the_literal_sample_gives_a_finite_comparison_for_every_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the faithful implementation behind the call, the value the equation test hands to
    `check` holds thirteen families and six measures, every one finite, and every p-value in the unit
    interval to within the 1e-6 by which scipy's Cramer-von Mises approximation can exceed one.

    Class: the literal sample the tested file chooses. NEW CAPABILITY: the plausible wrong sample is one on
    which a density product underflows, so a log-likelihood is infinite or a
    p-value is undefined; the comparison then agrees on `inf` or on a NaN for an arithmetic reason and
    leaves the definition untested. It is red here because a non-finite value appears.
    Reachability: `check` is replaced by a recorder, so the value is the one the test builds and no
    comparison hides a non-finite entry.
    Both-states anchor: the oracle is finiteness and the interval, a property of the values and not of any
    constant or wording in the tested file.
    Why: the tested file's docstring states that the sample is chosen so that no measure is infinite, and a
    sample edit that breaks it makes the test agree where it should compare.
    Kills: a sample of a few points; a sample with a zero or a negative value; an unsorted sample.
    Second correct implementation: any other literal sample for which every log-likelihood is finite.
    """
    subject = _subject()
    test = getattr(subject, EQUATION_TEST)
    recorded: dict[str, Any] = {}

    def record(request: object, observed: Any, expected: Any, licence: object) -> None:
        """Keep the two values `check` was handed."""
        recorded["observed"], recorded["expected"] = observed, expected

    _run_against(monkeypatch, _fit_dist())
    monkeypatch.setattr(equation_check, "check", record)
    test(_request_for(test))
    for side in ("observed", "expected"):
        metrics = recorded[side]["metrics"].to_numpy()
        assert metrics.shape == (len(MANUSCRIPT_FAMILIES), 6)
        assert np.isfinite(metrics).all()
        assert ((metrics[:, [CVM, KS]] >= 0.0) & (metrics[:, [CVM, KS]] <= 1.0 + 1e-6)).all()
    sample = np.asarray(subject.SAMPLE)
    assert sample.size >= 30
    assert (sample > 0).all()
    assert (np.diff(sample) > 0).all()


def test_the_tested_files_family_list_is_the_manuscripts_thirteen() -> None:
    """Property: the tested file's list of families equals, as a set, the thirteen names in the bracketed
    list at the head of the register's span for this equation, in scipy's spelling, and carries no
    duplicate.

    Class: the module-level family list of the tested file. NEW CAPABILITY: the plausible wrong
    implementation edits the list to match the code's (a family added, dropped or renamed), which makes the
    family comparison agree with the code by construction; it is red here because the list no longer equals
    the register's.
    Reachability: the register entry is read through the register package, and the names are the span's own.
    Both-states anchor: the oracle is the register's text, which the tested file does not write and the
    list's edit does not move.
    Why: the row's family-list comparison is the manuscript's list against the code's, and a list copied
    from the code compares the code with itself.
    Kills: a list of twelve; `sep` in place of `exponpow`; a list sorted differently with a name added.
    Second correct implementation: the same thirteen names in any order, or as a frozenset.
    """
    listed = tuple(_subject().PAPER_FAMILIES)
    assert MANUSCRIPT_FAMILIES
    assert all(hasattr(stats, each) for each in MANUSCRIPT_FAMILIES)
    assert len(set(listed)) == len(listed)
    assert set(listed) == set(MANUSCRIPT_FAMILIES)


def test_the_files_conformance_function_is_the_ledger_row_and_carries_no_skip_or_xfail() -> None:
    """Property: the module's conformance-marked functions are exactly the one named for this equation in the
    ledger, carrying one decorator-level marker whose `unit` is the id, whose `part` is its own name and
    whose `system` is the system slug, and no skip, no xfail and no parametrize.

    Class: the module's functions carrying a `conformance` marker. NEW CAPABILITY: the plausible wrong
    implementation marks the test at module level, names it by a typo, or xfails it so its divergence is
    not red.
    Reachability: the static inventory reads only a function's own decorator, so a module-level marker or a
    name that disagrees with the ledger is a test the coverage check cannot see.
    Both-states anchor: the oracle is the ledger's `test` key and the marker's own keywords; neither moves
    with the wording of any docstring.
    Why: the test name and the decorator site are fixed by the ledger, and a red hidden behind xfail or
    skip is a recorded divergence lost.
    Kills: `pytestmark = ...` at module level; a function renamed from the ledger's name; an added
    `@pytest.mark.xfail`; a second conformance function for the id.
    Second correct implementation: the same function with other helpers beside it in the module.
    """
    subject = _subject()
    carried = {
        name: each
        for name, each in vars(subject).items()
        if callable(each) and any(mark.name == "conformance" for mark in getattr(each, "pytestmark", []))
    }
    assert not hasattr(subject, "pytestmark")
    assert sorted(carried) == [EQUATION_TEST]
    row = bindings.load()[EQUATION_ID]
    assert row["binding"] == "TEST"
    assert row["test"] == EQUATION_TEST
    marks = carried[EQUATION_TEST].pytestmark
    assert [mark.name for mark in marks] == ["conformance"]
    assert marks[0].kwargs == {"unit": EQUATION_ID, "part": EQUATION_TEST, "system": SYSTEM}


def _equation_check_calls(function: ast.FunctionDef, attribute: str) -> list[ast.Call]:
    """Every call of `equation_check.{attribute}(...)` anywhere in the function, nested functions included."""
    return [
        each
        for each in ast.walk(function)
        if isinstance(each, ast.Call)
        and isinstance(each.func, ast.Attribute)
        and each.func.attr == attribute
        and isinstance(each.func.value, ast.Name)
        and each.func.value.id == "equation_check"
    ]


def test_the_equation_test_runs_its_two_units_and_ends_in_one_check_under_a_tolerance() -> None:
    """Property: in the equation test, exactly two `equation_check.implementation` calls, one for each unit
    constant the file names and both naming a unit one lift declaration carries; exactly one
    `equation_check.check` call, the last statement of the function, handed four arguments of which the
    fourth is a `compare.Tolerance`; and no lift engine and no other comparison in its body.

    Class: the equation test function of the file (a FITNESS FUNCTION over its source; the property is stated
    without reference to the equation's values). NEW CAPABILITY: the plausible wrong implementation runs a
    legacy text itself instead of through the helper, compares with a bare `assert` or a second `check`, or
    reads a unit no declaration carries; each is red here.
    Reachability: the test obtains its observed value only through the helper, "never through a legacy text
    and never through `compare`", and the lookup in the helper refuses an undeclared unit at run time, which
    this test restates statically.
    Both-states anchor: the oracle is a walk over the function's own calls and the real declarations; it does
    not turn on any literal, name or comment the change introduces.
    Why: a comparison that does not end in `check` cannot produce a `ConformanceDivergence`, and a run of a
    legacy text outside the helper escapes the stop-marker recorder the declaration binds.
    Kills: a call to `lift.run_lift` or `compare.assert_equivalent` in the body; a second `check`; a unit
    string typed in place of the constant; a `compare.Exact` licence on the measures.
    Second correct implementation: a test that calls the two units in the other order or through differently
    named locals.
    """
    path = Path(_subject().__file__)
    module = ast.parse(path.read_text(encoding="utf-8"))
    function = next(each for each in module.body if isinstance(each, ast.FunctionDef) and each.name == EQUATION_TEST)
    implementation_calls = _equation_check_calls(function, "implementation")
    check_calls = _equation_check_calls(function, "check")
    assert sorted(each.args[0].id for each in implementation_calls if isinstance(each.args[0], ast.Name)) == sorted(
        UNIT_NAMES
    )
    assert len(implementation_calls) == 2
    assert len(check_calls) == 1
    final = function.body[-1]
    assert isinstance(final, ast.Expr)
    assert final.value is check_calls[0]
    assert len(check_calls[0].args) == 4
    licence = check_calls[0].args[3]
    assert isinstance(licence, ast.Call)
    assert isinstance(licence.func, ast.Attribute)
    assert licence.func.attr == "Tolerance"

    names = {each.id for each in ast.walk(function) if isinstance(each, ast.Name)}
    assert not names & {"lift", "run_lift"}
    comparison_attributes = {
        each.attr
        for each in ast.walk(function)
        if isinstance(each, ast.Attribute) and isinstance(each.value, ast.Name) and each.value.id == "compare"
    }
    assert comparison_attributes == {"Tolerance"}

    declared = lift.discover()
    for constant in UNIT_NAMES:
        unit = getattr(_subject(), constant)
        assert [each.part for each in declared if each.unit == unit] == [None]
