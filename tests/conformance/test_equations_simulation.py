r"""The two simulation equations of the SWG supplement, each checked against the code that runs it.

Every function here is one equation of the supplementary manuscript's simulation section, named by the name the
equation register proposes for it and carrying that equation's id in its conformance marker. The observed side is
the legacy unit that executes the published mathematics today, reached as its stored extraction through the lift
harness and run by `equation_check.implementation` (nothing under `src/` implements these equations yet). The
reference side is the manuscript's formula, computed in the test's own body over literal inputs. One `check` call
compares them, so a disagreement arrives as a `ConformanceDivergence` carrying the ledger's binding for that row.

Both diverge, and each divergence is the register's own record rather than a surprise. E-18 asks for `y_g` years of
bootstrapped annual event counts and the code's preamble asks its bootstrap for `ceil(1.5 * y_g)` of them, so eight
requested years are drawn as twelve rows. E-19 asks for the real-space draw `T^{-1}(F^{-1}(u))` of each marginal,
and the cell reflects an upper-bounded marginal's simulated probability (`1 - u`) before the inverse distribution,
which the paper's composition does not contain.

What this module does NOT establish: that the class a reader assigns to each divergence (D1..D9) is right. A
divergence record carries the two values and the ledger's binding; class assignment is `taxonomy.classify` over an
authored diagnosis, and no test here authors one. Nor does it establish that the literal inputs span each
equation's domain: each row names the case it discriminates, and that case is the divergence it exists to see.
"""

from __future__ import annotations

import pathlib
import shutil

import numpy as np
import pandas as pd
import pytest
import pyvinecopulib as pv
import xarray as xr
from scipy import stats

from tests.conformance.equation_check import check, implementation, seeded_global_numpy
from tests.correspondence import compare

# The legacy units the two equations run — the register's implementation unit for each: E-18's two halves are the
# simulation script's preamble, which decides how many years to draw, and the cell that performs the bootstrap
# draw; E-19's is the cell that turns each copula-simulated probability back into a real-space value.
PREAMBLE_UNIT = "_b6_simulate_from_vincop.py:1-99"
BOOTSTRAP_UNIT = "_b6_simulate_from_vincop.py:479-488"
INVERSE_TRANSFORM_UNIT = "_b6_simulate_from_vincop.py:489-596"

# The comparison policy these tests take by default, the equivalence family's own: numbers within `1e-9 * |reference|`,
# and every other kind exact. `Tolerance` loosens numeric data and nothing else, so a boolean, integer or label carried
# in the same value is still compared exactly.
TOLERANCE = compare.Tolerance(rtol=1e-9, atol=0.0)

# The one seed each stochastic call sets. E-18's bootstrap names its own seed in the legacy text, and E-19's cell draws
# through `DataFrame.sample(random_state=None)` for the returned rows, so numpy's global state is the only channel that
# steers that draw.
SEED = 20240118

# E-18's literal inputs. The requested year count is the span's `y_g`; the observed table is the joint count vector the
# bootstrap resamples, with one row per observed 365-day window and `any` its per-row total.
YEARS_REQUESTED = 8
BOOTSTRAP_SEED = 7
COUNT_COLUMNS = ["surge", "rain", "compound"]
OBSERVED_OCCURRENCES = pd.DataFrame(
    {
        "surge": [2, 1, 3, 1, 2, 1],
        "rain": [0, 2, 1, 0, 1, 1],
        "compound": [1, 0, 1, 2, 0, 1],
        "any": [3, 3, 5, 3, 3, 3],
    }
)


@pytest.mark.conformance(unit="E-18", part="test_eq18_occurrence_bootstrap", system="publication-math-conformance")
def test_eq18_occurrence_bootstrap(request: pytest.FixtureRequest) -> None:
    r"""E-18: the annual event counts of `y_g` years, bootstrapped from the observed years' joint count vectors.

    The register's span for this equation, verbatim:

    \boldsymbol{n}_{\text{obs}}=\{[n_{\text{rain},1}, n_{\text{surge},1}, n_{\text{combined},1}], \dots, [n_{\text{rain},y}, n_{\text{surge},y}, n_{\text{combined},y}]\}$$
        \item Bootstrap $\boldsymbol{n}_{\text{obs}}$ $y_g$ times, yielding annual event counts for $y_g$ years, $\boldsymbol{n}_g$

    The span is the register's own string, unwrapped: its first line is one source line there, and its second
    begins with the four spaces the manuscript's item environment carries.

    Discriminating inputs. Eight years are requested, so the two readings separate on the row count alone and the
    separation does not depend on which rows the draw happens to pick: the manuscript asks for `y_g` annual vectors,
    and the preamble computes `ceil(1.5 * 8)` = twelve to cover invalid simulated events (`_b6:17`) before the
    bootstrap draws its `n_years_to_simulate` rows. The test therefore chains the two halves the legacy script chains:
    the preamble's number of years is handed to the bootstrap as its draw size, and the two compared components are
    what that chain produces.

    The second component is the register's other recorded property, and this row's control: the bootstrap resamples
    the joint vector, so every drawn row is one of the observed joint rows rather than three independently drawn
    counts. That is what the manuscript's notation implies, and it holds — so the count alone carries the divergence,
    and a value that also failed this second component would be a defect of the test rather than a second finding.

    The manuscript's third count vector is named `combined` and the code's column is `compound`; the row set is taken
    over the code's own three count columns on both sides, so the naming moves nothing.

    One value carries both components, each rendered as its own kind: the count, which is a number, and the
    containment, which is a boolean.
    """

    def reduce_preamble(finished):
        """The number of years the preamble asks the bootstrap to draw, and the removal of its scratch directory."""
        years = int(finished["n_years_to_simulate"])
        # The declaration builds its fixture tables in a temporary directory and removes it in its own reduction,
        # which `implementation` replaces, so the test removes it here instead of leaving one behind per run.
        shutil.rmtree(pathlib.Path(finished["dir_simulate_from_copula"]).parent, ignore_errors=True)
        return years

    drawn_years = implementation(
        PREAMBLE_UNIT, None, reduce_preamble, {"n_years_of_weather_to_generate": YEARS_REQUESTED}
    )

    observed_joint_rows = {
        tuple(int(value) for value in row) for row in OBSERVED_OCCURRENCES[COUNT_COLUMNS].itertuples(index=False)
    }

    def reduce_bootstrap(finished):
        """How many annual vectors the cell drew, and whether each of them is one of the observed joint vectors."""
        drawn = finished["df_occurences_sim"]
        drawn_joint_rows = {tuple(int(value) for value in row) for row in drawn[COUNT_COLUMNS].itertuples(index=False)}
        return len(drawn), drawn_joint_rows <= observed_joint_rows

    years_drawn, every_drawn_row_is_observed = implementation(
        BOOTSTRAP_UNIT,
        None,
        reduce_bootstrap,
        {
            "df_occurences_obs": OBSERVED_OCCURRENCES,
            "n_years_to_simulate": drawn_years,
            "n_years_of_weather_to_generate": YEARS_REQUESTED,
            "random_seed": BOOTSTRAP_SEED,
        },
    )

    def rendered(years, every_row_observed):
        """One value holding the count of drawn years beside the containment the span's joint vector gives."""
        return xr.Dataset({"years_drawn": ((), years), "every_drawn_row_is_observed": ((), every_row_observed)})

    check(
        request,
        rendered(years_drawn, every_drawn_row_is_observed),
        rendered(YEARS_REQUESTED, True),
        compare.Exact(),
    )


# E-19's literal inputs. The fit table is one row per simulation variable, in the columns the legacy fitting step
# saves; the third row is the one the register records, an upper-bounded marginal whose cdf values the cell flips.
# The two unbounded rows are this row's control: their composition is `T^{-1}` by identity, so they agree with the
# manuscript's formula and carry nothing about the reflection.
DATA_COLUMNS = ["precip_depth_mm", "surge_peak_ft", "duration_hr"]
MARGINAL_COLUMNS = [
    "data",
    "distribution",
    "n_params",
    "shape",
    "loc",
    "scale",
    "upper_bound",
    "boxcox",
    "lambda",
    "scalar_shift_for_boxcox",
    "normalize",
    "mean",
    "std",
    "scalar_shift",
]
NAN = float("nan")
MARGINAL_FITS = pd.DataFrame(
    [
        ("precip_depth_mm", "norm", 2, NAN, 1.0, 0.5, NAN, False, NAN, 0.0, False, NAN, NAN, 0.0),
        ("surge_peak_ft", "norm", 2, NAN, 10.0, 1.0, NAN, False, NAN, 0.0, False, NAN, NAN, 0.0),
        ("duration_hr", "gamma", 3, 2.0, 0.0, 6.0, 60.0, False, NAN, 0.0, False, NAN, NAN, 0.0),
    ],
    columns=MARGINAL_COLUMNS,
)
# The cell classifies each simulated row against the smallest observed peak and depth, so the two literal event tables
# below set thresholds no drawn row crosses on the rain side and every drawn row crosses on the surge side: the drawn
# rows are all surge events, none is a compound event, and the cell's own four count checks therefore agree.
SURGE_EVENTS = pd.DataFrame({"surge_peak_ft": [2.0, 3.0]})
RAIN_EVENTS = pd.DataFrame({"precip_depth_mm": [10.0, 25.0]})
# Five surge events over three observed years, so the cell over-simulates ten rows and returns five. Rain-only and
# compound counts are zero, which is what makes the cell's count checks agree.
OCCURRENCES = pd.DataFrame({"surge": [2, 1, 2], "rain": [0, 0, 0], "compound": [0, 0, 0], "any": [2, 1, 2]})


@pytest.mark.conformance(unit="E-19", part="test_eq19_inverse_transform", system="publication-math-conformance")
def test_eq19_inverse_transform(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> None:
    r"""E-19: each marginal's real-space value, the copula's simulated probability put back through its fit.

    The register's span for this equation, verbatim:

    \{T_{1}^{-1}(F_{1}^{-1}(u_{1})),\dots,\{T_{d}^{-1}(F_{d}^{-1}(u_{d}))\}

    Discriminating inputs. The fit table's third row carries an upper bound, which is the case the register records:
    the cell reflects that marginal's simulated probability (`1 - u`) before the inverse distribution, and composites
    the result with the back-transform's own reversal of the bound, while the manuscript's composition applies
    `F^{-1}` to `u` as the copula produced it. The two forms differ on every draw of that column, so no seed decides
    the outcome. The first two rows carry no transform and no bound, where both forms reduce to `F^{-1}(u)`: they
    separate a wrong reading of the general composition from the reflection alone.

    The value compared is taken over the cell's OWN simulated probabilities, not over a probability grid the test
    invents, so the reference measures the composition the cell performed rather than a composition of the test's
    choosing. The fit parameters, the bound and the distribution family are the test's literals; the probabilities
    are the cell's.

    The reading this equation needs, and it is not the register's defect note. `T` is the transform the fit was taken
    on, so for an upper-bounded marginal it is the reflection `x -> bound - x`, and `T^{-1}` is the same reflection:
    the reference therefore reads `bound - F^{-1}(u)`. Applying `F^{-1}` to `1 - u` as well is the cell's addition,
    which the paper's composition does not state.

    The cell reads its fit table from `dir_fitting_maringals`, and that binding alone does not govern the table: the
    declaration's `pd` binding answers `read_csv` by file name with a committed fixture, so the test binds both — the
    directory to `tmp_path` and `pd` to a stand-in that reads the literal table from that directory and refuses any
    other file. Nothing else the cell reads is a fit.
    """
    fit_table_path = tmp_path / "surge_events_distributions.csv"
    MARGINAL_FITS.to_csv(fit_table_path, index=False)

    class PdBinding:
        """pandas as the cell names it, except that `read_csv` reads the one literal fit table and refuses any other file."""

        def __getattr__(self, name):
            """Everything but `read_csv` is pandas' own."""
            return getattr(pd, name)

        def read_csv(self, path, **kwargs):
            """The literal fit table, for the one file the cell names; any other file is an error."""
            if pathlib.Path(path).resolve() != fit_table_path.resolve():
                raise FileNotFoundError(f"the test binds no csv named {path}")
            return pd.read_csv(path, **kwargs)

    def reduce(finished):
        """The cell's own simulated probabilities, and the real-space value it produced for each marginal."""
        draws = finished["df_simulated_cdf"].copy()
        frame = finished["dic_full_over_sim"]["surge_events"]
        return draws, {column: frame[column].to_numpy(dtype=float) for column in DATA_COLUMNS}

    with seeded_global_numpy(SEED):
        draws, observed = implementation(
            INVERSE_TRANSFORM_UNIT,
            None,
            reduce,
            {
                # One event type only, so the cell's per-type loop runs once and its last simulated probability table
                # is the one the compared real-space frame was built from. The copula is the independence vine over
                # the table's three variables: the transform composition is this row's subject, not the dependence.
                "dic_copulas": {"surge_events": pv.Vinecop(len(DATA_COLUMNS))},
                "df_occurences_sim": OCCURRENCES,
                "df_rain_events": RAIN_EVENTS,
                "df_surge_events": SURGE_EVENTS,
                "dir_fitting_maringals": f"{tmp_path}/",
                "pd": PdBinding(),
            },
        )

    def expected_realspace(probabilities):
        """`T^{-1}(F^{-1}(u))` for each fit: the fitted distribution's inverse at the cell's own `u`, then the transform reversed."""
        columns = {}
        for _index, fit in MARGINAL_FITS.iterrows():
            distribution = getattr(stats, fit["distribution"])
            parameters = (
                [fit["loc"], fit["scale"]] if fit["n_params"] == 2 else [fit["shape"], fit["loc"], fit["scale"]]
            )
            values = np.asarray(distribution.ppf(probabilities[fit["data"]].to_numpy(dtype=float), *parameters))
            if not np.isnan(fit["upper_bound"]):
                values = fit["upper_bound"] - values
            columns[fit["data"]] = np.asarray(values, dtype=float)
        return columns

    expected = expected_realspace(draws)

    def rendered(columns):
        """One variable per marginal, named, so the comparison reads each column's own value rather than a position."""
        return xr.Dataset({column: ("draw", columns[column]) for column in DATA_COLUMNS})

    check(request, rendered(observed), rendered(expected), TOLERANCE)
