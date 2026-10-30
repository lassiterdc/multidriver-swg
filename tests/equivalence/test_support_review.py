"""Check that each of the four statistical tests of each draw-law licence rejects, on its own, a sample made with the defect it exists to see.

A draw-law licence admits draws only when its four tests all pass, so a sample with any defect fails the licence
whichever test sees it. That makes the licence's verdict insensitive to one of its four tests being weakened: a
test that always passes leaves every defect to the other three. The tests below take the figure the licence
reports for each test (`DrawLaw.p_values`) and require the named test, alone, to reject a sample that holds the
defect it is meant for. Every sample satisfies the licence's exact preconditions, which `p_values` enforces, so
each is output the registered callable could return.
"""

import importlib.util

import numpy as np
import pandas as pd
import pytest

from tests.equivalence import _support

pytestmark = pytest.mark.state_check

HAS_SCIPY = importlib.util.find_spec("scipy") is not None
HAS_VINE = HAS_SCIPY and importlib.util.find_spec("pyvinecopulib") is not None
needs_scipy = pytest.mark.skipif(not HAS_SCIPY, reason="the statistical tests need scipy")
needs_vine = pytest.mark.skipif(not HAS_VINE, reason="the vine-copula law needs pyvinecopulib and scipy")

R = _support.DRAWS
ODD = np.arange(1, 20, 2)
WEIGHTS = 1 / np.arange(1, 8)
YEARS = np.array([2019, 2020, 2022])
TIDE = pd.Series({"window_hours": 336, "tstep_hours": 1, "valid_years": YEARS.tolist()})


def _rng(seed: int) -> np.random.Generator:
    """A generator seeded with a literal, so each sample below is the same on every run."""
    return np.random.default_rng(seed)


def _uniform_subsets(rng, pool: int, rows: int = R, k: int = 20) -> np.ndarray:
    """`rows` uniformly ordered k-subsets of `pool` positions."""
    return np.argsort(rng.random((rows, pool)), axis=1)[:, :k]


def _half_sorted(rng) -> np.ndarray:
    """Subsets of forty positions of which the first half of the rows come back in sorted order and the rest in random order."""
    rows = _uniform_subsets(rng, 40)
    rows[: R // 2] = np.sort(rows[: R // 2], axis=1)
    return rows


def _skewed_picks(rng, shape) -> np.ndarray:
    """Picks over twenty rows in which the first has probability 0.06 and each of the other nineteen has 0.94 / 19, about 0.0495."""
    p = np.full(20, 0.94 / 19)
    p[0] = 0.06
    return rng.choice(20, size=shape, p=p)


def _odd_offset_pairs(rng, pairs: int) -> np.ndarray:
    """`pairs` pairs (a, b), a uniform over twenty rows and b = a plus an offset that is 0 or odd, so b stays uniform and repeats stay at 1 in 20."""
    a = rng.integers(0, 20, pairs)
    offset = np.where(rng.random(pairs) < 0.05, 0, rng.choice(ODD, pairs))
    return np.stack([a, (a + offset) % 20], axis=1)


def _plackett_first_only(rng) -> np.ndarray:
    """Permutations whose first slot follows the weights and whose other slots are uniform."""
    keys = rng.random((R, 7))
    keys[np.arange(R), rng.choice(7, R, p=WEIGHTS / WEIGHTS.sum())] = -1
    return np.argsort(keys, axis=1) + 1


def _tide_rows(rng, years_p=None) -> np.ndarray:
    """(shift, year) rows: a rounded uniform shift and a year drawn with probabilities `years_p`."""
    shift = np.rint(rng.uniform(-168, 168, R)).astype(int)
    return np.column_stack([shift, rng.choice(YEARS, R, p=years_p)])


def _vine():
    """The three-dimensional D-vine the licence's tests are run on, its reference and a correct sample of it."""
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
    return reference, pd.DataFrame(copula.sample(R, seeds=[7, 11, 13]), columns=columns)


def _case(law, index, reference, sample, name):
    """One parametrized case: the licence factory, the zero-based index of the test, the reference, a sample builder and its name."""
    return pytest.param(law, index, reference, sample, id=name)


SUBSET = pd.Series({"pool": 40, "k": 20})
ROWS = pd.Series({"rows": 20})
STREAMS = pd.Series({"rows1": 20, "rows2": 20})


def _streams(first, second=None):
    """Two (10,000, 10) pick arrays, the second independent uniform unless given."""
    return first, second if second is not None else _rng(5).integers(0, 20, (10_000, 10))


CASES = [
    _case("subset_law", 0, SUBSET, lambda: _uniform_subsets(_rng(1), 39), "subset-S1-a-pool-row-never-chosen"),
    _case("subset_law", 1, SUBSET, lambda: _half_sorted(_rng(2)), "subset-S2-half-the-rows-sorted"),
    _case(
        "subset_law",
        2,
        SUBSET,
        lambda: _rng(3).integers(0, 21, (R, 1)) + np.argsort(_rng(4).random((R, 20)), axis=1),
        "subset-S3-contiguous-windows",
    ),
    _case(
        "subset_law",
        3,
        SUBSET,
        lambda: np.repeat(_uniform_subsets(_rng(6), 40, R // 2), 2, axis=0),
        "subset-S4-consecutive-realizations-identical",
    ),
    _case("bootstrap_law", 0, ROWS, lambda: _skewed_picks(_rng(7), R), "bootstrap-W1-one-row-too-likely"),
    _case(
        "bootstrap_law",
        1,
        ROWS,
        lambda: _odd_offset_pairs(_rng(8), R // 2).ravel(),
        "bootstrap-W2-pairs-confined-to-odd-offsets",
    ),
    _case(
        "bootstrap_law",
        2,
        ROWS,
        lambda: _uniform_subsets(_rng(9), 20, 10_000, 10),
        "bootstrap-W3-resamples-without-replacement",
    ),
    _case(
        "bootstrap_law",
        3,
        ROWS,
        lambda: np.sort(_rng(10).integers(0, 20, R)),
        "bootstrap-W4-picks-sorted",
    ),
    _case(
        "two_stream_law",
        0,
        STREAMS,
        lambda: _streams(_skewed_picks(_rng(11), (10_000, 10))),
        "two-stream-B1-stream-one-skewed",
    ),
    _case(
        "two_stream_law",
        1,
        STREAMS,
        lambda: _streams(_rng(12).integers(0, 20, (10_000, 10)), _skewed_picks(_rng(13), (10_000, 10))),
        "two-stream-B2-stream-two-skewed",
    ),
    _case(
        "two_stream_law",
        2,
        STREAMS,
        lambda: (
            _odd_offset_pairs(_rng(14), 50_000).reshape(10_000, 10),
            _odd_offset_pairs(_rng(15), 50_000).reshape(10_000, 10),
        ),
        "two-stream-B3-pairs-within-each-stream-confined",
    ),
    _case(
        "two_stream_law",
        3,
        STREAMS,
        lambda: (lambda one: (one, one))(_rng(16).integers(0, 20, (10_000, 10))),
        "two-stream-B4-one-draw-shared-by-both",
    ),
    _case(
        "plackett_luce_law",
        0,
        pd.Series(WEIGHTS),
        lambda: np.argsort(_rng(17).random((R, 7)), axis=1) + 1,
        "plackett-P1-first-slot-uniform",
    ),
    _case(
        "plackett_luce_law",
        1,
        pd.Series(WEIGHTS),
        lambda: _plackett_first_only(_rng(18)),
        "plackett-P2-only-the-first-slot-weighted",
    ),
    _case(
        "plackett_luce_law",
        2,
        pd.Series(WEIGHTS),
        lambda: np.argsort(_rng(19).random((R, 7)), axis=1) + 1,
        "plackett-P3-last-slot-uniform",
    ),
    _case(
        "plackett_luce_law",
        3,
        pd.Series(WEIGHTS),
        lambda: np.argsort(_rng(20).random((R, 7)), axis=1) + 1,
        "plackett-P4-nearest-neighbour-slot-uniform",
    ),
    _case(
        "tide_law",
        0,
        TIDE,
        lambda: np.column_stack([np.floor(_rng(21).uniform(-168, 168, R)).astype(int), _rng(22).choice(YEARS, R)]),
        "tide-T1-shift-floored",
    ),
    _case("tide_law", 1, TIDE, lambda: _tide_rows(_rng(23), [0.5, 0.25, 0.25]), "tide-T2-one-year-too-likely"),
    _case(
        "tide_law",
        2,
        TIDE,
        lambda: (lambda shift: np.column_stack([shift, YEARS[(shift + 168) * 3 // 337]]))(
            np.rint(_rng(24).uniform(-168, 168, R)).astype(int)
        ),
        "tide-T3-year-tied-to-shift",
    ),
    _case(
        "tide_law",
        3,
        TIDE,
        lambda: np.repeat(_tide_rows(_rng(25))[: R // 2], 2, axis=0),
        "tide-T4-consecutive-draws-identical",
    ),
]


@needs_scipy
@pytest.mark.parametrize(("law", "index", "reference", "sample"), CASES)
def test_each_statistical_test_of_a_draw_law_rejects_the_defect_it_exists_to_see(law, index, reference, sample):
    """Property: test number `index` of the law, read alone, gives a p-value below 0.001 on a sample holding its defect.

    Class: the twenty tests of the five laws other than the vine copula's, one in-support sample each. The defect is
    the one the test's definition names: a pool row never chosen, rows in sorted order, a window of neighbours,
    identical consecutive realizations, one row too likely, pairs confined to odd offsets, resamples without
    replacement, sorted picks, a skewed stream, a pair of streams that share a draw, a first slot that is uniform,
    a weighting of only the first slot, a floored shift, a skewed year, a year tied to the shift. Why it is
    required: the licence is a conjunction, and a conjunction is blind to one conjunct being weakened or replaced;
    these are the only readings that tell its four tests apart. Killed by: a test of the law replaced by one that
    always returns a p-value of 1. That replacement leaves the support's own tests green for 13 of these 20 tests
    (every test of the subset law but the first two, of the bootstrap law but the third, of the two-stream law
    but the last two, of the Plackett-Luce law, and of the tide law but the first and third), and fails here
    for every one. Second correct implementation: a different statistic aimed at the same defect passes whenever
    it rejects the sample; the preconditions hold for each sample, which `p_values` enforces.
    """
    licence = getattr(_support, law)()
    p = licence.p_values(reference, sample())
    assert p[index] < 0.001, p


@needs_vine
@pytest.mark.parametrize(
    ("index", "name"),
    [
        (0, "margin-squared"),
        (1, "columns-swapped"),
        (2, "margin-flipped"),
        (3, "rows-repeated"),
    ],
)
def test_each_statistical_test_of_the_vine_copula_law_rejects_the_defect_it_exists_to_see(index, name):
    """Property: test number `index` of the vine-copula law, read alone, gives a p-value below 0.001 on a sample holding its defect.

    Class: four samples of one three-dimensional D-vine: the first margin squared, two columns swapped, one margin
    flipped to one minus itself, and every row repeated once. Why it is required: C1 to C4 are four distinct readings
    (uniform margins, uniform Rosenblatt coordinates, independent coordinates, serial independence) and the
    licence's verdict does not show which of them still works. Killed by: a test of the law replaced by one that
    always returns a p-value of 1. Second correct implementation: any other uniformity or independence statistic
    on the same quantity passes whenever it rejects the sample.
    """
    reference, correct = _vine()
    samples = {
        "margin-squared": correct.assign(a=correct["a"] ** 2),
        "columns-swapped": correct[["b", "a", "c"]].set_axis(["a", "b", "c"], axis=1),
        "margin-flipped": correct.assign(b=1 - correct["b"]),
        "rows-repeated": pd.DataFrame(np.repeat(correct.to_numpy()[: R // 2], 2, axis=0), columns=["a", "b", "c"]),
    }
    p = _support.vine_copula_law().p_values(reference, samples[name])
    assert p[index] < 0.001, p


def _subsets_sorted_throughout():
    """Subsets of forty positions in which every row comes back sorted, so each row's first slot is its smallest position."""
    return np.sort(_uniform_subsets(_rng(26), 40), axis=1)


def _tide_without_a_valid_year():
    """(shift, year) rows that never draw the third valid year."""
    return _tide_rows(_rng(27), [0.5, 0.5, 0.0])


def _streams_without_a_row():
    """Two pick streams of which the first never picks the last of its twenty rows."""
    return _rng(28).integers(0, 19, (10_000, 10)), _rng(29).integers(0, 20, (10_000, 10))


@needs_scipy
@pytest.mark.parametrize(
    ("law", "index", "reference", "sample"),
    [
        _case("subset_law", 1, SUBSET, _subsets_sorted_throughout, "subset-rows-sorted-throughout"),
        _case("tide_law", 1, TIDE, _tide_without_a_valid_year, "tide-a-valid-year-never-drawn"),
        _case("two_stream_law", 0, STREAMS, _streams_without_a_row, "two-stream-a-row-never-picked"),
    ],
)
def test_the_four_figures_of_a_red_are_returned_for_a_sample_with_an_empty_row_or_column(law, index, reference, sample):
    """Property: `p_values` returns four floats for an in-support sample, and the test that sees the defect still rejects it.

    Class: in-support samples whose defect leaves a row or a column of a contingency table empty: subsets returned
    sorted (every first slot is a low position, so the high buckets are empty), a tide that never draws one of its
    valid years, and a stream that never picks one row. Why it is required: `p_values` is how a red is routed with
    its four figures, and each of these is a likely wrong output of a new routine; the independence test of each law
    builds a contingency table, and scipy refuses a table with an empty margin by raising `ValueError`, so the
    figures are lost for exactly the defects whose figures are wanted. `admits` is not affected (it stops at the
    first test that rejects, which comes before the table is built), so the licence still refuses the sample.
    Killed by: `_contingency` handing the table to `chi2_contingency` as built; the change that kills it leaves
    empty rows and columns out of the table, or reports the p-value of such a table as NaN. Second correct
    implementation: either of those passes the same three inputs, since only the length, the type and the rejecting
    test's figure are asserted.
    """
    licence = getattr(_support, law)()
    p = licence.p_values(reference, sample())
    assert len(p) == 4 and all(isinstance(figure, float) for figure in p), p
    assert p[index] < 0.001, p
    assert not licence.admits(reference, sample())
