"""Tests of the comparison helper and its two licences, across every kind of value they accept.

Whether an equivalence test passes depends on what its licence accepts, so these tests pin the
licences down: which kinds of value they take, what besides the numbers decides a match, how far
`Tolerance` loosens things, and that each licence's `outside` stand-in looks like its reference
and is turned down. Each test names the change to `compare.py` that makes it fail.

Most rules share one test, because breaking any one of them necessarily breaks an input some other
rule depends on. A broken numeric stand-in also breaks the NaN and infinity references and the
stand-in shape check; switching string and object comparison to numpy's NaN-aware equality raises
on every text and object input; and judging an xarray or pandas value by its data alone lets
every relabelled value through. The rules with tests of their own are the ones no other rule's
breakage can reach: the helper's failure report, the shape check, dates and time spans, refusing
foreign types, the order a `Dataset` was assembled in, and extension dtypes with no rule of their own.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pandas.api.extensions import ExtensionArray, ExtensionDtype, take

from tests.correspondence.compare import Exact, Tolerance, assert_equivalent

pytestmark = pytest.mark.state_check

REFERENCE = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
LICENCES = (Exact(), Tolerance(1e-9, 0))
IDS = ["exact", "tolerance"]
DATES = pd.date_range("2020-01-01", periods=3).values


def _data_array():
    """Two stations by three days of depths, with the station number and the day as coordinates, a name and units."""
    return xr.DataArray(
        REFERENCE.copy(),
        dims=("station", "time"),
        coords={"station": np.array([10, 20]), "time": DATES},
        name="depth",
        attrs={"units": "mm"},
    )


def _dataset():
    """Three days of rain totals, peak times and source labels, the rain carrying its units."""
    return xr.Dataset(
        {
            "rain": ("time", np.array([1.0, 2.0, 3.0]), {"units": "mm"}),
            "peak": ("time", DATES.copy()),
            "source": ("time", np.array(["gauge", "radar", "gauge"])),
        }
    )


def _series():
    """Three event depths on a named event index."""
    return pd.Series([1.0, 2.0, 3.0], index=pd.Index([1, 2, 3], name="event"), name="depth")


def _multi_series():
    """Three depths indexed by year (stored as integers) and by event type (stored as text)."""
    index = pd.MultiIndex.from_arrays(
        [np.array([2001, 2001, 2002]), np.array(["rain", "surge", "rain"])], names=["year", "event_type"]
    )
    return pd.Series([1.0, 2.0, 3.0], index=index, name="depth")


def _frame():
    """Depths, peak times and source labels for three events."""
    return pd.DataFrame(
        {"depth": [1.0, 2.0, 3.0], "peak": pd.to_datetime(DATES), "source": ["gauge", "radar", "gauge"]},
        index=pd.Index([1, 2, 3], name="event"),
    )


def _extension_series():
    """One `Series` each of UTC timestamps, categories, pandas strings and nullable integers, gaps included where possible."""
    return {
        "dates": pd.Series(pd.to_datetime(["2020-01-01", None, "2020-01-03"]).tz_localize("UTC")),
        "category": pd.Series(["rain", "surge", "rain"], dtype="category"),
        "string": pd.Series(["gauge", None, "radar"], dtype="string"),
        "integer": pd.Series([1, None, 3], dtype="Int64"),
    }


def _check_numeric_outside():
    """Both stand-ins for a 2×3 float array are turned down; `Exact` takes the array, `Tolerance` takes it nudged by half its allowance."""
    exact, tolerance = LICENCES
    assert not exact.admits(REFERENCE, exact.outside(REFERENCE))
    assert exact.admits(REFERENCE, REFERENCE)
    assert not tolerance.admits(REFERENCE, tolerance.outside(REFERENCE))
    assert tolerance.admits(REFERENCE, REFERENCE * (1 + 1e-9 / 2))


def _check_python_numbers(licence):
    """Plain Python numbers get a turned-down stand-in of their own class, and Python ints of any size are numbers.

    Ints past numpy's integer ranges, and past what a float can hold, are judged and moved in exact
    integer arithmetic, and `Tolerance` loosens them as it loosens any other int.
    """
    for reference in (2**64, 2**70, -(2**63) - 1):
        assert licence.admits(reference, reference), reference
        assert not licence.admits(reference, licence.outside(reference)), reference
    for reference in (3, True, 2.5, 1 + 2j, np.int64(3), np.bool_(True), np.float64(2.5)):
        out = licence.outside(reference)
        assert isinstance(out, type(reference)), (reference, type(out))
        assert not licence.admits(reference, out), reference
    for reference in (2**1100, -(2**1100)):  # past what a float can hold
        assert licence.admits(reference, reference), reference
        assert not licence.admits(reference, licence.outside(reference)), reference
    near, far = 2**70 + 1, 2**70 + 2**50
    if isinstance(licence, Tolerance):
        assert licence.admits(2**70, near) and licence.admits(2**1100, 2**1100 + 1)
        assert not licence.admits(2**70, far)
        assert licence.admits(2**40, 2**40 + 1)
    else:
        assert not licence.admits(2**70, near)


def _check_nan_and_infinities(licence):
    """Arrays with one NaN, and arrays made wholly of +inf, of -inf and of NaN, each match a copy and turn down their stand-in."""
    with_nan = REFERENCE.copy()
    with_nan[0, 1] = np.nan
    references = [with_nan] + [np.full((2, 3), fill) for fill in (np.inf, -np.inf, np.nan)]
    for reference in references:
        assert licence.admits(reference, reference.copy()), reference
        assert not licence.admits(reference, licence.outside(reference)), reference


def _check_strings_and_objects(licence):
    """Text and object arrays must match exactly, text width aside, and each turns down its stand-in."""
    for reference in (np.array(["", "ab", "abc"], dtype="U3"), np.array(["a", None, 3], dtype=object)):
        assert licence.admits(reference, reference.copy()), reference.dtype
        assert not licence.admits(reference, licence.outside(reference)), reference.dtype
    narrow = np.array(["ab", "c"], dtype="U2")
    assert licence.admits(narrow, narrow.astype("U5"))
    assert not licence.admits(narrow, np.array(["ax", "c"], dtype="U2"))


def _check_dtype(licence):
    """Integers never pass for floats holding the same numbers, whether bare, as xarray data, as a coordinate or in a `Series`."""
    as_float, as_int = np.array([10.0, 20.0]), np.array([10, 20])
    assert not licence.admits(as_float, as_int)
    reference = _data_array()
    assert not licence.admits(reference, reference.copy(data=REFERENCE.astype("int64")))
    assert not licence.admits(reference.assign_coords(station=as_float), reference)
    assert not licence.admits(pd.Series(as_float), pd.Series(as_int))


def _check_xarray_labels(licence):
    """Relabelled xarray values fail, untouched copies pass, and the stand-in keeps every coordinate and attr."""
    reference = _data_array()
    changed = {
        "transposed": reference.transpose("time", "station"),
        "coordinate shifted": reference.assign_coords(station=np.array([10, 21])),
        "date coordinate a nanosecond later": reference.assign_coords(time=DATES + np.timedelta64(1, "ns")),
        "another attr": reference.assign_attrs(units="m"),
        "another name": reference.rename("height"),
        "bare numpy values": reference.values,
    }
    for label, value in changed.items():
        assert not licence.admits(reference, value), label
    dataset = _dataset()
    other_attrs = dataset.copy(deep=True)
    other_attrs["rain"].attrs["units"] = "m"
    assert not licence.admits(dataset, dataset.assign(extra=("time", np.zeros(3))))
    assert not licence.admits(dataset, other_attrs)
    for ref in (reference, dataset):
        assert licence.admits(ref, ref.copy(deep=True))
        out = licence.outside(ref)
        assert dict(out.sizes) == dict(ref.sizes) and list(out.sizes) == list(ref.sizes)
        assert out.coords.to_dataset().identical(ref.coords.to_dataset())
        assert out.attrs == ref.attrs


def _check_pandas_labels(licence):
    """A pandas value with a different index, level dtype, column order, index name or series name fails; copies pass."""
    series = _series()
    assert not licence.admits(series, series.set_axis(pd.Index([4, 5, 6], name="event")))
    assert not licence.admits(series, series.rename("height"))
    multi = _multi_series()
    float_years = multi.copy()
    float_years.index = multi.index.set_levels(multi.index.levels[0].astype("float64"), level="year")
    assert not licence.admits(multi, float_years)
    frame = _frame()
    assert not licence.admits(frame, frame[["peak", "depth", "source"]])
    assert not licence.admits(frame, frame.rename_axis("storm"))
    for ref in (series, multi, frame):
        assert licence.admits(ref, ref.copy())


def _check_tolerance_reach():
    """A tiny rescale of numeric data passes under `Tolerance`, but not of a coordinate, and changed attrs never pass; `Exact` refuses all four."""
    tolerance, exact = Tolerance(1e-9, 0), Exact()
    reference = _data_array().assign_coords(elevation=("station", np.array([1.5, 2.5])))
    scaled_data = reference.copy(data=reference.values * (1 + 5e-10))
    scaled_coordinate = reference.assign_coords(elevation=reference["elevation"] * (1 + 5e-10))
    other_attrs = reference.assign_attrs(units="m")
    frame = _frame()
    scaled_column = frame.assign(depth=frame["depth"] * (1 + 5e-10))
    assert tolerance.admits(reference, scaled_data)
    assert tolerance.admits(frame, scaled_column)
    assert not tolerance.admits(reference, scaled_coordinate)
    assert not tolerance.admits(reference, other_attrs)
    pairs = ((reference, scaled_data), (frame, scaled_column), (reference, scaled_coordinate), (reference, other_attrs))
    for ref, value in pairs:
        assert not exact.admits(ref, value)


def _check_object_elements(licence):
    """Object elements match only with their own type and missing markers only with their own kind; an element whose `==` is not a boolean raises."""
    mixed = np.array([float("nan"), None, pd.NA, "a", 3], dtype=object)
    record = pd.Series(index=["kind", "count", "shape", "scale"], dtype=object)
    record["kind"], record["count"] = "rain", 3
    scalars = pd.Series([np.float64(1.5), "a", np.int64(3)], dtype=object)
    for reference in (mixed, record, scalars):
        assert licence.admits(reference, reference.copy())
    assert not licence.admits(np.array([2], dtype=object), np.array([2.0], dtype=object))
    assert not licence.admits(np.array([True], dtype=object), np.array([1], dtype=object))
    holding_array = np.empty(1, dtype=object)
    holding_array[0] = np.array([1.0, 2.0])
    with pytest.raises(TypeError, match=r"\bndarray\b"):
        licence.admits(holding_array, holding_array.copy())


def _check_attrs(licence):
    """Attrs match per key: arrays by dtype and values, NaN with NaN, other values by type and `==`."""
    attrs = {"valid_range": np.array([0.0, 100.0]), "missing_value": float("nan"), "units": "mm"}
    reference = _data_array().assign_attrs(attrs)
    assert licence.admits(reference, reference.copy(deep=True))
    assert not licence.admits(reference, reference.assign_attrs(valid_range=np.array([0, 100])))
    whole, fractional = _data_array(), _data_array()
    whole.attrs, fractional.attrs = {"v": 1}, {"v": 1.0}
    assert not licence.admits(whole, fractional)


def _check_coordinate_names_both_ways(licence):
    """A coordinate present on only one side fails the comparison, whichever side carries it."""
    reference = _data_array()
    kept, dropped = reference.isel(time=0), reference.isel(time=0, drop=True)
    assert not licence.admits(kept, dropped)
    assert not licence.admits(dropped, kept)


def _check_extension_dtypes(licence):
    """Extension-dtype values match on dtype and `equals`: copies pass, another time zone or plain objects fail, stand-ins fail."""
    extension = _extension_series()
    for name, reference in extension.items():
        assert licence.admits(reference, reference.copy()), name
        assert not licence.admits(reference, licence.outside(reference)), name
    assert not licence.admits(extension["dates"], extension["dates"].dt.tz_convert("Europe/Paris"))
    assert not licence.admits(extension["category"], extension["category"].astype(object))


def _check_outside_form(licence):
    """For one reference of every kind, the stand-in keeps the class, shape, dtype and labels, and is turned down."""
    extension = _extension_series()
    references = [
        np.array([1, 2, 3], dtype="int64"),
        np.array([True, False]),
        REFERENCE.copy(),
        np.array(["2020-01-01", "2020-02-01"], dtype="datetime64[D]"),
        np.array(["ab", "c"], dtype="U2"),
        _data_array(),
        _dataset(),
        _series(),
        _frame(),
        extension["dates"],
        extension["string"],
        extension["integer"],
        pd.Series(["rain", "surge"], dtype="category"),
        pd.Series(pd.period_range("2020-01", periods=2, freq="M")),
        pd.Series(pd.arrays.IntervalArray.from_breaks([0, 1, 2])),
    ]
    for reference in references:
        out = licence.outside(reference)
        label = getattr(reference, "dtype", type(reference).__name__)
        assert type(out) is type(reference), label
        if isinstance(reference, xr.Dataset):
            assert list(out.sizes) == list(reference.sizes) and dict(out.sizes) == dict(reference.sizes)
            assert all(
                out[n].dtype == reference[n].dtype for n in reference.data_vars if reference[n].dtype.kind != "U"
            )
            assert out.attrs == reference.attrs
        elif isinstance(reference, pd.DataFrame):
            assert out.shape == reference.shape and list(out.dtypes) == list(reference.dtypes), label
        else:
            assert out.shape == reference.shape, label
            assert reference.dtype.kind == "U" or out.dtype == reference.dtype, label
        if isinstance(reference, xr.DataArray | xr.Dataset):
            assert out.coords.to_dataset().identical(reference.coords.to_dataset()), label
        if isinstance(reference, xr.DataArray):
            assert out.dims == reference.dims and out.name == reference.name and out.attrs == reference.attrs
        if isinstance(reference, pd.Series | pd.DataFrame):
            pd.testing.assert_index_equal(out.index, reference.index)
        if isinstance(reference, pd.DataFrame):
            pd.testing.assert_index_equal(out.columns, reference.columns)
        if isinstance(reference, pd.Series):
            assert out.name == reference.name
        assert not licence.admits(reference, out), label


def test_a_failed_comparison_shows_both_operands():
    """Property: when `assert_equivalent` fails, the report shows what was compared, as pytest does for its own asserts.

    Class: any failing comparison through the helper. Why: the helper's `assert` stands in for the
    test's own, so its failure must read like one — showing what was compared — rather than as a
    bare `AssertionError`. Killed by: pointing assertion rewriting at some other module name.
    Second correct implementation: a helper that formats the operands into its own failure message
    while still asserting.
    """
    with pytest.raises(AssertionError) as excinfo:
        assert_equivalent(1, 2, Exact())
    text = str(excinfo.value)
    assert "where False" in text and "(2, 1)" in text, text


def test_each_licence_compares_values_dtypes_and_labels_and_rejects_its_outside():
    """Property: both licences judge every accepted kind by data, dtype and labels, and both turn down their own stand-ins.

    Class: one reference of each kind under each licence — numbers with NaN and infinities, plain
    Python numbers (ints past numpy's integer range among them), text
    and objects, xarray and pandas values with their coordinates, index levels, names and attrs,
    and pandas extension dtypes — beside altered values that each differ in a single respect.
    These rules share a test because breaking any one of them necessarily breaks an input another
    one needs; the module docstring says how.

    Why: an unregistered test receives the stand-in, so a stand-in the licence accepted would let
    that test pass before any new code exists; and accepting an altered dtype, label, name or attr
    would let a replacement with the wrong layout, units or index pass as equivalent.

    Killed by, each by itself: `Exact`'s numeric stand-in returning the reference unchanged;
    `Tolerance`'s returning the reference nudged by half its relative allowance; numeric comparison
    that no longer lets NaN match NaN; a float stand-in that shifts NaN and infinities instead of
    replacing them; text and objects compared through numpy's NaN-aware equality; numbers compared
    with their dtypes ignored; xarray values judged by their data alone; pandas values judged by
    `.to_numpy()` alone; labelled values judged only by `pandas.testing` and
    `xarray.testing.assert_allclose`; object elements matched by `==` with nothing else checked; attr dictionaries
    matched by `==`; coordinates checked only from the reference's side; extension-dtype data
    turned into objects before comparing; and integer and boolean stand-ins built with the float
    shift, which yields floats.

    Second correct implementation: licences that compare labels through `xarray.Variable.identical`
    and `pandas.Index.equals` plus an explicit per-level dtype check, and that build stand-ins by
    putting one fixed distant value of the right dtype into every cell instead of shifting cells.
    """
    _check_numeric_outside()
    _check_tolerance_reach()
    for licence in LICENCES:
        _check_python_numbers(licence)
        _check_nan_and_infinities(licence)
        _check_strings_and_objects(licence)
        _check_dtype(licence)
        _check_xarray_labels(licence)
        _check_pandas_labels(licence)
        _check_object_elements(licence)
        _check_attrs(licence)
        _check_coordinate_names_both_ways(licence)
        _check_extension_dtypes(licence)
        _check_outside_form(licence)


@pytest.mark.parametrize("licence", LICENCES, ids=IDS)
def test_neither_licence_admits_a_value_of_another_shape(licence):
    """Property: a value of another shape fails, even when it holds the very same numbers.

    Class: any reshaped value, and any value one broadcast step away, here the reference
    `[[1, 2, 3], [1, 2, 3]]` against its 3×2 reshape and against its first row. Why: flattening
    would let the reshape through and broadcasting would let the lone row through, so a
    replacement producing the wrong layout would pass. Killed by: flattening both arrays before
    comparing them, under either licence. Second correct implementation: comparing `np.shape` of
    both before any value comparison, outside the licences.
    """
    reference = np.array([[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]])
    assert not licence.admits(reference, reference.reshape(3, 2))
    assert not licence.admits(reference, reference[0])
    assert licence.admits(reference, reference)


@pytest.mark.parametrize("licence", LICENCES, ids=IDS)
def test_dates_and_time_spans_compare_exactly_under_both_licences(licence):
    """Property: dates and time spans must match exactly under both licences, NaT matching NaT, and their stand-ins fail.

    Class: day-, nanosecond- and hour-resolution arrays with NaT in them or made only of NaT, plus
    a date shifted by a single nanosecond. Why: a relative allowance on a count since the epoch is
    no measure of how close two dates are; near today's date a nanosecond sits well inside
    `rtol = 1e-9` of the count, so a wrong timestamp would pass. Killed by: letting `Tolerance`
    loosen dates and spans by treating their counts as numbers. Second correct implementation:
    comparing dates by their ISO strings and NaT by `numpy.isnat` masks.
    """
    references = (
        np.array(["2020-01-01", "NaT", "2020-03-01"], dtype="datetime64[D]"),
        np.array(["NaT", "NaT"], dtype="datetime64[ns]"),
        np.array([1, "NaT", 3], dtype="timedelta64[h]"),
    )
    for reference in references:
        assert licence.admits(reference, reference.copy()), reference.dtype
        assert not licence.admits(reference, licence.outside(reference)), reference.dtype
    date = np.array(["2020-01-01"], dtype="datetime64[ns]")
    assert not licence.admits(date, date + np.timedelta64(1, "ns"))


@pytest.mark.parametrize("licence", LICENCES, ids=IDS)
def test_a_value_of_a_type_outside_the_kinds_is_refused_by_name(licence):
    """Property: both methods raise `TypeError` with the type's name for anything outside the accepted kinds.

    Class: values a test could hand over by mistake — a dict, a bare `object()`, a pandas `Index`,
    a list mixing types and a tuple holding a bool. Why: such a value has to stop the test straight
    away; quietly failing it, or converting a list and losing its element types, would make the
    verdict about something other than the unit. Killed by: having `admits` answer False and
    `outside` hand the value back whenever the type is unknown. Second correct implementation: an
    explicit allowlist of accepted types checked as each licence method begins.
    """
    for value in ({"a": 1}, object(), pd.Index([1, 2]), [1, "a"], (True, 2)):
        name = type(value).__name__
        with pytest.raises(TypeError, match=rf"'{name}'"):
            licence.admits(value, value)
        with pytest.raises(TypeError, match=rf"'{name}'"):
            licence.outside(value)


@pytest.mark.parametrize("licence", LICENCES, ids=IDS)
def test_a_datasets_build_order_takes_no_part_in_equality(licence):
    """Property: a `Dataset` assembled in a different order from the reference still matches it.

    Class: any `Dataset` and the same variables added in another sequence. Why: assembly order
    says how a `Dataset` was built, not what it holds, so two correct replacements that assemble
    it differently must both pass. Killed by: comparing the lists of dimensions and of data
    variables in their listed order. Second correct implementation: sorting both name lists
    before comparing them.
    """
    a, b = np.zeros((2, 3)), np.ones(3)
    built = xr.Dataset({"a": (("x", "y"), a), "b": ("y", b)})
    reversed_build = xr.Dataset({"b": ("y", b), "a": (("x", "y"), a)})
    assert list(built.sizes) != list(reversed_build.sizes) and list(built.data_vars) != list(reversed_build.data_vars)
    assert licence.admits(built, reversed_build)


class _TupleOnlyDtype(ExtensionDtype):
    """A pandas extension dtype with no rule of its own whose array refuses anything but tuples."""

    name = "tuple_only"
    type = tuple
    na_value = None

    @classmethod
    def construct_array_type(cls):
        """The array class this dtype builds."""
        return _TupleOnlyArray


class _TupleOnlyArray(ExtensionArray):
    """The smallest extension array pandas will hold in a `Series`: tuples only, no missing entries."""

    def __init__(self, values):
        """Hold `values`, refusing any element that is not a tuple."""
        values = list(values)
        if not all(isinstance(v, tuple) for v in values):
            raise TypeError("a tuple_only array holds tuples and nothing else")
        self._data = np.empty(len(values), dtype=object)
        for i, v in enumerate(values):
            self._data[i] = v

    @classmethod
    def _from_sequence(cls, scalars, *, dtype=None, copy=False):
        """Build from any sequence of tuples."""
        return cls(scalars)

    @classmethod
    def _from_factorized(cls, values, original):
        """Build from factorized values."""
        return cls(values)

    def __getitem__(self, item):
        """One tuple for an integer position, an array for anything else."""
        if isinstance(item, int | np.integer):
            return self._data[item]
        return type(self)(self._data[item])

    def __len__(self):
        """The number of tuples."""
        return len(self._data)

    def __array__(self, dtype=None, copy=None):
        """The tuples as an object array, so numpy does not unpack them into rows."""
        return self._data

    def __eq__(self, other):
        """Position-by-position tuple equality."""
        return np.array([a == b for a, b in zip(self._data, other._data, strict=True)], dtype=bool)

    @property
    def dtype(self):
        """Always the tuple-only dtype."""
        return _TupleOnlyDtype()

    @property
    def nbytes(self):
        """The size of the held object array."""
        return self._data.nbytes

    def isna(self):
        """No entry is ever missing."""
        return np.zeros(len(self._data), dtype=bool)

    def take(self, indices, allow_fill=False, fill_value=None):
        """Select positions, as pandas requires of every extension array."""
        return type(self)(take(self._data, indices, allow_fill=allow_fill, fill_value=fill_value))

    def copy(self):
        """An independent copy."""
        return type(self)(self._data.copy())

    @classmethod
    def _concat_same_type(cls, to_concat):
        """Join several arrays end to end."""
        return cls([v for array in to_concat for v in array._data])


@pytest.mark.parametrize("licence", LICENCES, ids=IDS)
def test_an_extension_dtype_without_a_rule_of_its_own_takes_the_rule_its_values_fall_under(licence):
    """Property: an extension dtype no rule names gets a same-dtype stand-in from its values' rule, or a named refusal.

    Class: sparse arrays of numbers holding a gap, of booleans and of tuples, and a dtype defined
    here whose array accepts tuples alone. Why: an unregistered test with such a reference should
    fail at its own comparison wherever a stand-in of its dtype can be built, and should be refused
    with the dtype's name only where none can; and a differing sparse boolean pair must be judged
    rather than crash inside pandas. Killed by: refusing every such dtype from `outside` with a
    `TypeError`. Second correct implementation: deriving each stand-in from the dtype's own subtype
    (a sparse array's `subtype`) instead of from its values.
    """
    references = (
        pd.Series(pd.arrays.SparseArray([1.0, np.nan, 0.0])),
        pd.Series(pd.arrays.SparseArray(np.array([(1, 2), None, (3,)], dtype=object))),
        pd.Series(pd.arrays.SparseArray([True, False, False])),
    )
    for reference in references:
        assert licence.admits(reference, reference.copy()), reference.dtype
        out = licence.outside(reference)
        assert out.dtype == reference.dtype, (reference.dtype, out.dtype)
        assert not licence.admits(reference, out), reference.dtype
    flags = references[2]
    assert not licence.admits(flags, pd.Series(pd.arrays.SparseArray([True, True, False])))
    tuples = pd.Series(_TupleOnlyArray([(1, 2), (3,)]))
    assert licence.admits(tuples, tuples.copy())
    with pytest.raises(TypeError, match=r"\btuple_only\b"):
        licence.outside(tuples)
