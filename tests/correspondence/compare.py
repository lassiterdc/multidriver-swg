"""Decide whether a new side's value matches its reference value under a test's licence.

An equivalence test runs the legacy code and the new code in one session and compares what each
returns. The legacy code is written with numpy, pandas and xarray; its replacement may hand back objects
from any of those libraries, so a licence compares numpy arrays and scalars (with number,
boolean, date, time-span, text or object dtypes), xarray `DataArray` and `Dataset` objects, and
pandas `Series` and `DataFrame` objects, judging each by its labels as well as its data. A test
picks its licence: `Exact()` for identity, `Tolerance(rtol, atol)` for relative and absolute
closeness of numbers. Both answer the two questions the binding puts to a licence:
`admits(reference, value)`, whether the value passes, and `outside(reference)`, a stand-in built
like the reference that the licence turns down, which the binding hands an unregistered test.

The rules live here rather than in a library call because each library's comparator lets through
something these rules turn down, and each lets through something different: numpy's array
equality looks past dtype, xarray's equality looks past the dtype of data and coordinates, and
xarray's closeness assertion looks past names and attrs and lets a numeric coordinate drift. A
library assertion would also raise from inside the library, and the proof recorder would not
credit a failure raised there to the test. Library functions still do single pieces of the work where
they agree with these rules, such as an extension array's `equals`.

The rules, under both licences unless stated:

- Different kinds, different classes and different shapes never match. Neither side is flattened or
  broadcast, since flattening lets a value laid out in other rows and columns pass, and
  broadcasting lets one row stand in for a whole table of identical rows.
- Any other type, such as dicts, lists, tuples or pandas `Index` objects, causes both methods to
  raise `TypeError` with its type's name, so a test handed such a value stops immediately. Lists and tuples are not converted, because numpy would give every
  element one dtype, and a list holding the integer 1 and one holding the text "1" could then
  come out the same.
- Dtype counts, including a date's or time span's unit; the width of a numpy string does not.
  NaN matches NaN, and NaT matches NaT, in the same place.
- `Tolerance` loosens the data of numeric arrays and nothing else. Booleans, dates, time spans,
  text, objects, pandas extension dtypes, labels, names and attrs all stay exact under it.
- Labels count: for xarray the dimensions, the coordinates (the same set of names on both sides,
  each one exact), the name, the attrs, and every data variable's own attrs; for pandas the index
  one level at a time, the columns, and the name.
- `outside` leaves every label and attr alone and swaps each piece of data for one the licence
  turns down. When the reference holds no data at all (an empty array, a `Dataset` without data
  variables, a `DataFrame` without cells) nothing can lie outside it, so `outside` hands it back
  unchanged and the binding refuses it.

`assert_equivalent` holds the comparison's `assert`. Because the root configuration enrols this
file with pytest's rewriting hook ahead of its first import, a failing comparison shows up in the
calling test with both operands displayed, just like an `assert` the test makes itself.
"""

from __future__ import annotations

import math
from fractions import Fraction
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
from pandas.api.extensions import ExtensionArray

EQUAL_NAN = True  # in numeric data, a NaN matches a NaN in the same place

ACCEPTED = (
    "a Python int, float or complex; a numpy array or scalar whose dtype is a number, a boolean, a date, "
    "a time span, text or object; an xarray DataArray or Dataset; a pandas Series or DataFrame"
)

_NUMPY_KINDS = "iufcbMmUSO"  # the numpy dtype kinds a licence compares


class _EqualToNothing:
    """What fills an object array's `outside`: an element that compares unequal to everything, itself too."""

    def __eq__(self, other: object) -> bool:
        return False

    def __ne__(self, other: object) -> bool:
        return True

    __hash__ = object.__hash__

    def __repr__(self) -> str:
        return "<equal to nothing>"


_NOTHING = _EqualToNothing()


# ---- the kind of a value, and the refusal of every other type ----


def _kind(value: Any) -> str | None:
    """Name the kind of `value` among those a licence handles, or return None for anything else.

    The numpy kind covers arrays whose type is exactly `numpy.ndarray`, numpy scalars, and Python
    ints, floats and complexes (Python's bool counts as an int), provided the dtype is one this
    module handles. An ndarray subclass such as a masked array is turned away, because treating it
    as a plain array would throw away what the subclass carries, such as the mask.
    """
    if isinstance(value, xr.DataArray):
        return "DataArray"
    if isinstance(value, xr.Dataset):
        return "Dataset"
    if isinstance(value, pd.DataFrame):
        return "DataFrame"
    if isinstance(value, pd.Series):
        return "Series"
    numpy_value = type(value) is np.ndarray or isinstance(value, np.generic | int | float | complex)
    if numpy_value and np.asarray(value).dtype.kind in _NUMPY_KINDS:
        return "numpy"
    return None


def _require_kind(value: Any) -> str:
    """Return the kind of `value`, or raise `TypeError` with its type's name when no licence handles it."""
    kind = _kind(value)
    if kind is None:
        detail = f" of dtype {value.dtype}" if isinstance(value, np.ndarray | np.generic) else ""
        raise TypeError(
            f"no licence handles a value of type {type(value).__name__!r}{detail}; a licence handles {ACCEPTED}"
        )
    return kind


# ---- the two licences ----


class Exact:
    """The licence of identity: a value passes only when its data, dtypes and labels all match the reference."""

    def admits(self, reference: Any, value: Any) -> bool:
        """True when `value` matches `reference` under every rule of the module, with nothing loosened."""
        return _admits(self, reference, value)

    def outside(self, reference: Any) -> Any:
        """A stand-in built like `reference` that this licence turns down wherever the reference holds data."""
        return _outside(self, reference)

    def _numeric_admits(self, reference: np.ndarray, value: np.ndarray) -> bool:
        """Numeric arrays already matched in shape and dtype: identical values, a NaN matching a NaN."""
        return bool(np.array_equal(reference, value, equal_nan=EQUAL_NAN))

    def _numeric_outside(self, reference: np.ndarray) -> np.ndarray:
        """Move every number off its reference value; under identity any move at all is too far."""
        return _moved(reference, 0.0, 0.0)


class Tolerance:
    """The licence of closeness: numbers may differ by `atol + rtol * abs(reference)`; all else must match."""

    def __init__(self, rtol: float, atol: float):
        """Hold the relative and the absolute tolerance."""
        self.rtol, self.atol = rtol, atol

    def admits(self, reference: Any, value: Any) -> bool:
        """True when `value` matches `reference` under every rule of the module, numbers loosened by the tolerance."""
        return _admits(self, reference, value)

    def outside(self, reference: Any) -> Any:
        """A stand-in built like `reference` that this licence turns down wherever the reference holds data."""
        return _outside(self, reference)

    def _numeric_admits(self, reference: np.ndarray, value: np.ndarray) -> bool:
        """Numeric arrays already matched in shape and dtype: every value close enough, a NaN matching a NaN.

        Integers of magnitude 2**53 or more are checked in Python integer arithmetic, because a
        float check rounds them and would let a value one unit off pass even at zero tolerance.
        """
        if reference.dtype.kind in "iu" and _beyond_float_precision(reference, value):
            return all(
                abs(int(v) - int(r)) <= self.atol + self.rtol * abs(int(r))
                for r, v in zip(reference.flat, value.flat, strict=True)
            )
        return bool(np.all(np.isclose(value, reference, rtol=self.rtol, atol=self.atol, equal_nan=EQUAL_NAN)))

    def _numeric_outside(self, reference: np.ndarray) -> np.ndarray:
        """Move every number further from its reference value than `atol + rtol * abs(reference)` allows."""
        return _moved(reference, self.rtol, self.atol)


def assert_equivalent(value: Any, reference: Any, licence) -> None:
    """Fail at an `assert` unless `licence` admits `value` against `reference`."""
    assert licence.admits(reference, value)


# ---- admits: the walk over a value's labels and data ----


def _admits(licence, reference: Any, value: Any) -> bool:
    """Refuse unhandled types, then dispatch on the reference's kind to the matching comparison."""
    kind = _require_kind(reference)
    if _require_kind(value) != kind or (kind != "numpy" and type(value) is not type(reference)):
        return False
    if kind == "numpy":
        if _beyond_numpy_ints(reference) and type(value) is int:
            return _python_ints_admit(licence, reference, value)
        return _leaf_admits(licence, np.asarray(reference), np.asarray(value))
    if kind == "DataArray":
        return _data_array_admits(licence, reference, value)
    if kind == "Dataset":
        return _dataset_admits(licence, reference, value)
    if kind == "Series":
        return _series_admits(licence, reference, value)
    return _frame_admits(licence, reference, value)


def _beyond_float_precision(reference: np.ndarray, value: np.ndarray) -> bool:
    """True when either array holds an integer too large for a float64 to represent exactly."""
    limit = 2**53
    return any(abs(int(x)) >= limit for x in np.concatenate([reference.ravel(), value.ravel()]))


def _aligned(reference: np.ndarray, value: np.ndarray) -> tuple[np.ndarray, np.ndarray] | None:
    """Hand back both arrays when their shapes agree and None otherwise; nothing is reshaped to make them agree."""
    if reference.shape != value.shape:
        return None
    return reference, value


def _leaf_admits(licence, reference: np.ndarray | ExtensionArray, value: np.ndarray | ExtensionArray) -> bool:
    """Compare one piece of data (a numpy array or a pandas extension array) with its counterpart.

    `licence` is None when the piece is a label, which is always compared exactly; a licence's
    tolerance only ever loosens numeric numpy data.
    """
    if isinstance(reference, ExtensionArray) or isinstance(value, ExtensionArray):
        if not (isinstance(reference, ExtensionArray) and isinstance(value, ExtensionArray)):
            return False
        return _extension_admits(reference, value)
    pair = _aligned(reference, value)
    if pair is None:
        return False
    reference, value = pair
    kind = reference.dtype.kind
    if kind in "US":
        return _strings_admit(reference, value)
    if reference.dtype != value.dtype:
        return False
    if kind == "O":
        return _objects_admit(reference, value)
    if kind in "Mm":
        return _times_admit(licence, reference, value)
    if kind == "b":
        return bool(np.array_equal(reference, value))
    return (licence or Exact())._numeric_admits(reference, value)


def _beyond_numpy_ints(value: Any) -> bool:
    """True for a Python int too large for any numpy integer dtype, which numpy would hold only as an object."""
    return type(value) is int and np.asarray(value).dtype.kind == "O"


def _python_ints_admit(licence, reference: int, value: int) -> bool:
    """Two Python ints numpy can hold only as objects, compared as numbers under the licence's allowance.

    Python's own integer arithmetic is exact at any size, so the difference is never rounded;
    without this the pair would reach the `object` rule, which is exact even under `Tolerance`.
    """
    rtol, atol = (licence.rtol, licence.atol) if isinstance(licence, Tolerance) else (0.0, 0.0)
    return abs(value - reference) <= _int_allowance(reference, rtol, atol)


def _int_allowance(reference: int, rtol: float, atol: float) -> Fraction:
    """`atol + rtol * abs(reference)` for a Python int of any size, computed exactly.

    In float arithmetic the product overflows once the int reaches 2**1024, even when `rtol` is
    zero, and raises `OverflowError`; a `Fraction` holds both the float tolerances and the int exactly.
    """
    return Fraction(atol) + Fraction(rtol) * abs(reference)


def _strings_admit(reference: np.ndarray, value: np.ndarray) -> bool:
    """Numpy string arrays: both text or both bytes, holding the same strings, whatever their declared widths."""
    return value.dtype.kind == reference.dtype.kind and bool(np.array_equal(reference, value))


def _times_admit(licence, reference: np.ndarray, value: np.ndarray) -> bool:
    """Date or time-span arrays of one dtype, compared exactly whichever licence asks.

    Each is read as integer counts of its unit, where NaT is a single fixed count, so two NaTs in
    the same place match without any special rule. No tolerance applies: a relative tolerance on a
    count since the epoch would let today's dates drift by many units.
    """
    return bool(np.array_equal(reference.view("i8"), value.view("i8")))


def _missing(element: Any) -> bool:
    """True when an element marks a gap: `None`, `pandas.NA`, `NaT`, or a NaN float."""
    if element is None or element is pd.NA or element is pd.NaT:
        return True
    if isinstance(element, float | np.floating):
        return math.isnan(element)
    if isinstance(element, np.datetime64 | np.timedelta64):
        return bool(np.isnat(element))
    return False


def _objects_admit(reference: np.ndarray, value: np.ndarray) -> bool:
    """Object arrays of one shape, compared element by element: matching types first, then matching values.

    Missing markers of one type match each other and nothing else. Any other pair matches when
    `==` gives back a true boolean. When `==` gives back something that is not a boolean (an array,
    say), the element's type is named in a `TypeError` instead of guessing what its truth means.
    """
    for expected, actual in zip(reference.flat, value.flat, strict=True):
        if type(expected) is not type(actual):
            return False
        expected_missing, actual_missing = _missing(expected), _missing(actual)
        if expected_missing or actual_missing:
            if expected_missing and actual_missing:
                continue
            return False
        result = expected == actual
        if not isinstance(result, bool | np.bool_):
            raise TypeError(
                f"comparing two object elements of type {type(expected).__name__} by == gave a "
                f"{type(result).__name__} instead of a boolean; convert the value in the test itself"
            )
        if not result:
            return False
    return True


def _extension_admits(reference: ExtensionArray, value: ExtensionArray) -> bool:
    """Pandas extension arrays: matching dtypes, time zone and categories included, then `equals`.

    `ExtensionArray.equals` lets missing entries in the same places match. Tolerance never
    loosens an extension dtype.
    """
    if reference.dtype != value.dtype or len(reference) != len(value):
        return False
    if isinstance(reference.dtype, pd.SparseDtype) and reference.dtype.subtype == np.dtype(bool):
        # pandas' own `equals` raises AttributeError for two Sparse[bool] arrays that differ, in
        # pandas 2 and 3 alike. Such an array holds no missing entry, so its numpy values are its
        # whole content and comparing them gives the verdict `equals` would.
        return bool(np.array_equal(np.asarray(reference), np.asarray(value)))
    return bool(reference.equals(value))


def _attr_value_admits(reference: Any, value: Any) -> bool:
    """Compare one attr value, or one name, with its counterpart: matching types, then matching values.

    Arrays go through the array rules with no tolerance, two float NaNs match, and anything else
    is settled by `==`.
    """
    if type(reference) is not type(value):
        return False
    if isinstance(reference, np.ndarray):
        return _leaf_admits(None, reference, value)
    if isinstance(reference, float | np.floating) and math.isnan(reference) and math.isnan(value):
        return True
    return bool(reference == value)


def _attrs_admit(reference: dict, value: dict) -> bool:
    """Attr dictionaries: the same keys on both sides, with each value checked by the attr rule."""
    return set(reference) == set(value) and all(_attr_value_admits(reference[k], value[k]) for k in reference)


def _variable_leaf(variable: xr.Variable) -> np.ndarray | ExtensionArray:
    """Pull an xarray variable's data out as numpy values, or as the pandas extension array it wraps."""
    if isinstance(variable.dtype, np.dtype):
        return np.asarray(variable.values)
    return variable.data


def _variable_admits(licence, reference: xr.Variable, value: xr.Variable) -> bool:
    """xarray variables: dimension names in order, then attrs, then the data itself."""
    return (
        reference.dims == value.dims
        and _attrs_admit(reference.attrs, value.attrs)
        and _leaf_admits(licence, _variable_leaf(reference), _variable_leaf(value))
    )


def _coords_admit(reference, value) -> bool:
    """xarray coordinates: one set of names on both sides, every coordinate compared with no tolerance."""
    if set(reference.coords) != set(value.coords):
        return False
    return all(_variable_admits(None, reference.coords[n].variable, value.coords[n].variable) for n in reference.coords)


def _data_array_admits(licence, reference: xr.DataArray, value: xr.DataArray) -> bool:
    """`DataArray` against `DataArray`: dimension order and sizes, coordinates, name, attrs, then data."""
    return (
        reference.dims == value.dims
        and reference.shape == value.shape
        and _coords_admit(reference, value)
        and _attr_value_admits(reference.name, value.name)
        and _variable_admits(licence, reference.variable, value.variable)
    )


def _dataset_admits(licence, reference: xr.Dataset, value: xr.Dataset) -> bool:
    """`Dataset` against `Dataset`: sizes per dimension, the set of variable names, coordinates, attrs, then each variable.

    The sequence in which a `Dataset` lists its dimensions and variables only records how it was
    assembled, so it plays no part; the dimension order inside each variable does.
    """
    return (
        dict(reference.sizes) == dict(value.sizes)
        and set(reference.data_vars) == set(value.data_vars)
        and _coords_admit(reference, value)
        and _attrs_admit(reference.attrs, value.attrs)
        and all(_variable_admits(licence, reference[n].variable, value[n].variable) for n in reference.data_vars)
    )


def _pandas_leaf(values: pd.Series | pd.Index) -> np.ndarray | ExtensionArray:
    """Pull a pandas column or index level out as numpy values, or as its extension array."""
    if isinstance(values.dtype, np.dtype):
        return values.to_numpy()
    return values.array


def _index_admits(reference: pd.Index, value: pd.Index) -> bool:
    """Pandas indexes compared with no tolerance; a `MultiIndex` one level at a time, checking labels, dtype, name.

    One level at a time because a `MultiIndex` reports `object` as its dtype and `Index.equals`
    disregards dtype, so years stored as integers and years stored as floats would otherwise pass.
    """
    if isinstance(reference, pd.MultiIndex) != isinstance(value, pd.MultiIndex):
        return False
    if isinstance(reference, pd.MultiIndex):
        if reference.nlevels != value.nlevels:
            return False
        return all(
            _index_admits(reference.get_level_values(i), value.get_level_values(i)) for i in range(reference.nlevels)
        )
    return _attr_value_admits(reference.name, value.name) and _leaf_admits(
        None, _pandas_leaf(reference), _pandas_leaf(value)
    )


def _series_admits(licence, reference: pd.Series, value: pd.Series) -> bool:
    """`Series` against `Series`: the index with no tolerance, the name, then the data."""
    return (
        _index_admits(reference.index, value.index)
        and _attr_value_admits(reference.name, value.name)
        and _leaf_admits(licence, _pandas_leaf(reference), _pandas_leaf(value))
    )


def _frame_admits(licence, reference: pd.DataFrame, value: pd.DataFrame) -> bool:
    """`DataFrame` against `DataFrame`: index and column labels in order, then each column in turn."""
    return (
        reference.shape == value.shape
        and _index_admits(reference.index, value.index)
        and _index_admits(reference.columns, value.columns)
        and all(
            _leaf_admits(licence, _pandas_leaf(reference.iloc[:, i]), _pandas_leaf(value.iloc[:, i]))
            for i in range(reference.shape[1])
        )
    )


# ---- outside: the same labels, every data leaf replaced by a value its licence rejects ----


def _outside(licence, reference: Any) -> Any:
    """Copy `reference` with each piece of data swapped for one the licence turns down; labels and attrs stay."""
    kind = _require_kind(reference)
    if _data_positions(kind, reference) == 0:
        return reference
    if kind == "numpy":
        array = np.asarray(reference)
        if type(reference) is int and array.dtype.kind == "O":
            # A Python int no numpy integer dtype can hold: numpy keeps it as an object, and it
            # is still a number, so it is moved as one rather than replaced by a filler.
            moved = licence._numeric_outside(array)
        else:
            moved = _leaf_outside(licence, array)
        if isinstance(reference, np.ndarray):
            return moved
        # A plain Python int or bool gets a stand-in of its own class, not a numpy scalar, since
        # neither numpy scalar is an instance of `int` or `bool`.
        return type(reference)(moved[()]) if type(reference) in (int, bool) else moved[()]
    if kind == "DataArray":
        return reference.copy(data=_leaf_outside(licence, _variable_leaf(reference.variable)))
    if kind == "Dataset":
        return reference.copy(
            data={n: _leaf_outside(licence, _variable_leaf(reference[n].variable)) for n in reference.data_vars}
        )
    if kind == "Series":
        return pd.Series(_leaf_outside(licence, _pandas_leaf(reference)), index=reference.index, name=reference.name)
    frame = reference.copy()
    for i in range(reference.shape[1]):
        frame.isetitem(i, _leaf_outside(licence, _pandas_leaf(reference.iloc[:, i])))
    return frame


def _data_positions(kind: str, reference: Any) -> int:
    """Count the data cells in `reference`; with none, there is nothing to move."""
    if kind == "numpy":
        return np.asarray(reference).size
    if kind == "Dataset":
        return sum(variable.size for variable in reference.data_vars.values())
    return reference.size


def _leaf_outside(licence, reference: np.ndarray | ExtensionArray) -> np.ndarray | ExtensionArray:
    """Swap one piece of data for one of the same form that the licence turns down everywhere."""
    if isinstance(reference, ExtensionArray):
        return _extension_outside(reference)
    kind = reference.dtype.kind
    if kind in "fciu":
        return licence._numeric_outside(reference)
    if kind == "b":
        return _bool_outside(reference)
    if kind in "Mm":
        return _time_outside(reference)
    if kind in "US":
        return np.char.add(reference, "_" if kind == "U" else b"_")
    return _objects_outside(reference)


def _moved(reference: np.ndarray, rtol: float, atol: float) -> np.ndarray:
    """Move every number past `atol + rtol * abs(reference)`, keeping the array's dtype."""
    if reference.dtype.kind in "iuO":
        return _integer_outside(reference, rtol, atol)
    return _float_outside(reference, rtol, atol)


def _float_outside(reference: np.ndarray, rtol: float, atol: float) -> np.ndarray:
    """Float or complex data: finite numbers shifted, NaN and infinities set to 0.

    A finite number x shifts by double its allowance plus `abs(x) + 1`: the extra term still moves
    a zero under zero tolerance, and keeps a large number from absorbing the shift into rounding.
    NaN and infinities are replaced instead of shifted, since NaN plus anything stays NaN (which
    the NaN rule would accept) and an infinity plus anything finite stays that infinity.
    """
    with np.errstate(over="ignore", invalid="ignore"):
        moved = reference + 2 * (atol + rtol * np.abs(reference)) + np.abs(reference) + 1
    return np.where(np.isfinite(reference), moved, 0).astype(reference.dtype)


def _integer_outside(reference: np.ndarray, rtol: float, atol: float) -> np.ndarray:
    """Integer data: every number shifted past its allowance without leaving what the dtype can hold.

    Numbers shift upward unless that overflows, and downward if so. The sums are done in Python
    integers since numpy wraps around silently at the edge, turning the largest value plus one into
    the smallest. If the allowance is wider than the dtype in both directions, no number can lie
    outside it; the number is then set to whichever end is further away, which the licence still
    accepts, and the binding refuses that stand-in. An `object` array holds Python ints no numpy
    dtype can, and those have no range to stay inside, so they always shift upward.
    """
    if reference.dtype.kind == "O":
        shifted = [int(x) + math.floor(_int_allowance(int(x), rtol, atol)) + 1 for x in reference.flat]
        holder = np.empty(len(shifted), dtype=object)
        for i, x in enumerate(shifted):
            holder[i] = x
        return holder.reshape(reference.shape)
    info = np.iinfo(reference.dtype)

    def move(x: int) -> int:
        step = math.floor(atol + rtol * abs(x)) + 1
        if x + step <= info.max:
            return x + step
        if x - step >= info.min:
            return x - step
        return info.min if x - info.min > info.max - x else info.max

    moved = [move(int(x)) for x in reference.flat]
    return np.array(moved, dtype=reference.dtype).reshape(reference.shape)


def _bool_outside(reference: np.ndarray) -> np.ndarray:
    """Flip every boolean."""
    return np.logical_not(reference)


def _time_outside(reference: np.ndarray) -> np.ndarray:
    """Dates or time spans: each moved one tick of its unit forward, and each NaT set to zero.

    The largest representable value moves back a tick instead, since forward would wrap round to
    NaT. A NaT becomes the zero of its dtype, which is the epoch for dates and no time at all for
    spans.
    """
    counts = reference.view("i8")
    top = np.iinfo(np.int64).max
    moved = np.where(np.isnat(reference), 0, np.where(counts == top, counts - 1, counts + 1))
    return moved.astype("i8").view(reference.dtype)


def _objects_outside(reference: np.ndarray) -> np.ndarray:
    """Fill an object array of the reference's shape with elements that match nothing."""
    moved = np.empty(reference.shape, dtype=object)
    moved.fill(_NOTHING)
    return moved


def _extension_outside(reference: ExtensionArray) -> np.ndarray | ExtensionArray:
    """Build a stand-in for a pandas extension array, keeping its dtype whenever the dtype allows one.

    Missing entries first become the zero of what the array holds, then everything is shifted the
    way the matching numpy data would be, and the result goes back to the original dtype. A
    categorical with categories keeps its dtype by turning present entries missing and missing
    entries into its first category; a categorical without categories can only hold missing
    entries, so it becomes an `object` array of elements that match nothing. Every other dtype,
    such as a sparse, pyarrow or third-party one, is handed to `_unnamed_extension_outside`.
    """
    dtype = reference.dtype
    if isinstance(dtype, pd.CategoricalDtype):
        if len(dtype.categories) == 0:
            return _objects_outside(np.empty(len(reference), dtype=object))
        return pd.Categorical.from_codes(np.where(reference.codes == -1, 0, -1), dtype=dtype)
    if isinstance(dtype, pd.PeriodDtype):
        epoch = pd.Period("1970-01-01", freq=dtype.freq)
        filled = [epoch if pd.isna(p) else p for p in reference]
        return pd.array([p + 1 for p in filled], dtype=dtype)
    if isinstance(dtype, pd.IntervalDtype):
        return _interval_outside(reference)
    if isinstance(dtype, pd.DatetimeTZDtype):
        counts = reference.tz_convert(None).to_numpy()
        moved = _time_outside(np.where(np.isnat(counts), np.zeros_like(counts), counts))
        return pd.array(moved).tz_localize("UTC").tz_convert(dtype.tz).astype(dtype)
    if isinstance(dtype, pd.StringDtype):
        text = reference.to_numpy(dtype=object, na_value="")
        return pd.array([s + "_" for s in text], dtype=dtype)
    if dtype.kind in "iufb" and hasattr(dtype, "numpy_dtype"):
        values = reference.to_numpy(dtype=dtype.numpy_dtype, na_value=False if dtype.kind == "b" else 0)
        moved = _bool_outside(values) if dtype.kind == "b" else _moved(values, 0.0, 0.0)
        return pd.array(moved, dtype=dtype)
    return _unnamed_extension_outside(reference)


def _values_rule(values: np.ndarray, missing: np.ndarray) -> str | None:
    """Pick the numpy rule an extension array's values fall under, or None when none fits.

    The numpy dtype of the values decides, except that `object` values are looked at one by one:
    all text (gaps aside) is treated as text, all booleans (gaps aside) as booleans, and anything
    else as objects.
    """
    kind = values.dtype.kind
    if kind != "O":
        return {"i": "number", "u": "number", "f": "number", "c": "number", "b": "boolean"}.get(
            kind, "time" if kind in "Mm" else "text" if kind in "US" else None
        )
    present = [v for v, gap in zip(values.flat, missing.flat, strict=True) if not gap]
    if all(isinstance(v, str) for v in present):
        return "text"
    if all(isinstance(v, bool | np.bool_) for v in present):
        return "boolean"
    return "object"


def _unnamed_extension_outside(reference: ExtensionArray) -> ExtensionArray:
    """A stand-in for an extension dtype with no rule of its own, built by the rule its values fall under.

    Its values are read as a numpy array, gaps are first set to that rule's zero (0, False, the
    epoch, the empty string), the rule's stand-in is computed with no tolerance, and the result is
    converted back to the reference's dtype. Where the values fall under no rule, or the dtype
    refuses the stand-in (a dtype that holds only its own element type, say), this raises
    `TypeError` naming the dtype: no stand-in of that dtype exists, so none is returned.
    """
    dtype = reference.dtype
    values = np.asarray(reference)
    missing = np.asarray(reference.isna(), dtype=bool)
    rule = _values_rule(values, missing)
    if rule is None:
        raise TypeError(f"no stand-in for the pandas extension dtype {dtype}: its values fall under no rule")
    if rule == "number":
        stand_in = _moved(np.where(missing, 0, values).astype(values.dtype), 0.0, 0.0)
    elif rule == "boolean":
        stand_in = _bool_outside(np.array([False if gap else bool(v) for v, gap in zip(values, missing, strict=True)]))
    elif rule == "time":
        stand_in = _time_outside(np.where(missing, np.zeros_like(values), values))
    elif rule == "text":
        stand_in = np.array(
            [("" if gap else str(v)) + "_" for v, gap in zip(values, missing, strict=True)], dtype=object
        )
    else:
        stand_in = _objects_outside(values)
    try:
        converted = pd.array(stand_in, dtype=dtype)
    except Exception as exc:
        raise TypeError(f"no stand-in for the pandas extension dtype {dtype}: it cannot hold one") from exc
    if converted.dtype != dtype:
        raise TypeError(f"no stand-in for the pandas extension dtype {dtype}: it cannot hold one")
    return converted


def _interval_outside(reference: ExtensionArray) -> ExtensionArray:
    """Intervals: a missing interval first becomes (zero, zero + step], then every endpoint shifts by step.

    `zero` and `step` come from converting the integers 0 and 1 to the subtype, so no subtype
    needs its own case: numbers get 0 and 1, dates get the epoch and one tick of their unit with
    their time zone kept, and time spans get no time and one tick.
    """
    dtype = reference.dtype
    zero = pd.Series([0], dtype="int64").astype(dtype.subtype).iloc[0]
    step = pd.Series([1], dtype="int64").astype(dtype.subtype).iloc[0] - zero
    missing = np.asarray(reference.isna())
    left = pd.Series(reference.left).where(~missing, zero) + step
    right = pd.Series(reference.right).where(~missing, zero + step) + step
    return pd.arrays.IntervalArray.from_arrays(left, right, closed=dtype.closed).astype(dtype)
