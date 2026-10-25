"""Store a numpy, pandas or xarray value as bytes that cannot execute code when read back.

A synthetic input fixture and a stored reference value are files that many tests read and that a
digest identifies. Pickle is the usual way to save such a value and the wrong one here: reading a
pickle runs whatever code the file names, and the bytes depend on the versions of the libraries that
wrote them, so a library upgrade would move a digest nobody edited. This module writes the value as
a numpy `.npz` archive that is read with `allow_pickle=False`, plus a TOML description of the
container (its kind, dtypes, labels and names) kept inside the same archive, so that a reader can
inspect what a fixture holds by parsing text.

Which values it stores is decided by `compare.py`, the module that decides which values a licence can
compare: `encode` asks `compare._kind` for the kind of the value and refuses a value whose kind is
`None`, naming its type. The codec therefore has no kind list of its own to drift from the
comparator's; `handled_kinds` and `declared_kinds` expose the codec's list and the comparator's so a
test can show they are equal.

Two properties hold for every value `encode` returns bytes for.

- The bytes are deterministic. The archive is written member by member in key order, with a fixed
  timestamp and a fixed host-system byte in each member's header. `numpy.savez` leaves the order to
  the caller's keyword order and takes the host-system byte from the platform (`zipfile.ZipInfo`
  sets it to 0 on Windows and 3 elsewhere), so the bytes of one value, and the digest of a committed
  fixture, could differ between two machines.
- The value reads back exactly. `encode` decodes its own output and asks `compare.Exact` whether
  the decoded value matches, and refuses to return bytes when it does not. A dtype the codec only
  partly understands is therefore refused at write time, naming the value, instead of being stored
  with a quiet change that a later test would have to find.

Within a kind the codec stores a closed list of dtypes: every numpy dtype of the kinds the
comparator handles; object arrays whose elements are `None`, `pandas.NA`, `pandas.NaT`, `bool`,
`int`, `float`, `complex`, `str`, `bytes`, or tuples and lists of these; and the pandas extension
dtypes Int8 to Int64, UInt8 to UInt64, Float32, Float64, boolean, string, category and
time-zone-aware datetime. The comparator accepts further extension dtypes (period, interval, sparse
and Arrow-backed ones) that no input this project needs is built from; a value holding one is refused
naming its dtype, and the owner of an input that needs one extends this list together with a test.

Not established: the exact round trip is shown for the pandas and xarray versions the tests ran
under. `CODEC_VERSION` is raised when the written bytes of any value change, which the pinned-digest
test is there to notice.
"""

from __future__ import annotations

import ast
import base64
import datetime
import hashlib
import inspect
import io
import re
import textwrap
import tomllib
import zipfile
import zoneinfo
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
from pandas.api.extensions import ExtensionArray

from tests.correspondence import compare

CODEC_VERSION = 1
RULE = "npz-bytes"  # the name a digest label carries for the rule that produced the digested bytes

_FORMAT = f"value-codec:{CODEC_VERSION}"
_DESCRIPTOR_KEY = "descriptor"
_FIXED_TIME = (1980, 1, 1, 0, 0, 0)  # the earliest timestamp a zip member can carry
_MASKED_DTYPES = {
    "Int8": 0,
    "Int16": 0,
    "Int32": 0,
    "Int64": 0,
    "UInt8": 0,
    "UInt16": 0,
    "UInt32": 0,
    "UInt64": 0,
    "Float32": 0.0,
    "Float64": 0.0,
    "boolean": False,
}  # name of a masked pandas dtype -> the value written under a masked cell
_SAFE_TEXT = re.compile(r"[ !#-\[\]-~]*")  # printable ASCII except the double quote and the backslash
_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")


class CodecError(ValueError):
    """A value the codec will not store, or bytes it will not read, with the reason in the message."""


# ---- the digest of stored bytes ----


def label(data: bytes) -> str:
    """The labelled digest of stored bytes: `{rule}:{version}:{sha256}`, the rule being the one that wrote them."""
    return f"{RULE}:{CODEC_VERSION}:{hashlib.sha256(data).hexdigest()}"


# ---- text that is safe inside the TOML descriptor ----


def _text(value: str) -> str:
    """Encode any string as ASCII text that needs no TOML escaping, keeping plain text readable.

    A string of printable ASCII without a quote or a backslash is kept as `p:{text}`; anything else
    becomes `b:{base64}` of its UTF-8 bytes, with lone surrogates passed through, because TOML
    refuses a surrogate escape and a descriptor must stay parseable whatever a label contains.
    """
    if _SAFE_TEXT.fullmatch(value):
        return "p:" + value
    return "b:" + base64.b64encode(value.encode("utf-8", "surrogatepass")).decode("ascii")


def _untext(encoded: str) -> str:
    """Invert `_text`."""
    tag, _, rest = encoded.partition(":")
    if tag == "p":
        return rest
    if tag == "b":
        return base64.b64decode(rest.encode("ascii")).decode("utf-8", "surrogatepass")
    raise CodecError(f"the descriptor holds a text with the unknown tag {tag!r}")


def _toml(value: Any) -> str:
    """Write a descriptor value (bool, int, safe string, list or dict) as one line of TOML."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        if not _SAFE_TEXT.fullmatch(value):
            raise CodecError(f"internal error: descriptor text {value!r} would need TOML escaping")
        return f'"{value}"'
    if isinstance(value, list):
        return "[" + ", ".join(_toml(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_key(k)} = {_toml(v)}" for k, v in value.items()) + "}"
    raise CodecError(f"internal error: descriptor value of type {type(value).__name__}")


def _key(name: str) -> str:
    """A TOML key: bare when it can be, quoted otherwise."""
    return name if _BARE_KEY.fullmatch(name) else _toml(name)


# ---- the archive: deterministic bytes holding numpy arrays ----


class _Store:
    """The numpy arrays of one value, under the keys the descriptor refers to them by."""

    def __init__(self) -> None:
        self.arrays: dict[str, np.ndarray] = {}

    def put(self, array: np.ndarray) -> str:
        """Add an array and return its key."""
        key = f"a{len(self.arrays)}"
        self.arrays[key] = array
        return key


def _pack(arrays: dict[str, np.ndarray]) -> bytes:
    """Write arrays as an `.npz` archive in key order, uncompressed, every member stamped the same way on every platform.

    Written member by member rather than with `numpy.savez`, because `savez` takes the zip header's
    host-system byte from the platform, so one value would give different bytes, and a different
    digest, on Windows and on Linux; here the order, the timestamp and that byte are all fixed.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        for key in sorted(arrays):
            info = zipfile.ZipInfo(f"{key}.npy", date_time=_FIXED_TIME)
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            with archive.open(info, "w") as member:
                np.lib.format.write_array(member, arrays[key], allow_pickle=False)
    return buffer.getvalue()


def _unpack(data: bytes) -> dict[str, np.ndarray]:
    """Read every array of an archive with pickles refused, or raise `CodecError` naming what failed."""
    try:
        with np.load(io.BytesIO(data), allow_pickle=False) as archive:
            return {key: archive[key] for key in archive.files}
    except Exception as exc:  # a malformed archive or a refused pickle member raises many types
        raise CodecError(f"the bytes are not a readable value-codec archive: {type(exc).__name__}: {exc}") from exc


# ---- labels, names and attribute values ----


def _plain_array(array: np.ndarray, where: str) -> np.ndarray:
    """Check that an array inside a label has a dtype the codec stores directly, and return it."""
    if array.dtype.kind not in compare._NUMPY_KINDS or array.dtype.kind == "O":
        raise CodecError(f"{where} holds an array of dtype {array.dtype}, which the codec does not store")
    return array


def _tok(store: _Store, value: Any, *, scalars_only: bool = False) -> Any:
    """Describe a name, an attribute value or an object-array element as a TOML-safe token.

    A token is a string `{tag}:{payload}` for a scalar, or a one-key dict for a tuple, a list, a numpy
    scalar or a numpy array. Floats are written in hexadecimal so that no digit is lost. Only exact
    types are accepted: a subclass of `int` or `str` would come back as the base class.
    """
    if value is None:
        return "none"
    if value is pd.NA:
        return "na"
    if value is pd.NaT:
        return "nat"
    if isinstance(value, np.generic | np.ndarray):
        if scalars_only:
            raise CodecError(
                f"an object array holds an element of type {type(value).__name__}, which the codec does not store"
            )
        if isinstance(value, np.generic):
            return {"npscalar": store.put(_plain_array(np.asarray(value), "a label"))}
        if type(value) is not np.ndarray:
            raise CodecError(f"a label of type {type(value).__name__} is not stored; only a plain numpy array is")
        return {"array": store.put(_plain_array(value, "a label"))}
    kind = type(value)
    if kind is bool:
        return f"bool:{int(value)}"
    if kind is int:
        return f"int:{value}"
    if kind is float:
        return f"float:{value.hex()}"
    if kind is complex:
        return f"complex:{value.real.hex()}:{value.imag.hex()}"
    if kind is str:
        return "str:" + _text(value)
    if kind is bytes:
        return "bytes:" + base64.b64encode(value).decode("ascii")
    if kind is tuple or kind is list:
        return {kind.__name__: [_tok(store, item, scalars_only=scalars_only) for item in value]}
    raise CodecError(f"a name, attribute or element of type {kind.__name__} is not stored by the codec")


def _untok(arrays: dict[str, np.ndarray], token: Any) -> Any:
    """Invert `_tok`."""
    if isinstance(token, dict):
        ((tag, payload),) = token.items()
        if tag == "npscalar":
            return arrays[payload][()]
        if tag == "array":
            return arrays[payload]
        if tag == "tuple":
            return tuple(_untok(arrays, item) for item in payload)
        if tag == "list":
            return [_untok(arrays, item) for item in payload]
        raise CodecError(f"the descriptor holds a token with the unknown tag {tag!r}")
    tag, _, payload = token.partition(":")
    if tag == "none":
        return None
    if tag == "na":
        return pd.NA
    if tag == "nat":
        return pd.NaT
    if tag == "bool":
        return payload == "1"
    if tag == "int":
        return int(payload)
    if tag == "float":
        return float.fromhex(payload)
    if tag == "complex":
        real, _, imag = payload.partition(":")
        return complex(float.fromhex(real), float.fromhex(imag))
    if tag == "str":
        return _untext(payload)
    if tag == "bytes":
        return base64.b64decode(payload.encode("ascii"))
    raise CodecError(f"the descriptor holds a token with the unknown tag {tag!r}")


# ---- one piece of data: a numpy array or a pandas extension array ----


def _encode_leaf(store: _Store, leaf: Any) -> dict:
    """Describe one piece of data, putting its numbers in the store."""
    if isinstance(leaf, np.ndarray):
        if leaf.dtype.kind not in compare._NUMPY_KINDS:
            raise CodecError(f"a numpy array of dtype {leaf.dtype} is not stored by the codec")
        if leaf.dtype.kind == "O":
            elements = [_tok(store, element, scalars_only=True) for element in leaf.ravel(order="C")]
            return {"t": "obj", "shape": list(leaf.shape), "e": elements}
        return {"t": "nd", "dtype": leaf.dtype.str, "a": store.put(leaf)}
    if isinstance(leaf, ExtensionArray):
        return _encode_extension(store, leaf)
    raise CodecError(f"a piece of data of type {type(leaf).__name__} is not stored by the codec")


def _encode_extension(store: _Store, array: ExtensionArray) -> dict:
    """Describe a pandas extension array of one of the stored dtypes, or refuse its dtype by name."""
    dtype = array.dtype
    name = str(dtype)
    if name in _MASKED_DTYPES:
        values = array.to_numpy(dtype=dtype.numpy_dtype, na_value=_MASKED_DTYPES[name])
        mask = np.asarray(array.isna(), dtype=bool)
        return {"t": "masked", "dtype": name, "a": store.put(values), "m": store.put(mask)}
    if isinstance(dtype, pd.StringDtype):
        na_value = getattr(dtype, "na_value", pd.NA)
        return {
            "t": "string",
            "storage": str(dtype.storage),
            "na": _tok(store, na_value),
            "e": [_tok(store, element, scalars_only=True) for element in array],
        }
    if isinstance(dtype, pd.CategoricalDtype):
        return {
            "t": "cat",
            "ordered": bool(dtype.ordered),
            "codes": store.put(np.asarray(array.codes)),
            "cats": _encode_index(store, array.categories),
        }
    if isinstance(dtype, pd.DatetimeTZDtype):
        return {
            "t": "dttz",
            "unit": str(dtype.unit),
            "tz": _text(_zone_spec(dtype.tz)),
            "a": store.put(np.asarray(array.asi8)),
        }
    raise CodecError(f"the pandas extension dtype {dtype} is not stored by the codec")


def _zone_spec(zone: Any) -> str:
    """Describe a time zone as `{library}:{payload}` so that decoding rebuilds it with the library that supplied it.

    The comparator tells apart the same instants under zones from different libraries, so a zone
    rebuilt from its name alone through pandas' default library would read back as another value.
    A zone of a library or form not listed here is refused naming its type.
    """
    module = type(zone).__module__.split(".")[0]
    if isinstance(zone, zoneinfo.ZoneInfo):
        return f"zoneinfo:{zone.key}"
    if isinstance(zone, datetime.timezone):
        return f"stdlib:{int(zone.utcoffset(None).total_seconds())}"
    if module == "pytz":
        if getattr(zone, "zone", None):
            return f"pytz:{zone.zone}"
        return f"pytzfixed:{int(zone.utcoffset(None).total_seconds() // 60)}"
    if module == "dateutil" and hasattr(zone, "_filename"):
        name = str(zone._filename)
        return "dateutil:" + (name.rsplit("zoneinfo/", 1)[1] if "zoneinfo/" in name else name)
    raise CodecError(f"the time zone {zone!r} of type {type(zone).__name__} is not stored by the codec")


def _zone_from_spec(spec: str) -> Any:
    """Rebuild a time zone from `_zone_spec`'s description with the library it names."""
    library, _, payload = spec.partition(":")
    try:
        if library == "zoneinfo":
            return zoneinfo.ZoneInfo(payload)
        if library == "stdlib":
            return datetime.timezone(datetime.timedelta(seconds=int(payload)))
        if library == "pytz":
            import pytz

            return pytz.timezone(payload)
        if library == "pytzfixed":
            import pytz

            return pytz.FixedOffset(int(payload))
        if library == "dateutil":
            from dateutil import tz

            zone = tz.gettz(payload)
            if zone is None:
                raise CodecError(f"dateutil has no zone {payload!r}")
            return zone
    except ImportError as exc:
        raise CodecError(f"the time zone library {library!r} is not installed: {exc}") from exc
    raise CodecError(f"the descriptor holds a time zone of the unknown library {library!r}")


def _decode_leaf(arrays: dict[str, np.ndarray], node: dict) -> Any:
    """Rebuild one piece of data from its description."""
    tag = node["t"]
    if tag == "nd":
        array = arrays[node["a"]]
        if array.dtype.str != node["dtype"]:
            raise CodecError(
                f"the descriptor says dtype {node['dtype']} but the stored array has dtype {array.dtype.str}"
            )
        return array
    if tag == "obj":
        elements = [_untok(arrays, token) for token in node["e"]]
        out = np.empty(len(elements), dtype=object)
        for position, element in enumerate(elements):
            out[position] = element
        return out.reshape(tuple(node["shape"]))
    if tag == "masked":
        dtype = pd.api.types.pandas_dtype(node["dtype"])
        return dtype.construct_array_type()(arrays[node["a"]], arrays[node["m"]])
    if tag == "string":
        dtype = pd.StringDtype(storage=node["storage"], na_value=_untok(arrays, node["na"]))
        return pd.array([_untok(arrays, token) for token in node["e"]], dtype=dtype)
    if tag == "cat":
        categories = _decode_index(arrays, node["cats"])
        return pd.Categorical.from_codes(arrays[node["codes"]], categories=categories, ordered=node["ordered"])
    if tag == "dttz":
        naive = arrays[node["a"]].view(f"M8[{node['unit']}]")
        return pd.DatetimeIndex(naive, tz="UTC").tz_convert(_zone_from_spec(_untext(node["tz"]))).array
    raise CodecError(f"the descriptor holds a piece of data with the unknown tag {tag!r}")


# ---- pandas indexes ----


def _pandas_leaf(index: pd.Index) -> Any:
    """An index (or one level of one) as numpy values, or as the extension array it holds."""
    return index.to_numpy() if isinstance(index.dtype, np.dtype) else index.array


def _encode_index(store: _Store, index: pd.Index) -> dict:
    """Describe an index; a `MultiIndex` is described one level at a time, as the comparator reads it."""
    if isinstance(index, pd.MultiIndex):
        levels = [index.get_level_values(position) for position in range(index.nlevels)]
        multi = True
    else:
        levels = [index]
        multi = False
    return {
        "multi": multi,
        "names": [_tok(store, name) for name in index.names],
        "levels": [_encode_leaf(store, _pandas_leaf(level)) for level in levels],
    }


def _make_index(leaf: Any, name: Any) -> pd.Index:
    """An index of one level, keeping the dtype of numpy values instead of letting pandas infer one."""
    if isinstance(leaf, np.ndarray):
        return pd.Index(leaf, dtype=leaf.dtype, name=name)
    return pd.Index(leaf, name=name)


def _decode_index(arrays: dict[str, np.ndarray], node: dict) -> pd.Index:
    """Rebuild an index from its description."""
    names = [_untok(arrays, token) for token in node["names"]]
    leaves = [_decode_leaf(arrays, level) for level in node["levels"]]
    if not node["multi"]:
        return _make_index(leaves[0], names[0])
    levels = [_make_index(leaf, None) for leaf in leaves]
    return pd.MultiIndex.from_arrays(levels, names=names)


# ---- the five kinds ----


def _encode_numpy(store: _Store, value: Any) -> dict:
    """A numpy array, a numpy scalar, or a Python int, float, complex or bool."""
    if isinstance(value, np.generic):
        array = np.asarray(value)
        if array.dtype.kind == "O":
            raise CodecError("a numpy scalar of dtype object is not stored by the codec")
        return {"form": "npscalar", "dtype": array.dtype.str, "a": store.put(array)}
    if type(value) is np.ndarray:
        return {"form": "array", "leaf": _encode_leaf(store, value)}
    return {"form": "py", "v": _tok(store, value)}


def _decode_numpy(arrays: dict[str, np.ndarray], body: dict) -> Any:
    """Invert `_encode_numpy`."""
    if body["form"] == "npscalar":
        array = arrays[body["a"]]
        if array.dtype.str != body["dtype"]:
            raise CodecError(
                f"the descriptor says dtype {body['dtype']} but the stored array has dtype {array.dtype.str}"
            )
        return array[()]
    if body["form"] == "array":
        return _decode_leaf(arrays, body["leaf"])
    return _untok(arrays, body["v"])


def _encode_variable(store: _Store, variable: xr.Variable) -> dict:
    """An xarray variable: its dimension names, attrs and data."""
    leaf = np.asarray(variable.values) if isinstance(variable.dtype, np.dtype) else variable.data
    return {
        "dims": [_tok(store, dim) for dim in variable.dims],
        "attrs": [[_tok(store, key), _tok(store, item)] for key, item in variable.attrs.items()],
        "data": _encode_leaf(store, leaf),
    }


def _decode_variable(arrays: dict[str, np.ndarray], node: dict) -> xr.Variable:
    """Rebuild an xarray variable.

    A time-zone-aware or categorical extension array is handed to xarray inside a pandas `Index`,
    because xarray turns the bare array into an object array and keeps the dtype only through an index.
    """
    leaf = _decode_leaf(arrays, node["data"])
    if isinstance(leaf, ExtensionArray) and str(leaf.dtype) not in _MASKED_DTYPES:
        leaf = pd.Index(leaf)
    dims = tuple(_untok(arrays, dim) for dim in node["dims"])
    attrs = {_untok(arrays, key): _untok(arrays, item) for key, item in node["attrs"]}
    return xr.Variable(dims, leaf, attrs)


def _encode_named_variables(store: _Store, mapping: Any) -> list:
    """The variables of a mapping, in its order, each with its name."""
    return [[_tok(store, name), _encode_variable(store, mapping[name].variable)] for name in mapping]


def _decode_named_variables(arrays: dict[str, np.ndarray], nodes: list) -> dict:
    """Invert `_encode_named_variables`."""
    return {_untok(arrays, name): _decode_variable(arrays, variable) for name, variable in nodes}


def _encode_data_array(store: _Store, value: xr.DataArray) -> dict:
    return {
        "name": _tok(store, value.name),
        "var": _encode_variable(store, value.variable),
        "coords": _encode_named_variables(store, value.coords),
    }


def _decode_data_array(arrays: dict[str, np.ndarray], body: dict) -> xr.DataArray:
    return xr.DataArray(
        _decode_variable(arrays, body["var"]),
        coords=_decode_named_variables(arrays, body["coords"]),
        name=_untok(arrays, body["name"]),
    )


def _encode_dataset(store: _Store, value: xr.Dataset) -> dict:
    return {
        "attrs": [[_tok(store, key), _tok(store, item)] for key, item in value.attrs.items()],
        "vars": _encode_named_variables(store, {name: value[name] for name in value.data_vars}),
        "coords": _encode_named_variables(store, value.coords),
    }


def _decode_dataset(arrays: dict[str, np.ndarray], body: dict) -> xr.Dataset:
    return xr.Dataset(
        data_vars=_decode_named_variables(arrays, body["vars"]),
        coords=_decode_named_variables(arrays, body["coords"]),
        attrs={_untok(arrays, key): _untok(arrays, item) for key, item in body["attrs"]},
    )


def _encode_series(store: _Store, value: pd.Series) -> dict:
    return {
        "index": _encode_index(store, value.index),
        "name": _tok(store, value.name),
        "data": _encode_leaf(store, _pandas_leaf(value)),
    }


def _decode_series(arrays: dict[str, np.ndarray], body: dict) -> pd.Series:
    leaf = _decode_leaf(arrays, body["data"])
    dtype = leaf.dtype if isinstance(leaf, np.ndarray) else None
    return pd.Series(leaf, index=_decode_index(arrays, body["index"]), name=_untok(arrays, body["name"]), dtype=dtype)


def _encode_frame(store: _Store, value: pd.DataFrame) -> dict:
    return {
        "index": _encode_index(store, value.index),
        "columns": _encode_index(store, value.columns),
        "cols": [_encode_leaf(store, _pandas_leaf(value.iloc[:, position])) for position in range(value.shape[1])],
    }


def _decode_frame(arrays: dict[str, np.ndarray], body: dict) -> pd.DataFrame:
    index = _decode_index(arrays, body["index"])
    frame = pd.DataFrame(index=index)
    for position, node in enumerate(body["cols"]):
        frame[position] = _decode_leaf(arrays, node)
    frame.columns = _decode_index(arrays, body["columns"])
    return frame


_ENCODERS: dict[str, Callable[[_Store, Any], dict]] = {
    "numpy": _encode_numpy,
    "DataArray": _encode_data_array,
    "Dataset": _encode_dataset,
    "Series": _encode_series,
    "DataFrame": _encode_frame,
}
_DECODERS: dict[str, Callable[[dict[str, np.ndarray], dict], Any]] = {
    "numpy": _decode_numpy,
    "DataArray": _decode_data_array,
    "Dataset": _decode_dataset,
    "Series": _decode_series,
    "DataFrame": _decode_frame,
}


def handled_kinds() -> frozenset[str]:
    """The kind names this codec has an encoder for."""
    return frozenset(_ENCODERS)


def declared_kinds() -> frozenset[str]:
    """The kind names `compare._kind` can return, read from its source.

    Read from the source and not restated, so that a kind added to the comparator appears here the
    moment it exists, and a test comparing this set with `handled_kinds` fails until the codec
    handles it.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(compare._kind)))
    return frozenset(
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    )


# ---- encode and decode ----


def encode(value: Any) -> bytes:
    """The bytes of `value`, or `CodecError` naming why it cannot be stored or would not read back exactly."""
    kind = compare._kind(value)
    if kind is None:
        detail = f" of dtype {value.dtype}" if isinstance(value, np.ndarray | np.generic) else ""
        raise CodecError(
            f"the codec stores only what a licence can compare, and no licence handles a value of type "
            f"{type(value).__name__!r}{detail}; a licence handles {compare.ACCEPTED}"
        )
    if kind not in _ENCODERS:
        raise CodecError(f"the comparator names the kind {kind!r} and the codec has no encoder for it")
    store = _Store()
    body = _ENCODERS[kind](store, value)
    descriptor = f"codec = {_toml(_FORMAT)}\nkind = {_toml(kind)}\nbody = {_toml(body)}\n"
    arrays = dict(store.arrays)
    arrays[_DESCRIPTOR_KEY] = np.frombuffer(descriptor.encode("ascii"), dtype=np.uint8)
    data = _pack(arrays)
    _require_exact_round_trip(value, data)
    return data


def _require_exact_round_trip(value: Any, data: bytes) -> None:
    """Refuse bytes that do not read back as a value `compare.Exact` admits against the original."""
    try:
        back = decode(data)
        admitted = compare.Exact().admits(value, back)
    except Exception as exc:  # any failure to read back or to compare means the value was not stored faithfully
        raise CodecError(
            f"a value of type {type(value).__name__} did not read back: {type(exc).__name__}: {exc}"
        ) from exc
    if not admitted:
        raise CodecError(
            f"a value of type {type(value).__name__} does not read back exactly: the decoded value is not "
            "admitted by the Exact licence against the original"
        )


def _descriptor_of(arrays: dict[str, np.ndarray]) -> str:
    """The TOML description inside an unpacked archive."""
    if _DESCRIPTOR_KEY not in arrays:
        raise CodecError("the archive holds no descriptor")
    try:
        return arrays[_DESCRIPTOR_KEY].tobytes().decode("ascii")
    except UnicodeDecodeError as exc:
        raise CodecError(f"the descriptor is not ASCII text: {exc}") from exc


def descriptor_text(data: bytes) -> str:
    """The TOML description stored inside bytes `encode` produced, for a reader who wants to see what a file holds."""
    return _descriptor_of(_unpack(data))


def decode(data: bytes) -> Any:
    """The value stored in bytes `encode` produced, or `CodecError` when the bytes are not such an archive."""
    arrays = _unpack(data)
    try:
        description = tomllib.loads(_descriptor_of(arrays))
    except tomllib.TOMLDecodeError as exc:
        raise CodecError(f"the descriptor is not TOML: {exc}") from exc
    if description.get("codec") != _FORMAT:
        raise CodecError(f"the archive was written by {description.get('codec')!r}, and this codec reads {_FORMAT!r}")
    kind = description.get("kind")
    if kind not in _DECODERS:
        raise CodecError(f"the archive holds a value of the unknown kind {kind!r}")
    try:
        return _DECODERS[kind](arrays, description["body"])
    except CodecError:
        raise
    except (KeyError, ValueError, TypeError, IndexError) as exc:
        raise CodecError(
            f"the archive does not describe a value this codec can rebuild: {type(exc).__name__}: {exc}"
        ) from exc
