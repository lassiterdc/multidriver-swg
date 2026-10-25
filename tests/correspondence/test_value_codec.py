"""Tests of the value codec that stores fixtures and reference values without pickle.

Each test docstring names the property it checks, why a reader needs it, the change to the codec
that makes it fail, and a second correct codec that it still passes. Every test writes only under
pytest's temporary directory.
"""

from __future__ import annotations

import ast
import io
import re
import tomllib
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.correspondence import compare, value_codec

pytestmark = pytest.mark.state_check

MODULE_SOURCES = [Path(value_codec.__file__), Path(value_codec.__file__).with_name("fixtures.py")]
ZONE = pd.date_range("2020-01-01", periods=3, freq="h", tz="America/New_York")


def _samples() -> dict[str, object]:
    """One or more values of every kind the comparator handles, covering the dtypes the codec stores."""
    samples: dict[str, object] = {
        "int32 matrix": np.arange(6, dtype="int32").reshape(2, 3),
        "float with nan, inf and negative zero": np.array([1.5, np.nan, -0.0, np.inf]),
        "complex": np.array([1 + 2j, 3 - 1j]),
        "text, width 2": np.array(["a", "é"]),
        "bytes": np.array([b"a", b"bc"]),
        "datetime with NaT, seconds": np.array(["2020-01-01", "NaT"], dtype="M8[s]"),
        "timedelta, milliseconds": np.array([1, 2], dtype="m8[ms]"),
        "bool": np.array([True, False]),
        "empty": np.empty((0, 3)),
        "zero-dimensional array": np.array(5.0),
        "object elements of every stored type": np.array(
            [1, None, "x", 2**70, (1, "a"), pd.NA, pd.NaT, 1.5, b"q", True, 1j], dtype=object
        ),
        "Fortran-ordered": np.asfortranarray(np.arange(6.0).reshape(2, 3)),
        "numpy float32 scalar": np.float32(3.5),
        "numpy datetime scalar": np.datetime64("2020-01-01", "ns"),
        "python int": 7,
        "python int beyond int64": 2**70,
        "python float": 1.25,
        "python complex": 1j,
        "python bool": True,
        "DataArray with coordinates, name and attrs": xr.DataArray(
            np.arange(6.0).reshape(2, 3),
            dims=("x", "y"),
            coords={"x": [1, 2], "y": list("abc")},
            name="v",
            attrs={"units": "m", "levels": np.arange(3)},
        ),
        "DataArray on a time-zone-aware index": xr.DataArray(np.arange(3), dims="time", coords={"time": ZONE}),
        "Dataset with a nullable variable and an auxiliary coordinate": xr.Dataset(
            {"a": (("x",), np.arange(3)), "b": (("x",), pd.array([1, None, 3], dtype="Int64"))},
            coords={"x": [0, 1, 2], "aux": ("x", np.array(list("pqr")))},
            attrs={"title": "run"},
        ),
        "Series": pd.Series([1, 2, 3], index=pd.Index([10, 20, 30], name="i"), name="s"),
        "Series, nullable ints on a zone-aware index": pd.Series(pd.array([1, None, 3], dtype="Int64"), index=ZONE),
        "Series on a MultiIndex of two dtypes": pd.Series(
            [1.0, 2.0],
            index=pd.MultiIndex.from_arrays(
                [pd.Index(np.array([1, 1], dtype="int32")), pd.Index(["a", "b"], dtype=object)], names=["x", "y"]
            ),
        ),
        "DataFrame of mixed dtypes on a duplicated index": pd.DataFrame(
            {
                "a": [1, 2],
                "b": pd.array(["x", None], dtype="string"),
                "c": pd.Categorical(["u", "v"], ordered=True),
                "d": ZONE[:2],
                "e": pd.array([True, None], dtype="boolean"),
                "f": pd.array([0.5, None], dtype="Float32"),
            },
            index=pd.Index([5, 5]),
        ),
        "DataFrame with duplicate column labels": pd.DataFrame([[1, 2], [3, 4]], columns=["a", "a"]),
        "DataFrame with MultiIndex columns": pd.DataFrame(
            np.arange(4).reshape(2, 2), columns=pd.MultiIndex.from_tuples([("a", 1), ("a", 2)])
        ),
        "DataFrame with no columns": pd.DataFrame(index=pd.RangeIndex(3)),
        "DataFrame with no rows or columns": pd.DataFrame(),
    }
    return samples


SAMPLES = _samples()


@pytest.mark.parametrize("name", list(SAMPLES))
def test_every_kind_the_comparator_handles_round_trips_exactly(name):
    """Property: `decode(encode(v))` is admitted by `compare.Exact` against `v`, and a numpy value keeps its container type.

    Class: NEW CAPABILITY; the plausible wrong codec is one that rebuilds through pandas inference
    (an object column of text comes back as another dtype, a Python int as a numpy int).
    Why required: a fixture that reads back differently from what the generator returned moves
    every verdict computed from it with no file edited.
    Killed by: decoding an int32 array as int64, or a time-zone-aware column as naive.
    A second correct codec: any format that preserves dtype, labels and names, such as one per-kind JSON.
    """
    value = SAMPLES[name]
    back = value_codec.decode(value_codec.encode(value))
    assert compare.Exact().admits(value, back)
    if compare._kind(value) == "numpy":
        assert type(back) is type(value)
        if isinstance(value, np.ndarray):
            assert back.dtype == value.dtype
            assert back.shape == value.shape


def test_the_samples_cover_every_kind_the_comparator_names():
    """Property: the round-trip samples include at least one value of each kind `compare._kind` can return.

    Class: NEW CAPABILITY; the wrong suite is one whose samples silently omit a kind.
    Why required: the previous test is only as wide as its samples.
    Killed by: removing the DataFrame samples.
    A second correct suite: samples generated per kind from the declared kind list.
    """
    kinds = {compare._kind(value) for value in SAMPLES.values()}
    assert kinds == value_codec.declared_kinds()


def test_the_codec_handles_exactly_the_kinds_the_comparator_names():
    """Property: the codec's encoder kinds equal the kind names read from `compare._kind`'s source.

    Class: NEW CAPABILITY; the plausible wrong codec keeps its own kind list, which drifts.
    Why required: the codec reads its kind set from the comparator so the two lists cannot disagree.
    Killed by: deleting an encoder, or adding a kind to the comparator without an encoder.
    A second correct implementation: deriving the encoder table from the comparator's list.
    """
    assert value_codec.handled_kinds() == value_codec.declared_kinds()
    assert {"numpy", "DataArray", "Dataset", "Series", "DataFrame"} <= value_codec.declared_kinds()


def test_the_declared_kinds_reader_sees_a_kind_added_to_the_comparator(monkeypatch):
    """Property: `declared_kinds` follows `compare._kind`, so a kind added there makes the equality test fail.

    Class: NEW CAPABILITY; the wrong reader returns a constant, which passes the test above forever.
    Why required: the control that shows the comparison in the previous test can fail.
    Killed by: a `declared_kinds` that returns a literal set.
    A second correct implementation: calling `_kind` on a probe value of each kind.
    """

    def kind_with_an_extra(value):
        """A `_kind` that names one kind the codec has no encoder for, to show the declared set follows the source."""
        if isinstance(value, bytearray):
            return "Zarr"
        return "numpy"

    monkeypatch.setattr(compare, "_kind", kind_with_an_extra)
    assert "Zarr" in value_codec.declared_kinds()
    assert value_codec.handled_kinds() != value_codec.declared_kinds()


@pytest.mark.parametrize(
    "value",
    [
        [1, 2],
        (1, 2),
        {"a": 1},
        "text",
        None,
        pd.Index([1]),
        np.ma.masked_array([1, 2]),
        np.array([(1, 2)], dtype=[("a", "i4"), ("b", "i4")]),
    ],
    ids=["list", "tuple", "dict", "str", "None", "Index", "masked array", "structured array"],
)
def test_a_value_of_a_kind_the_comparator_turns_away_is_refused_by_name(value):
    """Property: `encode` raises `CodecError` naming the value's type when `compare._kind` returns None.

    Class: NEW CAPABILITY; the wrong codec pickles or stringifies what it does not know.
    Why required: a value no licence compares would be stored and never usable.
    Killed by: a fallback that stores any object.
    A second correct implementation: raising the comparator's own TypeError message.
    """
    with pytest.raises(value_codec.CodecError, match=type(value).__name__):
        value_codec.encode(value)


@pytest.mark.parametrize(
    ("value", "dtype_text"),
    [
        (pd.Series(pd.period_range("2020", periods=2, freq="M")), "period[M]"),
        (pd.Series(pd.arrays.SparseArray([0, 1])), "Sparse"),
        (pd.Series(pd.arrays.IntervalArray.from_breaks([0, 1, 2])), "interval"),
        (np.array([np.float64(1.0)], dtype=object), "float64"),
        (np.array([object()], dtype=object), "object"),
    ],
    ids=["period", "sparse", "interval", "numpy scalar in an object array", "arbitrary object"],
)
def test_a_dtype_or_element_outside_the_stored_list_is_refused_naming_it(value, dtype_text):
    """Property: a value of a handled kind that holds a dtype or element outside the closed stored list is refused naming it.

    Class: NEW CAPABILITY; the wrong codec stores such a value through a lossy path (a string, a pickle).
    Why required: the list is closed on purpose, and an unlisted dtype must fail loudly where it is written.
    Killed by: storing an unlisted extension dtype as its object values.
    A second correct implementation: extending the list with a test for each added dtype.
    """
    with pytest.raises(value_codec.CodecError, match=re.escape(dtype_text)):
        value_codec.encode(value)


def test_a_codec_that_reads_back_a_different_dtype_makes_encode_refuse(monkeypatch):
    """Property: `encode` refuses bytes whose decoded value `compare.Exact` does not admit, saying it did not read back exactly.

    Class: NEW CAPABILITY; the wrong codec stores a value it decodes with a quiet change.
    Why required: this self-check is what turns every unforeseen dtype corner into a write-time refusal.
    Killed by: removing `_require_exact_round_trip` from `encode`.
    A second correct implementation: comparing the encoded descriptor with a re-encoding of the decoded value.
    """
    original = value_codec._decode_leaf

    def widening(arrays, node):
        """A leaf decoder that returns every integer array as int64, the plausible quiet change the self-check must catch."""
        leaf = original(arrays, node)
        return leaf.astype("int64") if isinstance(leaf, np.ndarray) and leaf.dtype.kind == "i" else leaf

    value = np.arange(3, dtype="int32")
    assert value_codec.decode(value_codec.encode(value)).dtype == np.dtype("int32")  # control: unpatched reads back
    monkeypatch.setattr(value_codec, "_decode_leaf", widening)
    with pytest.raises(value_codec.CodecError, match="exactly"):
        value_codec.encode(value)


@pytest.mark.parametrize("name", list(SAMPLES))
def test_the_bytes_of_a_value_are_the_same_on_every_encoding(name):
    """Property: encoding one value twice gives identical bytes, and every archive member carries the fixed timestamp and host-system byte.

    Class: NEW CAPABILITY; the wrong codec lets the platform or the clock reach the archive's header.
    Why required: a fixture's digest must be a function of its content alone, on every machine.
    Killed by: writing members with a `ZipInfo` whose timestamp or `create_system` is not the pinned one.
    A second correct codec: hashing a canonical form of the content instead of the archive's bytes.
    Not established: that two platforms give equal bytes; only the header fields that differ by platform are pinned.
    """
    first = value_codec.encode(SAMPLES[name])
    assert value_codec.encode(SAMPLES[name]) == first
    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        assert {info.date_time for info in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}
        assert {info.compress_type for info in archive.infolist()} == {zipfile.ZIP_STORED}
        assert {info.create_system for info in archive.infolist()} == {3}


def test_the_label_of_a_fixed_value_is_the_pinned_one():
    """Property: one fixed integer matrix encodes to the committed digest under the running numpy.

    Class: NEW CAPABILITY; the wrong codec lets a numpy or zip change alter bytes of committed fixtures unnoticed.
    Why required: a committed fixture is identified by this digest, and a change of the written bytes
    must come with a raised `CODEC_VERSION`, not as every fixture reading as edited.
    Killed by: changing the member order, the header, the descriptor text or the timestamp.
    A second correct implementation: a different archive layout with this constant and `CODEC_VERSION` raised together.
    If this fails: the written bytes changed. Raise `value_codec.CODEC_VERSION` and regenerate every fixture.
    Not established: the pin holds for the numpy versions the suite ran under.
    """
    data = value_codec.encode(np.arange(6, dtype="<i8").reshape(2, 3))
    assert value_codec.label(data) == "npz-bytes:1:ed26154a921dfb9ea069e58257aba007abfc34a516ff7446d513fb2c9047fdfa"


def _archive_with_a_pickled_member(tmp_path: Path) -> tuple[bytes, Path]:
    """An archive shaped like the codec's whose array member is a pickle that creates a file when loaded."""
    marker = tmp_path / "pickle-ran"
    payload = np.empty(1, dtype=object)
    payload[0] = _Creates(str(marker))
    buffer = io.BytesIO()
    descriptor = value_codec.descriptor_text(value_codec.encode(np.arange(3.0)))
    np.savez(buffer, a0=payload, descriptor=np.frombuffer(descriptor.encode("ascii"), dtype=np.uint8))
    return buffer.getvalue(), marker


class _Creates:
    """An object whose pickle opens a file for writing when it is loaded."""

    def __init__(self, path: str):
        self.path = path

    def __reduce__(self):
        return (open, (self.path, "w"))


def test_reading_an_archive_with_a_pickled_member_runs_nothing(tmp_path):
    """Property: `decode` refuses an archive whose member is a pickle, and the pickle's code does not run.

    Class: NEW CAPABILITY; the wrong codec loads with `allow_pickle=True`.
    Why required: loading a fixture must not execute code; that is the whole reason for this codec.
    Killed by: `np.load(..., allow_pickle=True)` in `_unpack`.
    A second correct codec: refusing every member whose dtype is object before reading it.
    """
    data, marker = _archive_with_a_pickled_member(tmp_path)
    with pytest.raises(value_codec.CodecError):
        value_codec.decode(data)
    assert not marker.exists()


def test_the_pickled_archive_does_run_code_when_pickles_are_allowed(tmp_path):
    """Property (the control of the test above): the same archive creates the marker file when loaded with pickles allowed.

    Class: NEW CAPABILITY; without this, an absent marker could mean the archive never carried a runnable pickle.
    Why required: it makes the absent marker of the previous test evidence of the refusal.
    Killed by: building the archive without a pickled member.
    A second correct implementation: any payload with a side effect on load.
    """
    data, marker = _archive_with_a_pickled_member(tmp_path)
    with np.load(io.BytesIO(data), allow_pickle=True) as archive:
        archive["a0"]
    assert marker.exists()


@pytest.mark.parametrize("path", MODULE_SOURCES, ids=lambda p: p.name)
def test_neither_module_can_load_a_pickle(path):
    """Property: neither the codec nor the fixture module imports a pickle library or passes `allow_pickle=True`.

    Class: NEW CAPABILITY; the wrong module reaches for pickle for a value the codec could not store.
    Why required: the guarantee that loading cannot run code is only as good as the absence of any pickle path.
    Killed by: `import pickle` or `allow_pickle=True` added to either file.
    A second correct implementation: a lint rule banning those imports.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    } | {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
    assert not imported & {"pickle", "dill", "cloudpickle", "shelve", "marshal"}
    enabled = [
        keyword
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "allow_pickle"
        and not (isinstance(keyword.value, ast.Constant) and keyword.value.value is False)
    ]
    assert enabled == []


def _rewrite(data: bytes, change) -> bytes:
    """The same archive with its arrays changed by `change(arrays)`, written by the codec's own packer."""
    arrays = value_codec._unpack(data)
    change(arrays)
    return value_codec._pack(arrays)


def test_decode_refuses_bytes_that_are_not_a_codec_archive():
    """Property: `decode` raises `CodecError` for bytes that are not an archive, and for an archive without a descriptor.

    Class: NEW CAPABILITY; the wrong decoder lets numpy's or zipfile's own error escape unnamed.
    Why required: a damaged fixture is reported as such, not as a crash in the library.
    Killed by: removing the wrapping in `_unpack` or the descriptor check.
    A second correct implementation: checking the zip magic first.
    """
    with pytest.raises(value_codec.CodecError, match="not a readable"):
        value_codec.decode(b"this is not an archive")
    without = _rewrite(value_codec.encode(np.arange(3.0)), lambda arrays: arrays.pop("descriptor"))
    with pytest.raises(value_codec.CodecError, match="no descriptor"):
        value_codec.decode(without)


def test_decode_refuses_an_archive_of_another_codec_version():
    """Property: an archive whose descriptor names another codec version is refused, naming both versions.

    Class: NEW CAPABILITY; the wrong decoder reads old bytes under new rules.
    Why required: a raised `CODEC_VERSION` must make old files fail by name, not decode differently.
    Killed by: dropping the version comparison.
    A second correct implementation: a table of readers by version.
    """
    descriptor = value_codec.descriptor_text(value_codec.encode(np.arange(3.0))).replace(
        "value-codec:1", "value-codec:99"
    )
    data = _rewrite(
        value_codec.encode(np.arange(3.0)),
        lambda arrays: arrays.__setitem__("descriptor", np.frombuffer(descriptor.encode("ascii"), dtype=np.uint8)),
    )
    with pytest.raises(value_codec.CodecError, match="value-codec:99"):
        value_codec.decode(data)


def test_decode_refuses_an_array_whose_dtype_disagrees_with_the_descriptor():
    """Property: an array swapped for one of another dtype is refused, naming both dtypes.

    Class: NEW CAPABILITY; the wrong decoder trusts the array and ignores the descriptor.
    Why required: the descriptor's dtypes are the readable record of what a fixture holds; they must be true.
    Killed by: dropping the dtype comparison in `_decode_leaf`.
    A second correct implementation: omitting dtypes from the descriptor, at the cost of the readable record.
    """
    data = value_codec.encode(np.arange(3, dtype="<i8"))
    swapped = _rewrite(data, lambda arrays: arrays.__setitem__("a0", arrays["a0"].astype("<f8")))
    with pytest.raises(value_codec.CodecError, match="<i8"):
        value_codec.decode(swapped)


def test_the_descriptor_is_toml_that_states_the_kind_and_the_dtypes():
    """Property: the descriptor inside the archive parses as TOML and states the kind, the codec version and every dtype.

    Class: NEW CAPABILITY; the wrong codec stores the container description as opaque bytes.
    Why required: a reviewer reads what a fixture holds by parsing text.
    Killed by: writing the descriptor as a binary or non-TOML format.
    A second correct codec: JSON, if the descriptor were allowed to be something other than TOML.
    """
    frame = pd.DataFrame({"a": [1.5], "b": pd.array([1], dtype="Int64")})
    document = tomllib.loads(value_codec.descriptor_text(value_codec.encode(frame)))
    assert document["codec"] == "value-codec:1"
    assert document["kind"] == "DataFrame"
    columns = document["body"]["cols"]
    assert [column.get("dtype") for column in columns] == ["<f8", "Int64"]


def test_awkward_text_in_labels_round_trips_and_the_descriptor_still_parses():
    """Property: quotes, backslashes, newlines, non-BMP characters and a lone surrogate in names, attrs and text round-trip.

    Class: NEW CAPABILITY; the wrong codec writes text into TOML unescaped, or with JSON escapes that TOML refuses.
    Why required: labels are user text, and a descriptor that cannot be parsed hides the whole fixture.
    Killed by: writing `str` tokens as raw TOML strings.
    A second correct codec: base64 for every string, losing the readable plain form.
    """
    awkward = 'q"uo\\te\n\U0001f600 \ud800'
    frame = pd.DataFrame({awkward: [awkward]}, index=pd.Index([awkward], name=awkward))
    frame.attrs = {}
    array = xr.DataArray([1.0], dims=awkward, coords={awkward: [awkward]}, name=awkward, attrs={awkward: awkward})
    for value in (frame, array, pd.Series([awkward], name=awkward)):
        data = value_codec.encode(value)
        tomllib.loads(value_codec.descriptor_text(data))
        assert compare.Exact().admits(value, value_codec.decode(data))


def test_the_labelled_digest_names_its_rule_and_version():
    """Property: `label` reads `{rule}:{version}:{sha256}` and moves with one changed byte.

    Class: NEW CAPABILITY; the wrong label carries only a bare hash, naming no rule.
    Why required: the shared label form lets a reader say which rule a digest came from.
    Killed by: returning `hexdigest()` alone.
    A second correct implementation: a different rule name with the same three fields.
    """
    data = value_codec.encode(np.arange(3.0))
    rule, version, digest = value_codec.label(data).split(":")
    assert (rule, version) == (value_codec.RULE, str(value_codec.CODEC_VERSION))
    assert len(digest) == 64
    assert value_codec.label(data[:-1] + bytes([data[-1] ^ 1])) != value_codec.label(data)
