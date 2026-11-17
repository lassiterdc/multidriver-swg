"""Check the run-start record: its one versioned object, its field refusals, and the file's round trip.

The record is read inside an engine parse, where the exception's text is the only channel to the
operator, so each refusal here is asserted to name the FIELD and the FILE rather than the defect alone.
The malformed inputs are the ones a hand-written record produces: a misspelled key, an omitted field, a
path written relative to the run output, and a judgement state invented for a branch that does not exist.

Every check names the wrong implementation it is red under, because a check that cannot fail for the
reason it claims constrains nothing.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

import multidriver_swg.workflow.run_start as run_start
from multidriver_swg.workflow.run_start import (
    JUDGEMENT_STATES,
    RUN_START_FILE,
    RUN_START_RULE,
    RunStart,
    RunStartError,
    read_run_start,
)

# The three judgement states, as the record's own tuple declares them.
EXPECTED_STATES = ("present", "kept-absent", "delegated")


def _record(**overrides: object) -> RunStart:
    """A conforming record, with `overrides` applied to the four value fields."""
    values: dict[str, object] = {
        "main_configuration": "/estate/config/run.toml",
        "targets": ("observed_record_assembly/observed_record.json",),
        "selection": "/estate/projects/smoke/selection.toml",
        "judgement": "present",
    }
    values.update(overrides)
    return RunStart(**values)  # type: ignore[arg-type]


def _document(**overrides: object) -> dict[str, object]:
    """A conforming document, with `overrides` applied or a value of `_ABSENT` removing the key."""
    document = _record().to_document()
    for key, value in overrides.items():
        if value is _ABSENT:
            document.pop(key, None)
        else:
            document[key] = value
    return document


_ABSENT = object()


def _written(tmp_path: Path, document: object) -> Path:
    """Write `document` as JSON to the record's file name under `tmp_path` and return the path."""
    path = tmp_path / RUN_START_FILE
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return path


def test_a_conforming_record_round_trips_through_the_file(tmp_path: Path) -> None:
    """Property: `write` then `read_run_start` returns an equal record, the file holds exactly the five keys, and the version literal is first.

    Class: a conforming record with a selection and the `present` state.
    Why it is required: the record is the only channel from the preparing step to every engine parse, so a
    field the round trip loses is a fact the parse cannot recover.
    What kills it: writing the record without the format key, renaming a field, or reading a field from the
    wrong key.
    Second correct implementation: any serializer that writes the same object; the test parses the file and
    compares values.
    """
    path = tmp_path / RUN_START_FILE
    record = _record()

    record.write(path)

    assert read_run_start(path) == record
    document = json.loads(path.read_text(encoding="utf-8"))
    assert list(document) == ["format", "main_configuration", "targets", "selection", "judgement"]
    assert document["format"] == RUN_START_RULE
    assert path.read_text(encoding="utf-8").endswith("\n")


def test_the_record_is_frozen_and_its_targets_are_a_tuple() -> None:
    """Property: a record refuses reassignment and stores targets as a tuple.

    Class: a conforming record and each of its four fields.
    Why it is required: a parse holds the record while it renders rules, so a mutable record would let one
    consumer change what another renders.
    What kills it: dropping `frozen=True`, or storing `targets` as a list.
    Second correct implementation: a named tuple; the same assignments are refused.
    """
    record = _record()

    assert isinstance(record.targets, tuple)
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.judgement = "delegated"  # type: ignore[misc]


def test_the_form_constants_are_the_file_name_the_literal_and_the_three_states() -> None:
    """Property: the module publishes `run_start.json`, `run-start:1` and the three judgement states.

    Class: the three module constants.
    Why it is required: two parties spell the file name and the version literal — the preparer writes,
    every parse reads — so a drift between them would make the gate refuse every run.
    What kills it: renaming the file, editing the literal, or dropping a state.
    Second correct implementation: constants at any position; the values decide.
    """
    assert RUN_START_FILE == "run_start.json"
    assert RUN_START_RULE == "run-start:1"
    assert JUDGEMENT_STATES == EXPECTED_STATES


def test_an_absent_file_is_left_to_the_operating_system(tmp_path: Path) -> None:
    """Property: a path that names no file raises the operating system's error rather than this module's refusal.

    Class: a path in an empty directory.
    Why it is required: the gate distinguishes "the preparer never wrote a record" from "it wrote one this
    form refuses", and it can only do that if the two arrive as different exceptions.
    What kills it: catching `OSError` inside `read_run_start` and re-raising `RunStartError`.
    Second correct implementation: an explicit existence check raising `FileNotFoundError`; the same class
    arrives.
    """
    with pytest.raises(FileNotFoundError):
        read_run_start(tmp_path / RUN_START_FILE)


@pytest.mark.parametrize(
    ("content", "case"),
    [
        pytest.param(b"not json at all", "invalid-json", id="invalid-json"),
        pytest.param(b"[]", "array", id="array"),
        pytest.param(b'"a string"', "scalar", id="scalar"),
        pytest.param(b'{"rule": "run-start:1"}', "wrong-key", id="wrong-key"),
        pytest.param(b"\xff\xfe", "not-utf8", id="not-utf8"),
    ],
)
def test_a_file_that_is_not_one_conforming_object_is_refused_naming_it(
    tmp_path: Path, content: bytes, case: str
) -> None:
    """Property: invalid JSON, a non-object, bytes that are not UTF-8, and an object without the fields each raise `RunStartError` naming the file.

    Class: four documents no parse can read, plus the conforming object as the admitted control.
    Why it is required: the reader runs inside an engine parse, so a raw `JSONDecodeError` or
    `UnicodeDecodeError` would reach the operator with a byte offset and no file.
    What kills it: parsing without catching, or letting a non-object reach the field lookups.
    Second correct implementation: reading bytes and decoding inside the translating block; the same five
    inputs decide as here.
    """
    path = tmp_path / RUN_START_FILE
    path.write_bytes(content)

    with pytest.raises(RunStartError) as caught:
        read_run_start(path)

    assert str(path) in str(caught.value)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"format": "run-start:2"}, id="another-version"),
        pytest.param({"format": _ABSENT}, id="absent-format"),
        pytest.param({"unexpected": 1}, id="unnamed-key"),
        pytest.param({"main_configuration": _ABSENT}, id="absent-configuration"),
        pytest.param({"targets": _ABSENT}, id="absent-targets"),
        pytest.param({"selection": _ABSENT}, id="absent-selection"),
        pytest.param({"judgement": _ABSENT}, id="absent-judgement"),
    ],
)
def test_a_wrong_version_an_unnamed_key_or_an_absent_field_is_refused(
    tmp_path: Path, overrides: dict[str, object]
) -> None:
    """Property: a version other than the literal, a key the form does not name, and each omitted field raise `RunStartError` naming the failing key.

    Class: the version literal, one unnamed key, and the five fields omitted one at a time.
    Why it is required: the header makes a later form change loud, an unnamed key is a fact its writer
    believes is read, and an omitted field is one the parse cannot recover — three different repairs, all
    reaching the same exception text.
    What kills it: reading only the keys the reader needs, or defaulting an absent field.
    Second correct implementation: a closed key set plus per-field presence checks in either order; the
    same seven documents are refused.
    """
    path = _written(tmp_path, _document(**overrides))

    with pytest.raises(RunStartError) as caught:
        read_run_start(path)

    message = str(caught.value)
    assert str(path) in message
    expected = next(iter(overrides))
    assert expected in message


@pytest.mark.parametrize(
    ("overrides", "field_name"),
    [
        pytest.param({"main_configuration": "config/run.toml"}, "main_configuration", id="relative-configuration"),
        pytest.param({"main_configuration": 7}, "main_configuration", id="non-text-configuration"),
        pytest.param({"selection": "selection.toml"}, "selection", id="relative-selection"),
        pytest.param({"selection": 7}, "selection", id="non-text-selection"),
        pytest.param({"targets": ()}, "targets", id="empty-targets"),
        pytest.param({"targets": "a.json"}, "targets", id="targets-not-tuple"),
        pytest.param({"targets": ("/absolute.json",)}, "targets", id="absolute-target"),
        pytest.param({"targets": ("",)}, "targets", id="empty-target"),
        pytest.param({"judgement": "deferred"}, "judgement", id="unknown-state"),
    ],
)
def test_a_field_outside_the_form_is_refused_by_construction_and_by_read(
    tmp_path: Path, overrides: dict[str, object], field_name: str
) -> None:
    """Property: each malformed value is refused both by direct construction and by the file reader, with the field named.

    Class: a relative or non-text configuration path, the same for the selection, four malformed target
    collections, and a judgement state outside the three.
    Why it is required: the construct is public, so validating only in `read_run_start` would leave a
    hand-built record unchecked; and each value asks for a different repair.
    What kills it: absolute-path checks only on one path field, a targets check that only tests emptiness,
    or a judgement membership test that accepts any text.
    Second correct implementation: the checks in a classmethod used by both routes; the same nine values
    are refused.
    """
    document = _record().to_document()
    document.update(overrides)
    path = _written(tmp_path, document)

    with pytest.raises(RunStartError) as from_file:
        read_run_start(path)
    assert repr(field_name) in str(from_file.value)

    values: dict[str, object] = {
        "main_configuration": "/estate/config/run.toml",
        "targets": ("observed_record_assembly/observed_record.json",),
        "selection": "/estate/projects/smoke/selection.toml",
        "judgement": "present",
    }
    values.update(overrides)
    with pytest.raises(RunStartError):
        RunStart(**values)  # type: ignore[arg-type]


def test_the_module_docstring_states_the_three_commitments() -> None:
    """Property: the module docstring contains the three statements the record implements and names its writer and reader.

    Class: any version of the docstring, searched whole.
    Why it is required: the statements are the deliverable text; a docstring reduced to a field list would
    leave the record's reason unstated for the next editor.
    What kills it: deleting or rephrasing a statement, or ceasing to name `write` and `read_run_start`.
    Second correct implementation: the sentences in any surrounding prose; containment only is asserted.
    """
    docstring = run_start.__doc__ or ""

    assert "One file, one versioned object, five fields" in docstring
    assert "The record is written by the run's preparation and READ by every engine parse" in docstring
    assert "A record that does not parse as the form is refused naming the field" in docstring
    assert "write" in docstring
    assert "read_run_start" in docstring
