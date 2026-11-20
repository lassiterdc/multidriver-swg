"""Check the main configuration's form: one versioned file, five required fields, three conditional tables.

Each refusal here is worth asserting only if it fires on the input an easier implementation would
admit. The easier implementation is a plain `tomllib.load` whose values are read where they are used,
and it admits every defect below: a key nothing reads, a relative estate root, an absolute binding
path, a project with three segments, a mode this code cannot launch, and a limit that is a boolean.
Those are the inputs the checks are written against.

The loading tests go through a real file, because the property that matters to an operator is that a
refusal names the FILE as well as the field: an in-memory mapping cannot show that the path is
attached. The direct-construction tests then show the same value checks are reachable without a file,
which is what keeps a hand-built configuration validated.

Every check names the wrong implementation it is red under, because a check that cannot fail for the
reason it claims constrains nothing.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import multidriver_swg.run_configuration.main_configuration as main_configuration
from multidriver_swg.run_configuration.main_configuration import (
    MAIN_CONFIGURATION_RULE,
    MODES,
    PATH_KEY_KINDS,
    JudgementConfiguration,
    LocalConfiguration,
    MainConfiguration,
    MainConfigurationError,
    SlurmConfiguration,
    VersionStackingConfiguration,
)
from multidriver_swg.run_configuration.path_resolver import PathKey, PathResolutionError

# A sentinel for a test that removes a key from the conforming document. `None` cannot serve: it is a
# value a malformed document can legitimately carry, and the refusal for an absent field and the one
# for a null field would then be the same input.
_ABSENT = object()

# The two commitments the module states in its own words. The docstring check below asserts these exact
# sentences, so a later editor dropping one makes the check red rather than leaving the module silent.
ONE_FILE_PER_MACHINE = (
    "One run configuration file exists per machine and project, and it is held by the estate rather than the toolkit, "
    "so a second machine adds its own file and edits none."
)
NO_MACHINE_VALUE_IN_THE_TOOLKIT = (
    "No tracked toolkit file holds a machine-specific value: the configuration states every value the run reads from "
    "this machine, and the toolkit's only route to it is a path the caller supplies."
)


def _document(*, mode: str = "local", **overrides: object) -> dict[str, object]:
    """A conforming local configuration, with `overrides` applied and `_ABSENT` removing a key."""
    document: dict[str, object] = {
        "rule": MAIN_CONFIGURATION_RULE,
        "estate_root": "/estate",
        "bindings": "config/paths_this_machine.toml",
        "project": "somewhere.river",
        "mode": mode,
        "local": {"cores": 4},
    }
    for key, value in overrides.items():
        if value is _ABSENT:
            document.pop(key, None)
        else:
            document[key] = value
    return document


def _hpc_document(mode: str, **slurm_overrides: object) -> dict[str, object]:
    """A conforming document in one HPC mode, carrying the limit that mode reads, with `slurm_overrides` applied.

    An override value of `_ABSENT` removes the key, so a test can assert the refusal for a table that
    omits the very limit its mode reads.
    """
    slurm: dict[str, object] = {"partition": "compute", "account": "allocation", "time_minutes": 60}
    slurm["jobs" if mode == "batch_job" else "cpus_per_task"] = 8
    for key, value in slurm_overrides.items():
        if value is _ABSENT:
            slurm.pop(key, None)
        else:
            slurm[key] = value
    return _document(mode=mode, local=_ABSENT, slurm=slurm)


def _toml_scalar(value: object) -> str:
    """Render one value as a TOML scalar.

    Strings go through `json.dumps`, whose basic-string escapes coincide with a TOML basic string's for
    every value these tests write; a value outside that overlap would need a real writer, and carrying a
    dependency for a fixture is not worth it.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value) if isinstance(value, float) else str(value)
    return json.dumps(value)


def _toml(document: Mapping[str, object]) -> str:
    """Render `document` as the one-level TOML this form uses, scalars before tables."""
    lines: list[str] = []
    tables: list[tuple[str, Mapping[str, Any]]] = []
    for key, value in document.items():
        if isinstance(value, dict):
            tables.append((key, value))
        else:
            lines.append(f"{key} = {_toml_scalar(value)}")
    for key, table in tables:
        lines.append("")
        lines.append(f"[{key}]")
        for inner_key, inner_value in table.items():
            lines.append(f"{inner_key} = {_toml_scalar(inner_value)}")
    return "\n".join(lines) + "\n"


def _written(tmp_path: Path, document: Mapping[str, object]) -> Path:
    """Write `document` to a file in `tmp_path` and return the path."""
    path = tmp_path / "run_configuration.toml"
    path.write_text(_toml(document), encoding="utf-8")
    return path


def _refused(tmp_path: Path, document: Mapping[str, object]) -> str:
    """Load `document` from a file, assert a refusal was raised, and return its message."""
    path = _written(tmp_path, document)
    with pytest.raises(MainConfigurationError) as caught:
        MainConfiguration.load(path)
    return str(caught.value)


def test_a_conforming_local_configuration_loads_and_carries_every_field(tmp_path: Path) -> None:
    """Property: a conforming local file loads, and every field of the form is readable from the result.

    Class: every file stating the five required fields and a `[local]` table with a positive core count,
    whose estate root is absolute and whose binding path is relative.
    Why it is required: the loader is the only route to a run's configuration, so a conforming file it
    refused would make every run impossible, and a field it silently dropped would be a setting the
    operator wrote and no code read.
    What kills it: reading a field from the wrong table, defaulting one of the five required fields, or
    dropping the `[judgement]` default so an absent table raises.
    Second correct implementation: one that reads the document into a mapping and then constructs the
    same value fields; it returns an equal configuration.
    """
    configuration = MainConfiguration.load(_written(tmp_path, _document()))

    assert configuration == MainConfiguration(
        rule=MAIN_CONFIGURATION_RULE,
        estate_root="/estate",
        bindings="config/paths_this_machine.toml",
        project="somewhere.river",
        mode="local",
        judgement=JudgementConfiguration(),
        local=LocalConfiguration(cores=4),
        slurm=None,
    )


@pytest.mark.parametrize("mode", MODES)
def test_every_mode_the_form_names_loads_from_its_own_required_fields(tmp_path: Path, mode: str) -> None:
    """Property: each of the three modes loads from a document carrying exactly the fields that mode reads.

    Class: one conforming document per mode — `local` with `[local] cores`, `batch_job` with `[slurm]`
    connection and `jobs`, `1_job_many_srun_tasks` with `[slurm]` connection and `cpus_per_task`.
    Why it is required: the conditional table is the one place a mode's requirements live, so a mode
    whose own document were refused would be unlaunchable.
    What kills it: requiring `jobs` in the single-allocation mode, requiring `cpus_per_task` in the batch
    mode, or demanding both in each.
    Second correct implementation: requirements checked per mode in a dispatch table rather than in
    branches; the same three documents load.
    """
    document = _document() if mode == "local" else _hpc_document(mode)

    configuration = MainConfiguration.load(_written(tmp_path, document))

    assert configuration.mode == mode


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"rule": _ABSENT}, id="absent"),
        pytest.param({"rule": "main-configuration:2"}, id="another-version"),
        pytest.param({"rule": 7}, id="not-text"),
    ],
)
def test_the_rule_header_is_a_fixed_literal_compared_exactly(tmp_path: Path, overrides: dict[str, object]) -> None:
    """Property: `rule` is refused unless it is the literal `main-configuration:1`, and the refusal names the field and the file.

    Class: absent, another version, and a non-text value.
    Why it is required: the header exists so a later change to the form is loud; a tolerant read would
    let an older file drop the field a newer form added.
    What kills it: `startswith("main-configuration")`, or reading `rule` as an optional field.
    Second correct implementation: comparing against the module constant at any point in the load.
    """
    document = _document(**overrides)

    message = _refused(tmp_path, document)

    assert "'rule'" in message
    assert str(tmp_path / "run_configuration.toml") in message


@pytest.mark.parametrize(
    "estate_root",
    [
        pytest.param("estate", id="relative"),
        pytest.param("", id="empty"),
        pytest.param(7, id="not-text"),
    ],
)
def test_estate_root_is_refused_unless_it_is_an_absolute_non_empty_path(tmp_path: Path, estate_root: object) -> None:
    """Property: `estate_root` is refused unless it is non-empty absolute text, naming the field and the file.

    Class: a relative path, an empty path, and a non-text value.
    Why it is required: a relative root resolves against the working directory a run happens to start in,
    so the same file would name different estates on two invocations.
    What kills it: accepting a relative path, or defaulting the root.
    Second correct implementation: `Path(value).is_absolute()` on a constructed path; the same three
    inputs are refused.
    """
    message = _refused(tmp_path, _document(estate_root=estate_root))

    assert "'estate_root'" in message


@pytest.mark.parametrize(
    "bindings",
    [
        pytest.param("/config/paths_this_machine.toml", id="absolute"),
        pytest.param("", id="empty"),
        pytest.param(7, id="not-text"),
    ],
)
def test_bindings_is_refused_unless_it_is_a_relative_non_empty_path(tmp_path: Path, bindings: object) -> None:
    """Property: `bindings` is refused unless it is non-empty text relative to the estate root.

    Class: an absolute path, an empty path, and a non-text value.
    Why it is required: the field composes with `estate_root` into one location, so an absolute value
    carries a second, uncomposed statement of where the estate is — the leak the two fields exist to
    prevent.
    What kills it: accepting an absolute path, or resolving it against the working directory.
    Second correct implementation: checking the value against `PurePath.is_absolute()`; the same three
    inputs are refused.
    """
    message = _refused(tmp_path, _document(bindings=bindings))

    assert "'bindings'" in message


@pytest.mark.parametrize(
    "project",
    [
        pytest.param("somewhere", id="one-segment"),
        pytest.param("somewhere.river.extra", id="three-segments"),
        pytest.param("somewhere.riv/er", id="separator-in-segment"),
        pytest.param("somewhere.shared", id="reserved-project"),
        pytest.param("some where.river", id="space-in-segment"),
        pytest.param(7, id="not-text"),
    ],
)
def test_project_is_refused_unless_it_is_two_path_key_segments(tmp_path: Path, project: object) -> None:
    """Property: `project` is refused unless it is `domain.project`, two segments of the key charset, the second not `shared`.

    Class: one segment, three segments, a separator inside a segment, the reserved project name, a space,
    and a non-text value.
    Why it is required: the project is what the three fixed path keys are built from, so a spelling the
    key grammar refuses would fail later at construction, and one the grammar admits but the resolver
    reserves would shadow a domain's shared directory.
    What kills it: passing the field through unvalidated, or validating the segment count only — which
    admits `somewhere.riv/er`.
    Second correct implementation: validating the whole two-segment spelling against the key grammar in
    one step; the same six inputs are refused.
    """
    message = _refused(tmp_path, _document(project=project))

    assert "'project'" in message


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param("distributed", id="unimplemented"),
        pytest.param("", id="empty"),
        pytest.param(4, id="not-text"),
    ],
)
def test_mode_is_refused_unless_the_form_names_it(tmp_path: Path, mode: object) -> None:
    """Property: `mode` is refused unless it is one of the three launch bindings this code holds.

    Class: an unimplemented mode name, an empty value, and a non-text value.
    Why it is required: a mode selects a launch binding, and an unknown name would reach a branch with
    no binding behind it.
    What kills it: accepting any text and letting a later `if` fall through to a default launch.
    Second correct implementation: a `Literal` type with an explicit membership check; the same inputs
    are refused.
    """
    message = _refused(tmp_path, _document(mode=mode, local={"cores": 4}))

    assert "'mode'" in message


@pytest.mark.parametrize("mode", ["batch_job", "1_job_many_srun_tasks"])
def test_an_hpc_mode_requires_the_slurm_table_and_its_connection_fields(tmp_path: Path, mode: str) -> None:
    """Property: each HPC mode refuses a document with no `[slurm]` table, and one whose table omits a connection field.

    Class: the two HPC modes crossed with four omissions — the table, `partition`, `account` and
    `time_minutes`.
    Why it is required: every HPC launch reads the connection, so a configuration omitting it would start
    no work, and the refusal must name the field the mode reads rather than the table alone.
    What kills it: requiring the connection only in `batch_job`, or defaulting a missing partition.
    Second correct implementation: a per-mode required-field list checked in one loop; the same eight
    cases are refused.
    """
    omissions: list[tuple[str, object]] = [("slurm", _ABSENT)]
    omissions += [(field, _ABSENT) for field in ("partition", "account", "time_minutes")]

    for field, value in omissions:
        document = _document(mode=mode, local=_ABSENT) if field == "slurm" else _hpc_document(mode, **{field: value})
        message = _refused(tmp_path, document)
        assert "'slurm." in message


@pytest.mark.parametrize(
    ("mode", "omitted", "field"),
    [
        pytest.param("batch_job", "jobs", "slurm.jobs", id="batch-job-without-jobs"),
        pytest.param(
            "1_job_many_srun_tasks", "cpus_per_task", "slurm.cpus_per_task", id="one-allocation-without-tasks"
        ),
    ],
)
def test_each_hpc_mode_requires_the_one_limit_it_reads(tmp_path: Path, mode: str, omitted: str, field: str) -> None:
    """Property: `batch_job` requires `jobs` and `1_job_many_srun_tasks` requires `cpus_per_task`, each refusal naming the field.

    Class: each HPC mode with the limit it reads removed from an otherwise conforming document.
    Why it is required: the two limits are optional fields of the table, so only the mode can require
    one, and a run started without it would either invent a job count or over-subscribe one node.
    What kills it: requiring both limits in both modes, or requiring neither.
    Second correct implementation: one mode-to-field mapping consulted once; both documents are refused
    with the same field names.
    """
    message = _refused(tmp_path, _hpc_document(mode, **{omitted: _ABSENT}))

    assert repr(field) in message


@pytest.mark.parametrize(
    ("document", "field"),
    [
        pytest.param(_document(local={"cores": 0}), "local.cores", id="cores-zero"),
        pytest.param(_document(local={"cores": -2}), "local.cores", id="cores-negative"),
        pytest.param(_document(local={"cores": True}), "local.cores", id="cores-boolean"),
        pytest.param(_document(local={"cores": "4"}), "local.cores", id="cores-text"),
        pytest.param(_hpc_document("batch_job", time_minutes=0), "slurm.time_minutes", id="time-zero"),
        pytest.param(_hpc_document("batch_job", jobs=False), "slurm.jobs", id="jobs-boolean"),
        pytest.param(_hpc_document("1_job_many_srun_tasks", cpus_per_task=0), "slurm.cpus_per_task", id="tasks-zero"),
    ],
)
def test_a_limit_is_refused_unless_it_is_a_positive_integer(
    tmp_path: Path, document: Mapping[str, object], field: str
) -> None:
    """Property: every numeric limit is refused unless it is a positive integer, a boolean included, naming the field.

    Class: zero, a negative value, a boolean and a text spelling, over the four limits the form carries.
    Why it is required: `bool` is an `int` subclass, so an integer check alone admits `cores = true` as
    one core, and a scheduler cannot honour zero, a negative count or text as a limit.
    What kills it: `isinstance(value, int)` without the boolean exclusion, or a `>= 0` bound.
    Second correct implementation: comparing a converted value against `1` and the type separately; the
    same seven documents are refused.
    """
    message = _refused(tmp_path, document)

    assert repr(field) in message


def test_an_unnamed_top_level_key_is_refused(tmp_path: Path) -> None:
    """Property: a top-level key the form does not name is refused, naming the key.

    Class: any conforming document carrying one extra top-level scalar — here `threads`.
    Why it is required: an unnamed key is a setting its writer believes is live while nothing reads it,
    and a run configured by a value no code reads is a run whose configuration lies.
    What kills it: iterating only the keys the loader wants, so every extra key is silently dropped.
    Second correct implementation: comparing the document's key set with the form's closed set; the same
    document is refused.
    """
    message = _refused(tmp_path, _document(threads=8))

    assert "threads" in message


@pytest.mark.parametrize(
    ("table", "key"),
    [
        pytest.param("judgement", "threshold", id="judgement"),
        pytest.param("local", "threads", id="local"),
        pytest.param("slurm", "queue", id="slurm"),
        pytest.param("version_stacking", "rerun", id="version_stacking"),
    ],
)
def test_an_unnamed_key_inside_a_named_table_is_refused(tmp_path: Path, table: str, key: str) -> None:
    """Property: a key the form does not name inside `[judgement]`, `[local]`, `[slurm]` or `[version_stacking]` is refused, naming the table and the key.

    Class: one extra key in each of the four tables the form names.
    Why it is required: the closed key set of the top level does not reach inside the tables, and a
    setting written into one of them is exactly the value its author expects to be read.
    What kills it: validating the top-level keys only.
    Second correct implementation: one closed-set check applied to each table; the same four documents
    are refused.
    """
    document = _document()
    document[table] = {**document.get(table, {}), key: "value"}

    message = _refused(tmp_path, document)

    assert key in message
    assert table in message


@pytest.mark.parametrize("table", ["judgement", "local", "slurm", "version_stacking"])
def test_a_named_table_that_is_not_a_table_is_refused(tmp_path: Path, table: str) -> None:
    """Property: a named table given as a scalar is refused, naming the field.

    Class: each of the three tables rendered as text.
    Why it is required: the loader reads keys inside each table, and a scalar would either raise an
    uncaught attribute error or be ignored while the writer believed their settings were read.
    What kills it: `document.get(table, {})` without a type check.
    Second correct implementation: reading each table through a helper that narrows the type; the same
    three documents are refused.
    """
    message = _refused(tmp_path, _document(**{table: "not-a-table"}))

    assert repr(table) in message


@pytest.mark.parametrize("value", ["", "   ", 7])
def test_automatic_selection_is_refused_unless_it_is_non_empty_text(tmp_path: Path, value: object) -> None:
    """Property: `[judgement] automatic_selection` is optional, and when present is refused unless it is non-empty text.

    Class: an empty string, whitespace only, and a non-text value, against the same document without the
    table as the admitted control.
    Why it is required: the field names a procedure that would choose the models without a human, so an
    empty name names no procedure while looking like a configuration that delegates.
    What kills it: admitting any present value, or requiring the field so a kept judgement stage cannot
    be expressed.
    Second correct implementation: a `strip()`-based emptiness check; the same three values are refused
    and the absent table is admitted.
    """
    document = _document(judgement={"automatic_selection": value})

    message = _refused(tmp_path, document)

    assert "'judgement.automatic_selection'" in message

    admitted = MainConfiguration.load(_written(tmp_path, _document(judgement={"automatic_selection": "nearest"})))
    assert admitted.judgement.automatic_selection == "nearest"

    omitted = MainConfiguration.load(_written(tmp_path, _document()))
    assert omitted.judgement.automatic_selection is None


def test_a_file_that_is_not_valid_toml_is_refused_naming_the_path(tmp_path: Path) -> None:
    """Property: a file that does not parse is refused with its path in the message.

    Class: any text that is not TOML.
    Why it is required: the object exists only once parsing succeeds, so this refusal is the loader's own
    and must name the file an operator has to fix.
    What kills it: letting `TOMLDecodeError` escape, which names no file in this module's terms and
    belongs to a different exception hierarchy.
    Second correct implementation: catching the decoder error around any read of the text; the same input
    is refused.
    """
    path = tmp_path / "broken.toml"
    path.write_text("rule = [unclosed\n", encoding="utf-8")

    with pytest.raises(MainConfigurationError) as caught:
        MainConfiguration.load(path)

    assert str(path) in str(caught.value)


def test_a_missing_file_is_left_to_the_operating_system(tmp_path: Path) -> None:
    """Property: a path that does not exist raises the operating system's error rather than this module's refusal.

    Class: any path naming no file.
    Why it is required: the operating system's message already names the file, and this form's refusal
    set has no row for an unreadable path — a wrapper would report a refusal the contract does not carry
    and would hide the path behind a second message.
    What kills it: wrapping `read_text` in a `try` that raises `MainConfigurationError`.
    Second correct implementation: none — the contract is that the error is not wrapped, so any wrapper
    is red.
    """
    with pytest.raises(FileNotFoundError):
        MainConfiguration.load(tmp_path / "absent.toml")


def test_direct_construction_is_validated_and_the_value_is_frozen() -> None:
    """Property: a hand-built configuration is refused for the same defects a file is, and an admitted one cannot be mutated.

    Class: a relative estate root and a project with one segment built directly, plus a conforming
    instance whose field is reassigned.
    Why it is required: the value fields are public, so a caller can construct the type without a file;
    validation reachable only through `load` would leave that call path unchecked.
    What kills it: moving the estate-root or project check into `load`, or dropping `frozen=True` from
    the dataclass.
    Second correct implementation: validation in `__post_init__` or in a `__setattr__` guard; the
    conforming instance is frozen and the two defective ones are refused either way.
    """
    with pytest.raises(MainConfigurationError):
        MainConfiguration(
            rule=MAIN_CONFIGURATION_RULE,
            estate_root="estate",
            bindings="config/paths.toml",
            project="somewhere.river",
            mode="local",
            local=LocalConfiguration(cores=4),
        )

    with pytest.raises(MainConfigurationError):
        MainConfiguration(
            rule=MAIN_CONFIGURATION_RULE,
            estate_root="/estate",
            bindings="config/paths.toml",
            project="somewhere",
            mode="local",
            local=LocalConfiguration(cores=4),
        )

    configuration = MainConfiguration(
        rule=MAIN_CONFIGURATION_RULE,
        estate_root="/estate",
        bindings="config/paths.toml",
        project="somewhere.river",
        mode="local",
        local=LocalConfiguration(cores=4),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        configuration.mode = "batch_job"  # type: ignore[misc]


def test_the_three_fixed_path_keys_are_built_from_the_project() -> None:
    """Property: `path_keys` returns the three fixed kinds in order as keys of this project, and `path_key` refuses a kind outside the grammar.

    Class: the conforming project `somewhere.river`, plus a kind carrying a separator.
    Why it is required: three consumers resolve these keys, so each must be constructed the same way and
    from the project the configuration validated rather than from a respelled string.
    What kills it: returning the kinds as strings, or building the key from the unvalidated field without
    the key grammar.
    Second correct implementation: constructing each `PathKey` from its segments rather than through
    `parse`; the same values and the same refusal result.
    """
    configuration = MainConfiguration(
        rule=MAIN_CONFIGURATION_RULE,
        estate_root="/estate",
        bindings="config/paths.toml",
        project="somewhere.river",
        mode="local",
        local=LocalConfiguration(cores=4),
    )

    assert configuration.path_keys() == tuple(
        PathKey(domain="somewhere", project="river", kind=kind) for kind in PATH_KEY_KINDS
    )
    assert [key.kind for key in configuration.path_keys()] == list(PATH_KEY_KINDS)

    with pytest.raises(PathResolutionError):
        configuration.path_key("run/output")


def test_the_module_docstring_states_the_two_commitments() -> None:
    """Property: the module docstring contains the two sentences this form implements, and names the three tables and the refusal root.

    Class: any version of the docstring, searched whole.
    Why it is required: the statements are the deliverable text, and a docstring reduced to a description
    of the fields would leave the reason the file exists unstated for the next editor.
    What kills it: deleting or rephrasing either sentence, or ceasing to name the three tables.
    Second correct implementation: the sentences at any position in any surrounding prose — the check
    asserts containment, so it constrains neither order nor the wording around them.
    """
    docstring = main_configuration.__doc__ or ""

    assert ONE_FILE_PER_MACHINE in docstring
    assert NO_MACHINE_VALUE_IN_THE_TOOLKIT in docstring
    assert "[judgement]" in docstring
    assert "[local]" in docstring
    assert "[slurm]" in docstring
    assert "[version_stacking]" in docstring
    assert "MainConfigurationError" in docstring


def test_the_form_names_exactly_the_modes_and_path_key_kinds_the_documentation_lists() -> None:
    """Property: `MODES` and `PATH_KEY_KINDS` are the tuples the module publishes, and each is non-empty and repetition-free.

    Class: the two constants themselves.
    Why it is required: the documentation and the loader both read them, so a duplicated entry would make
    `path_keys()` return a key twice and a mode list with a repeat would make one branch unreachable.
    What kills it: appending a name to one tuple without the other appearing in the docs table, or a
    duplicated member.
    Second correct implementation: tuples declared in any order; the checks are set-based and order-blind
    for distinctness, order-sensitive only where `MODES[0]` is read.
    """
    assert MODES == ("local", "batch_job", "1_job_many_srun_tasks")
    assert PATH_KEY_KINDS == ("run_output", "project_directory", "selection")
    assert len(set(MODES)) == len(MODES)
    assert len(set(PATH_KEY_KINDS)) == len(PATH_KEY_KINDS)


def test_an_hpc_document_with_an_optional_limit_present_is_admitted(tmp_path: Path) -> None:
    """Property: a limit the mode does not read is admitted and validated when it is present.

    Class: `batch_job` carrying `cpus_per_task` as well as `jobs`, and `1_job_many_srun_tasks` carrying
    `jobs` as well as `cpus_per_task`.
    Why it is required: the two limits are named fields of the table, so a configuration that states both
    is not outside the form; requiring a writer to delete a field because their mode does not read it
    would be a refusal this form does not state.
    What kills it: refusing an unnamed-for-this-mode limit, or accepting it unvalidated while refusing a
    required one.
    Second correct implementation: validating every present limit and requiring only the mode's own; both
    documents load.
    """
    batch = MainConfiguration.load(_written(tmp_path, _hpc_document("batch_job", cpus_per_task=4)))
    single = MainConfiguration.load(_written(tmp_path, _hpc_document("1_job_many_srun_tasks", jobs=4)))

    assert batch.slurm is not None and batch.slurm.cpus_per_task == 4
    assert single.slurm is not None and single.slurm.jobs == 4


def test_a_local_document_carrying_a_complete_slurm_table_is_admitted(tmp_path: Path) -> None:
    """Property: a local run may carry a complete `[slurm]` table, which is read without being required.

    Class: the conforming local document plus a `[slurm]` table holding all three connection fields.
    Why it is required: the table is a named field, so a local configuration that also states a cluster
    connection is not outside the form; the mode decides what is REQUIRED, not what may be present.
    What kills it: refusing `[slurm]` whenever the mode is local.
    Second correct implementation: none beyond admitting the named table; the document loads with both
    tables populated.
    """
    configuration = MainConfiguration.load(
        _written(tmp_path, _document(slurm={"partition": "compute", "account": "allocation", "time_minutes": 60}))
    )

    assert configuration.local == LocalConfiguration(cores=4)
    assert configuration.slurm == SlurmConfiguration(partition="compute", account="allocation", time_minutes=60)


def test_a_slurm_table_missing_a_connection_field_is_refused_in_a_local_run_too(tmp_path: Path) -> None:
    """Property: a `[slurm]` table present at all must carry all three connection fields, whatever the mode.

    Class: the conforming local document plus a `[slurm]` table omitting `account`.
    Why it is required: the table's own form names the three fields, so a partial table is a writer's
    belief that a setting is live; if a local run could carry part of it, the same document copied to an
    HPC mode would be read differently from how it looked.
    What kills it: requiring the connection only when the mode is an HPC mode, which admits the partial
    table here.
    Second correct implementation: validating the table's own fields at construction; the same document is
    refused.
    """
    message = _refused(tmp_path, _document(slurm={"partition": "compute", "time_minutes": 60}))

    assert "'slurm.account'" in message


def test_an_absent_version_stacking_table_is_the_refusing_default_and_a_present_one_is_read(tmp_path: Path) -> None:
    """Property: with no `[version_stacking]` table the configuration holds no rerun rule and refuses mixed versions; a present table is read value for value.

    Class: the conforming document without the table, with a full table, and with an empty one.
    Why it is required: the refusal is the default and the override an explicit act, so an absent table must
    mean "no rerun, no override"; and the two values the pre-run step and the command read must arrive as
    written, in order.
    What kills it: a default of `true`, a loader that drops the table, or one that sorts the rule names.
    Second correct implementation: a table read through any helper; the same three configurations result.
    """
    absent = MainConfiguration.load(_written(tmp_path, _document()))
    assert absent.version_stacking == VersionStackingConfiguration()
    assert absent.version_stacking.rerun_rules == ()
    assert absent.version_stacking.allow_mixed_versions is False

    present = MainConfiguration.load(
        _written(
            tmp_path,
            _document(
                version_stacking={
                    "rerun_rules": ["observed_record_assembly", "marginal_distribution_fitting"],
                    "allow_mixed_versions": True,
                }
            ),
        )
    )
    assert present.version_stacking == VersionStackingConfiguration(
        rerun_rules=("observed_record_assembly", "marginal_distribution_fitting"), allow_mixed_versions=True
    )

    empty = MainConfiguration.load(_written(tmp_path, _document(version_stacking={})))
    assert empty.version_stacking == VersionStackingConfiguration()


@pytest.mark.parametrize(
    ("table", "field_name"),
    [
        pytest.param({"rerun_rules": "observed_record_assembly"}, "version_stacking.rerun_rules", id="text-list"),
        pytest.param({"rerun_rules": 7}, "version_stacking.rerun_rules", id="integer-list"),
        pytest.param({"rerun_rules": [7]}, "version_stacking.rerun_rules", id="non-text-name"),
        pytest.param({"rerun_rules": [""]}, "version_stacking.rerun_rules", id="empty-name"),
        pytest.param({"rerun_rules": ["a", "  "]}, "version_stacking.rerun_rules", id="blank-name"),
        pytest.param({"allow_mixed_versions": "true"}, "version_stacking.allow_mixed_versions", id="text-flag"),
        pytest.param({"allow_mixed_versions": 1}, "version_stacking.allow_mixed_versions", id="integer-flag"),
    ],
)
def test_a_version_stacking_value_of_the_wrong_kind_is_refused_naming_its_field(
    tmp_path: Path, table: dict[str, object], field_name: str
) -> None:
    """Property: a rule list that is not a list of non-empty text and a flag that is not a boolean are each refused, naming the field in the configuration's spelling, from a file and by construction.

    Class: a list given as text or a number, three malformed names, and the flag as text and as `1`.
    Why it is required: `1` is a Python `int` that is also truthy, so an unchecked flag would admit mixed
    versions on a value the writer did not mean as a boolean; a text list would iterate by character and name
    rules that do not exist.
    What kills it: `bool(value)` instead of a type test, or iterating a string as a list.
    Second correct implementation: validation in the loader only; the construction half would then be red.
    """
    message = _refused(tmp_path, _document(version_stacking=table))

    assert repr(field_name) in message

    with pytest.raises(MainConfigurationError):
        VersionStackingConfiguration(**table)  # type: ignore[arg-type]
