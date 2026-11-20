"""The run-start record: the one file an engine invocation declares what it is building and why.

An engine start is a fresh parse with no memory of the command that launched it, so the facts a parse
needs — which configuration this run belongs to, what it should build, whether a selection artifact
exists, and whether the judgement stage is present — are written once, by the step that prepares the run,
and read at every parse. This module owns that file's form and nothing else: it writes one JSON object,
reads it back validated, and refuses anything that is not the form, naming the field and the file.

The three commitments this module implements, in its own words:

    One file, one versioned object, six fields: the version literal, the configuration path, the target list, the selection path or null, the judgement state, and whether a system built at mixed toolkit commits is admitted.

    The record is written by the run's preparation and READ by every engine parse, so a field the record omits is a fact the parse cannot recover.

    A record that does not parse as the form is refused naming the field, because the reader is a parse-time gate with no other channel to report what is wrong.

The surface is two functions and their record: `RunStart.write` writes the object, `read_run_start`
reads it back validated, and `RunStart` is the value both sides carry.

The refusal is `RunStartError` for every defect in the file, because every repair is one act by the same
person on one file: rewrite the flagged field. The graph-start gate catches it and re-raises the engine's
own error, so an engine started without a valid record exits non-zero before a job is scheduled.

The judgement state is one of three strings rather than a boolean, because the three states ask for
different work: `present` means the run has a recorded selection to consume, `kept-absent` means the
selection does not exist and the judgement stage is kept (so the run builds the smaller graph), and
`delegated` means the configuration names a procedure that would choose the models without a human. A
boolean would collapse the last two, and the step that computes the target list branches on all three.

Not established: that the paths the record holds exist, that the targets are the outputs of rules in the
table, or that the configuration it names is the one this machine is configured with. The record is a
statement by the preparer; the gate and the rules that read it check what they can, and the resolver
checks the estate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePath

RUN_START_RULE = "run-start:2"

# The file name the engine's working directory holds. It is a constant because TWO parties state it: the
# step that prepares the run writes it, and every parse reads it, and a spelling that drifted between the
# two would make the gate refuse every run.
RUN_START_FILE = "run_start.json"

# The three judgement states, as strings. Ordered as the run's lifecycle meets them: a recorded selection
# is present, or it is absent and the judgement stage is kept, or the run delegates the choice.
JUDGEMENT_STATES = ("present", "kept-absent", "delegated")

_FIELDS = ("format", "main_configuration", "targets", "selection", "judgement", "allow_mixed_versions")


class RunStartError(Exception):
    """A run-start record this module refuses: absent, unreadable, or outside the form.

    Deliberately NOT derived from `KeyError` or a parse error whose `__str__` quotes: the message is a
    parse-time gate's whole report, and it names the field and the file.
    """


@dataclass(frozen=True)
class RunStart:
    """One validated run-start record: what the run is, what it builds, and how it judges.

    - `main_configuration` — the ABSOLUTE path of the configuration this run reads. Absolute because the
      engine's working directory is the run output, so a relative path would resolve inside it and name a
      file the preparer never meant.
    - `targets` — the paths the run builds, relative to the engine's working directory. Non-empty: a run
      with no target builds nothing, and the target rule's input would be empty.
    - `selection` — the ABSOLUTE path of the run's recorded model selection, or `None` when the run has
      none. The absence is a state the target list branches on, so it is `None` rather than an empty string.
    - `judgement` — one of `JUDGEMENT_STATES`.
    - `allow_mixed_versions` — whether the end-of-run step admits a system whose rules persisted unequal
      implementation identities, copied from the main configuration's `[version_stacking]` table by the
      pre-run step. Last and defaulted, so a record built without it is one that refuses mixed versions.
      The table's `rerun_rules` is deliberately not recorded: its one reader is the command, which reads
      the configuration at every invocation, so a copy here would be a second statement of one fact.

    Frozen, so a record a parse holds cannot be edited into one the preparer never wrote. The record is
    written WHOLE and never appended to, so a repeated write replaces the answer rather than accumulating
    two.

    Not established: that the named files exist, or that the targets belong to the graph. The record is
    the preparer's statement; the gate checks the checkout, and the engine checks the targets.
    """

    main_configuration: str
    targets: tuple[str, ...]
    selection: str | None
    judgement: str
    allow_mixed_versions: bool = False

    def __post_init__(self) -> None:
        """Refuse a path that is not absolute as the field requires, a target that is not relative, and a judgement state outside the three."""
        _require_absolute_text("main_configuration", self.main_configuration)

        if not isinstance(self.targets, tuple) or not self.targets:
            raise RunStartError(
                f"field 'targets' is {self.targets!r}: it is a NON-EMPTY tuple of paths the run builds, "
                "because a run with no target has nothing for the engine's default target to depend on"
            )
        for index, target in enumerate(self.targets):
            if not isinstance(target, str) or not target.strip():
                raise RunStartError(
                    f"field 'targets' entry {index} is {target!r}: a target is the non-empty path of one "
                    "artifact, relative to the engine's working directory"
                )
            if PurePath(target).is_absolute():
                raise RunStartError(
                    f"field 'targets' entry {index} is {target!r}, which is absolute: a target is relative "
                    "to the engine's working directory, so an absolute path would name a location outside "
                    "the run it belongs to"
                )

        if self.selection is not None:
            _require_absolute_text("selection", self.selection)

        if self.judgement not in JUDGEMENT_STATES:
            raise RunStartError(
                f"field 'judgement' is {self.judgement!r}: the form names exactly "
                f"{', '.join(repr(state) for state in JUDGEMENT_STATES)}, because the target list branches "
                "on which of the three the run is in"
            )

        if not isinstance(self.allow_mixed_versions, bool):
            raise RunStartError(
                f"field 'allow_mixed_versions' is {self.allow_mixed_versions!r}: it is true or false, because "
                "the end-of-run step either admits a system built at mixed toolkit commits or refuses it"
            )

    def to_document(self) -> dict[str, object]:
        """Return the record as the one JSON object the file carries, the version literal first."""
        return {
            "format": RUN_START_RULE,
            "main_configuration": self.main_configuration,
            "targets": list(self.targets),
            "selection": self.selection,
            "judgement": self.judgement,
            "allow_mixed_versions": self.allow_mixed_versions,
        }

    def write(self, path: str | Path) -> None:
        """Write the record to `path` as the form's one JSON object, replacing whatever that file held.

        The parent directory is not created: the preparer writes this file into the run's output
        directory, which the same step resolves from the configuration, so a mistyped path must fail here
        rather than silently acquiring a directory tree.

        Deliberately NOT wrapped: an unwritable path. `Path.write_text` raises the operating system's
        error, which names the file, and there is no second fact a wrapper could add.
        """
        target = Path(path)
        target.write_text(json.dumps(self.to_document(), indent=2) + "\n", encoding="utf-8")


def read_run_start(path: str | Path) -> RunStart:
    """Read the record at `path` and return it validated, refusing every defect with the field and the file named.

    Every refusal below names the FILE, because the reader runs inside an engine parse where the only
    channel to the operator is an exception's text:

    - the file cannot be decoded as UTF-8, or is not valid JSON, or is not one JSON object;
    - a key the form does not name is present, or one of the six is absent — the closed key set is what
      makes a misspelled field loud instead of silently absent;
    - `format` is not the literal `RUN_START_RULE`;
    - `main_configuration` or `selection` is present but not absolute text, `targets` is not a non-empty
      array of relative paths, `judgement` is outside its three states, or `allow_mixed_versions` is not a
      boolean.

    A file that does not exist raises the operating system's error rather than this refusal, so a gate can
    distinguish "the preparer never wrote one" from "it wrote one this form refuses" without parsing a
    message.

    Deliberately NOT a `load` that defaults a missing field: every field is a fact the parse needs, and a
    default would be a second, silent statement of the preparer's intent.
    """
    target = Path(path)

    try:
        text = target.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise RunStartError(
            f"run-start record {target} is not UTF-8 text, so there is nothing to read: {error}"
        ) from error

    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise RunStartError(
            f"run-start record {target} is not valid JSON, so there is nothing to read: {error}"
        ) from error

    if not isinstance(document, dict):
        raise RunStartError(
            f"run-start record {target} is {document!r}: the record is ONE JSON object carrying "
            f"{', '.join(repr(name) for name in _FIELDS)}, and a value of another shape names no run"
        )

    for key in document:
        if key not in _FIELDS:
            raise RunStartError(
                f"run-start record {target} carries the key {key!r}: the form is exactly "
                f"{', '.join(repr(name) for name in _FIELDS)}, so a key it does not name is a fact its writer "
                "believes the parse reads while nothing does"
            )
    for key in _FIELDS:
        if key not in document:
            raise RunStartError(
                f"run-start record {target} omits the field {key!r}: the form names exactly "
                f"{', '.join(repr(name) for name in _FIELDS)}, and a parse cannot recover a fact the "
                "preparer never stated"
            )

    if document["format"] != RUN_START_RULE:
        raise RunStartError(
            f"run-start record {target} field 'format' is {document['format']!r}: the file must declare "
            f"format = {RUN_START_RULE!r}, the version header that makes a later change to the record's "
            "fields loud rather than silently misread by an older parse"
        )

    raw_targets = document["targets"]
    if not isinstance(raw_targets, list):
        raise RunStartError(
            f"run-start record {target} field 'targets' is {raw_targets!r}: targets is an ARRAY of relative "
            "paths, one per artifact the run builds"
        )

    try:
        record = RunStart(
            main_configuration=document["main_configuration"],
            targets=tuple(raw_targets),
            selection=document["selection"],
            judgement=document["judgement"],
            allow_mixed_versions=document["allow_mixed_versions"],
        )
    except RunStartError as error:
        raise RunStartError(f"run-start record {target}: {error}") from error

    return record


def _require_absolute_text(field_name: str, value: object) -> None:
    """Refuse a value that is not non-empty absolute text, naming the field."""
    if not isinstance(value, str) or not value.strip():
        raise RunStartError(
            f"field {field_name!r} is {value!r}: it is the non-empty absolute path of a file this run reads"
        )
    if not PurePath(value).is_absolute():
        raise RunStartError(
            f"field {field_name!r} is {value!r}, which is not absolute: the engine's working directory is "
            "the run output, so a relative path would resolve inside it rather than naming the file the "
            "preparer meant"
        )
