"""Read the one file that configures a run: its estate, its project, its mode and its machine values.

A run needs machine-specific facts — which estate directory it reads, which binding document names
this machine's paths, how many cores it may use or which scheduler account it bills — and none of
those facts may live in a tracked toolkit file, because writing one machine's arrangement into the
public repository publishes the private layout and bakes that machine into every other machine's
copy. So the facts live in a run configuration INSTANCE: one file, held by the estate, supplied to
the run by path. This module owns the form of that file and nothing else; it reads no bound path,
starts nothing and knows no machine.

The two statements this module implements follow, stated in its own words so a later reader can
check the code against them rather than against a paraphrase:

    One run configuration file exists per machine and project, and it is held by the estate rather than the toolkit, so a second machine adds its own file and edits none.

    No tracked toolkit file holds a machine-specific value: the configuration states every value the run reads from this machine, and the toolkit's only route to it is a path the caller supplies.

The form is five top-level fields — `rule`, `estate_root`, `bindings`, `project` and `mode` — plus up
to four tables: `[judgement]`, which carries the procedure a run may delegate the model choice to;
`[local]`, which carries a local launch's core count; `[slurm]`, which carries a cluster
connection and the two HPC modes' limits; and `[version_stacking]`, which carries the rules a run
rebuilds at the running toolkit commit and whether a system whose rules were built at different
commits is admitted.

The refusals are the mechanism, not an error-reporting detail. An operator holding a refused
configuration has to repair one field of a file they cannot see from inside the toolkit, so every
refusal names the FIELD in the configuration's own spelling (`slurm.partition`, not `partition`) and
`load` prefixes every refusal with the file it read, which is the one fact the operator needs to know
which machine's file failed. That is also why the loader takes its path and has NO default: a default
would be a toolkit-side well-known location, so an unset machine would resolve against whatever ships
in the wheel instead of failing loudly, and the refusal would name no file for the operator to fix.

`MainConfigurationError` is the root of every refusal in this module and its only class. A leaf would
separate defects by who can fix them, and here every refusal is one act on one file by one person:
an operator edits the named field. The version header is the one refusal whose repair is
regeneration rather than an edit, and it stays under the root because the same operator performs the
same act — they rewrite the file against the form this code names.

What the module deliberately does NOT do, because each would be a second source that drifts:

- It does not read the binding document the `bindings` field names. Resolving a key is
  `path_resolver`'s act, and reading one now would make the configuration's load depend on an estate
  the caller may be checking separately.
- It does not read `toolkit_revision.toml`. Comparing the declared revision is `revision_gate`'s act.
- It does not enumerate legal procedure names for `judgement.automatic_selection`, legal partitions,
  legal accounts, or estate domains and projects. Those vocabularies are the estate's, and a
  toolkit-side list would make every new value a toolkit release.
- It does not check the rule names in `version_stacking.rerun_rules` against the workflow's rule table.
  The module does not import the workflow graph, so a name is held as text and the pre-run step, which
  knows the table, refuses a name the table does not hold.

Not established: that the file was authored on the machine the run is on, or that the project it
names is the project the estate means. The file carries no machine field by design — its machine
identity is its filename, chosen by the operator — and no field can be compared against an estate
the toolkit cannot read.
"""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePath

from multidriver_swg.run_configuration.path_resolver import PathKey, PathResolutionError

MAIN_CONFIGURATION_RULE = "main-configuration:1"

# The three launch bindings the form names. A mode decides what the run reads rather than what it
# computes, so it is stated here as one closed set and the conditional requirements below are drawn
# over it: a fourth mode would be a new set member, not a new branch in every check.
MODES = ("local", "batch_job", "1_job_many_srun_tasks")

# The three path keys the toolkit fixes for a project. A run resolves each of them through the
# machine's binding document, so the kinds are stated once here and every caller constructs them from
# the configuration's project rather than respelling the strings.
PATH_KEY_KINDS = ("run_output", "project_directory", "selection")

# The closed key set of each table the form names. A key outside its set is refused, because a setting
# this form does not name is one its writer believes is live while nothing reads it.
_TOP_LEVEL_KEYS = (
    "rule",
    "estate_root",
    "bindings",
    "project",
    "mode",
    "judgement",
    "local",
    "slurm",
    "version_stacking",
)

_JUDGEMENT_KEYS = ("automatic_selection",)

_LOCAL_KEYS = ("cores",)

_SLURM_KEYS = ("partition", "account", "time_minutes", "jobs", "cpus_per_task")

_VERSION_STACKING_KEYS = ("rerun_rules", "allow_mixed_versions")

# A project is `domain.project`: exactly two segments. The segment RULES are the path-key grammar's
# own, and they are asked of `PathKey` below rather than restated here, so a key this configuration
# builds and a key the resolver accepts cannot disagree. The kind the probe passes is one the
# configuration fixes anyway, and only the domain and project segments are under test.
_PROJECT_SEGMENT_COUNT = 2

_PROJECT_PROBE_KIND = "run_output"


class MainConfigurationError(Exception):
    """A run configuration this module refuses: a field outside the form, or a value the form forbids.

    Deliberately NOT derived from `KeyError`: a `KeyError` renders its message inside repr quotes, and
    a refusal's message is the operator's whole recovery path, so quoting it would deliver the
    actionable part as a quoted literal.
    """


@dataclass(frozen=True)
class JudgementConfiguration:
    """The `[judgement]` table: what the run may do when no human selection artifact exists.

    `automatic_selection` names a procedure that would choose the marginal and joint models without a
    human. It is OPTIONAL, and its absence means the run keeps the judgement stage — it does not mean
    the run has no rule. The name is held as text and is not compared against any list: the procedures
    belong to the computing systems, so a toolkit-side enumeration here would refuse a procedure the
    moment it is designed.

    Not established: that any procedure by this name exists. Whether a named procedure is provided is
    asked by the step that would call it, so a configuration naming one loads here and is refused
    there.
    """

    automatic_selection: str | None = None

    def __post_init__(self) -> None:
        """Refuse a procedure name that is not non-empty text."""
        if self.automatic_selection is None:
            return
        if not isinstance(self.automatic_selection, str) or not self.automatic_selection.strip():
            raise MainConfigurationError(
                f"field 'judgement.automatic_selection' is {self.automatic_selection!r}: it names a "
                "procedure that would choose the models without a human, so it is non-empty text and "
                "nothing else; omit the field to keep the judgement stage"
            )


@dataclass(frozen=True)
class LocalConfiguration:
    """The `[local]` table: how much of this machine a local-mode run may use.

    `cores` is a positive integer and is required in this table. It is the engine's `--cores` value,
    so it bounds every job the run schedules on this machine.

    Not established: that the machine HAS that many usable cores. A value larger than the host is the
    scheduler's or the operating system's to refuse, and this type checks the form only.
    """

    cores: int

    def __post_init__(self) -> None:
        """Refuse a core count that is not a positive integer."""
        _positive_integer_value(self.cores, "local.cores")


@dataclass(frozen=True)
class SlurmConfiguration:
    """The `[slurm]` table: the scheduler connection and the two HPC modes' limits.

    `partition`, `account` and `time_minutes` are the connection every HPC mode needs, so a `[slurm]`
    table present at all must carry them; a local run simply omits the table. `jobs` is read only by
    `batch_job`, which runs the engine on this host and lets it submit one job per rule, and
    `cpus_per_task` only by `1_job_many_srun_tasks`, which runs the engine inside one allocation. Each
    of those two is OPTIONAL here and required by the mode in `MainConfiguration`, so the table's own
    type cannot refuse a configuration that simply is not in that mode.

    Not established: that the partition and account exist on the cluster, or that the time limit is
    within the partition's maximum. Those are the scheduler's refusals, and they arrive when a job is
    submitted.
    """

    partition: str
    account: str
    time_minutes: int
    jobs: int | None = None
    cpus_per_task: int | None = None

    def __post_init__(self) -> None:
        """Refuse a text field that is empty and a limit that is not a positive integer."""
        _text_value(self.partition, "slurm.partition")
        _text_value(self.account, "slurm.account")
        _positive_integer_value(self.time_minutes, "slurm.time_minutes")
        if self.jobs is not None:
            _positive_integer_value(self.jobs, "slurm.jobs")
        if self.cpus_per_task is not None:
            _positive_integer_value(self.cpus_per_task, "slurm.cpus_per_task")


@dataclass(frozen=True)
class VersionStackingConfiguration:
    """The `[version_stacking]` table: which rules a run rebuilds, and whether mixed toolkit commits are admitted.

    A run at a new toolkit commit leaves every rule whose rendered text did not change as it was, so the
    outputs it keeps carry the earlier commit. This table is how a user asks for the mix on purpose:

    - `rerun_rules` — the names of rules the command passes to the engine as rules to rebuild in every
      run, whatever the engine thinks is up to date. Empty by default. The list is read at EVERY run, so a
      name left in it is rebuilt again by each later run until it is removed. Names are held as text;
      whether each is a row of the rule table is the pre-run step's question.
    - `allow_mixed_versions` — whether the end-of-run step admits a system whose rules persisted unequal
      identities. `False` by default: the run is then refused, naming the two rules. `True` records every
      rule's identity and names the mixed systems instead.

    Whether two commits are compatible is the user's judgement; this type records the choice and decides
    nothing about it.

    Not established: that a rule name exists, or that rebuilding it leaves a coherent set of outputs.
    """

    rerun_rules: tuple[str, ...] = ()
    allow_mixed_versions: bool = False

    def __post_init__(self) -> None:
        """Store the rule names as a tuple, then refuse a name that is not non-empty text and a flag that is not a boolean."""
        object.__setattr__(self, "rerun_rules", _name_list_value(self.rerun_rules, "version_stacking.rerun_rules"))
        _boolean_value(self.allow_mixed_versions, "version_stacking.allow_mixed_versions")


@dataclass(frozen=True)
class MainConfiguration:
    """One validated run configuration: where the estate is, what the project is, and how to launch.

    Frozen, so a configuration a caller has validated cannot be mutated into one the run never
    checked.

    THE CONSTRUCTOR TAKES THE VALIDATED VALUES, and the DOCUMENT's key space belongs to `load`. That
    split is deliberate and it is what makes the refusals reachable from two directions. This type
    checks every VALUE it is handed — the version literal, an absolute estate root, a relative
    binding path, the project's two segments, the mode, and which fields that mode requires — so a
    hand-built instance is validated at construction and not only on a file read. A key the form does
    not name, and a field the file omits, are properties of a DOCUMENT rather than of a value, so
    they are refused where a document exists: in `load`. The residual is stated rather than papered
    over: an instance built directly cannot be carrying an unnamed key, because it has no document to
    carry one.

    `judgement` defaults to the empty table and `local`/`slurm` to absent, which are the values a
    configuration that omits those tables has. Which of them is REQUIRED is decided by `mode`, and
    that requirement is checked here, in one place, so no caller has to remember which mode reads
    which table.

    Not established: that the estate root exists on this machine, that the binding document the
    `bindings` field names exists beneath it, or that the project named is one the estate holds. The
    resolver and the step that reads the estate ask those questions, and this type asks the form's.
    """

    rule: str
    estate_root: str
    bindings: str
    project: str
    mode: str
    judgement: JudgementConfiguration = field(default_factory=JudgementConfiguration)
    local: LocalConfiguration | None = None
    slurm: SlurmConfiguration | None = None
    version_stacking: VersionStackingConfiguration = field(default_factory=VersionStackingConfiguration)

    def __post_init__(self) -> None:
        """Refuse a value outside the form, then the fields the chosen mode requires but this one omits."""
        if self.rule != MAIN_CONFIGURATION_RULE:
            raise MainConfigurationError(
                f"field 'rule' is {self.rule!r}: a run configuration must declare rule = "
                f"{MAIN_CONFIGURATION_RULE!r}, the version header that makes a later change to the "
                "configuration's fields loud rather than silently misread by an older loader; a "
                "DIFFERENT value names a file written against another form, which is regenerated "
                "rather than edited"
            )

        _text_value(self.estate_root, "estate_root")
        if not PurePath(self.estate_root).is_absolute():
            raise MainConfigurationError(
                f"field 'estate_root' is {self.estate_root!r}, which is not absolute: a relative root "
                "resolves against whatever working directory the run happens to start in, so the same "
                "configuration would name different estates on two invocations; give the absolute path "
                "of this machine's estate directory"
            )

        _text_value(self.bindings, "bindings")
        if PurePath(self.bindings).is_absolute():
            raise MainConfigurationError(
                f"field 'bindings' is {self.bindings!r}, which is absolute: the field is the path of "
                "this machine's binding document RELATIVE to estate_root, so the two fields compose "
                "into one location — an absolute value would carry a second, uncomposed statement of "
                "where the estate is; give the path beneath the root"
            )

        self._validated_project()

        _text_value(self.mode, "mode")
        if self.mode not in MODES:
            raise MainConfigurationError(
                f"field 'mode' is {self.mode!r}: the form names exactly "
                f"{', '.join(repr(name) for name in MODES)}, because a mode selects a launch binding "
                "and this code holds no other"
            )

        self._validated_tables()
        self._validated_mode_requirements()

    def _validated_project(self) -> None:
        """Refuse a project spelling that is not two path-key segments, or that names the reserved one.

        The segment rules are the path-key grammar's, so the probe constructs a `PathKey` and lets it
        refuse a charset violation or the reserved middle segment; only the segment COUNT is decided
        here, because a `PathKey` cannot state a two-segment spelling.
        """
        _text_value(self.project, "project")
        segments = self.project.split(".")
        if len(segments) != _PROJECT_SEGMENT_COUNT:
            raise MainConfigurationError(
                f"field 'project' is {self.project!r}: a project is 'domain.project', exactly two dotted "
                "segments naming one estate domain and one project inside it, so the configuration can "
                "build this project's path keys from it"
            )
        try:
            PathKey(domain=segments[0], project=segments[1], kind=_PROJECT_PROBE_KIND)
        except PathResolutionError as error:
            raise MainConfigurationError(f"field 'project' is {self.project!r}: {error}") from error

    def _validated_tables(self) -> None:
        """Refuse a nested table that is not the table type this form names."""
        if not isinstance(self.judgement, JudgementConfiguration):
            raise MainConfigurationError(
                f"field 'judgement' is {self.judgement!r}: the form's [judgement] table is a "
                "JudgementConfiguration, so a value of another kind carries settings nothing reads"
            )
        if self.local is not None and not isinstance(self.local, LocalConfiguration):
            raise MainConfigurationError(
                f"field 'local' is {self.local!r}: the form's [local] table is a LocalConfiguration, so "
                "a value of another kind carries settings nothing reads"
            )
        if self.slurm is not None and not isinstance(self.slurm, SlurmConfiguration):
            raise MainConfigurationError(
                f"field 'slurm' is {self.slurm!r}: the form's [slurm] table is a SlurmConfiguration, so "
                "a value of another kind carries settings nothing reads"
            )
        if not isinstance(self.version_stacking, VersionStackingConfiguration):
            raise MainConfigurationError(
                f"field 'version_stacking' is {self.version_stacking!r}: the form's [version_stacking] table is "
                "a VersionStackingConfiguration, so a value of another kind carries settings nothing reads"
            )

    def _validated_mode_requirements(self) -> None:
        """Refuse a mode whose own table, or whose own field, the configuration omits.

        This is one method rather than a check inside each branch because the requirement is one
        property: the mode names the fields it reads, and a configuration in that mode that omits one
        of them is a run that would fail later with a value it invented. Each refusal names the field
        in the configuration's own spelling.
        """
        if self.mode == "local":
            if self.local is None:
                raise MainConfigurationError(
                    "field 'local.cores' is absent: mode 'local' reads it to bound the jobs this "
                    "machine schedules, so a local run must carry a [local] table with a positive "
                    "integer cores"
                )
            return

        if self.slurm is None:
            raise MainConfigurationError(
                "field 'slurm.partition' is absent: an HPC mode reads the [slurm] table's partition, "
                f"account and time_minutes, so mode {self.mode!r} must carry that table"
            )
        if self.mode == "batch_job" and self.slurm.jobs is None:
            raise MainConfigurationError(
                "field 'slurm.jobs' is absent: mode 'batch_job' submits one job per rule and reads this "
                "field as the engine's job limit, so the [slurm] table must carry a positive integer jobs"
            )
        if self.mode == "1_job_many_srun_tasks" and self.slurm.cpus_per_task is None:
            raise MainConfigurationError(
                "field 'slurm.cpus_per_task' is absent: mode '1_job_many_srun_tasks' runs every job "
                "inside one allocation and reads this field for the allocation's task width, so the "
                "[slurm] table must carry a positive integer cpus_per_task"
            )

    def path_key(self, kind: str) -> PathKey:
        """Return this project's path key of `kind`, refused by the key grammar if `kind` has no spelling.

        This is the one place the configuration's project becomes a key, so a consumer of one of the
        three fixed kinds never respells `domain.project.kind` itself and a malformed kind is refused
        where it is written rather than at the first lookup.
        """
        return PathKey.parse(f"{self.project}.{kind}")

    def path_keys(self) -> tuple[PathKey, ...]:
        """Return the three fixed path keys this project reads, in the order `PATH_KEY_KINDS` states."""
        return tuple(self.path_key(kind) for kind in PATH_KEY_KINDS)

    @classmethod
    def load(cls, path: str | Path) -> MainConfiguration:
        """Read the configuration file at `path` and return it validated.

        The argument is REQUIRED and there is no default, for the reason the binding loader gives about
        its own path: a default would be a toolkit-side well-known location, so an unconfigured run
        would read whatever ships in the wheel instead of failing loudly, and the refusal would name no
        file.

        A file that is not valid TOML is refused HERE, because a document must parse before an object
        exists to carry the remaining checks. Every other refusal is raised by this class and re-raised
        with this path attached, under the SAME class, so a caller that catches `MainConfigurationError`
        still catches it after loading and every message names both the field and the file.

        Deliberately NOT wrapped: an unreadable path. `Path.read_text` raises the operating system's
        error, which already names the file an operator must fix, and this form's refusal set has no row
        for it. A file that is not UTF-8 text IS refused here, under this module's class and with the
        path, because the interpreter's `UnicodeDecodeError` names a byte offset and a codec and no
        file — so a caller catching this class would miss it, and the operator would be told where a
        byte sits rather than which configuration to repair.
        """
        document_path = Path(path)

        try:
            text = document_path.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            raise MainConfigurationError(
                f"main configuration {document_path} is not UTF-8 text, so there is nothing to read: {error}"
            ) from error

        try:
            document = tomllib.loads(text)
        except tomllib.TOMLDecodeError as error:
            raise MainConfigurationError(
                f"main configuration {document_path} is not valid TOML, so there is nothing to read: {error}"
            ) from error

        try:
            return cls._from_document(document)
        except MainConfigurationError as error:
            raise MainConfigurationError(f"main configuration {document_path}: {error}") from error

    @classmethod
    def _from_document(cls, document: Mapping[str, object]) -> MainConfiguration:
        """Build the configuration from a parsed document, refusing the key space this form does not name."""
        _require_named_keys(document, _TOP_LEVEL_KEYS, label="top level")
        return cls(
            rule=_required_text(document, "rule", "rule"),
            estate_root=_required_text(document, "estate_root", "estate_root"),
            bindings=_required_text(document, "bindings", "bindings"),
            project=_required_text(document, "project", "project"),
            mode=_required_text(document, "mode", "mode"),
            judgement=_judgement_configuration(document),
            local=_local_configuration(document),
            slurm=_slurm_configuration(document),
            version_stacking=_version_stacking_configuration(document),
        )


def _text_value(value: object, field_name: str) -> str:
    """Return `value` as non-empty text, refusing anything else under `field_name` in the configuration's spelling."""
    if not isinstance(value, str) or not value.strip():
        raise MainConfigurationError(
            f"field {field_name!r} is {value!r}: it is non-empty text, and a value of another kind or an "
            "empty one carries no setting the run can read"
        )
    return value


def _positive_integer_value(value: object, field_name: str) -> int:
    """Return `value` as a positive integer, refusing anything else under `field_name` in the configuration's spelling.

    A boolean is refused BEFORE the integer test because `bool` is an `int` subclass here: `True` is
    `1`, so a bare integer check would admit `cores = true` as a core count of one.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise MainConfigurationError(
            f"field {field_name!r} is {value!r}: it is a positive integer — at least 1 — and a boolean, a "
            "text value, a float or a number below one is not a limit any scheduler can honour"
        )
    return value


def _name_list_value(value: object, field_name: str) -> tuple[str, ...]:
    """Return `value` as a tuple of non-empty rule names, refusing a non-list or a non-text entry under `field_name`."""
    if isinstance(value, str) or not isinstance(value, list | tuple):
        raise MainConfigurationError(
            f"field {field_name!r} is {value!r}: it is a list of rule names, and a value of another kind "
            "names no rule to rebuild"
        )
    for index, name in enumerate(value):
        if not isinstance(name, str) or not name.strip():
            raise MainConfigurationError(
                f"field {field_name!r} entry {index} is {name!r}: a rule is named by non-empty text"
            )
    return tuple(value)


def _boolean_value(value: object, field_name: str) -> bool:
    """Return `value` as a boolean, refusing anything else under `field_name` in the configuration's spelling."""
    if not isinstance(value, bool):
        raise MainConfigurationError(
            f"field {field_name!r} is {value!r}: it is true or false, and a value of another kind names neither choice"
        )
    return value


def _require_named_keys(table: Mapping[str, object], names: tuple[str, ...], *, label: str) -> None:
    """Refuse a key this form does not name, in `label`'s own spelling of where it was found."""
    for key in table:
        if key not in names:
            raise MainConfigurationError(
                f"the run configuration's {label} carries the key {key!r}: the form names exactly "
                f"{', '.join(repr(name) for name in names)}, so a key this form does not name is a "
                "setting its writer believes is live while nothing reads it; remove the key, or move its "
                "value under a key the form names"
            )


def _required_text(table: Mapping[str, object], key: str, label: str) -> str:
    """Return the text `table` carries at `key`, refusing its absence or a non-text value under `label`."""
    if key not in table:
        raise MainConfigurationError(
            f"field {label!r} is absent: this file is the only place the run's configuration is stated, so "
            "a field it omits is one the run cannot read"
        )
    return _text_value(table[key], label)


def _optional_text(table: Mapping[str, object], key: str, label: str) -> str | None:
    """Return the text `table` carries at `key`, or `None` when absent; refuse a non-text value under `label`."""
    if key not in table:
        return None
    return _text_value(table[key], label)


def _required_integer(table: Mapping[str, object], key: str, label: str) -> int:
    """Return the integer `table` carries at `key`, refusing its absence or a non-integer under `label`."""
    if key not in table:
        raise MainConfigurationError(
            f"field {label!r} is absent: this file is the only place the run's configuration is stated, so "
            "a field it omits is one the run cannot read"
        )
    return _integer_value(table[key], label)


def _optional_integer(table: Mapping[str, object], key: str, label: str) -> int | None:
    """Return the integer `table` carries at `key`, or `None` when absent; refuse a non-integer under `label`."""
    if key not in table:
        return None
    return _integer_value(table[key], label)


def _integer_value(value: object, label: str) -> int:
    """Return `value` as an integer, refusing a boolean or a value of another kind under `label`."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise MainConfigurationError(
            f"field {label!r} is {value!r}: it is an integer, and a boolean or a text value names no "
            "setting the run can read"
        )
    return value


def _table(table: Mapping[str, object], key: str, label: str) -> Mapping[str, object] | None:
    """Return the table `table` carries at `key`, or `None` when absent; refuse a non-table under `label`."""
    if key not in table:
        return None
    value = table[key]
    if not isinstance(value, dict):
        raise MainConfigurationError(
            f"field {label!r} is {value!r}: it is a table of the fields the form names for it, and a value "
            "of another kind carries no field this loader can read"
        )
    return value


def _judgement_configuration(document: Mapping[str, object]) -> JudgementConfiguration:
    """Return the `[judgement]` table as a configuration, or the empty table when it is absent."""
    declared = _table(document, "judgement", "judgement")
    if declared is None:
        return JudgementConfiguration()
    _require_named_keys(declared, _JUDGEMENT_KEYS, label="'judgement' table")
    return JudgementConfiguration(
        automatic_selection=_optional_text(declared, "automatic_selection", "judgement.automatic_selection"),
    )


def _local_configuration(document: Mapping[str, object]) -> LocalConfiguration | None:
    """Return the `[local]` table as a configuration, or `None` when it is absent."""
    declared = _table(document, "local", "local")
    if declared is None:
        return None
    _require_named_keys(declared, _LOCAL_KEYS, label="'local' table")
    return LocalConfiguration(cores=_required_integer(declared, "cores", "local.cores"))


def _slurm_configuration(document: Mapping[str, object]) -> SlurmConfiguration | None:
    """Return the `[slurm]` table as a configuration, or `None` when it is absent.

    The three connection fields are required whenever the table is present, whatever the mode: the
    table's own form names them, and a local run that does not read them omits the table rather than
    writing part of it. The two mode-specific limits stay optional here, because whether one is
    required is `MainConfiguration`'s question — it is the only type that knows the mode.
    """
    declared = _table(document, "slurm", "slurm")
    if declared is None:
        return None
    _require_named_keys(declared, _SLURM_KEYS, label="'slurm' table")
    return SlurmConfiguration(
        partition=_required_text(declared, "partition", "slurm.partition"),
        account=_required_text(declared, "account", "slurm.account"),
        time_minutes=_required_integer(declared, "time_minutes", "slurm.time_minutes"),
        jobs=_optional_integer(declared, "jobs", "slurm.jobs"),
        cpus_per_task=_optional_integer(declared, "cpus_per_task", "slurm.cpus_per_task"),
    )


def _version_stacking_configuration(document: Mapping[str, object]) -> VersionStackingConfiguration:
    """Return the `[version_stacking]` table as a configuration, or the default table when it is absent."""
    declared = _table(document, "version_stacking", "version_stacking")
    if declared is None:
        return VersionStackingConfiguration()
    _require_named_keys(declared, _VERSION_STACKING_KEYS, label="'version_stacking' table")
    return VersionStackingConfiguration(
        rerun_rules=_name_list_value(declared.get("rerun_rules", ()), "version_stacking.rerun_rules"),
        allow_mixed_versions=_boolean_value(
            declared.get("allow_mixed_versions", False), "version_stacking.allow_mixed_versions"
        ),
    )
