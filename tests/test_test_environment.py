"""The suite's environment is declared once, and it is the environment its stored values were computed in.

Every stored value under `tests/correspondence/reference_values/` records, in its `[diagnostics]`
table, the interpreter and library versions it was computed under. A comparison against such a value
under another version is a moved environment and not a divergence (`reference_values.stored_value_findings`
reports it as `STALE environment`), so those versions are not a detail of the runner: they decide what
the suite's red means. At `20825f1` the Whole suite job listed its own distributions, pip resolved
`pandas 3.1.1` where all 103 stores record `pandas 2.3.3`, and eleven environment failures were read
as failures of the code under test (E2, E3, E5).

`pyproject.toml`'s `test` dependency group is the one declaration of that environment, and both suite
jobs install it. Four rules follow from that, each a function of the files' text:

1. the four distributions that decide a stored value — `numpy`, `pandas`, `xarray` and `scipy` — are
   pinned `==` in the group to the one version every store records for them, and the stores record one
   version each, so two stores cannot disagree about the environment they were built in;
2. every member of the group is a `==` pin, so nothing in the environment floats between two runs of
   one commit;
3. the group's members name pytest in no spelling but the one `pytest==` pin. This is the rule
   `tests/test_whole_suite_workflow_review.py` applies to the install line today, and it has to move
   here with the list: once the members live in `pyproject.toml` that reader sees no install line and
   a `pytest-…` plugin added to the group would reach the runner unremarked (E17);
4. `whole-suite.yml`'s `python-version` is the recorded interpreter's major and minor, so the
   interpreter the stores name is the interpreter the job runs.

Two more facts are held over the two jobs' install steps rather than by a rule over values: each job
carries one such step, and that step installs the declared group (this module); and the Whole suite
job's step is pinned by equality to the one command (`tests/test_whole_suite_workflow.py`), because
that job also installs the runtime stack from the committed lock and its line is therefore its own.

`pyproject.toml` is parsed with `tomllib`, because a group is a list of requirement strings and the
question is its membership rather than its text. A stored value's descriptor is read as text: what
matters is what its `[diagnostics]` table records, as written, and a text scan reads that table over
103 files without taking a position on the rest of the file. Neither a workflow nor a descriptor is
parsed as YAML: no YAML library is installed in the jobs that run these checks, which is the reason
`tests/test_whole_suite_workflow.py` and `tests/correspondence/test_divergence_mounts.py` give.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
WHOLE_SUITE = REPO_ROOT / ".github" / "workflows" / "whole-suite.yml"
STATE_CHECK = REPO_ROOT / ".github" / "workflows" / "state-check.yml"
STORE = REPO_ROOT / "tests" / "correspondence" / "reference_values"

GROUP = "test"
# The lock arm the Whole suite job's install line carries: the committed requirement list, installed
# under hashes. `pylock.toml` is the PEP 751 form of the same resolution and no install line reads it,
# so the file an install names is this one, and it is what the rule below holds the job to.
LOCK_REQUIREMENTS = "requirements.txt"
# The distributions whose versions decide a stored value: `reference_values._diagnostics` records
# exactly these beside the payload, and the comparison moves when one of them moves.
REFERENCE_DISTRIBUTIONS = ("numpy", "pandas", "xarray", "scipy")

# A group member that pins a distribution exactly, as `name==version`. `version` admits the local and
# post-release spellings a pin may carry (`2.9.0.post0`).
_PIN = re.compile(r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\s*==\s*(?P<version>[A-Za-z0-9][A-Za-z0-9.+!-]*)$")
# A `[table]` header of a descriptor, and one `name = "value"` line of its `[diagnostics]` table,
# with a trailing `# comment` admitted as TOML admits it.
_TABLE = re.compile(r"^\[(?P<name>[A-Za-z0-9_.-]+)\]$")
_DIAGNOSTIC = re.compile(r'^(?P<name>[A-Za-z0-9_.-]+)\s*=\s*"(?P<value>[^"]*)"(?:\s+#.*)?$')
# The `python-version` the job sets up, in the spellings a workflow may write it: `'3.11'` or `3.11`.
_PYTHON_VERSION = re.compile(r"^\s*python-version:\s*[\"']?(?P<version>[0-9]+(?:\.[0-9]+)*)[\"']?\s*$", re.MULTILINE)
# The `run:` value that installs with pip, which both suite jobs carry exactly once.
_INSTALL = re.compile(r"^\s*run:\s*(?P<command>python -m pip install\s.*)$", re.MULTILINE)
# The lock arm inside an install command: a requirement-file flag naming the COMMITTED ROOT lock. The
# token is matched whole and preceded by whitespace, so `-r docs/requirements.txt` is not read as the
# root file, and `--require-hashes` is not read as `-r` (the flag alternatives end where that token
# continues). A quoted path is admitted because a quoted argument is the same argument to the shell.
_REQUIREMENT_FILE = re.compile(
    r"(?:^|\s)(?:-r|--requirement)\s+[\"']?" + re.escape(LOCK_REQUIREMENTS) + r"[\"']?(?=\s|$)"
)
# The pin spelling, and every other spelling, of the runner's name: copied from
# `tests/test_whole_suite_workflow_review.py` rather than imported, so that a rewrite of that module's
# reader cannot change what this module holds the group to.
_PYTEST_PIN = re.compile(r"""["']?pytest(?:==|>=|<=|~=|!=)[^\s"']*["']?""", re.IGNORECASE)
_PYTEST_NAME = re.compile(r"py\.?test", re.IGNORECASE)


def _members(pyproject_text: str) -> list[object]:
    """The `test` dependency group's members, as `pyproject.toml` declares them."""
    groups = tomllib.loads(pyproject_text).get("dependency-groups")
    assert isinstance(groups, dict), "pyproject.toml declares no [dependency-groups] table"
    members = groups.get(GROUP)
    assert isinstance(members, list), f"the {GROUP!r} dependency group is absent or is not a list of requirements"
    return members


def _distribution(name: str) -> str:
    """A distribution name in the form one pin and one `[diagnostics]` key compare under."""
    return name.lower().replace("_", "-")


def _pins(members: list[object]) -> dict[str, tuple[str, ...]]:
    """Every exact pin of the group, as distribution name -> the versions it is pinned at, in file order.

    A name is recorded once per member that pins it, never folded onto a single version: a group that
    pins one distribution at two versions is a declaration of two environments, and a dict that kept
    only the last pin would compare the earlier one with nothing and admit the group.
    """
    seen: dict[str, list[str]] = {}
    for member in members:
        matched = _PIN.match(member) if isinstance(member, str) else None
        if matched is not None:
            seen.setdefault(_distribution(matched.group("name")), []).append(matched.group("version"))
    return {name: tuple(versions) for name, versions in seen.items()}


def _diagnostics(descriptor_text: str) -> dict[str, str]:
    """The `[diagnostics]` table of one stored value's descriptor, as name -> version, read as text."""
    found: dict[str, str] = {}
    table = ""
    for raw in descriptor_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        header = _TABLE.match(line)
        if header is not None:
            table = header.group("name")
            continue
        if table != "diagnostics":
            continue
        entry = _DIAGNOSTIC.match(line)
        assert entry is not None, f'not a `name = "value"` line of [diagnostics]: {line!r}'
        found[entry.group("name")] = entry.group("value")
    return found


def _recorded(store: Path = STORE) -> list[tuple[str, dict[str, str]]]:
    """Every stored value's descriptor under `store` as (path relative to the repository, its diagnostics)."""
    return [
        (path.relative_to(REPO_ROOT).as_posix(), _diagnostics(path.read_text(encoding="utf-8")))
        for path in sorted(store.glob("*/*.toml"))
    ]


def _major_minor(version: str) -> str:
    """The first two components of a dotted version, which is what a `python-version` names."""
    return ".".join(version.split(".")[:2])


def _environment_problems(
    pyproject_text: str, descriptors: list[tuple[str, dict[str, str]]], workflow_text: str
) -> list[str]:
    """Where the declared environment and the environment the stored values record part ways.

    One finding per disagreement, so a caller prints the whole gap rather than its first member: a
    member that is not a `==` pin, a member that names pytest in a spelling other than the pin, a
    distribution the group pins at two versions, a store that records no version of a reference
    distribution or another one, a store that records a version of its own for one of them, a
    `python-version` that is not the recorded interpreter's major and minor, and an empty store,
    whose conforming report would otherwise be vacuous.
    """
    problems: list[str] = []
    if not descriptors:
        problems.append(f"no stored value is recorded under {STORE.relative_to(REPO_ROOT).as_posix()}")
    members = _members(pyproject_text)
    pins = _pins(members)
    for member in members:
        if not isinstance(member, str) or _PIN.match(member) is None:
            problems.append(f"the member {member!r} is not a `==` pin")
        if isinstance(member, str) and _PYTEST_NAME.search(_PYTEST_PIN.sub("", member)):
            problems.append(f"the member {member!r} names pytest in a spelling the pin does not cover")
    repeated = {name: versions for name, versions in pins.items() if len(set(versions)) > 1}
    for name, versions in repeated.items():
        problems.append(f"the {GROUP!r} group pins {name} at two versions: {sorted(set(versions))}")
    recorded: dict[str, set[str]] = {name: set() for name in REFERENCE_DISTRIBUTIONS}
    interpreters: set[str] = set()
    for path, diagnostics in descriptors:
        for name in REFERENCE_DISTRIBUTIONS:
            version = diagnostics.get(name)
            if version is None:
                problems.append(f"{path} records no {name} version")
                continue
            recorded[name].add(version)
            pinned = pins.get(name)
            if name in repeated:
                # The repeated name is one finding of its own above; comparing it per store would
                # print the same disagreement once per descriptor.
                continue
            if pinned is None:
                problems.append(f"{path} records {name} {version}, the {GROUP!r} group does not pin it")
            elif pinned[0] != version:
                problems.append(f"{path} records {name} {version}, the {GROUP!r} group pins {pinned[0]!r}")
        interpreter = diagnostics.get("python")
        if interpreter:
            interpreters.add(interpreter)
    for name, versions in recorded.items():
        if len(versions) > 1:
            problems.append(f"the stores record {name} at more than one version: {sorted(versions)}")
    declared = _PYTHON_VERSION.search(workflow_text)
    if declared is None:
        problems.append("whole-suite.yml declares no `python-version`")
    elif not interpreters:
        problems.append("no stored value records a python version")
    else:
        for interpreter in sorted(interpreters):
            if _major_minor(declared.group("version")) != _major_minor(interpreter):
                problems.append(
                    f"whole-suite.yml runs python {declared.group('version')}, the stores record {interpreter}"
                )
        if len(interpreters) > 1:
            problems.append(f"the stores record more than one interpreter: {sorted(interpreters)}")
    return problems


def _install_commands(workflow_text: str) -> list[str]:
    """Every `run:` value of a workflow that installs with pip, in file order."""
    return [matched.group("command").strip() for matched in _INSTALL.finditer(workflow_text)]


def test_the_test_group_pins_the_environment_every_stored_value_records():
    """Property: the group's pins and the stores' recorded versions are one environment, and the job's interpreter is the recorded one.

    Class: NEW CAPABILITY — `pyproject.toml`'s `test` group, the 103 descriptors under
    `tests/correspondence/reference_values/`, and `whole-suite.yml`'s `python-version`. Plausible
    wrong implementations are a group member moved (`numpy==2.4.5`), a member relaxed to a range
    (`pandas>=2.3.3`), a pin and a store that disagree (`pandas==3.1.1` beside stores recording
    `2.3.3`), one name pinned at two versions in either order (`pandas==3.1.1` beside
    `pandas==2.3.3`, whichever is written last), a store regenerated under another version so the
    stores disagree with each other, `python-version: '3.12'` beside stores recording `3.11.17`, and a
    `pytest-cov==…` member. Each is refused by `_environment_problems`, and the refused cases are
    driven in `tests/test_test_environment_review.py`, the two-version one by a case of its own.

    Why: the whole point of pinning the group is that the suite's red is the code's, and a pin that
    can drift from the version the stores were built under restores the failure this declaration
    removes — eleven environment failures read as failures of the code under test. The two-version
    case is the one a rule that folds the members into a name-to-version dict admits: the earlier pin
    of the name is compared with nothing, so a group naming `pandas 3.1.1` and `pandas 2.3.3` reads as
    one environment.

    Killed by: any of the seven edits above. Second correct implementation: the same group with another
    pinned distribution beside the sixteen (`joblib==1.4.2`), with a descriptor carrying a trailing
    comment on a `[diagnostics]` line, with `python-version` written `"3.11"`, and with the members in
    another order.

    Not established: that the store corpus under `tests/correspondence/reference_values/` is the whole
    of the suite's environment, and that a version missing from these four distributions cannot move a
    stored value; `reference_values._diagnostics` records these four and no more.
    """
    problems = _environment_problems(
        PYPROJECT.read_text(encoding="utf-8"), _recorded(), WHOLE_SUITE.read_text(encoding="utf-8")
    )
    assert problems == [], problems


def test_both_suite_jobs_install_the_one_declared_environment():
    """Property: each suite job carries one install step, each step installs the declared `test` group,
    and the Whole suite job's step also installs the runtime stack from the committed lock.

    Class: NEW CAPABILITY — the two install steps. Plausible wrong implementations are the State check
    job left on its own list, which is the defect that has held that job red on `main` since `0c53486`
    (`ModuleNotFoundError: No module named 'pyvinecopulib'`); either job installing the project with no
    `--group test`; the Whole suite job installing the project without the lock, so it tests whatever
    its moment resolves while the release compares and ships the lock's bytes; and either job gaining
    a second pip install step, which is a second declaration that only one assertion below reaches.
    Each is red here.

    Why: the State check job carries a required check on `main`, so a second list there is a second
    environment and a second failure mode on the branch that must stay green; a line that installs no
    group leaves pip to resolve the project's runtime declaration, which is not a test environment;
    and the lock arm is what makes this job's versions the release's, so its absence leaves a green
    job that measured an environment other than the one the tag ships.

    Deliberately NOT the equality of the two commands. The State check job installs the declared group
    alone and the Whole suite job installs the group and the lock, so what the two share is the
    declaration and not one string: the equality held until the lock existed, and holding it now would
    refuse the very lock arm this rule requires.

    Killed by: either command losing `--group test`, the Whole suite command losing `--require-hashes`
    or its requirement file, the flag naming another file than the committed root lock, the root
    `requirements.txt` deleted, or a job gaining a second pip install step. Second correct
    implementation: the lock arm written `--requirement requirements.txt` or with the path quoted, the
    group flag before the lock arm, the flags in another order, and the pip upgrade quoted with single
    quotes — none of which this rule reads as a difference, because it reads the group flag and the
    lock arm rather than the whole string.

    Not established: what pip resolves inside the group; the lock (DR-7) is what makes the transitive
    set recorded, and this module reads the direct members only.
    """
    whole_suite = _install_commands(WHOLE_SUITE.read_text(encoding="utf-8"))
    state_check = _install_commands(STATE_CHECK.read_text(encoding="utf-8"))
    assert len(whole_suite) == 1, whole_suite
    assert len(state_check) == 1, state_check

    # The group flag is asserted against the declaration rather than against a bare literal, so a job
    # naming a group `pyproject.toml` no longer declares is caught instead of read as one spelling.
    assert _members(PYPROJECT.read_text(encoding="utf-8")), f"the {GROUP!r} dependency group is empty"
    group_flag = f"--group {GROUP}"
    for command in (whole_suite[0], state_check[0]):
        assert group_flag in command, (group_flag, command)

    assert "--require-hashes" in whole_suite[0], whole_suite[0]
    assert _REQUIREMENT_FILE.search(whole_suite[0]) is not None, whole_suite[0]
    assert (REPO_ROOT / LOCK_REQUIREMENTS).is_file(), REPO_ROOT / LOCK_REQUIREMENTS


def test_the_reader_reads_a_diagnostics_table_beside_a_payload_table_and_a_commented_value():
    """Property: the descriptor reader takes the `[diagnostics]` table and only it, and a trailing comment is not part of a version.

    Class: NEW CAPABILITY — the reader's two blind spots. Plausible wrong implementations are a reader
    that stops at the first table header, so a descriptor whose `[diagnostics]` table precedes
    `[payload]` reads as no environment at all; and one that returns `"2.4.6"  # pinned` as the
    version, which disagrees with every pin and reds on a correct store. Both are caught here.

    Why: the rule compares versions as text, and the descriptor is the system that owns its own
    format; a reader that misreads one field reports a moved environment on a store that never moved.

    Killed by: reading past `[diagnostics]` into `[payload]`, or keeping the comment in the value.
    Second correct implementation: a `tomllib` parse of the descriptor, which agrees with this reader
    on every descriptor the harness writes.

    Not established: that a `[diagnostics]` table written with an inline table (`diagnostics = {…}`)
    is refused rather than misread; it is not a spelling `reference_values._render` writes.
    """
    text = (
        'unit = "a"\n'
        "[diagnostics]\n"
        'numpy = "2.4.6"  # pinned\n'
        'python = "3.11.17"\n'
        "[payload]\n"
        'file = "a.npz"\n'
        'digest = "npz-bytes:1:00"\n'
    )
    assert _diagnostics(text) == {"numpy": "2.4.6", "python": "3.11.17"}, _diagnostics(text)
