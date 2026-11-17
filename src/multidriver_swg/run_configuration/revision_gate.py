"""Compare the toolkit a run executes against the commit its project declares, and refuse a mismatch.

A run's output is attributable only if the code that produced it is the code the project named. Two
things make that non-obvious, and each has its own check here.

The first is that "the running package" and "a checkout" are not the same fact. A copy installed into
some enclosing repository — a non-editable install under a clone's ignored virtual environment, for
example — belongs to a checkout whose commit has nothing to do with it, and `git rev-parse
--show-toplevel` run inside that copy happily returns the enclosing repository. So the gate asks two
questions: that the package directory has an enclosing checkout at all, and that the checkout's own
source tree IS the directory the package was imported from.

The second is that a commit says nothing about the working tree. A run from a checkout at the
declared commit with three edited modules executes code no commit names, and an untracked module can
change what runs just as a tracked edit can. So the gate also refuses a checkout whose status is not
empty.

The three statements this module implements, in its own words, so a later reader can check the code
against them rather than against a paraphrase:

    A run executes the package of a git checkout, and that checkout is the one whose source tree holds the package the run imported.

    The commit the run executes equals the forty-character commit its project declares, and the checkout holding it is clean.

    The commit governs: a tag is carried as a statement of intent and is never what the comparison reads.

The refusals are the mechanism, not an error-reporting detail. Each one names the two facts a reader
needs to decide which side to change — the running value and the declared value, or the package
directory and the checkout's source root — because a gate that said only "revision mismatch" would
send an operator to look at a commit when the defect is an installed copy.

`RevisionGateError` is the root of every refusal in this module and its only class. The three checks
have different repairs, but they share one reader at one moment: whoever discovers that the run will
not start. A leaf per check would name a party who cannot act alone, because the repair is always
"make this checkout be the thing the project declares".

WHY THE GATE IS ALSO A COMPOSED FUNCTION. Two call sites run these checks — the workflow file at every
parse, and the step that prepares a run before an engine exists — and both must refuse the SAME fact
first, because the pre-run refusal is what keeps a broken tree from being submitted as an allocation
whose engine then refuses at parse. `require_revision_gate` is that one order, so neither call site
restates it and the two cannot drift into refusing different facts.

The surface is one function per check plus the composed gate: `running_revision` reads the executed
commit, `require_package_checkout` asks that the package belongs to a checkout,
`load_declared_revision` reads the project's declaration, `require_declared_revision` compares the two
commits, `require_clean_checkout` reads the working tree, and `require_revision_gate` runs all three
in their fixed order. `DeclaredRevision` is the declaration as a value.

What this module deliberately does NOT do: it does not fetch, tag, log or otherwise interpret a
repository, and it does not cache a revision. A cached revision would report what a checkout held when
the cache was written rather than what the run executes, which is the one fact the gate exists to
observe.

Not established: that the checkout is the one the operator intended, that it is on any particular
branch, that the declared commit is reachable from it, or that the run's OTHER inputs match the
project's other declarations. The gate compares one commit and one status, and the project's remaining
files are their readers' to check.
"""

from __future__ import annotations

import os
import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path

REVISION_RULE = "toolkit-revision:1"

# Forty lowercase hexadecimal characters: the spelling `git rev-parse` prints in full. An abbreviated
# commit is refused rather than expanded, because the gate must compare one value against the file the
# project holds, and expanding here would read the checkout to decide what the checkout should be.
_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")

# The closed key set of the declaration document. A key outside it is refused, because a declaration
# field this form does not name is one its writer believes is read while nothing reads it.
_DOCUMENT_KEYS = ("rule", "commit", "tag")

# How many dirty entries a refusal quotes before it counts the rest. The message exists so an operator
# can start cleaning; a checkout with two hundred changes needs a count, not the whole listing.
_QUOTED_STATUS_ENTRIES = 5

# The environment variables that name WHERE git finds a repository, an index or an object store. Git
# exports the first three to every process a hook or `rebase --exec` starts, and a child that inherits
# one answers `rev-parse` and `status` from the named repository whatever its working directory is — so
# the gate would report ANOTHER checkout's commit while the code that runs is this one, silently.
# Deliberately NOT stripped: every `GIT_CONFIG_*` variable. The operator's configuration is where a
# checkout's own ignore rules and settings live, and the one setting that would change what a check
# sees is overridden where that check runs, not by discarding the configuration.
_GIT_LOCATION_VARIABLES = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_PREFIX",
)


class RevisionGateError(Exception):
    """A checkout that cannot be compared, a commit that differs, or a checkout that is not clean.

    Deliberately NOT derived from `KeyError`: a `KeyError` renders its message inside repr quotes, and
    these messages are an operator's recovery path, so quoting one would deliver the actionable part as
    a quoted literal.
    """


@dataclass(frozen=True)
class DeclaredRevision:
    """The commit a project declares, and the tag that motivated it.

    - `commit` — the forty lowercase hexadecimal characters the comparison reads.
    - `tag` — the tag the commit was reached by, or `None`. It is carried so an operator can see which
      named release a commit belonged to; it is NEVER compared, because a tag is a movable name and a
      commit is not.

    Frozen, because a declaration read from a file is a value the run compares and never edits.
    """

    commit: str
    tag: str | None = None

    def __post_init__(self) -> None:
        """Refuse a commit that is not forty lowercase hex characters, or a tag that is not non-empty text."""
        if not isinstance(self.commit, str) or not _COMMIT_PATTERN.fullmatch(self.commit):
            raise RevisionGateError(
                f"field 'commit' is {self.commit!r}: the declaration names the commit the run must execute "
                "as exactly forty LOWERCASE hexadecimal characters, the spelling a full commit id is "
                "printed in, because an abbreviated or upper-case value names no single object to compare"
            )
        if self.tag is not None and (not isinstance(self.tag, str) or not self.tag.strip()):
            raise RevisionGateError(
                f"field 'tag' is {self.tag!r}: a tag is the non-empty text a release was named by, or the "
                "field is absent; the commit is what the comparison reads, so a value of another kind here "
                "states nothing the run can use"
            )


def running_package_directory() -> Path:
    """Return the directory of the `multidriver_swg` package this code was imported from.

    Derived from this module's own file rather than from the working directory, the interpreter's path
    or an environment variable: those three describe how the process was started, while the package
    directory describes which copy of the toolkit is executing, and that is the fact the gate compares.

    This module lives at `src/multidriver_swg/run_configuration/revision_gate.py`, so the package
    directory is its grandparent. A relocated module would move the answer, which is why the derivation
    is stated here rather than left to a reader to reconstruct.
    """
    return Path(__file__).resolve().parent.parent


def running_revision(start: str | Path | None = None) -> str:
    """Return the full commit of the checkout containing `start`, or of the running package when omitted.

    `start` is a DIRECTORY inside the checkout whose commit is asked for. It defaults to
    `running_package_directory()`, so a rule that records the version of the code that produced its
    output calls this with no argument and gets the commit of the package it is executing. The default
    is a directory derived from an imported module, deliberately NOT the working directory: a run whose
    outputs record the commit of whatever directory the engine happened to start in would attribute its
    work to the wrong code, silently.

    Raises `RevisionGateError` when `start` has no enclosing checkout, when git itself cannot be run,
    and when the output is not a full commit id. An operating-system error is translated rather than
    propagated: the caller's recovery is to point the run at a checkout, and an error naming a missing
    executable or a non-directory would send them to look at the machine rather than at the run.

    Not established: that the returned commit is reachable from any branch, that it is the commit the
    project declares (that comparison is `require_declared_revision`'s), or that the worktree is clean.
    """
    directory = _as_directory(start) if start is not None else running_package_directory()

    completed = _git(("rev-parse", "HEAD"), directory)

    if completed.returncode != 0:
        raise RevisionGateError(
            f"the directory {directory} has no commit to report: `git rev-parse HEAD` failed there with "
            f"{_git_message(completed)}, so the run cannot say which code it executes; run from a git "
            "checkout"
        )

    commit = completed.stdout.strip()
    if not _COMMIT_PATTERN.fullmatch(commit):
        raise RevisionGateError(
            f"`git rev-parse HEAD` in {directory} printed {commit!r}: a commit id is exactly forty "
            "lowercase hexadecimal characters, so a value of another shape names no object the run could "
            "compare against the commit its project declares"
        )
    return commit


def require_package_checkout(package_directory: str | Path) -> Path:
    """Return the checkout holding `package_directory`, refused unless that checkout's source tree IS it.

    `package_directory` is the directory the `multidriver_swg` package was imported from — the value
    `running_package_directory` returns. Two clauses, and neither covers the other:

    - the directory has an enclosing checkout at all. An installed wheel outside any repository has
      none, so `git rev-parse --show-toplevel` fails and there is no commit to compare.
    - the checkout's own source tree, `{toplevel}/src/multidriver_swg`, resolves to that same
      directory. This is the clause a bare `--show-toplevel` cannot replace: a copy installed inside
      some enclosing repository (a non-editable install under a clone's ignored virtual environment,
      say) returns that repository's root while the package being imported is the installed copy, so
      the gate would compare one checkout's commit with another package's code.

    Both refusals name the paths a reader must reconcile, so the recovery is an edit to a command line
    or a checkout rather than an exercise in guessing which copy ran.

    Not established: that the returned checkout is the one the operator meant, or that its working tree
    is clean. The status check is `require_clean_checkout`'s.
    """
    directory = _as_directory(package_directory)

    completed = _git(("rev-parse", "--show-toplevel"), directory)

    if completed.returncode != 0:
        raise RevisionGateError(
            f"the running package at {directory} is not the package of a git checkout: `git rev-parse "
            f"--show-toplevel` failed there with {_git_message(completed)}, so there is no commit to "
            "compare against the one the project declares; run the toolkit from a checkout rather than an "
            "installed copy"
        )

    toplevel = Path(completed.stdout.strip())
    checkout_source = (toplevel / "src" / "multidriver_swg").resolve()
    imported = directory.resolve()

    if checkout_source != imported:
        raise RevisionGateError(
            f"the running package at {imported} is not the package the checkout {toplevel} holds: that "
            f"checkout's source tree is {checkout_source}, so the commit the gate would compare "
            f"({toplevel}) does not describe the code being executed ({imported}); an installed copy "
            "inside an unrelated repository belongs to that repository and not to the toolkit, so run "
            "from the checkout's own source"
        )

    return toplevel


def load_declared_revision(document_path: str | Path) -> DeclaredRevision:
    """Read the revision declaration at `document_path` and return it validated.

    The declaration is one TOML file the project holds: `rule`, the version header; `commit`, the forty
    lowercase hexadecimal characters the run must execute; and optionally `tag`, the name the commit was
    released under. The commit governs, and the tag is never compared.

    The argument is REQUIRED and there is no default, for the reason the other loaders in this package
    give: a default would be a toolkit-side well-known location, so a project with no declaration would
    be compared against whatever ships in the wheel instead of failing loudly.

    A file that is not valid TOML is refused here, because the object exists only once parsing has
    succeeded. Every other refusal is `DeclaredRevision`'s and is re-raised with this path attached,
    under the same class, so a caller catching `RevisionGateError` still catches it after loading and
    every message names the file.

    Deliberately NOT wrapped: an unreadable path. `Path.read_text` raises the operating system's error,
    which already names the file to fix. A file that is not UTF-8 text IS refused here, under this
    module's class and with the path: the interpreter's `UnicodeDecodeError` names a byte offset and a
    codec and no file, so a caller catching this module's refusal would miss it and an operator holding
    several declarations could not tell which one failed.
    """
    path = Path(document_path)

    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise RevisionGateError(
            f"revision declaration {path} is not UTF-8 text, so there is nothing to read: {error}"
        ) from error

    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise RevisionGateError(
            f"revision declaration {path} is not valid TOML, so there is nothing to read: {error}"
        ) from error

    for key in document:
        if key not in _DOCUMENT_KEYS:
            raise RevisionGateError(
                f"revision declaration {path} carries the top-level key {key!r}: a declaration is exactly "
                f"{', '.join(repr(name) for name in _DOCUMENT_KEYS)}, so a key this form does not name is a "
                "field its writer believes is read while nothing reads it; remove the key"
            )

    if document.get("rule") != REVISION_RULE:
        raise RevisionGateError(
            f"revision declaration {path} declares rule = {document.get('rule')!r}: a declaration must "
            f"declare rule = {REVISION_RULE!r}, the version header that makes a later change to the "
            "declaration's fields loud rather than silently misread"
        )

    try:
        declared = DeclaredRevision(commit=document.get("commit"), tag=document.get("tag"))
    except RevisionGateError as error:
        raise RevisionGateError(f"revision declaration {path}: {error}") from error

    return declared


def require_declared_revision(declared: str, running: str) -> None:
    """Refuse a running commit that differs from the declared one, naming both values.

    The comparison is EQUALITY of two full commit ids, and both operands are checked for that form
    first, because an abbreviated or upper-case value would compare unequal to a correct one for a
    reason the message could not explain. The declared value is the project's, read from its
    declaration; the running value is the checkout's, read by `running_revision`.

    Deliberately NOT a prefix comparison: two abbreviated ids can share a prefix and name different
    objects, and a full id compared against a full id has no such failure.

    Not established: that either operand came from the source this docstring names. The function reads
    the two strings it is given and cannot tell a declared commit from a running one.
    """
    for label, value in (("declared", declared), ("running", running)):
        if not isinstance(value, str) or not _COMMIT_PATTERN.fullmatch(value):
            raise RevisionGateError(
                f"the {label} commit is {value!r}: the comparison reads two full commit ids — forty "
                "lowercase hexadecimal characters each — and a value of another shape names no object to "
                "compare"
            )

    if declared == running:
        return

    raise RevisionGateError(
        f"the running toolkit commit {running} differs from the commit the project declares {declared}: a "
        "run whose executed code is not the code its project named produces output no commit accounts "
        "for; check out the declared commit, or change the declaration to the commit this checkout holds, "
        "before starting the run"
    )


def require_clean_checkout(toplevel: str | Path) -> None:
    """Refuse a checkout whose `git status --porcelain` is not empty, naming what it reports.

    Both kinds of change count, because both change what runs: a tracked edit alters a module, and an
    untracked file the checkout's ignore rules do not cover can add one. Ignored files are not
    reported by `--porcelain`, so a virtual environment or a build directory is not a refusal — which
    is the point of using the checkout's own ignore rules rather than a list here.

    The refusal quotes up to five entries and counts the rest. An operator needs to know that the
    listing is truncated, and a design that printed two hundred paths would bury the one fact the
    message exists to deliver.

    `--untracked-files=normal` is passed even though it is git's default, because
    `status.showUntrackedFiles = no` in the operator's own configuration changes that default: the
    output of `git status --porcelain` then omits untracked files entirely, which is exactly the case
    this check exists for. The command-line option outranks the setting, so the checkout's ignore rules
    still decide what counts as untracked while a setting that would suppress the listing does not.

    Not established: that a clean checkout is the declared commit. That comparison is
    `require_declared_revision`'s, and this check runs after it.
    """
    directory = _as_directory(toplevel)

    completed = _git(("status", "--porcelain", "--untracked-files=normal"), directory)

    if completed.returncode != 0:
        raise RevisionGateError(
            f"the checkout at {directory} has no readable status: `git status --porcelain "
            "--untracked-files=normal` failed there "
            f"with {_git_message(completed)}, so the run cannot tell whether the code it executes is a "
            "commit's"
        )

    entries = [line for line in completed.stdout.splitlines() if line.strip()]
    if not entries:
        return

    quoted = ", ".join(repr(entry) for entry in entries[:_QUOTED_STATUS_ENTRIES])
    remainder = len(entries) - _QUOTED_STATUS_ENTRIES
    more = f", and {remainder} more" if remainder > 0 else ""
    raise RevisionGateError(
        f"the checkout at {directory} is not clean: `git status --porcelain` reports {len(entries)} "
        f"changed or untracked {'entry' if len(entries) == 1 else 'entries'} — {quoted}{more} — so the "
        "code being executed is not the code any commit holds; commit or remove those changes before "
        "starting the run"
    )


def require_revision_gate(package_directory: str | Path, declared: DeclaredRevision) -> str:
    """Run the gate's three checks in their fixed order and return the commit the run executes.

    The order is stated once, here, because two call sites run it and both must refuse the SAME fact
    first. The package-within-checkout check runs first: without a checkout there is no commit to
    compare, and the remaining two have no operand. The declared-versus-running comparison runs second,
    so a tree at the wrong commit is reported as that, rather than as the dirty tree it may also be. The
    status check runs last, because it is the only check whose repair leaves the commit alone.

    Returns the running commit, which a caller needs to record what it executed.

    Not established: that the declared commit is the one the operator wanted, or that the checkout's
    ignore rules admit every file that should not count as a change. The gate reads the declaration it
    is handed and the checkout `package_directory` resolves inside.
    """
    toplevel = require_package_checkout(package_directory)
    running = running_revision(toplevel)
    require_declared_revision(declared.commit, running)
    require_clean_checkout(toplevel)
    return running


def _as_directory(value: str | Path) -> Path:
    """Return `value` as a path, refused when it names no directory at all.

    The refusal is this module's rather than an operating system's because the caller's recovery is to
    point the gate at a real directory, and an error naming a file operation would send them looking for
    a missing file when the operand is a directory that does not exist.
    """
    directory = Path(value)
    if not directory.is_dir():
        raise RevisionGateError(
            f"{value!r} is not a directory: the gate runs git inside a directory of the checkout it is "
            "asked about, so an operand that names no directory has nothing to ask"
        )
    return directory


def _git(arguments: tuple[str, ...], directory: Path) -> subprocess.CompletedProcess[str]:
    """Run one git command inside `directory` and return the completed process, refusing an unrunnable git.

    `check=False` is deliberate: every caller inspects the exit code itself, because a non-zero git exit
    is one of the defects the caller reports. A missing executable or a directory that vanished between
    the check and the run is translated into this module's refusal, since either would otherwise escape
    as a `FileNotFoundError` naming git rather than naming the run's problem.

    THE CHILD DOES NOT INHERIT THE REPOSITORY-LOCATION VARIABLES. Git exports `GIT_DIR`,
    `GIT_WORK_TREE` and `GIT_INDEX_FILE` to every process a hook or `rebase --exec` starts, and a child
    that inherits one answers from the named repository whatever `cwd` is — so the gate would compare
    another checkout's commit while this one runs. The variables naming where git's CONFIGURATION lives
    are kept, because that configuration is what a checkout's ignore rules live in.
    """
    environment = {name: value for name, value in os.environ.items() if name not in _GIT_LOCATION_VARIABLES}
    try:
        return subprocess.run(
            ["git", *arguments],
            cwd=directory,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise RevisionGateError(
            f"git could not be run in {directory}: {error}; the revision gate compares a checkout's commit, "
            "so a machine without a runnable git cannot start a run"
        ) from error


def _git_message(completed: subprocess.CompletedProcess[str]) -> str:
    """Return git's own explanation of a failed command, trimmed, or its exit code when it printed none."""
    message = (completed.stderr or completed.stdout).strip()
    return message or f"exit code {completed.returncode}"
