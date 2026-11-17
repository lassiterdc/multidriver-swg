"""Check the revision gate: the checkout's commit against the declared one, and the status after it.

Every check here runs against a throwaway git repository, because the gate's subject is a checkout and
a mocked subprocess would let an implementation pass while examining the wrong directory. Each refusal
is reached by changing ONE fact of the same repository — a commit, a declaration, a file on disk — so
the check names the fact it is red without.

Each check names the plausible wrong implementation it is red under, because a check that cannot fail
for the reason it claims constrains nothing.
"""

from __future__ import annotations

import dataclasses
import subprocess
from pathlib import Path

import pytest

import multidriver_swg.run_configuration.revision_gate as revision_gate
from multidriver_swg.run_configuration.revision_gate import (
    REVISION_RULE,
    DeclaredRevision,
    RevisionGateError,
    load_declared_revision,
    require_clean_checkout,
    require_declared_revision,
    require_package_checkout,
    require_revision_gate,
    running_package_directory,
    running_revision,
)

# The three statements the module is the deliverable for, asserted verbatim by the docstring check.
THE_PACKAGE_IS_THE_CHECKOUTS = (
    "A run executes the package of a git checkout, and that checkout is the one whose source tree holds the package "
    "the run imported."
)
THE_COMMIT_MATCHES = (
    "The commit the run executes equals the forty-character commit its project declares, and the checkout holding it "
    "is clean."
)
THE_COMMIT_GOVERNS = (
    "The commit governs: a tag is carried as a statement of intent and is never what the comparison reads."
)

_ABSENT = object()

# A commit id no checkout here holds. Forty lowercase hex characters by construction, so it is refused by
# the comparison and not by the shape check.
_ELSEWHERE = "0" * 40


def _git(repository: Path, *arguments: str) -> str:
    """Run one git command in `repository` with a throwaway identity and return its trimmed standard output."""
    completed = subprocess.run(
        [
            "git",
            "-c",
            "user.name=gate-test",
            "-c",
            "user.email=gate-test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *arguments,
        ],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _checkout(tmp_path: Path) -> Path:
    """A committed repository whose source tree holds a `multidriver_swg` package, with one commit."""
    repository = tmp_path / "toolkit"
    package = repository / "src" / "multidriver_swg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (repository / ".gitignore").write_text("*.ignored\n", encoding="utf-8")
    _git(repository, "init", "-q")
    _git(repository, "add", "-A")
    _git(repository, "commit", "-q", "-m", "initial")
    return repository


def _declaration(tmp_path: Path, commit: str, *, tag: object = _ABSENT) -> Path:
    """Write a revision declaration for `commit` beside the checkout and return its path."""
    lines = [f'rule = "{REVISION_RULE}"', f'commit = "{commit}"']
    if tag is not _ABSENT:
        lines.append(f'tag = "{tag}"')
    path = tmp_path / "toolkit_revision.toml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_running_revision_returns_the_checkouts_full_head_commit(tmp_path: Path) -> None:
    """Property: `running_revision` returns the forty-character commit `git rev-parse HEAD` reports in that directory.

    Class: any checkout with at least one commit.
    Why it is required: the value is what a producer records as its code version and what the declared
    comparison reads, so an abbreviated or re-derived value would name no object.
    What kills it: `git rev-parse --short HEAD`, or returning the branch name.
    Second correct implementation: reading `.git/refs` directly; the same commit is returned.
    """
    repository = _checkout(tmp_path)

    commit = running_revision(repository)

    assert commit == _git(repository, "rev-parse", "HEAD")
    assert len(commit) == 40


def test_running_revision_defaults_to_the_running_package_and_not_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Property: called with no argument, `running_revision` reports the running package's checkout even when the working directory is outside any checkout.

    Class: the process's own package, with the working directory moved to a temporary directory that is
    not a repository.
    Why it is required: a rule recording the code version of its output must not record whatever
    directory the engine happened to start in, and a default read from `os.getcwd()` would do exactly
    that while looking correct from a checkout.
    What kills it: `start=None` forwarded to `subprocess.run` without `cwd`, which inherits the working
    directory and raises here.
    Second correct implementation: deriving the package directory from `multidriver_swg.__file__`; the
    same commit is returned.
    """
    monkeypatch.chdir(tmp_path)

    assert running_revision() == running_revision(running_package_directory())
    assert len(running_revision()) == 40


def test_a_directory_with_no_checkout_is_refused_naming_it(tmp_path: Path) -> None:
    """Property: a directory outside any git checkout is refused, with the directory in the message.

    Class: a temporary directory with no repository at or above it.
    Why it is required: an installed copy has no commit, and a gate that returned an empty value would
    compare nothing against the declaration and pass.
    What kills it: returning `stdout.strip()` without checking the exit code.
    Second correct implementation: checking for a `.git` entry before running git; the same directory is
    refused.
    """
    with pytest.raises(RevisionGateError) as caught:
        running_revision(tmp_path)

    assert str(tmp_path) in str(caught.value)


def test_an_operand_that_is_not_a_directory_is_refused(tmp_path: Path) -> None:
    """Property: a revision operand naming no directory is refused by this module rather than by the operating system.

    Class: a path to a regular file.
    Why it is required: the caller's recovery is to point the gate at a checkout, and a `NotADirectoryError`
    would send them looking for a missing file when the operand is a directory that is not one.
    What kills it: passing the value straight to `subprocess.run`, whose error names the file operation.
    Second correct implementation: an explicit `is_dir` check at any point before the command; the same
    operand is refused.
    """
    path = tmp_path / "a_file"
    path.write_text("", encoding="utf-8")

    with pytest.raises(RevisionGateError):
        running_revision(path)


def test_the_package_check_returns_the_checkout_that_holds_the_package(tmp_path: Path) -> None:
    """Property: `require_package_checkout` returns the repository whose `src/multidriver_swg` resolves to the directory handed in.

    Class: a checkout with the package at its source root, and the package directory itself as operand.
    Why it is required: the returned root is what the commit and status checks then read, so a wrong root
    would gate a different checkout.
    What kills it: returning the package directory rather than `--show-toplevel`'s answer.
    Second correct implementation: resolving the package's parent chain to the repository root; the same
    directory is returned.
    """
    repository = _checkout(tmp_path)

    assert require_package_checkout(repository / "src" / "multidriver_swg") == repository.resolve()


def test_a_package_inside_a_checkout_but_outside_its_source_tree_is_refused(tmp_path: Path) -> None:
    """Property: a package directory inside a repository whose source tree does not hold it is refused, naming both paths.

    Class: a directory named `multidriver_swg` inside a committed repository at a path other than
    `src/multidriver_swg` — the shape of a non-editable install under a clone's ignored directory.
    Why it is required: `git rev-parse --show-toplevel` succeeds there, so without the second clause the
    gate would compare the enclosing repository's commit against a package it does not contain.
    What kills it: the toplevel lookup alone.
    Second correct implementation: comparing the package's `__file__` against the checkout's source file;
    the same directory is refused with the same two paths.
    """
    repository = _checkout(tmp_path)
    elsewhere = repository / "install" / "multidriver_swg"
    elsewhere.mkdir(parents=True)

    with pytest.raises(RevisionGateError) as caught:
        require_package_checkout(elsewhere)

    message = str(caught.value)
    assert str(elsewhere.resolve()) in message
    assert str((repository / "src" / "multidriver_swg").resolve()) in message


def test_a_package_outside_any_checkout_is_refused(tmp_path: Path) -> None:
    """Property: a package directory with no enclosing checkout is refused, naming the directory.

    Class: an installed-copy shape with no repository at or above it.
    Why it is required: there is no commit to compare, and the reader must be told that the defect is
    the absence of a checkout rather than a mismatch.
    What kills it: treating a failed `--show-toplevel` as an empty root.
    Second correct implementation: an explicit repository-presence check before the command; the same
    directory is refused.
    """
    package = tmp_path / "site-packages" / "multidriver_swg"
    package.mkdir(parents=True)

    with pytest.raises(RevisionGateError) as caught:
        require_package_checkout(package)

    assert str(package) in str(caught.value)


def test_the_declaration_loads_its_rule_commit_and_optional_tag(tmp_path: Path) -> None:
    """Property: a conforming declaration loads, carrying the tag when it is written and `None` when it is not.

    Class: the same commit with and without a `tag` field.
    Why it is required: the tag is commentary an operator reads, and its absence must be a normal state
    rather than a refused one.
    What kills it: requiring `tag`, or dropping it from the returned value.
    Second correct implementation: reading the document into the value either way; both files load.
    """
    commit = _git(_checkout(tmp_path), "rev-parse", "HEAD")

    tagged = load_declared_revision(_declaration(tmp_path, commit, tag="v1.2.3"))
    untagged = load_declared_revision(_declaration(tmp_path, commit))

    assert tagged == DeclaredRevision(commit=commit, tag="v1.2.3")
    assert untagged == DeclaredRevision(commit=commit, tag=None)


def test_the_declaration_refuses_a_key_or_rule_the_form_does_not_name(tmp_path: Path) -> None:
    """Property: a top-level key the form does not name, and a rule that is not the literal, are each refused with the file named.

    Class: one extra top-level key (`branch`), and a declaration naming another version.
    Why it is required: the header makes a later change to the declaration loud, and an unnamed key is a
    field its writer believes is compared while nothing reads it — a mistyped `committ` would otherwise
    silently fall back to the absent-commit refusal, which points at the wrong defect.
    What kills it: reading the two named keys and ignoring the rest, or accepting any `toolkit-revision:N`.
    Second correct implementation: a closed key-set check plus a literal comparison, in either order;
    both documents are refused.
    """
    commit = "1" * 40
    path = _declaration(tmp_path, commit)
    path.write_text(f'rule = "{REVISION_RULE}"\ncommit = "{commit}"\nbranch = "main"\n', encoding="utf-8")

    with pytest.raises(RevisionGateError) as unknown_key:
        load_declared_revision(path)
    assert "branch" in str(unknown_key.value)

    other_rule = tmp_path / "other_rule.toml"
    other_rule.write_text(f'rule = "toolkit-revision:2"\ncommit = "{commit}"\n', encoding="utf-8")

    with pytest.raises(RevisionGateError) as wrong_rule:
        load_declared_revision(other_rule)
    assert str(other_rule) in str(wrong_rule.value)


@pytest.mark.parametrize(
    "commit",
    [
        pytest.param("A" * 40, id="uppercase"),
        pytest.param("a" * 39, id="short"),
        pytest.param("a" * 41, id="long"),
        pytest.param("g" * 40, id="not-hex"),
        pytest.param(_ABSENT, id="absent"),
        pytest.param(7, id="not-text"),
    ],
)
def test_a_commit_that_is_not_forty_lowercase_hex_is_refused(tmp_path: Path, commit: object) -> None:
    """Property: a commit that is not exactly forty lowercase hexadecimal characters is refused, absent and non-text included.

    Class: upper case, one character short, one long, a non-hex letter, absence, and a non-text value.
    Why it is required: an abbreviated or upper-case value cannot be compared with a correct one and the
    comparison's mismatch message would mislead; the shape must be refused where the value is read.
    What kills it: validating with `len(value) == 40`, which admits upper case and `g`; or using
    `int(value, 16)` alone, which admits a short value.
    Second correct implementation: a full-match regular expression, or a per-character check; the same
    six declarations are refused.
    """
    if commit is _ABSENT:
        path = tmp_path / "declaration.toml"
        path.write_text(f'rule = "{REVISION_RULE}"\n', encoding="utf-8")
    elif isinstance(commit, str):
        path = _declaration(tmp_path, commit)
    else:
        path = tmp_path / "declaration.toml"
        path.write_text(f'rule = "{REVISION_RULE}"\ncommit = {commit}\n', encoding="utf-8")

    with pytest.raises(RevisionGateError) as caught:
        load_declared_revision(path)

    assert "'commit'" in str(caught.value)


@pytest.mark.parametrize("tag", ["", "   ", 7])
def test_an_empty_or_non_text_tag_is_refused(tmp_path: Path, tag: object) -> None:
    """Property: a present `tag` is refused unless it is non-empty text; an absent tag is admitted.

    Class: an empty string, whitespace only, and a non-text value, against the absent-tag control.
    Why it is required: the tag is commentary, and an empty one reads as a named release that names
    nothing.
    What kills it: accepting any present value.
    Second correct implementation: a `strip()`-based emptiness check; the same three values are refused.
    """
    commit = "2" * 40
    path = tmp_path / "declaration.toml"
    lines = [f'rule = "{REVISION_RULE}"', f'commit = "{commit}"']
    lines.append(f'tag = "{tag}"' if isinstance(tag, str) else f"tag = {tag}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(RevisionGateError) as caught:
        load_declared_revision(path)

    assert "'tag'" in str(caught.value)


def test_the_comparison_admits_equal_commits_and_names_both_when_they_differ() -> None:
    """Property: equal commits pass, and different ones raise a refusal carrying both values.

    Class: one equal pair and one pair differing in a single character.
    Why it is required: a mismatch is the run's stop condition, and an operator has to know which side to
    change; a message naming one commit cannot support that.
    What kills it: comparing only one direction's presence, or a message naming a single value.
    Second correct implementation: any equality comparison; the same pair is refused with both values.
    """
    commit = "a" * 40

    require_declared_revision(commit, commit)

    with pytest.raises(RevisionGateError) as caught:
        require_declared_revision(commit, "a" * 39 + "b")

    message = str(caught.value)
    assert commit in message
    assert ("a" * 39 + "b") in message


@pytest.mark.parametrize("operand", ["abc123", "A" * 40, 7])
def test_the_comparison_refuses_an_operand_that_is_not_a_full_commit(operand: object) -> None:
    """Property: an operand that is not forty lowercase hex characters is refused as an operand, in either position.

    Class: a short value, an upper-case value and a non-text value, supplied as the declared operand and
    as the running operand.
    Why it is required: two malformed values could compare equal and pass, so the shape check must run
    before the comparison rather than only on the declared side.
    What kills it: validating only the declared argument, or comparing with `==` before any shape check.
    Second correct implementation: one loop over the two labelled operands; the same six calls are
    refused.
    """
    valid = "b" * 40

    with pytest.raises(RevisionGateError):
        require_declared_revision(operand, valid)  # type: ignore[arg-type]
    with pytest.raises(RevisionGateError):
        require_declared_revision(valid, operand)  # type: ignore[arg-type]


def test_a_clean_checkout_is_admitted_and_each_kind_of_change_is_refused(tmp_path: Path) -> None:
    """Property: a clean tree passes; a tracked edit, an untracked file and a staged change are each refused naming the entry; an ignored file is admitted.

    Class: one checkout with each of those five facts in turn.
    Why it is required: both kinds of change alter what runs, so a gate that read only tracked edits
    would let an untracked module execute; and an ignored build directory must not stop a run.
    What kills it: reading `git diff` instead of `git status`, which misses an untracked file; or reading
    `git status` without `--porcelain`, which admits an ignored file.
    Second correct implementation: `git status --porcelain=v1 -z` split on NUL; the same five facts
    decide.
    """
    repository = _checkout(tmp_path)

    require_clean_checkout(repository)

    (repository / "scratch.ignored").write_text("ignored\n", encoding="utf-8")
    require_clean_checkout(repository)

    (repository / "src" / "multidriver_swg" / "__init__.py").write_text("# edited\n", encoding="utf-8")
    with pytest.raises(RevisionGateError) as tracked:
        require_clean_checkout(repository)
    assert "src/multidriver_swg/__init__.py" in str(tracked.value)

    _git(repository, "add", "-A")
    _git(repository, "commit", "-q", "-m", "edit")

    (repository / "untracked.py").write_text("x = 1\n", encoding="utf-8")
    with pytest.raises(RevisionGateError) as untracked:
        require_clean_checkout(repository)
    assert "untracked.py" in str(untracked.value)

    (repository / "untracked.py").unlink()
    (repository / "src" / "multidriver_swg" / "__init__.py").write_text("# staged\n", encoding="utf-8")
    _git(repository, "add", "src/multidriver_swg/__init__.py")
    with pytest.raises(RevisionGateError):
        require_clean_checkout(repository)


def test_the_composed_gate_runs_its_checks_in_the_documented_order(tmp_path: Path) -> None:
    """Property: the gate returns the running commit for a conforming checkout, and with two defects reports the earlier check's defect.

    Class: a conforming checkout; one that is both at an undeclared commit and dirty; one that is only
    dirty; and a package directory outside the checkout.
    Why it is required: both call sites run this order, so an order that reported the dirty tree before
    the commit mismatch would send an operator to fix files when checking out the declared commit is the
    action — and would report a different fact at the two call sites if either restated the order.
    What kills it: running `require_clean_checkout` first, or omitting it so a dirty conforming tree
    returns a commit.
    Second correct implementation: the same three calls in the same order, written inline at the call
    sites rather than in one function; the same four cases decide as here.
    """
    repository = _checkout(tmp_path)
    head = running_revision(repository)

    assert require_revision_gate(repository / "src" / "multidriver_swg", DeclaredRevision(commit=head)) == head

    (repository / "untracked.py").write_text("x = 1\n", encoding="utf-8")

    with pytest.raises(RevisionGateError) as both:
        require_revision_gate(repository / "src" / "multidriver_swg", DeclaredRevision(commit=_ELSEWHERE))
    assert "differs from the commit the project declares" in str(both.value)

    with pytest.raises(RevisionGateError) as dirty:
        require_revision_gate(repository / "src" / "multidriver_swg", DeclaredRevision(commit=head))
    assert "is not clean" in str(dirty.value)

    outside = tmp_path / "outside" / "multidriver_swg"
    outside.mkdir(parents=True)
    with pytest.raises(RevisionGateError) as absent_checkout:
        require_revision_gate(outside, DeclaredRevision(commit=head))
    assert "not the package of a git checkout" in str(absent_checkout.value)


def test_the_declared_revision_value_is_frozen() -> None:
    """Property: a `DeclaredRevision` refuses reassignment of either field.

    Class: a conforming value and each of its two fields.
    Why it is required: the value is what a run compares against, so a mutated instance would compare a
    commit its file never carried.
    What kills it: dropping `frozen=True`.
    Second correct implementation: a named tuple with no setter; the same two assignments are refused.
    """
    declared = DeclaredRevision(commit="c" * 40, tag="v0")

    with pytest.raises(dataclasses.FrozenInstanceError):
        declared.commit = "d" * 40  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        declared.tag = None  # type: ignore[misc]


def test_the_refusal_root_is_not_a_key_error() -> None:
    """Property: `RevisionGateError` derives from `Exception` and not from `KeyError`.

    Class: the class object.
    Why it is required: `KeyError.__str__` renders a message inside repr quotes, which would deliver the
    actionable part of every refusal as a quoted literal.
    What kills it: deriving from `KeyError`, or from a stdlib error whose `__str__` quotes.
    Second correct implementation: any `Exception` subclass not inheriting a quoting error; the check is
    the inheritance itself.
    """
    assert issubclass(RevisionGateError, Exception)
    assert not issubclass(RevisionGateError, KeyError)


def test_the_module_docstring_states_the_three_commitments() -> None:
    """Property: the module docstring contains the three statements the gate implements, and names its public functions.

    Class: any version of the docstring, searched whole.
    Why it is required: the statements are the deliverable text, and a docstring reduced to a list of
    functions would leave the reason a commit is compared unstated.
    What kills it: deleting or rephrasing any statement, or ceasing to name the gate's entry points.
    Second correct implementation: the sentences at any position in any surrounding prose — the check
    asserts containment only.
    """
    docstring = revision_gate.__doc__ or ""

    assert THE_PACKAGE_IS_THE_CHECKOUTS in docstring
    assert THE_COMMIT_MATCHES in docstring
    assert THE_COMMIT_GOVERNS in docstring
    assert "running_revision" in docstring
    assert "require_declared_revision" in docstring
    assert "require_revision_gate" in docstring
