"""Tests of the main-tag report: its two sentences, its warning, its summary line, and its exit status.

The report is the only evidence that a `main` head was never tagged, and the run that prints it
fails nothing -- so its output is the whole product. Three facts about that output are load-bearing
and each is asserted by equality rather than by a substring: the sentence names the commit the
workflow passed and either the tags that carry it or the interval that carried none; the warning
annotation carries the title GitHub reads and nothing else; and the same sentence is appended to the
runner's step-summary file when, and only when, the report says no tag carries the commit.

The tag list is input, and the interesting inputs are the two kinds of tag. A lightweight tag's one
line names the commit, while an annotated tag's first line names the tag OBJECT and only its
`refs/tags/{name}^{}` line names the commit -- so a reader that matched the first line of each ref
would report every annotated tag as carrying nothing, which on this repository is every tag a
`git tag -a` produced. The tests cover a lightweight tag, an annotated tag, a commit no tag carries,
an empty list, and a list no `git ls-remote --tags` could have written. One test drives the real
command over a throwaway repository, because hand-written lines encode this reader's own assumption
about the format; the real listing is what says the assumption holds.

The exit status is 0 for either report, because nothing here blocks a release, and 2 for a usage
error and for a line that does not parse -- a report built over a line it cannot read would name the
wrong tags, and a broken invocation must not read as a clean report.

The script is invoked as a subprocess everywhere and not imported, the form
`tests/test_check_release_tag.py` takes: its deliverable is its stdout, its stderr and its exit
status, and all three are what the workflow reads.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_main_tag.py"

# The commit a tag carries, another commit nothing carries, and the object id of an annotated tag --
# which is a tag object and never a commit, so no comparison can mistake one for the other.
COMMIT = "4f1c2a9d7b3e5f6081a2b3c4d5e6f708192a3b4c"
OTHER = "0badc0de5f4e3d2c1b0a99887766554433221100"
TAG_OBJECT = "9a8b7c6d5e4f3021a1b2c3d4e5f60718293a4b5c"
WAITED = 60
WARNING = "::warning title=Untagged main head::"
SUMMARY_VARIABLE = "GITHUB_STEP_SUMMARY"
# Passed to every git call: a runner has no identity configured, and `git commit` refuses without one.
IDENTITY = ("-c", "user.name=Main Tag Check", "-c", "user.email=main-tag-check@example.invalid")


def _tag_line(object_id: str, name: str) -> str:
    """One `git ls-remote --tags` line for a tag, as git writes it: the object id, a tab, the ref."""
    return f"{object_id}\trefs/tags/{name}"


def _peeled_line(object_id: str, name: str) -> str:
    """The second line an annotated tag gets, naming the commit it peels to."""
    return f"{object_id}\trefs/tags/{name}^{{}}"


def _environment(summary: Path | None) -> dict[str, str]:
    """The run's environment, with the runner's summary variable set to `summary` or absent entirely.

    The variable is removed rather than left as it is on the machine that runs the suite, so a
    developer whose shell exports one does not get a different result from a runner.
    """
    environment = {name: value for name, value in os.environ.items() if name != SUMMARY_VARIABLE}
    if summary is not None:
        environment[SUMMARY_VARIABLE] = str(summary)
    return environment


def _run(arguments: list[str], listing: list[str] = (), summary: Path | None = None) -> subprocess.CompletedProcess:
    """One invocation of the script with `listing` on stdin, in the form the workflow gives it."""
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        input="".join(f"{line}\n" for line in listing),
        capture_output=True,
        text=True,
        check=False,
        env=_environment(summary),
    )


def _report(
    listing: list[str], *, commit: str = COMMIT, waited: int = WAITED, summary: Path | None = None
) -> subprocess.CompletedProcess:
    """One report run over `listing`, for the pushed commit and the interval the workflow states."""
    return _run(["--commit", commit, "--waited", str(waited)], listing, summary)


def _sentence(commit: str, waited: int, tags: tuple[str, ...] = ()) -> str:
    """The one sentence a report prints, from the tags that carry the commit or from their absence."""
    if tags:
        return f"main head {commit} carries tag(s): {', '.join(tags)}"
    return f"main head {commit} carries no tag {waited} minute(s) after its push"


def _git(repo: Path, *arguments: str) -> str:
    """One git command in `repo`, its stdout returned and a failure raising with git's own message."""
    completed = subprocess.run(["git", *IDENTITY, *arguments], cwd=repo, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, f"git {' '.join(arguments)}: {completed.stderr}"
    return completed.stdout


def _commit(repo: Path, message: str) -> str:
    """Stage everything in `repo`, commit it as `message`, and return the new commit's id.

    The listing this test reads is the remote's, so the throwaway repository needs real commits and
    not tags pointed at nothing: a lightweight tag's line names the commit and an annotated tag's
    peeled line names the commit the tag object points at, and neither line exists otherwise.
    """
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "-m", message)
    return _git(repo, "rev-parse", "HEAD").strip()


def test_a_lightweight_tag_on_the_commit_is_the_whole_report():
    """Property: a lightweight tag naming the commit produces the one sentence, on one line, and exits 0.

    This is the control the other tests are read against: without it, a script that printed the
    untagged sentence for every input would satisfy every test of the untagged case. The line is
    asserted whole, because the workflow's log is the only place a reader meets it, and a script
    that printed the tag names alone would satisfy a substring check while saying nothing about the
    commit the report is about. The run holds no summary file, so the stdout equality is the whole
    of what it printed.
    """
    result = _report([_tag_line(COMMIT, "v1.0.0")])

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [_sentence(COMMIT, WAITED, ("v1.0.0",))]
    assert result.stderr == "", result.stderr
    assert WARNING not in result.stdout, result.stdout


def test_an_annotated_tag_carries_the_commit_through_its_peeled_line():
    """Property: a tag whose first line names a tag object and whose peeled line names the commit carries it.

    Class: NEW CAPABILITY -- the plausible wrong implementation matches each ref's first line only,
    which reports every annotated tag as carrying nothing. That is the expensive version: a
    maintainer who tags with `git tag -a`, as a signed or a message-carrying release is tagged,
    would get a warning annotation on a `main` head that is correctly tagged, and the annotation
    would then be ignored on the push where it is true.

    Why: the tag object id is not the commit it points at, so the only line that names the commit is
    the peeled one. Both lines are supplied here, in git's order, which is the order that matters --
    a reader that stopped at the first line would never reach the second.
    """
    assert TAG_OBJECT != COMMIT
    result = _report([_tag_line(TAG_OBJECT, "v2.0.0"), _peeled_line(COMMIT, "v2.0.0")])

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [_sentence(COMMIT, WAITED, ("v2.0.0",))]


def test_a_tag_that_peels_to_another_commit_carries_nothing():
    """Property: an annotated tag on another commit names neither this commit nor its tag object as a carrier.

    Class: NEW CAPABILITY -- a reader that recorded every ref it saw, or that matched the tag object
    against the commit with a prefix comparison, would report a tag from an earlier release as
    carrying the pushed head.
    """
    result = _report([_tag_line(TAG_OBJECT, "v0.9.0"), _peeled_line(OTHER, "v0.9.0")])

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [_sentence(COMMIT, WAITED), WARNING]


def test_an_empty_list_is_the_untagged_report_with_a_warning_and_exit_zero(tmp_path):
    """Property: no tag lines at all produce the untagged sentence, the warning, and exit 0.

    Class: NEW CAPABILITY -- the plausible wrong implementation exits non-zero when no tag carries
    the commit, or treats an empty stdin as a broken pipe. The first turns the check into a required
    failure on a `main` head that a release is about to tag; the second reports nothing at all on
    the one input this check exists to report.

    Why: `git ls-remote --tags` prints nothing for a remote holding no tags, so an empty list is a
    fact about the remote and not an error. Exit 0 is the ruling's: this check blocks nothing.
    """
    result = _report([])

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [_sentence(COMMIT, WAITED), WARNING]


def test_every_tag_that_carries_the_commit_is_named_once_and_in_order():
    """Property: the tag names are sorted, de-duplicated, and joined in one list.

    Class: NEW CAPABILITY -- the plausible wrong implementation prints them in the order the remote
    listed them, so two runs over the same remote can print two different lines, or prints the whole
    ref (`refs/tags/v2.0.0`) rather than the tag's name.

    Why: a set read in listing order is a set whose printed form depends on an order nothing holds,
    and the report is compared by eye and by log search. The name and not the ref is what a maintainer
    types at the tag they are looking for, and the ref carries the constant prefix.
    """
    listing = [
        _tag_line(OTHER, "v0.8.0"),
        _peeled_line(OTHER, "v0.8.0"),
        _tag_line(COMMIT, "v1.0.0"),
        _tag_line(TAG_OBJECT, "v2.0.0"),
        _peeled_line(COMMIT, "v2.0.0"),
    ]
    result = _report(listing)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [_sentence(COMMIT, WAITED, ("v1.0.0", "v2.0.0"))]


def test_the_sentence_reaches_the_step_summary_only_when_no_tag_carries_the_commit(tmp_path):
    """Property: the untagged sentence is appended to `$GITHUB_STEP_SUMMARY`; the tagged one is not.

    Class: NEW CAPABILITY -- the plausible wrong implementation appends in both cases (the summary
    then says the head carries no tag on every tagged push), appends the warning annotation instead
    of the sentence (the summary reads `::warning ...` in a page that renders no workflow commands),
    or overwrites the file instead of appending to it (a later step's summary line is lost).

    Why: the summary is the page a maintainer reads after the run has scrolled away, so it must carry
    the sentence the log carries, once, beside whatever other steps wrote. One test asserts both
    halves, because a script that appended unconditionally and a script that never appended each
    satisfy one of them.
    """
    summary = tmp_path / "summary.md"
    summary.write_text("an earlier step's line\n", encoding="utf-8")

    untagged = _report([], summary=summary)

    assert untagged.returncode == 0, untagged.stdout + untagged.stderr
    assert summary.read_text(encoding="utf-8") == ("an earlier step's line\n" + _sentence(COMMIT, WAITED) + "\n")

    tagged = _report([_tag_line(COMMIT, "v1.0.0")], summary=summary)

    assert tagged.returncode == 0, tagged.stdout + tagged.stderr
    assert summary.read_text(encoding="utf-8") == "an earlier step's line\n" + _sentence(COMMIT, WAITED) + "\n"


def test_a_run_with_no_summary_variable_still_reports_and_exits_zero():
    """Property: with `GITHUB_STEP_SUMMARY` unset the report is printed unchanged and writes nothing.

    Class: NEW CAPABILITY -- the plausible wrong implementation treats the variable as required and
    exits non-zero without it, which makes the script unrunnable on a workstation and hides every
    defect from a local run.

    Why: the variable is a runner's, and the same script is the one a maintainer runs by hand over a
    saved listing. A missing summary file costs the summary and nothing else.
    """
    result = _report([], summary=None)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [_sentence(COMMIT, WAITED), WARNING]
    assert result.stderr == "", result.stderr


@pytest.mark.parametrize(
    "line",
    [
        f"{COMMIT} refs/tags/v1.0.0",
        f"{COMMIT}\trefs/heads/main",
        "\trefs/tags/v1.0.0",
        f"{COMMIT}\trefs/tags/",
        "refs/tags/v1.0.0",
    ],
    ids=("no-tab", "not-a-tag", "no-object-id", "no-name", "ref-only"),
)
def test_a_line_no_remote_wrote_refuses_the_report_and_exits_two(line):
    """Property: a malformed line exits 2, prints nothing on stdout, and names the line on stderr.

    Class: NEW CAPABILITY -- the plausible wrong implementation skips the line and reports over the
    rest, which is the dangerous one: a listing whose every line is malformed then reports "carries
    no tag" -- a warning annotation on a head that may be tagged -- and a listing with one malformed
    line reports a tag set that is quietly short. Ignoring an input this reader does not understand
    is how a report becomes a false alarm nobody can trace.

    Why: exit 2 and an empty stdout, because a partial report is worse than no report here, and the
    workflow must not print a sentence it built over unread input. A line split on whitespace rather
    than on the tab is included because a tag name cannot carry a space, so a space-separated line
    was written by something other than `git ls-remote`.
    """
    result = _report([_tag_line(COMMIT, "v1.0.0"), line])

    assert result.returncode == 2, result.stdout + result.stderr
    assert result.stdout == "", result.stdout
    assert "unparseable line" in result.stderr, result.stderr
    # The line is named by its ref, because the repr of a tabbed line escapes the tab and a literal
    # tab never reaches stderr: the assertion has to hold the part a reader could search for.
    assert line.split("\t")[-1] in result.stderr, result.stderr


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        ["--commit", COMMIT],
        ["--waited", str(WAITED)],
        ["--commit", COMMIT, "--waited", "soon"],
        ["--commit", COMMIT, "--waited", str(WAITED), "--tag", "v1.0.0"],
    ],
    ids=("no-arguments", "no-waited", "no-commit", "waited-not-a-number", "unknown-flag"),
)
def test_a_usage_error_exits_two(arguments):
    """Property: a missing, a mistyped or an unknown argument exits 2 before anything is read.

    Class: NEW CAPABILITY -- the plausible wrong implementation defaults either argument, so a run
    with no `--commit` reports over an empty commit and prints "carries no tag" for a head it never
    named, and a run with no `--waited` prints an interval the run did not wait.

    Why: both values are facts only the workflow knows. A default for either is a report about
    something nobody asked about, and the exit status is what a maintainer reads to tell that apart
    from a report.
    """
    result = _run(arguments, [_tag_line(COMMIT, "v1.0.0")])

    assert result.returncode == 2, result.stdout + result.stderr
    assert result.stdout == "", result.stdout


def test_the_report_reads_the_tag_list_a_remote_actually_lists(tmp_path):
    """Property: over a real `git ls-remote --tags` listing, a lightweight tag and an annotated tag both carry.

    Class: NEW CAPABILITY -- the plausible wrong implementation is one that parses the format as this
    file writes it: an annotated tag's first line is a tag object id, and a reader that matched on
    the first line of each ref reports the annotated tag as carrying nothing. Hand-written lines
    encode that assumption; the real command is what falsifies it.

    Why: the format belongs to git and not to this test tree, so the one test that can catch a change
    in it -- a new separator, a peeled line written differently, an annotated tag listed once -- is
    the one that runs the command. The two assertions about the listing itself say the case is the
    one intended: it carries a `^{}` line for the annotated tag, and the annotated tag's own line
    names something other than the commit, so nothing but the peeled line could have carried it.
    """
    repo = tmp_path / "remote"
    repo.mkdir()
    _git(repo, "init", "--quiet", "-b", "main")
    (repo / "file.txt").write_text("one\n", encoding="utf-8")
    tagged = _commit(repo, "the commit a lightweight tag names")
    (repo / "file.txt").write_text("two\n", encoding="utf-8")
    annotated = _commit(repo, "the commit an annotated tag names")
    _git(repo, "tag", "v1.0.0", tagged)
    _git(repo, "tag", "--annotate", "--message", "release 2.0.0", "v2.0.0", annotated)
    listing = _git(repo, "ls-remote", "--tags", str(repo)).splitlines()

    assert any(line.endswith("refs/tags/v2.0.0^{}") for line in listing), listing
    own_lines = [line for line in listing if line.endswith("refs/tags/v2.0.0")]
    assert len(own_lines) == 1, listing
    assert own_lines[0].split("\t")[0] != annotated, own_lines

    lightweight = _report(listing, commit=tagged)
    assert lightweight.returncode == 0, lightweight.stdout + lightweight.stderr
    assert lightweight.stdout.splitlines() == [_sentence(tagged, WAITED, ("v1.0.0",))]

    annotated_report = _report(listing, commit=annotated)
    assert annotated_report.returncode == 0, annotated_report.stdout + annotated_report.stderr
    assert annotated_report.stdout.splitlines() == [_sentence(annotated, WAITED, ("v2.0.0",))]
