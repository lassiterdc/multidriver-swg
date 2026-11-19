"""Reviewer tests of the main-tag report: the inputs the builder's own tests do not position.

The report script reads the tag list a remote holds and states whether the pushed commit carries a
tag. The builder's tests fix one commit, one interval and tag names of one shape, so an
implementation that hard-codes any of the three still passes them. These tests vary what those
tests hold constant: the interval and the commit the report echoes, the near-miss object id a
substring match would accept, the tag-name shapes a path split would cut, the order a remote lists
its lines in, and the interpreter the runner starts the script with.

Every test drives the script as a subprocess in the form the workflow gives it, tag lines on stdin
and the commit and interval as arguments, so none of them patches a call the implementation happens
to make. The script's path is derived from this file, so the run resolves the checked-out tree and
never an installed copy of the package.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_main_tag.py"

# A 40-character id is what GitHub's `$GITHUB_SHA` carries in a SHA-1 repository; the 64-character id
# is the form a SHA-256 repository would give. The report echoes whichever it is handed.
COMMIT = "4f1c2a9d7b3e5f6081a2b3c4d5e6f708192a3b4c"
LONG_COMMIT = "0123456789abcdef" * 4
OTHER = "0badc0de5f4e3d2c1b0a99887766554433221100"
TAG_OBJECT = "9a8b7c6d5e4f3021a1b2c3d4e5f60718293a4b5c"
WARNING = "::warning title=Untagged main head::"
TAGGED_PREFIX = "carries tag(s): "


def _report(listing: list[str], commit: str, waited: int, *, flags: tuple[str, ...] = (), cwd: Path | None = None):
    """One run of the script over `listing`, with the runner's summary variable absent."""
    environment = {name: value for name, value in os.environ.items() if name != "GITHUB_STEP_SUMMARY"}
    return subprocess.run(
        [sys.executable, *flags, str(SCRIPT), "--commit", commit, "--waited", str(waited)],
        input="".join(line + "\n" for line in listing),
        capture_output=True,
        text=True,
        env=environment,
        cwd=cwd,
        check=False,
    )


def _untagged(commit: str, waited: int) -> list[str]:
    """The two stdout lines of an untagged report, as the criterion words them."""
    return [f"main head {commit} carries no tag {waited} minute(s) after its push", WARNING]


def _carriers(stdout: str, commit: str) -> set[str]:
    """The tag names a tagged report lists, read tolerantly of the separator between them."""
    (line,) = stdout.splitlines()
    prefix = f"main head {commit} {TAGGED_PREFIX}"
    assert line.startswith(prefix), line
    return {name for name in re.split(r"[,\s]+", line[len(prefix) :].strip()) if name}


@pytest.mark.parametrize("commit", [COMMIT, LONG_COMMIT], ids=("sha1", "sha256"))
@pytest.mark.parametrize("waited", [0, 1, 59, 61, 1440])
def test_the_report_states_the_interval_and_commit_it_was_given(waited, commit):
    """Property: the interval and the commit in an untagged report are the arguments, whatever they are.

    Class: NEW CAPABILITY -- every interval the workflow's `--waited` could carry (here 0, 1, 59,
    61 and 1440, none of them the 60 the builder's tests use) crossed with both object-id lengths.
    Plausible wrong implementation: a sentence that writes the shipped default `60` into itself, or
    a script that computes the interval from the clock; the builder's fixtures pass it because they
    only ever hand it 60.
    Reachability: the workflow passes `"$WAIT_MINUTES"` to `--waited`, so a maintainer who changes
    the one declared interval changes this argument and nothing else; artifact:
    `.github/workflows/main-tag-check.yml`, the `--waited` argument of the report step.
    Both-states anchor: the assertion turns on the argument value, which is present in the current
    code and in any implementation, and not on wording this change introduces.

    Why: the shape fixes the sentence as stating "{waited} minute(s)" from the argument, and the
    docstring of the script says the value is echoed and never computed; a report that states 60
    after a 90-minute wait is a false record in the one place a missing tag is noticed.

    Killed by: replacing `{waited}` in the sentence with the literal 60. Second correct
    implementation: a script that formats the sentence with `str.format` over a template constant,
    which still echoes the argument.
    """
    result = _report([f"{OTHER}\trefs/tags/v1.0.0"], commit, waited)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == _untagged(commit, waited), result.stdout

    tagged = _report([f"{commit}\trefs/tags/v1.0.0"], commit, waited)

    assert tagged.returncode == 0, tagged.stderr
    assert tagged.stdout.splitlines() == [f"main head {commit} {TAGGED_PREFIX}v1.0.0"], tagged.stdout


@pytest.mark.parametrize(
    "line",
    [
        f"{COMMIT}0\trefs/tags/v1.0.0",
        f"{COMMIT[:-1]}\trefs/tags/v1.0.0",
        f"{OTHER}\trefs/tags/{COMMIT}",
        f"{OTHER}\trefs/tags/release-{COMMIT}^{{}}",
    ],
    ids=("id-extends-commit", "id-truncates-commit", "name-is-commit", "peeled-name-contains-commit"),
)
def test_an_object_id_or_name_that_only_resembles_the_commit_carries_nothing(line):
    """Property: only a tag line whose object id IS the commit carries it; resemblance does not.

    Class: NEW CAPABILITY -- every line whose object id extends or truncates the commit, and every
    line that merely names the commit's text in its ref. Plausible wrong implementation: a match by
    `commit in line` or `object_id.startswith(commit)`, red under all four lines. The builder's one
    non-carrying line uses an id sharing no characters with the commit, which no substring match
    accepts either.
    Reachability: a remote lists a tag named after a commit (people do tag by sha) or a SHA-256 id
    beside a SHA-1 commit; the two input classes occur in a repository that tags by commit id.
    Artifact: the ruling's sentence "a tag carries the commit when its line's object id equals the
    commit".
    Both-states anchor: equality of the object id with the commit, which holds for the carrying
    case in the current code and in any implementation.

    Why: a false carrier suppresses the warning for the head this check exists to flag, which is
    the one error with no second reader.

    Killed by: matching by substring or prefix. Second correct implementation: a script that
    splits each line into fields first and compares the id field by equality, however it names the
    fields.
    """
    result = _report([line], COMMIT, 60)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == _untagged(COMMIT, 60), result.stdout


def test_tag_names_of_every_shape_are_reported_whole():
    """Property: the names printed are the full tag names, whatever characters a tag name may hold.

    Class: NEW CAPABILITY -- tag names with a slash hierarchy, dots, a plus sign and a hyphen,
    carried once on a lightweight line and once through an annotated tag's peeled line. Plausible
    wrong implementation: a name taken as the last `/`-separated segment of the ref, or cut at the
    first dot; the builder's names are all one `v{n}.{n}.{n}` segment, where both give the same
    result as the whole name.
    Reachability: git accepts `releases/2026/v1.0.0+build.5` as a tag name, and a repository that
    namespaces its tags produces one; artifact: git's ref-name rules.
    Both-states anchor: the names compared are the ones the input lines carry.

    Why: the report is read by the person deciding whether the head is tagged, and a name cut to
    its tail names a tag that may not exist.

    Killed by: `name = ref.rsplit("/", 1)[-1]`. Second correct implementation: a script that
    removes the `refs/tags/` prefix with a regular expression rather than slicing.
    """
    lightweight = "releases/2026/v1.0.0+build.5"
    annotated = "rel/v2-rc.1"
    listing = [
        f"{COMMIT}\trefs/tags/{lightweight}",
        f"{TAG_OBJECT}\trefs/tags/{annotated}",
        f"{COMMIT}\trefs/tags/{annotated}^{{}}",
    ]

    result = _report(listing, COMMIT, 60)

    assert result.returncode == 0, result.stderr
    assert _carriers(result.stdout, COMMIT) == {lightweight, annotated}, result.stdout


def test_the_carrying_tags_do_not_depend_on_the_order_a_remote_lists_its_lines():
    """Property: the same listing in the opposite order names the same carrying tags.

    Class: NEW CAPABILITY -- orderings of one fixed listing in which an annotated tag's peeled line
    precedes its tag-object line, and the carrying tags come last. Plausible wrong implementation:
    one that remembers the previous line to pair a peeled line with its tag object, or that stops
    at the first carrying line; the builder's listings are in git's own order only.
    Reachability: `git ls-remote` orders refs by name and an annotated tag's two lines are adjacent
    in the order the remote chose, but nothing in the ruling fixes that order, and a remote that
    lists the peeled line first is a correct remote.
    Both-states anchor: set equality of the names, which does not read the order.

    Why: the ruling's rule is per line (an id equals the commit, or a peeled id does), and a result
    that depends on neighbouring lines is a different rule.

    Killed by: pairing a peeled line with the line above it. Second correct implementation: one
    that collects every carrying name into a set after reading the whole input.
    """
    listing = [
        f"{OTHER}\trefs/tags/v0.9.0",
        f"{COMMIT}\trefs/tags/v1.0.0-lightweight",
        f"{TAG_OBJECT}\trefs/tags/v1.0.0",
        f"{COMMIT}\trefs/tags/v1.0.0^{{}}",
        f"{OTHER}\trefs/tags/v2.0.0^{{}}",
        f"{TAG_OBJECT}\trefs/tags/v2.0.0",
    ]

    forward = _report(listing, COMMIT, 60)
    backward = _report(list(reversed(listing)), COMMIT, 60)

    assert forward.returncode == 0 and backward.returncode == 0, forward.stderr + backward.stderr
    assert _carriers(forward.stdout, COMMIT) == {"v1.0.0-lightweight", "v1.0.0"}, forward.stdout
    assert _carriers(backward.stdout, COMMIT) == _carriers(forward.stdout, COMMIT), backward.stdout


def test_the_script_runs_on_a_bare_interpreter_with_no_installed_package(tmp_path):
    """Property: the script reports under an interpreter that holds only the standard library.

    Class: NEW CAPABILITY -- both outcomes, tagged and untagged, under `python -I -S` from a
    directory that is not the repository. Plausible wrong implementation: a script that imports the
    project's package, numpy, yaml or any other installed distribution; it passes the builder's
    tests, which run under the project environment where all of those import.
    Reachability: the workflow's report step runs `python3 scripts/check_main_tag.py` on a runner
    after a checkout and nothing else, so no distribution is installed there; artifact: the step
    list of `.github/workflows/main-tag-check.yml`.
    Both-states anchor: the report's sentences, which any correct implementation prints.

    Why: the shape calls the script stdlib-only, and an import the runner cannot satisfy turns a
    passing report job into a traceback with no sentence at all.

    Killed by: `import yaml` (or any non-standard-library import) in the script. Second correct
    implementation: a script that uses `sys.stdin` and `re` instead of `argparse`.
    """
    isolated = ("-I", "-S")

    untagged = _report([], COMMIT, 60, flags=isolated, cwd=tmp_path)
    tagged = _report([f"{COMMIT}\trefs/tags/v1.0.0"], COMMIT, 60, flags=isolated, cwd=tmp_path)

    assert untagged.returncode == 0, untagged.stderr
    assert untagged.stdout.splitlines() == _untagged(COMMIT, 60), untagged.stdout
    assert tagged.returncode == 0, tagged.stderr
    assert tagged.stdout.splitlines() == [f"main head {COMMIT} {TAGGED_PREFIX}v1.0.0"], tagged.stdout
