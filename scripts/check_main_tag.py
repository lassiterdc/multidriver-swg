#!/usr/bin/env python3
"""Report whether the head of `main` carries a tag, and warn when it does not.

A push to `main` is meant to be followed by a release tag naming the pushed commit. GitHub fires no
event when the tag never appears, so a delayed re-check is the only evidence that it did: this
report runs sixty minutes after the push, reads the tags the remote holds, and says which of them
carry the pushed commit, or that none does.

The tag list arrives on stdin in the form `git ls-remote --tags` writes it -- one
`<object id><tab><ref>` line per ref -- because that command asks the remote for the tags rather
than reading the checkout, and a checkout of the pushed commit holds only the tags it happened to
fetch. An annotated tag appears twice, once for the tag object and once for the commit it peels to
(`refs/tags/{name}^{}`); a lightweight tag appears once, its object id being the commit itself. A
tag therefore carries the commit when either of its lines names it, which is one rule over the two
kinds.

Nothing here blocks a release, so the exit status is 0 whether or not a tag carries the commit. It
is 2 on a usage error and on a line this reader cannot parse, because a report built over a line it
does not understand would name the wrong tags in the one place a missing tag is noticed.

`--waited` is echoed and never computed: the workflow owns the interval, and the report has to state
the interval the run actually waited rather than one this script assumed.
"""

from __future__ import annotations

import argparse
import os
import sys

TAG_REF = "refs/tags/"
PEELED_SUFFIX = "^{}"
# The annotation GitHub reads off stdout. It carries the title alone; the sentence printed above it
# names the commit and the interval, and repeating that sentence here would print it twice in a log
# whose first line already says it.
UNTAGGED_WARNING = "::warning title=Untagged main head::"
# The runner's step-summary file, present on a GitHub runner and absent everywhere else.
STEP_SUMMARY_VARIABLE = "GITHUB_STEP_SUMMARY"
EXIT_USAGE = 2


def _parse_line(line: str) -> tuple[str, str, str]:
    """One `git ls-remote` line as (object id, tag name, problem), the peel marker dropped from the name.

    `problem` is empty when the line parsed. Git writes one tab between the object id and the ref: a
    line split on whitespace admits a ref whose name carries a space, and a tag name cannot, so a
    line without a tab is a line no `git ls-remote --tags` wrote.
    """
    object_id, separator, ref = line.partition("\t")
    if not separator:
        return "", "", f"no tab between the object id and the ref: {line!r}"
    if not object_id:
        return "", "", f"no object id: {line!r}"
    if not ref.startswith(TAG_REF):
        return "", "", f"not a tag: {ref!r}"
    name = ref[len(TAG_REF) :]
    if name.endswith(PEELED_SUFFIX):
        name = name[: -len(PEELED_SUFFIX)]
    if not name:
        return "", "", f"no tag name: {ref!r}"
    return object_id, name, ""


def _carriers(lines: list[str], commit: str) -> tuple[list[str], str]:
    """Every tag name one of whose lines carries `commit`, sorted, and the first line that did not parse.

    The names are sorted and de-duplicated because an annotated tag and a lightweight tag can both
    carry one commit, and a set read in the order a remote listed it would print a different report
    for the same remote.
    """
    names: set[str] = set()
    for line in lines:
        if not line.strip():
            continue
        object_id, name, problem = _parse_line(line)
        if problem:
            return [], problem
        if object_id == commit:
            names.add(name)
    return sorted(names), ""


def _sentence(commit: str, waited: int, carriers: list[str]) -> str:
    """The report's one sentence: the tags that carry the commit, or the interval that carried none."""
    if carriers:
        return f"main head {commit} carries tag(s): {', '.join(carriers)}"
    return f"main head {commit} carries no tag {waited} minute(s) after its push"


def _append_summary(sentence: str) -> None:
    """Append `sentence` to the step-summary file, when the runner provides one."""
    path = os.environ.get(STEP_SUMMARY_VARIABLE)
    if not path:
        return
    with open(path, "a", encoding="utf-8") as summary:
        summary.write(sentence + "\n")


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The pushed commit and the interval waited, both required because the report states both."""
    parser = argparse.ArgumentParser(
        description="Report whether the head of main carries a tag, reading `git ls-remote --tags` on stdin."
    )
    parser.add_argument("--commit", required=True, help="the full object id of the pushed commit")
    parser.add_argument("--waited", required=True, type=int, help="the minutes this run waited before reading")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Print the report and return the exit status: 0 for either outcome, 2 for a usage error."""
    arguments = _parse_arguments(argv)
    carriers, problem = _carriers(sys.stdin.read().splitlines(), arguments.commit)
    if problem:
        print(f"unparseable line: {problem}", file=sys.stderr)
        return EXIT_USAGE
    sentence = _sentence(arguments.commit, arguments.waited, carriers)
    print(sentence)
    if not carriers:
        print(UNTAGGED_WARNING)
        _append_summary(sentence)
    return 0


if __name__ == "__main__":
    sys.exit(main())
