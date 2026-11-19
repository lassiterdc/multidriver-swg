#!/usr/bin/env python3
"""The content scanner's entry point: one reading contract over four classes of content.

Run as `python3 scripts/content_scan [options]`. Python runs a DIRECTORY holding this file as a
script, with the directory on `sys.path`, so the scanner needs no install and keeps working before
any project environment exists -- which matters, because repo scaffolding is exactly when a private
path is most likely to be committed.

The core in `core.py` owns what is read and how a run is reported; each of `identity`,
`machine_path`, `planning_reference` and `secret` owns one class and nothing else. This module
assembles them from the options and dispatches one of three forms: a located scan (the default), a
count-only scan (`--count-only`, and always on a GitHub runner), and `--list`, which says what a
scan would read without failing on anything it finds.

Each option below is a reading option, never a policy one: which classes run, which corpus root,
where the identity class's private carrier and ledger are, and which report form is printed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:  # imported as part of the scanner's package (the test suite, and `python3 -m`)
    from . import core, identity, machine_path, planning_reference, secret
except ImportError:  # run as a directory: `python3 scripts/content_scan` puts this directory on sys.path
    import core  # type: ignore[no-redef]
    import identity  # type: ignore[no-redef]
    import machine_path  # type: ignore[no-redef]
    import planning_reference  # type: ignore[no-redef]
    import secret  # type: ignore[no-redef]

MODULES = (identity, machine_path, planning_reference, secret)
CLASS_NAMES = tuple(module.NAME for module in MODULES)

BLOCKLIST_HELP = (
    "path of the blocklist file; the carrier and ledger are read from its directory "
    "(default: <root>/scripts/anonymization_blocklist.txt, or the same path in the main checkout "
    "when this checkout has no carrier)"
)


def build_matchers(args: argparse.Namespace) -> list[core.Matcher]:
    """Load every selected class's matcher, in the order the modules are listed."""
    chosen = set(args.classes) if args.classes else set(CLASS_NAMES)
    matchers: list[core.Matcher] = []
    for module in MODULES:
        if module.NAME not in chosen:
            continue
        if module is identity:
            matchers.append(
                identity.load(
                    args.root,
                    blocklist=args.blocklist,
                    baseline=args.baseline,
                    require_supplement=args.require_supplement,
                )
            )
        else:
            matchers.append(module.load())
    return matchers


def _count_only(args: argparse.Namespace) -> int:
    """Scan and print ONE line of counts; never a path, a line number or a label's own text.

    Every failure below is reported by a FIXED sentence. An exception's own text can quote a
    carrier or a ledger cell (a malformed ordinal names the cell it read), and in CI that text
    would land in a public log.
    """
    if args.list_only or args.reveal:
        print(f"{core.COUNT_ONLY_PREFIX} --list and --reveal are refused in count-only mode", file=sys.stderr)
        return core.EXIT_UNUSABLE
    try:
        report = core.scan(args.root, build_matchers(args))
    except (SystemExit, Exception):
        print(core.COUNT_ONLY_UNREADABLE_ERROR, file=sys.stderr)
        return core.EXIT_UNUSABLE
    if report.scanned == 0:
        # The count line would read `0 finding(s)` over a scan that opened no file, and the runner
        # always takes this form, so the push job would go green over nothing. A corpus with no
        # readable file is the vacuous green this scanner exists to refuse; a fixed sentence keeps
        # the form's promise that nothing here locates a finding.
        print(f"{core.COUNT_ONLY_PREFIX} nothing to scan", file=sys.stderr)
        return core.EXIT_UNUSABLE
    print(core.count_line(report))
    return core.EXIT_FINDINGS if report.refused else core.EXIT_CLEAN


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=core.REPO_ROOT, help="repo root to scan")
    parser.add_argument(
        "--class", dest="classes", action="append", choices=CLASS_NAMES, help="scan one class only; repeatable"
    )
    parser.add_argument("--blocklist", type=Path, default=None, help=BLOCKLIST_HELP)
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help=f"acceptance ledger (default: the gitignored {identity.LOCAL_LEDGER} beside the carrier)",
    )
    parser.add_argument(
        "--list",
        "--dry-run",
        dest="list_only",
        action="store_true",
        help=(
            "print each class's labels + the scan scope and exit 0 without failing. --list does not "
            "fail on FINDINGS; --require-supplement still applies, because a missing carrier is a "
            "coverage fact, not a finding"
        ),
    )
    parser.add_argument(
        "--reveal",
        action="store_true",
        help="with --list, print each label's name as well as its index (never use in CI: the logs are public)",
    )
    parser.add_argument(
        "--require-supplement",
        action="store_true",
        help=(
            "exit 2 if the identity class's private carrier is absent. For a caller that has just "
            "established it (the estate's setup.sh); NOT for the tracked pre-commit config, which "
            "every clone shares and no third party can satisfy"
        ),
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help=(
            "print one line of counts and nothing that locates a finding; always in force when "
            "GITHUB_ACTIONS=true. Refuses --list and --reveal"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)

    if core.count_only_in_force(args.count_only):
        return _count_only(args)

    try:
        matchers = build_matchers(args)
        if args.list_only:
            for line in core.listing_lines(args.root, matchers, args.reveal):
                print(line)
            return core.EXIT_CLEAN
        report = core.scan(args.root, matchers)
    except core.UnreadableError as exc:
        print(f"{core.COUNT_ONLY_PREFIX} {exc}", file=sys.stderr)
        return core.EXIT_UNUSABLE

    for line in report.skipped:
        print(line, file=sys.stderr)
    if report.scanned == 0:
        # A corpus with no readable text file is not a clean tree: it is a run that checked
        # nothing, which is the same vacuous green a carrier with no tokens would produce.
        print(f"{core.COUNT_ONLY_PREFIX} nothing to scan under {args.root}", file=sys.stderr)
        return core.EXIT_UNUSABLE
    if not report.refused:
        return core.EXIT_CLEAN
    print(core.failure_line(report), file=sys.stderr)
    for line in core.located_lines(report):
        print(f"  {line}", file=sys.stderr)
    return core.EXIT_FINDINGS


if __name__ == "__main__":
    sys.exit(main())
