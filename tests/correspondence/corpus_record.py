"""Check that the corpus record still describes the legacy files git tracks.

The disposition ledger and the tests in this package cite line ranges of the legacy code in
`_old_code_to_refactor/`.
Those citations mean something only while the cited files are exactly the files the
record `legacy_corpus_record.toml` describes, so this module recomputes every derived
field of that record from the tracked files and reports each disagreement by file and
field. It is the head of the check chain: a check that reads the corpus can call
`require_record()` first, and then reports that it did not measure, rather than failing,
when the record itself is wrong — one broken record then shows as one failure with
one cause instead of hundreds. Today the extent check calls it, from `extent.require_extent`,
and the known-defect register's test reaches it through that call; the ledger's checks read the
corpus through `extent` and call neither.

Run as a script (`python -m tests.correspondence.corpus_record`) it prints one summary
line and exits 0 when the record matches, 1 when it does not.
"""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RECORD_NAME = "legacy_corpus_record.toml"
RECORD_RULE = "corpus-record:1"
SET_DIGEST_PREFIX = "sha256-manifest:1:"
NOT_MEASURED = "NOT MEASURED: corpus record check failed"

# Hand-written fields of a section; each must be present and non-empty.
SECTION_FIELDS = (
    "name",
    "directory",
    "upstream",
    "upstream_branch",
    "revision",
    "upstream_path",
    "candidate_rule",
    "extent_rule",
    "verification_date",
    "verification_command",
    "verification_result",
)
GROUPS = ("retained", "ceded")
RUNS = ("UNESTABLISHED", "DOES-NOT-RUN")
SITE_RUNS = "DOES-NOT-RUN-PAST"


def tracked_files(root: Path) -> list[str]:
    """Return the repository-relative paths git tracks under `root`.

    Every check of this package obtains the tracked set here and nowhere else, so
    an untracked or ignored file in the corpus directory (a stray script, a
    `__pycache__/` that an import left behind) never enters a count or a digest.
    """
    out = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True)
    return [p for p in out.stdout.decode("utf-8").split("\0") if p]


def _is_candidate(rel: str) -> bool:
    """A top-level `.py` file of the section directory is a candidate for unit extraction."""
    return "/" not in rel and rel.endswith(".py")


def _stop_marker_lines(tree: ast.AST) -> list[int]:
    """Lines of `sys.exit(...)` calls not lexically inside an `if`, a `try` or an `except`.

    The test is lexical only. A returned call at module level and outside any loop stops the
    script where it stands, so nothing after it at that level runs at the pinned revision. A
    returned call inside a function or loop body stops the script only if that body runs, and
    is returned all the same: a `def` whose body is a bare `sys.exit(1)` yields its line even if
    the function is never called. `_section_findings` still requires every returned site's
    record row to read `DOES-NOT-RUN-PAST`. Not established here: whether each in-body site is
    reached.
    """
    hits: list[int] = []

    def walk(node: ast.AST, guarded: bool) -> None:
        is_guard = isinstance(node, ast.If | ast.Try | ast.ExceptHandler) or (
            hasattr(ast, "TryStar") and isinstance(node, ast.TryStar)
        )
        if (
            not guarded
            and isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "exit"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "sys"
        ):
            hits.append(node.lineno)
        for child in ast.iter_child_nodes(node):
            walk(child, guarded or is_guard)

    walk(tree, False)
    return sorted(hits)


def _digest_finding(rel: str, recorded: object, actual: str) -> str | None:
    """Name the file and the field when a file's recorded digest is not its digest."""
    if recorded != actual:
        return f"{rel}: sha256 recorded {recorded!r}, recomputed {actual!r}"
    return None


def _presence_findings(section: dict) -> list[str]:
    """Name each hand-written section field that is absent or empty."""
    name = section.get("name", "?")
    findings = []
    for field in SECTION_FIELDS:
        value = section.get(field)
        if not isinstance(value, str) or not value.strip():
            findings.append(f"section {name}: hand-written field {field} is absent or empty")
    date = section.get("verification_date")
    if isinstance(date, str) and date.strip() and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        findings.append(f"section {name}: verification_date {date!r} is not YYYY-MM-DD")
    return findings


def _section_findings(root: Path, section: dict, tracked: list[str]) -> tuple[list[str], dict]:
    """Recompute one section's derived fields and compare them with the record.

    Returns the findings and the recomputed figures the summary line reports.
    """
    findings = _presence_findings(section)
    directory = section.get("directory")
    if not isinstance(directory, str) or not directory:
        return findings, {"files": 0, "unparseable": 0, "sites": 0}
    prefix = directory.rstrip("/") + "/"
    rels = sorted(p[len(prefix) :] for p in tracked if p.startswith(prefix))
    entries = {e.get("path"): e for e in section.get("file", [])}
    for missing in sorted(set(rels) - set(entries)):
        findings.append(f"{missing}: tracked in {directory} and absent from the record")
    for extra in sorted(set(entries) - set(rels)):
        findings.append(f"{extra}: in the record and not tracked in {directory}")

    manifest = []
    unparseable = 0
    sites: set[tuple[str, int]] = set()
    for rel in rels:
        try:
            data = (root / prefix / rel).read_bytes()
        except OSError as exc:
            # Reported, never raised: the record is the chain head, and an exception here
            # would make every dependent check error instead of skipping as not measured.
            findings.append(f"{rel}: tracked and not readable from the working tree ({type(exc).__name__})")
            continue
        digest = hashlib.sha256(data).hexdigest()
        manifest.append(f"{digest}  {rel}\n")
        entry = entries.get(rel)
        candidate = _is_candidate(rel)
        if candidate:
            try:
                sites.update((rel, line) for line in _stop_marker_lines(ast.parse(data.decode("utf-8"))))
            except (SyntaxError, UnicodeDecodeError):
                unparseable += 1
        if entry is None:
            continue
        finding = _digest_finding(rel, entry.get("sha256"), digest)
        if finding:
            findings.append(finding)
        role = "candidate" if candidate else "NOT A UNIT SOURCE"
        if entry.get("role") != role:
            findings.append(f"{rel}: role recorded {entry.get('role')!r}, recomputed {role!r}")
        try:
            lines = len(data.decode("utf-8").splitlines())
        except UnicodeDecodeError:
            lines = None
        if entry.get("lines") != lines:
            findings.append(f"{rel}: lines recorded {entry.get('lines')!r}, recomputed {lines!r}")
        if not isinstance(entry.get("upstream_sourced"), bool):
            findings.append(f"{rel}: upstream_sourced is absent or not true/false")
        if candidate:
            if entry.get("group") not in GROUPS:
                findings.append(f"{rel}: group {entry.get('group')!r} is not one of {GROUPS}")
            if entry.get("runs") not in RUNS:
                findings.append(f"{rel}: runs {entry.get('runs')!r} is not one of {RUNS}")
            if not isinstance(entry.get("runs_ground"), str) or not entry["runs_ground"].strip():
                findings.append(f"{rel}: runs_ground is absent or empty")

    set_digest = SET_DIGEST_PREFIX + hashlib.sha256("".join(manifest).encode("utf-8")).hexdigest()
    if section.get("set_digest") != set_digest:
        findings.append(
            f"section {section.get('name', '?')}: set_digest recorded {section.get('set_digest')!r}, recomputed {set_digest!r}"
        )
    if section.get("unparseable_candidate_files") != unparseable:
        findings.append(
            f"section {section.get('name', '?')}: unparseable_candidate_files recorded "
            f"{section.get('unparseable_candidate_files')!r}, recomputed {unparseable}"
        )
    recorded_sites = set()
    for site in section.get("site", []):
        recorded_sites.add((site.get("path"), site.get("line")))
        if (
            site.get("runs") != SITE_RUNS
            or not isinstance(site.get("runs_ground"), str)
            or not site["runs_ground"].strip()
        ):
            findings.append(f"{site.get('path')}:{site.get('line')}: site runs must be {SITE_RUNS} with a runs_ground")
    for path, line in sorted(sites - recorded_sites):
        findings.append(f"{path}:{line}: stop-marker site not in the record")
    for path, line in sorted(recorded_sites - sites, key=str):
        findings.append(f"{path}:{line}: recorded stop-marker site not found")
    return findings, {"files": len(rels), "unparseable": unparseable, "sites": len(sites)}


def _check(root: Path) -> tuple[list[str], list[dict]]:
    """Read the record under `root` and return every finding plus per-section figures."""
    path = root / RECORD_NAME
    if not path.is_file():
        return [f"{RECORD_NAME}: absent"], []
    try:
        record = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        return [f"{RECORD_NAME}: not valid TOML ({exc})"], []
    findings = []
    if record.get("rule") != RECORD_RULE:
        findings.append(f"{RECORD_NAME}: rule {record.get('rule')!r} is not {RECORD_RULE!r}")
    sections = record.get("section", [])
    if not sections:
        findings.append(f"{RECORD_NAME}: no [[section]]")
    tracked = tracked_files(root)
    figures = []
    for section in sections:
        section_findings, section_figures = _section_findings(root, section, tracked)
        findings.extend(section_findings)
        figures.append(section_figures)
    return findings, figures


def record_findings(root: Path = REPO_ROOT) -> list[str]:
    """Return every disagreement between the corpus record and the tracked files; empty on pass."""
    return _check(root)[0]


def require_record(root: Path = REPO_ROOT) -> None:
    """Skip the calling test as not measured when the corpus record check fails.

    A check below the record in the chain calls this first. It skips rather than
    fails, because the cause is the record and is already reported, by name, by the
    record's own test.
    """
    findings = record_findings(root)
    if findings:
        import pytest

        pytest.skip(f"{NOT_MEASURED} ({len(findings)} finding(s))")


def main(root: Path = REPO_ROOT) -> int:
    """Print one summary line; return 0 when the record matches, 1 otherwise."""
    findings, figures = _check(root)
    if findings:
        print(f"corpus record: {len(findings)} finding(s)")
        for finding in findings:
            print(f"  {finding}", file=sys.stderr)
        return 1
    print(
        f"corpus record: {len(figures)} section, {sum(f['files'] for f in figures)} files, "
        f"{sum(f['unparseable'] for f in figures)} unparseable candidate files, "
        f"{sum(f['sites'] for f in figures)} stop-marker sites; derived fields match"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
