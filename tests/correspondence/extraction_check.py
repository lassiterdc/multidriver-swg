"""Check that every stored copy of a cited legacy range still equals its range, and that the record still describes it.

A stored copy of legacy code is trusted by the tests that run lifts on it only while three things
agree: the range cut from the corpus now (C), the stored text on disk now (S), and the digest
recorded for it when the generator wrote it (R). This module compares them for the row of
each range in `extraction_record.toml`, and reports exactly one cause for each disagreement, so that a person
reading one failure knows which of the three moved and which command restores agreement: an edited
copy is restored by re-running the generator, a changed cutting function is not.

The record is read by the key names the generator writes. Before the three are compared, each row's
two labels are compared with the current cutting rules, because a label recorded under an older
rule version cannot be checked against a cut made under the current one. The record holds one row per range, as the generator writes it, and a range with a second row
is reported as an edit and is not compared row by row. Completeness is checked
in both directions: every range a scope-in key cites has a row, every row is such a range, nothing
else lies under `extractions/` but its `LICENSE`, and the `LICENSE` is there.

The check sits below the corpus record and the extent check in the chain: `require_extractions`
skips its callers as not measured when either fails, and skips them again when this check has a
finding of its own, so one broken input shows as one failure and not as one per stored text.
"""

from __future__ import annotations

import re
import sys
import tomllib
from collections import Counter
from pathlib import Path

from tests.correspondence import corpus_record, extent, extraction, generate_extractions

REPO_ROOT = extraction.REPO_ROOT
NOT_MEASURED = "NOT MEASURED: extraction check failed"
LABEL_RE = re.compile(r"^(?P<rule>[^:\s]+):(?P<version>\d+):(?P<digest>[0-9a-f]{64})\Z")
REGENERATE = f"re-running `{generate_extractions.COMMAND}` restores it"


def _parse(label: object) -> tuple[str, int, str] | None:
    """Split a label into (rule, version, digest), or None when it is not of the labelled form."""
    match = LABEL_RE.match(label) if isinstance(label, str) else None
    return (match["rule"], int(match["version"]), match["digest"]) if match else None


def _row_name(row: dict) -> str:
    return f"{row.get('file')}:{row.get('first')}-{row.get('last')}"


def _rule_finding(row: dict, field: str, rule: str) -> str | None:
    """Return a finding when the row's `field` label is not of the current `rule` at its current version."""
    label = row.get(field)
    parsed = _parse(label)
    current = f"{rule}:{extraction.RULE_VERSIONS[rule]}"
    if parsed is None or f"{parsed[0]}:{parsed[1]}" != current:
        return (
            f"record row {_row_name(row)}: label {label!r} is not under the current rule {current}; "
            "the generator must be re-run under the new rule"
        )
    return None


def _outcome(row: dict, cut_now: bytes | None, stored_now: bytes | None) -> str | None:
    """Return the one finding for a row whose C, S and R do not all agree, or None when they do.

    C is the cut now, S the stored text, and R the recorded pair of digests. R is coherent only
    when its two digests are equal, because the generator writes them equal; a row whose two
    differ disagrees with C and with S alike. The five outcomes are: all agree; S alone differs
    (C and R agree); C alone differs (S and R agree); R alone differs (C and S agree); all three
    differ.
    """
    name, path = _row_name(row), row.get("path")
    cut_digest = _parse(row.get("cut"))[2]
    stored_digest = _parse(row.get("stored"))[2]
    coherent = cut_digest == stored_digest
    c = extraction.label(extraction.CUT_RULE, cut_now).rsplit(":", 1)[1]
    s = extraction.label(extraction.STORED_RULE, stored_now).rsplit(":", 1)[1]
    c_matches_r, s_matches_r, c_matches_s = coherent and c == cut_digest, coherent and s == stored_digest, c == s
    if c_matches_r and s_matches_r:
        return None
    if c_matches_s:
        return f"record row {name}: the range cut now and the stored text {path} agree and the record differs; the record was edited, and {REGENERATE}"
    if c_matches_r:
        return f"stored text {path}: it differs from the range cut now and from the recorded digest; the copy was edited, and {REGENERATE}"
    if s_matches_r:
        return f"range {name}: the cut now differs from stored text {path} and from the record, which agree; the cutting function's output changed without a rule-version change"
    return f"stored text {path} and record row {name}: the range cut now, the stored text and the record all differ"


def _row_findings(root: Path, row: dict) -> list[str]:
    """Return the findings of one record row: its labels' rules, then its stored text, then the three-way comparison."""
    findings = [
        f
        for f in (_rule_finding(row, "cut", extraction.CUT_RULE), _rule_finding(row, "stored", extraction.STORED_RULE))
        if f
    ]
    if findings:
        return findings
    path = root / str(row.get("path"))
    if not path.is_file():
        return [f"stored text {row.get('path')}: missing for record row {_row_name(row)}; {REGENERATE}"]
    try:
        cut_now = extraction.cut(root, row["section"], row["file"], row["first"], row["last"])
    except (extraction.ExtractionError, KeyError) as exc:
        return [f"range {_row_name(row)}: the cut was refused ({exc})"]
    outcome = _outcome(row, cut_now, path.read_bytes())
    return [outcome] if outcome else []


def _read_record(root: Path) -> tuple[list[dict], list[str]]:
    """Return the record's rows and any finding about reading it."""
    path = root / generate_extractions.RECORD_PATH
    if not path.is_file():
        return [], [f"{generate_extractions.RECORD_PATH}: absent; {REGENERATE}"]
    try:
        record = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        return [], [f"{generate_extractions.RECORD_PATH}: not valid TOML ({exc})"]
    findings = []
    if record.get("rule") != generate_extractions.RECORD_RULE:
        findings.append(
            f"{generate_extractions.RECORD_PATH}: rule {record.get('rule')!r} is not {generate_extractions.RECORD_RULE!r}"
        )
    return record.get("extraction", []), findings


def extraction_findings(root: Path = REPO_ROOT, unit: str | None = None) -> list[str]:
    """Return every finding of the extraction check under `root`; empty when it passes, and it never skips.

    With `unit` (a scope-in key), only that key's ranges and the record rows of those ranges are
    checked; the directory-wide arms (a row that is no key's range, a stray file, the licence) are
    left to a whole-tree run, since one unit cannot see the others' files.
    """
    rows, findings = _read_record(root)
    try:
        keys, cited = generate_extractions.population(root)
        section = extent._section(root)["name"]
    except extraction.ExtractionError as exc:
        return [*findings, f"scope-in population: {exc}"]
    wanted = {(section, f, a, b) for f, a, b in cited}
    if unit is not None:
        if unit not in keys:
            return [*findings, f"unit {unit!r}: not a scope-in key of the ledger"]
        wanted = {(section, f, a, b) for f, a, b in generate_extractions._ranges(unit)}
    by_range = {(r.get("section"), r.get("file"), r.get("first"), r.get("last")): r for r in rows}
    # Deliberately NOT a comparison of every row as read: the generator writes one row per range, so a
    # second row for one range is itself the edit, and it is reported as such. Collapsing the rows into
    # the mapping above alone would compare only the last row of a range and never read an edited
    # earlier one.
    copies = Counter((r.get("section"), r.get("file"), r.get("first"), r.get("last")) for r in rows)
    for key, count in sorted(copies.items(), key=lambda kv: str(kv[0])):
        if count > 1 and (unit is None or key in wanted):
            findings.append(
                f"range {key[1]}:{key[2]}-{key[3]}: {count} record rows for one range, and the generator writes one; the record was edited, and {REGENERATE}"
            )
    for missing in sorted(wanted - by_range.keys()):
        findings.append(
            f"range {missing[1]}:{missing[2]}-{missing[3]}: a scope-in key's range with no record row; {REGENERATE}"
        )
    for key, row in sorted(by_range.items(), key=lambda kv: str(kv[0])):
        if key in wanted:
            findings.extend(_row_findings(root, row))
        elif unit is None:
            findings.append(f"record row {_row_name(row)}: not the range of any scope-in key; {REGENERATE}")
    if unit is None:
        findings.extend(_directory_findings(root, rows))
    return findings


def _directory_findings(root: Path, rows: list[dict]) -> list[str]:
    """Name each file under `extractions/` that is neither a recorded stored text nor `LICENSE`, and a missing `LICENSE`."""
    stored_root = root / extraction.STORED_DIR
    recorded = {row.get("path") for row in rows}
    findings = []
    if stored_root.is_dir():
        for file in sorted(p for p in stored_root.rglob("*") if p.is_file()):
            relative = file.relative_to(root).as_posix()
            if relative not in recorded and relative != f"{extraction.STORED_DIR}/LICENSE":
                findings.append(
                    f"{relative}: under {extraction.STORED_DIR} and neither a recorded stored text nor LICENSE"
                )
    if not (stored_root / "LICENSE").is_file():
        findings.append(f"{extraction.STORED_DIR}/LICENSE: absent; the MIT notice must sit beside the stored texts")
    return findings


def require_chain(root: Path = REPO_ROOT) -> None:
    """Skip the calling test as not measured when the corpus record or the key resolution check fails.

    The corpus record is called first and then the extent check, so each failure shows its own
    not-measured sentence. Once the record has passed the corpus cannot have moved, which is why
    this check never reports the corpus as the cause of a stored text's disagreement.
    """
    corpus_record.require_record(root)
    extent.require_extent(root)


def require_extractions(root: Path = REPO_ROOT) -> None:
    """Skip the calling test as not measured when the chain above fails or this check has any finding."""
    require_chain(root)
    findings = extraction_findings(root)
    if findings:
        import pytest

        pytest.skip(f"{NOT_MEASURED} ({len(findings)} finding(s))")


def main(root: Path = REPO_ROOT) -> int:
    """Print one summary line and each finding; return 0 when the check passes, 1 otherwise."""
    findings = extraction_findings(root)
    rows = len(_read_record(root)[0])
    print(f"extraction check: {rows} record row(s), {len(findings)} finding(s)")
    for finding in findings:
        print(f"  {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
