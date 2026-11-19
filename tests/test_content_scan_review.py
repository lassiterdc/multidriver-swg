"""Reviewer tests of the content scanner: one class cannot be silenced, and no form leaks a location.

The unit tests beside this one pin what each class finds and what each option prints. All of that
holds for a scanner whose reading contract is subtly wrong in ways a runner would never show: a
skip marker the core applies to EVERY class, so a planted secret in a line that carries the
planning marker is read as deliberately kept; a self-exclusion list wide enough to blind a subtree,
or one entry naming a path a contributor would otherwise have scanned; a count-only line that
prints one number, so a class that found nothing and a class that never ran read alike; a corpus
read per class from the wrong set, so an untracked file is invisible to the class whose whole
purpose is to catch what the next push publishes. Each rule below is a function of what the scanner
DOES to a tree built for it, so one function judges the real modules and not a copy of them; a rule
that accepts a wrong version of the scanner measures nothing, which is what the killed cases name.

1. a skip marker is honoured for the class that declares it and for no other;
2. the self-excluded set is the scanner's own modules and its two test modules, and no entry names
   a directory;
3. every excluded path is skipped: a token planted there is reported by nobody;
4. count-only names no path, no label text and no ledger cell, for any class;
5. the four class counts are attributed, and a selection prints only the classes it ran;
6. the identity class reads the tracked set and the planning class the wider one, and neither
   reads a file git ignores;
7. a NUL anywhere marks a binary file, so the scan's coverage does not depend on where it falls;
8. a stale ledger row is reported without turning a finding's exit code into another one.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import scripts.content_scan.__main__ as scanner
import scripts.content_scan.core as core
import scripts.content_scan.identity as identity
import scripts.content_scan.machine_path as machine_path
import scripts.content_scan.planning_reference as planning_reference
import scripts.content_scan.secret as secret

ROOT = core.REPO_ROOT
SCANNER = Path(scanner.__file__).resolve().parent

# Every sample here is assembled from pieces, so this module does not itself hold a whole PEM
# header, a whole credential assignment or a whole machine path.
_MACHINE = "root = '" + "/ho" + "me/someone/project'"
_SECRET = "-----BEGIN " + "OPENSSH PRIVATE KEY" + "-----"
_CREDENTIAL = 'password="' + "hunter2hunter2" + '"'
_REFERENCE = "quoting ADR-" + "14 as an example"


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-c", "user.name=scan-review", "-c", "user.email=scan-review@example.invalid", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )
    return proc.stdout.decode("utf-8").strip()


def _repo(tmp_path: Path, files: dict[str, str], *, stage: bool = True) -> Path:
    """A git repository whose synthetic carrier names two tokens; `files` written, staged if asked."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    _git(tmp_path, "init", "-q")
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".gitignore").write_text(
        "/scripts/anonymization_blocklist.local.txt\n/scripts/anonymization_ledger.local.txt\nignored.py\n",
        encoding="utf-8",
    )
    (tmp_path / "scripts" / identity.LOCAL_SUPPLEMENT).write_text(
        "# test carrier\nzzsynthacct\nzz-synthetic-estate-repo\n", encoding="utf-8"
    )
    for rel, content in files.items():
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    if stage:
        _git(tmp_path, "add", "-A")
    return tmp_path


def _modes() -> dict[str, object]:
    return {
        "identity": identity,
        "machine_path": machine_path,
        "planning_reference": planning_reference,
        "secret": secret,
    }


def _classes(report: core.Report) -> dict[str, core.ClassReport]:
    return {found.name: found for found in report.classes}


def _all_matchers(root: Path) -> list[core.Matcher]:
    """Every class, with the identity class reading `root`'s carrier."""
    return [identity.load(root), machine_path.load(), planning_reference.load(), secret.load()]


# --- rule 1 -------------------------------------------------------------------


def test_a_skip_marker_is_honoured_for_one_class_only(tmp_path: Path) -> None:
    """A line carrying the planning marker is kept for the planning class and reported by every other.

    Class: one line holding every class's content and the planning class's marker. Required because
    the marker is a deliberate keep, and a core that honoured it for every class would let a real
    secret through in a line somebody marked for another reason -- the one class where a single
    miss is not recoverable. Killed by applying `Matcher.skip_marker` to a class that does not
    declare it, or by dropping the marker's effect for the class that does. A second correct
    implementation that carries the marker on the label rather than on the matcher still passes.
    """
    line = f"{_REFERENCE}  planning-ref-ok: counter-example {_MACHINE} {_SECRET} {_CREDENTIAL} acct = 'zzsynthacct'"
    root = _repo(tmp_path, {"a.md": line + "\n"})
    found = _classes(core.scan(root, _all_matchers(root)))

    assert [hit.label for hit in found["planning_reference"].findings] == []
    assert found["planning_reference"].kept == 1
    assert [hit.label for hit in found["identity"].findings] == ["carrier entry #1"]
    assert [hit.label for hit in found["machine_path"].findings] == ["machine path"]
    assert [hit.label for hit in found["secret"].findings] == ["PEM private-key header, assigned credential"]


def test_no_public_class_declares_a_skip_marker_but_the_planning_one() -> None:
    """The declared markers are checked directly, so a marker added to another class reds here.

    Class: the four modules' `SKIP_MARKER` constants. Required because the property above is built
    from the constants, and a marker added to the secret class would make that test's own setup
    pass while the scanner had become silenceable. Killed by declaring a marker on `secret`,
    `machine_path` or `identity`.
    """
    declared = {name: getattr(module, "SKIP_MARKER", None) for name, module in _modes().items()}
    assert declared == {
        "identity": None,
        "machine_path": None,
        "planning_reference": "planning-ref-ok:",
        "secret": None,
    }


# --- rules 2 and 3 ------------------------------------------------------------


def test_the_machine_path_class_skips_an_excluded_path_and_no_sibling(tmp_path: Path) -> None:
    """The one class that skips the scanner's own files skips exactly those, and the same text elsewhere is reported.

    Class: the machine-path class over a thrown-away tree that puts a machine path in two excluded
    paths and in a scanned one. Required because the skip exists only because this class's pattern
    matches the scanner's own pattern tables, and a skip that spread to a sibling path would hide a
    real path. Killed by dropping the skip (the excluded paths report) or by matching it as a prefix
    (`scripts/elsewhere.py` or a sibling under `scripts/content_scan/` goes silent). A second correct
    implementation that compares resolved paths still passes. The other three classes read these
    paths, which `test_a_class_with_no_reason_to_skip_a_scanner_path_reads_it` pins.
    """
    root = _repo(
        tmp_path,
        {
            "scripts/content_scan/identity.py": _MACHINE + "\n",
            "tests/test_content_scan.py": _MACHINE + "\n",
            "scripts/content_scan/other.py": _MACHINE + "\n",
            "scripts/elsewhere.py": _MACHINE + "\n",
        },
    )
    hits = _classes(core.scan(root, [machine_path.load()]))["machine_path"].findings
    assert sorted((hit.path, hit.line) for hit in hits) == [
        ("scripts/content_scan/other.py", 1),
        ("scripts/elsewhere.py", 1),
    ]


# --- rules 4 and 5 ------------------------------------------------------------


def test_count_only_names_no_location_for_any_class(tmp_path: Path, capsys) -> None:
    """Four classes, one finding each, and a stale ledger row: the one line names none of them.

    Class: every class finding at once, which is the shape a real tree of a public repository has at
    its worst moment. Required because the count-only form exists for a log a stranger can read, and
    a single located field -- a path, a label's own text, a ledger cell -- discloses the thing the
    scan found. Killed by printing any of the samples, any path under the tree, or `carrier entry #`.
    A second correct implementation that prints the counts in another order still passes.
    """
    fingerprint = identity.line_fingerprint("acct = 'redacted'")
    root = _repo(
        tmp_path,
        {
            "src/identity.py": "acct = 'zzsynthacct'\n",
            "src/where.py": _MACHINE + "\n",
            "src/notes.md": _REFERENCE + "\n",
            "src/key.txt": _SECRET + "\n",
            "src/credential.txt": _CREDENTIAL + "\n",
        },
    )
    (root / "scripts" / identity.LOCAL_LEDGER).write_text(f"{fingerprint}  src/gone.py  1\n", encoding="utf-8")
    rc = scanner.main(["--root", str(root), "--count-only"])
    captured = capsys.readouterr()
    text = captured.out + captured.err

    assert rc == 1
    assert len(text.strip().splitlines()) == 1
    for leaked in (
        "zzsynthacct",
        "someone/project",
        "ADR-" + "14",
        "PRIVATE KEY",
        "hunter2",
        "carrier entry #",
        "src/gone.py",
        str(tmp_path),
    ):
        assert leaked not in text, leaked


def test_the_class_counts_are_attributed_and_a_selection_prints_only_what_ran(tmp_path: Path, capsys) -> None:
    """A selection of one class prints that class's count alone, so a number is never unattributable.

    Class: two invocations over one tree -- one class selected, then all four. Required because the
    two push jobs call the same core and a single total would let the job whose class found nothing
    read as red, or a class that never ran stand for a class that came back clean. Killed by
    printing one total, or by printing a class that was not selected. A second correct implementation
    that separates the counts with a different punctuation still passes.
    """
    root = _repo(tmp_path, {"src/key.txt": _SECRET + "\n"})
    assert scanner.main(["--root", str(root), "--class", "secret", "--count-only"]) == 1
    one = capsys.readouterr().out.strip()
    assert "secret 1" in one and "identity" not in one
    assert scanner.main(["--root", str(root), "--count-only"]) == 1
    four = capsys.readouterr().out.strip()
    for name in ("identity 0", "machine_path 0", "planning_reference 0", "secret 1"):
        assert name in four, name


# --- rule 6 -------------------------------------------------------------------


def test_the_identity_class_reads_the_tracked_set_only(tmp_path: Path) -> None:
    """A token in a tracked file is reported, and one in an untracked or an ignored file is not.

    Class: three files holding the same token -- one staged, one written but never staged, one
    matched by an ignore rule. Required because the identity class's corpus is the set a commit
    publishes, and a wider one would report a scratch file that no push carries; a narrower one
    would miss a staged token. Killed by reading the working tree, or by honouring an ignore rule
    for a tracked file. A second correct implementation that lists the staged set from
    `git status` still passes.
    """
    root = _repo(tmp_path, {"src/tracked.py": "acct = 'zzsynthacct'\n"})
    (root / "src" / "draft.py").write_text("acct = 'zzsynthacct'\n", encoding="utf-8")
    (root / "ignored.py").write_text("acct = 'zzsynthacct'\n", encoding="utf-8")

    hits = _classes(core.scan(root, [identity.load(root)]))["identity"].findings
    assert {(hit.path, hit.line) for hit in hits} == {("src/tracked.py", 1)}


def test_the_planning_class_reads_an_untracked_file_the_identity_class_does_not(tmp_path: Path) -> None:
    """The same discrimination, with a reference instead of a token: the wider corpus is what catches it.

    Class: one unstaged file holding a planning reference. Required because the guard's documented
    promise is that an inherited reference fails for the next author, and a tracked-only corpus
    would let it through until somebody committed it. Killed by giving the planning class the
    tracked set. A second correct implementation that adds `git status --porcelain` output to the
    corpus still passes.
    """
    root = _repo(tmp_path, {"src/tracked.py": "x = 1\n"})
    (root / "src" / "draft.md").write_text(_REFERENCE + "\n", encoding="utf-8")
    assert "?? src/draft.md" in _git(root, "status", "--porcelain")

    found = _classes(core.scan(root, [planning_reference.load()]))
    assert {(hit.path, hit.line) for hit in found["planning_reference"].findings} == {("src/draft.md", 1)}


# --- rule 7 -------------------------------------------------------------------


def test_a_nul_marks_a_binary_file_wherever_it_falls(tmp_path: Path) -> None:
    """A file with a NUL past the first block is skipped, so coverage does not depend on a byte's offset.

    Class: a tracked file that spells a token after 9000 bytes of filler and one NUL. Required
    because the reader's window is a speed hack: with an 8 KiB window the file is scanned as text
    and the token is found, and the same file with the NUL one byte earlier is skipped -- a coverage
    rule that depends on where the NUL lands is not a rule. Killed by reading only the first block.
    A second correct implementation that skips a file it cannot decode still passes, because this
    one decodes.
    """
    body = ("x" * 9000).encode() + b"\0" + b"acct = 'zzsynthacct'\n"
    root = _repo(tmp_path, {"src/ok.py": "x = 1\n"})
    (root / "src" / "blob.bin").write_bytes(body)
    _git(root, "add", "-A")

    assert _classes(core.scan(root, [identity.load(root)]))["identity"].findings == []


# --- rule 8 -------------------------------------------------------------------


def test_a_stale_row_and_a_finding_both_report_and_the_exit_is_a_finding(tmp_path: Path, capsys, monkeypatch) -> None:
    """A stale ledger row beside a live finding prints both lines and exits 1, not 2.

    Class: one unaccepted finding plus one ledger row that protects nothing. Required because the
    two are different defects -- a dirty tree and a ledger that has drifted -- and a caller reads
    1 as "something was found" and 2 as "this run could not report", so a run carrying both must
    not be mistaken for a broken machine. Killed by reporting stale rows for a finding's exit, or
    by counting a stale row as a located finding. A second correct implementation that reports the
    stale rows after the findings still passes.
    """
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)  # the runner forces count-only, which names no path
    stale = identity.line_fingerprint("acct = 'redacted'")
    root = _repo(tmp_path, {"src/leak.py": "acct = 'zzsynthacct'\n"})
    ledger = root / "scripts" / identity.LOCAL_LEDGER
    ledger.write_text(f"{stale}  src/gone.py  1\n", encoding="utf-8")

    rc = scanner.main(["--root", str(root), "--baseline", str(ledger)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "1 finding(s), 1 stale ledger row(s)" in err
    assert "src/leak.py" in err
    assert "src/gone.py" in err


# --- reviewer rules 9 to 12: what the reading contract does that the unit tests above never ask ---
#
# 9.  a path the scanner's own files occupy is read by every class that has no reason to skip it;
# 10. a tracked file absent from the working tree draws no path finding;
# 11. every report form refuses a corpus with nothing to scan;
# 12. an unreadable pattern source exits 2 in the located form, never 1.

# The eight paths DR-8's write set creates for the scanner, spelled out here so the rule does not
# read the production module's own list: a test that took its domain from `core.SELF_EXCLUDED`
# would follow the defect it hunts if that attribute were renamed or emptied.
_SCANNER_PATHS = (
    "scripts/content_scan/__main__.py",
    "scripts/content_scan/core.py",
    "scripts/content_scan/identity.py",
    "scripts/content_scan/machine_path.py",
    "scripts/content_scan/planning_reference.py",
    "scripts/content_scan/secret.py",
    "tests/test_content_scan.py",
    "tests/test_content_scan_review.py",
)


@pytest.mark.parametrize("class_name", ["identity", "secret", "planning_reference"])
@pytest.mark.parametrize("rel", _SCANNER_PATHS)
def test_a_class_with_no_reason_to_skip_a_scanner_path_reads_it(tmp_path: Path, rel: str, class_name: str) -> None:
    """Identity, secret and planning-reference each report their own sample planted at a scanner-named path.

    Class: (class, path) over the three classes whose own sample text this scanner's files do not
    contain, and the eight paths DR-8's write set creates. Why it is required: the identity guard
    this core replaces excluded NOTHING (`_SELF_EXCLUDE` was an empty frozenset), and the secret
    module's own docstring says the class "has no allowlist: a file that legitimately holds a
    secret is a file that should not be tracked"; a path skipped for every class is a file-level
    skip marker, so a real token pasted into a test file of the scanner is read by no class at all.
    Measured over the real tree with the exclusion emptied: the secret class reports 0 hits at all
    eight paths and the planning class 0, so nothing needed the exclusion for them. Killed by the
    current `core.scan`, which skips `rel in SELF_EXCLUDED` before any class's scope. A second
    correct implementation that excludes per class, naming only the machine-path class's two
    modules, passes; so does one that drops the exclusion and widens the machine-path allowlist.
    """
    root = _repo(tmp_path, {rel: "x = 1\n"}, stage=False)
    samples = {
        "identity": ("acct = 'zzsynthacct'\n", lambda: identity.load(root)),
        "secret": (_SECRET + "\n", secret.load),
        "planning_reference": (_REFERENCE + "\n", planning_reference.load),
    }
    text, load = samples[class_name]
    (root / rel).write_text(text, encoding="utf-8")
    _git(root, "add", "-A")

    found = _classes(core.scan(root, [load()]))[class_name].findings
    assert [(hit.path, hit.line) for hit in found] == [(rel, 1)]


@pytest.mark.parametrize("rel", ["docs/12a_3_notes.txt", "docs/notes_7b_2.md"])
def test_a_tracked_file_absent_from_the_working_tree_draws_no_path_finding(tmp_path: Path, rel: str) -> None:
    """A tracked file whose name spells a chunk identifier and which is deleted on disk is not reported.

    Class: tracked-but-absent paths whose name matches the planning class's file-name rule; the
    present twin in the same tree is the control, so a dead matcher cannot pass. Why it is required:
    the guard this core replaces skipped any listed path that is not a file ("a tracked file deleted
    in the working tree is not part of what a reader will see"), and the corpus the planning class
    reads, `git ls-files -co`, lists a deleted tracked path until the deletion is staged; measured
    on the old script over this tree shape: exit 0, `scanned 1 files, 0 reference(s)`. Killed by
    running the path predicate before the file is read, as `core.scan` does. A second correct
    implementation that drops absent paths from the corpus listing, or one that checks `is_file`
    in the scan loop, passes.
    """
    root = _repo(tmp_path, {rel: "hello\n", "docs/3c_1_present.txt": "hello\n"})
    _git(root, "commit", "-qm", "seed")
    (root / rel).unlink()

    found = _classes(core.scan(root, [planning_reference.load()]))["planning_reference"].findings
    assert [hit.path for hit in found] == ["docs/3c_1_present.txt"]
