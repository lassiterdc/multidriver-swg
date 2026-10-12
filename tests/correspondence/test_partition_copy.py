"""Tests of the partition copy: the generator's refusals and withholding, the committed copy's internal consistency, and the guard-case/copy equality.

Generator tests run on a synthetic architecture document committed in a `tmp_path` git
repository and a synthetic carrier in `tmp_path`; they never read the real architecture or
the real carrier. Commits pass their identity on the command line, so they do not depend on
the machine's git configuration.
"""

import copy as copying
import json
import re
import subprocess
from pathlib import Path

import pytest

from _refactor_tools import generate_partition_copy as generator
from tests.correspondence import partition_copy

pytestmark = pytest.mark.state_check

TOKEN = "zebrafjord"  # a synthetic carrier token; the real carrier is never read here
ARCHITECTURE = """# Synthetic architecture

| System | system_status | description | assignment_criteria |
|--------|---------------|-------------|---------------------|
| `alpha-sys` | pending | The alpha system. | Belongs here if it computes alpha. Does NOT belong here if it computes beta (`beta-sys`) |
| `beta-sys` | pending | The beta system. | Belongs here if it computes beta; and does NOT belong here if it mentions {token} |

Text after the table.
"""


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _commit(root: Path, message: str) -> None:
    """Commit everything staged, with an identity given on the command line (no machine configuration)."""
    _git(root, "add", "-A")
    _git(root, "-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-q", "-m", message)


def _source(tmp_path: Path, text: str | None = None) -> Path:
    """A git repository holding a committed synthetic architecture; return the document's path."""
    repo = tmp_path / "source"
    repo.mkdir()
    _git(repo, "init", "-q")
    doc = repo / "architecture.md"
    doc.write_text(ARCHITECTURE.format(token=TOKEN) if text is None else text, encoding="utf-8")
    _commit(repo, "architecture")
    return doc


def _anchor(tmp_path: Path, with_supplement: bool = True) -> Path:
    """A synthetic carrier: the tracked prose-only blocklist and, optionally, the private supplement."""
    folder = tmp_path / "carrier"
    folder.mkdir()
    anchor = folder / "anonymization_blocklist.txt"
    anchor.write_text("# prose only\n", encoding="utf-8")
    if with_supplement:
        (folder / "anonymization_blocklist.local.txt").write_text(f"{TOKEN}\n", encoding="utf-8")
    return anchor


def _run(doc: Path, anchor: Path, output: Path, *extra: str) -> int:
    return generator.main(
        ["--architecture", str(doc), "--carrier-anchor", str(anchor), "--output", str(output), *extra]
    )


def _write_copy(root: Path, data: dict) -> Path:
    path = root / partition_copy.COPY_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return root


def _quoted(findings: list[str]) -> set[str]:
    """The backtick-quoted names in findings, compared as whole tokens, never as substrings."""
    return {name for finding in findings for name in re.findall(r"`([^`]+)`", finding)}


# --- the generator -------------------------------------------------------------------------


def test_generation_then_check_agree(tmp_path, capsys):
    """Property: a fresh generation followed by `--check` exits 0 (satisfying arm for rows 7 to 11).

    Class: any committed architecture and present carrier. Why: every refusal test below is
    meaningful only if the unrefused path succeeds. Killed by: any nondeterminism in rendering
    (a set-ordered field, a timestamp). Second correct implementation: a writer that sorts keys
    differently but identically on both runs still passes.
    """
    doc, anchor, out = _source(tmp_path), _anchor(tmp_path), tmp_path / "copy.json"
    assert _run(doc, anchor, out) == 0
    assert _run(doc, anchor, out, "--check") == 0
    assert "equal to the committed file" in capsys.readouterr().out


def test_check_names_the_slug_of_a_changed_clause(tmp_path, capsys):
    """Property: `--check` exits 1 naming the first differing slug when the source changed after generation (arm row 7).

    Class: any committed edit of a clause after the copy was written. Why: the copy is a second
    statement of the partition, admitted only with this scripted verification. Killed by:
    making `--check` report equality without comparing. Second correct implementation: one
    naming every differing slug rather than the first still passes.
    """
    doc, anchor, out = _source(tmp_path), _anchor(tmp_path), tmp_path / "copy.json"
    assert _run(doc, anchor, out) == 0
    doc.write_text(doc.read_text(encoding="utf-8").replace("computes beta;", "derives beta;"), encoding="utf-8")
    _commit(doc.parent, "edit")
    capsys.readouterr()
    assert _run(doc, anchor, out, "--check") == 1
    assert "beta-sys" in capsys.readouterr().out


def test_refuses_without_a_private_carrier(tmp_path):
    """Property: generation exits 2 when no private carrier sits beside the anchor (arm row 8).

    Class: any machine without the carrier linked. Why: an unscreened copy could publish a
    private token. Killed by: removing that refusal (the guard's own loader then raises out of
    `main`). Second correct implementation: one that reads the carrier itself and refuses on
    an empty token set still passes.
    """
    doc, anchor, out = _source(tmp_path), _anchor(tmp_path, with_supplement=False), tmp_path / "copy.json"
    assert _run(doc, anchor, out) == 2
    assert not out.exists()


def test_refuses_without_the_header_line(tmp_path, capsys):
    """Property: generation exits 2 naming the header line when the descriptor table's header is absent (arm row 9).

    Class: any architecture whose table header moved or changed. Why: without it the generator
    would read the wrong table or none. Killed by: removing the header refusal (a later
    no-rows refusal still exits 2, but does not name the header). Second correct
    implementation: matching the header by its column names rather than its exact line.
    """
    doc = _source(tmp_path, "# No table here\n\nNothing.\n")
    assert _run(doc, _anchor(tmp_path), tmp_path / "copy.json") == 2
    assert "header line" in capsys.readouterr().err


def test_refuses_an_uncommitted_source(tmp_path, capsys):
    """Property: generation exits 2 naming uncommitted changes when the source is modified after its commit (arm row 10).

    Class: any working-tree edit of the architecture not yet committed. Why: the recorded
    commit would not be the content the copy came from. Killed by: removing that refusal.
    Second correct implementation: one comparing the file with `git show HEAD:{path}`.
    """
    doc = _source(tmp_path)
    doc.write_text(doc.read_text(encoding="utf-8") + "\nedited\n", encoding="utf-8")
    assert _run(doc, _anchor(tmp_path), tmp_path / "copy.json") == 2
    assert "uncommitted" in capsys.readouterr().err


def test_a_carrier_match_is_withheld_and_counted(tmp_path):
    """Property: a carrier token inside a clause is stored as `[withheld]` and counted (arm row 11).

    Class: any clause containing any carrier token. Why: the copy is published in a public
    repository. Killed by: skipping the withholding step. Second correct implementation: one
    applying the guard's patterns to the whole cell before splitting it into clauses.
    """
    doc, anchor, out = _source(tmp_path), _anchor(tmp_path), tmp_path / "copy.json"
    assert _run(doc, anchor, out) == 0
    written = out.read_text(encoding="utf-8")
    assert TOKEN not in written
    data = json.loads(written)
    assert data["withheld_count"] == 1
    assert sum(c["text"].count("[withheld]") for s in data["systems"] for c in s["clauses"]) == 1


# --- the committed copy ---------------------------------------------------------------------


def test_the_committed_copy_is_consistent():
    """Property: the committed copy has no inconsistency CI can recompute (satisfying arm for rows 12 and 30 to 33).

    Class: the real committed copy. Why: CI cannot regenerate it, so this is its only CI check.
    Killed by: a hand edit of any clause text. Second correct implementation: the generator's
    own `--check`, which compares the whole file, also passes on it.
    """
    findings = partition_copy.copy_findings()
    assert not findings, "\n".join(findings)


def _mutated(tmp_path: Path, change) -> list[str]:
    data = copying.deepcopy(partition_copy.load())
    change(data)
    return partition_copy.copy_findings(_write_copy(tmp_path, data))


def test_an_edited_clause_text_is_named_by_its_id(tmp_path):
    """Property: a clause whose text changed while its id did not is named by that id (arm row 12).

    Class: any stored text edited by hand. Killed by: dropping the id recomputation. Second
    correct implementation: one recomputing the whole id rather than comparing the prefix.
    """
    victim = partition_copy.load()["systems"][0]["clauses"][0]["id"]

    def change(data):
        data["systems"][0]["clauses"][0]["text"] += " edited"

    assert victim in _quoted(_mutated(tmp_path, change))


def test_a_repeated_id_is_named(tmp_path):
    """Property: an identifier occurring twice is named (arm row 30).

    Class: two clauses with identical text. Killed by: dropping the uniqueness test. Second
    correct implementation: a `collections.Counter` over the ids.
    """
    data = partition_copy.load()
    first = data["systems"][0]["clauses"][0]

    def change(d):
        d["systems"][0]["clauses"].append(dict(first))

    assert first["id"] in _quoted(_mutated(tmp_path, change))


def test_an_unknown_polarity_is_named(tmp_path):
    """Property: a polarity outside the closed vocabulary is named by its clause id (arm row 31).

    Class: any polarity string other than the two. Killed by: accepting any string. Second
    correct implementation: an enum lookup that rejects unknown values.
    """
    victim = partition_copy.load()["systems"][1]["clauses"][0]["id"]

    def change(data):
        data["systems"][1]["clauses"][0]["polarity"] = "neutral"

    assert victim in _quoted(_mutated(tmp_path, change))


def test_a_wrong_withheld_count_is_named(tmp_path):
    """Property: a `withheld_count` disagreeing with the stored texts is named with both numbers (arm row 32).

    Class: any count other than the marker's occurrences. Killed by: dropping the comparison.
    Second correct implementation: counting with a regular expression over the texts.
    """

    def change(data):
        data["withheld_count"] = 0

    findings = _mutated(tmp_path, change)
    assert any("withheld_count" in f and "recorded 0" in f and "hold 1" in f for f in findings), "\n".join(findings)


def test_a_system_without_a_belongs_clause_is_named(tmp_path):
    """Property: a system all of whose clauses are does-not-belong is named by its slug (arm row 33).

    Class: any system left with no routing clause. Killed by: dropping the belongs test. Second
    correct implementation: counting belongs clauses per system and refusing zero.
    """
    slug = partition_copy.load()["systems"][2]["slug"]

    def change(data):
        for entry in data["systems"][2]["clauses"]:
            entry["polarity"] = "does-not-belong"

    assert slug in _quoted(_mutated(tmp_path, change))


# --- the guard cases against the copy (row 27's arm; the real-tree comparison lands with chunk 1a.8) --


def _guard_file(tmp_path: Path, systems: list[str]) -> Path:
    """Synthesize a guard-case file of literal decorated functions, one per system."""
    body = ["import pytest", ""]
    for slug in systems:
        body += [f'@pytest.mark.guard(system="{slug}")', f"def test_guard_{slug.replace('-', '_')}():", "    pass", ""]
    path = tmp_path / "test_guard_cases.py"
    path.write_text("\n".join(body), encoding="utf-8")
    return path


def test_guard_cases_equal_to_the_copy_have_no_finding(tmp_path):
    """Property: a guard file with exactly one literal per copy slug yields no finding (satisfying arm for row 27).

    Class: any guard file whose literals equal the slugs. Killed by: reading literals from the
    wrong keyword. Second correct implementation: a regular-expression scan of the decorators.
    """
    findings = partition_copy.guard_case_findings(_guard_file(tmp_path, partition_copy.slugs()))
    assert not findings, "\n".join(findings)


def test_a_misspelled_guard_literal_is_named_with_the_slug_it_displaced(tmp_path):
    """Property: one misspelled `system=` literal is named, and so is the slug it displaced, as whole slugs (arm row 27).

    Class: any guard file whose literals and the copy's slugs differ while their counts are
    equal. Why: the equality must hold in both directions. Killed by: comparing counts instead
    of sets, or comparing either direction alone. Second correct implementation: a symmetric
    difference reported in one finding naming both slugs.
    """
    systems = partition_copy.slugs()
    displaced = systems[3]
    misspelled = displaced + "x"
    guard = _guard_file(tmp_path, [misspelled if s == displaced else s for s in systems])
    assert {displaced, misspelled} <= _quoted(partition_copy.guard_case_findings(guard))


def test_the_real_guard_cases_equal_the_copy():
    """Property: the real guard file's `system=` literals equal the committed copy's slugs, in both directions.

    Class: the guard file at every edit of the partition. Why: the guard cases are twenty hand-
    written functions, so a system added to, removed from or renamed in the partition copy without
    the matching guard-case edit must fail by name here, in the State check job, rather than leave
    a system with no guard case or a guard case naming no system. Killed by: a guard file whose
    literals differ from the copy's slugs (the misspelled-literal arm above runs that input).
    Second correct implementation: comparing the slugs of the copy with the decorator literals
    read by a regular expression, as two sets.
    """
    findings = partition_copy.guard_case_findings(partition_copy.REPO_ROOT / "tests/correspondence/test_guard_cases.py")
    assert not findings, "\n".join(findings)
