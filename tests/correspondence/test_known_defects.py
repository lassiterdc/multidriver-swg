"""Tests of the known-defect register: the real register is clean, and one violating arm per refusal.

Fixtures copy the record, the tracked corpus and a register written by the test into a git
repository inside `tmp_path` (staged, never committed), so key resolution meets a real
`git ls-files` and reads nothing outside the copy.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, known_defects

pytestmark = pytest.mark.state_check

TEXTS = 'defect = "a defect"\nkind = "a kind"\nreason = "a reason"\n'


def _fixture(tmp_path: Path, scope: str, command: str, entries: str = "") -> Path:
    """Copy the corpus and record into a staged git repository at `tmp_path`, write a register there; return it."""
    root = corpus_record.REPO_ROOT
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--", "_old_code_to_refactor"], cwd=root, capture_output=True, check=True
    ).stdout.decode("utf-8")
    for rel in [p for p in listed.split("\0") if p] + [corpus_record.RECORD_NAME]:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / rel, tmp_path / rel)
    register = tmp_path / known_defects.REGISTER_PATH
    register.parent.mkdir(parents=True, exist_ok=True)
    register.write_text(
        f'rule = "{known_defects.RULE}"\nexamined_scope = "{scope}"\nexamination_command = "{command}"\n'
        f'statement = "a statement"\n{entries}',
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _names(finding: str, *tokens: str) -> bool:
    """Whether the finding names every token as a whole word, never as a substring.

    Unit keys can contain one another (`:1-1` inside `:1-16`), so a substring test would pass on a
    finding that names a different unit.
    """
    words = {word.strip("'\":,") for word in finding.split()}
    return set(tokens) <= words


def test_the_real_register_has_no_finding():
    """Property: the committed register loads with no form or key finding.

    Class: the register at every stage — unexamined and empty at landing, later examined, later
    with entries. Why: the State check job runs this module only through this test, so a register
    the crawl breaks would otherwise fail nothing. Killed by: any change that reports a finding on a
    well-formed register, for example refusing a register with no entry, which is the committed
    register's state. Second correct implementation: a schema-validating reader that checks the
    scope pair before reading any entry.
    """
    extent.require_extent()
    _register, findings = known_defects.load()
    assert not findings, "\n".join(findings)


def test_an_entry_whose_key_names_no_unit_is_named(tmp_path):
    """Property: an entry keyed to no unit is named by its key (arm row 22).

    Class: any entry key that no unit of the extent partition carries, including a key one range
    off a real unit. Why: an entry whose key names no unit cannot be consulted when a test of that
    unit is authored. Killed by: skipping key resolution. Second correct implementation: resolve
    every entry against one set of unit keys built from the partition once.
    """
    entry = '[[entry]]\nkey = "_utils.py:1-1"\nline = 1\nruling = "UNRULED"\n' + TEXTS
    root = _fixture(tmp_path, known_defects.UNEXAMINED, known_defects.UNEXAMINED, entry)
    _register, findings = known_defects.load(root)
    assert any(_names(f, "_utils.py:1-1") and "resolves to no unit" in f for f in findings), findings


def test_the_scope_fields_must_agree_and_an_examined_empty_register_is_clean(tmp_path):
    """Property: the two scope fields must both read UNEXAMINED or both be stated — and nothing else (arm row 23).

    Class: the violating arm is any register whose scope is UNEXAMINED while its command is
    stated (or the reverse); the satisfying arm is an examined register with no entry, which must
    load with no finding. Why: the two fields are what separate an empty register from an
    unexamined corpus, so they must agree; whether the entry list is empty separates nothing.
    Killed by: keying the check on whether the entry list is empty. Second correct implementation:
    a check comparing `examined_scope == "UNEXAMINED"` with `examination_command == "UNEXAMINED"`.
    """
    violating = _fixture(tmp_path / "violating", known_defects.UNEXAMINED, "python -m some.examination")
    _register, findings = known_defects.load(violating)
    assert any(_names(f, "examined_scope", "examination_command") for f in findings), findings
    satisfying = _fixture(tmp_path / "satisfying", "every retained unit", "python -m some.examination")
    _register, findings = known_defects.load(satisfying)
    assert not findings, "\n".join(findings)


def test_a_ruling_outside_the_three_is_named(tmp_path):
    """Property: an entry whose ruling is not OBLIGED, LICENSED or UNRULED is named with its key and the ruling (arm row 24).

    Class: any ruling string outside the three. Why: "a test cannot be authored over an entry that
    has no ruling" must read a value, so an unknown word must not pass as one. Killed by: accepting
    any string. Second correct implementation: an enum lookup that raises and is caught per entry.
    """
    found, _counts = extent.extents("_utils.py", extent._read(corpus_record.REPO_ROOT, "_utils.py"))
    key, line = found[0]["key"], found[0]["lines"][0]
    entry = f'[[entry]]\nkey = "{key}"\nline = {line}\nruling = "DONE"\n' + TEXTS
    root = _fixture(tmp_path, known_defects.UNEXAMINED, known_defects.UNEXAMINED, entry)
    _register, findings = known_defects.load(root)
    assert any(_names(f, key, "DONE") for f in findings), findings
