"""Tests of the system-scope file's reader and its seven checks.

Six checks read the file alone and one compares it with the guard cases, so each test writes a
small file of two systems into its own temporary tree, changes one thing, and expects exactly
the finding that one change earns. The last test runs the real file and the real guard cases and
pins the summary line, so a change to the committed statements changes that line on purpose.
"""

import hashlib
import json

import pytest

from tests.correspondence import system_scope

pytestmark = pytest.mark.state_check

LANDING = "system scope: 20 systems, 42 statements (21 covers, 21 excludes); findings 0"


def _id(slug: str, text: str) -> str:
    """The identifier the format computes for a statement."""
    return f"{slug}/{hashlib.sha256(text.encode('utf-8')).hexdigest()[:8]}"


def _good():
    """Two systems, each with one covers and one excludes statement, every identifier computed."""
    systems = []
    for slug in ("alpha-system", "beta-system"):
        statements = []
        for mark, text in (("covers", f"What {slug} owns."), ("excludes", f"What {slug} leaves to others.")):
            statements.append({"mark": mark, "text": text, "id": _id(slug, text)})
        systems.append({"slug": slug, "statement": statements})
    return {"rule": system_scope.RULE, "system": systems}


def _write(tmp_path, data) -> list[str]:
    """Write `data` as the system-scope file under `tmp_path` and return the reader's findings."""
    lines = []
    for key, value in data.items():
        if key != "system":
            lines.append(f"{key} = {json.dumps(value)}")
    for system in data["system"]:
        lines += ["", "[[system]]"] + [f"{k} = {json.dumps(v)}" for k, v in system.items() if k != "statement"]
        for entry in system["statement"]:
            lines += ["", "[[system.statement]]"] + [f"{k} = {json.dumps(v)}" for k, v in entry.items()]
    path = tmp_path / system_scope.SCOPE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return system_scope.load(tmp_path)[1]


def _names(finding: str, token: str) -> bool:
    """Whether `finding` names `token` as a whole, backtick- or quote-delimited token."""
    return f"`{token}`" in finding or repr(token) in finding


def test_the_good_file_has_no_finding(tmp_path):
    """Property: a file of computed identifiers, valid marks and the format's keys earns no finding.

    Class: the satisfying arm every other test changes one thing of. Why: a check that fires on a
    correct file would turn every test below into a test of noise. Killed by: any check firing on
    valid input. Second correct implementation: the same checks run in another order.
    """
    assert _write(tmp_path, _good()) == []


def test_an_identifier_not_computed_from_its_slug_and_text_is_named(tmp_path):
    """Property: an identifier that is not its slug, a slash and its text's 8-hex prefix is named, for each such statement.

    Class: wrong digits after the right slug, and the right digits after another system's slug.
    Why: the identifier is what a ledger row cites, so one that does not follow from the text lets a
    citation survive a change of the statement it named. Killed by: skipping the recomputation.
    Second correct implementation: comparing the parsed slug and digits separately.
    """
    data = _good()
    wrong_digits = "alpha-system/00000000"
    data["system"][0]["statement"][0]["id"] = wrong_digits
    entry = data["system"][1]["statement"][0]
    other_slug = "alpha-system/" + entry["id"].split("/", 1)[1]
    entry["id"] = other_slug
    findings = _write(tmp_path, data)
    assert any(_names(f, wrong_digits) for f in findings), findings
    assert any(_names(f, other_slug) for f in findings), findings


def test_an_identifier_that_occurs_twice_is_named(tmp_path):
    """Property: two statements carrying one identifier are named by it.

    Class: a system holding the same text twice. Why: a citation of a repeated
    identifier could mean either statement. Killed by: dropping the duplicate check. Second correct
    implementation: counting identifiers with a `Counter`.
    """
    data = _good()
    data["system"][0]["statement"].append(dict(data["system"][0]["statement"][0]))
    sid = data["system"][0]["statement"][0]["id"]
    assert any(_names(f, sid) and "more than once" in f for f in _write(tmp_path, data))


def test_a_statement_holding_a_line_break_is_named(tmp_path):
    """Property: a statement whose text holds a newline or a carriage return is named, for each.

    Class: either line-break character, with the identifier recomputed so only the break is wrong.
    Why: a statement is one line, so a break would let a second claim hide inside the first.
    Killed by: dropping the line-break check. Second correct implementation: `str.splitlines()`
    returning more than one part.
    """
    data = _good()
    ids = []
    for system, text in zip(data["system"], ("First line\nsecond line.", "First line\rsecond line."), strict=True):
        system["statement"][0].update(text=text, id=_id(system["slug"], text))
        ids.append(system["statement"][0]["id"])
    findings = _write(tmp_path, data)
    for sid in ids:
        assert any(_names(f, sid) and "line break" in f for f in findings), (sid, findings)


def test_a_mark_outside_the_two_is_named(tmp_path):
    """Property: a mark other than covers or excludes is named with the statement and the word `mark`.

    Class: any other mark, on a system that still has a covers statement. Why: only those two
    marks say what a citation of the statement means. Killed by: accepting any mark. Second correct
    implementation: a set membership test on the two marks.
    """
    data = _good()
    data["system"][0]["statement"][1]["mark"] = "includes"
    sid = data["system"][0]["statement"][1]["id"]
    assert any(_names(f, sid) and "mark" in f for f in _write(tmp_path, data))


def test_a_system_with_no_covers_statement_is_named(tmp_path):
    """Property: a system whose statements are all excludes is named by its slug.

    Class: a system left with only an excludes statement. Why: no scoped-in unit could cite such a
    system, so its tests could never own anything. Killed by: dropping the covers check. Second
    correct implementation: comparing the set of systems with covers statements to all systems.
    """
    data = _good()
    data["system"][1]["statement"] = data["system"][1]["statement"][1:]
    assert any(_names(f, "beta-system") and "covers" in f for f in _write(tmp_path, data))


def test_a_key_outside_the_format_is_named_at_each_level(tmp_path):
    """Property: an unknown key is named at the top level, in a system table and in a statement.

    Class: an extra key at the top, in a system table and in a statement. Why: an unread key such as a
    source note would carry text no check reads into a public file. Killed by: accepting any key.
    Second correct implementation: a schema validator listing allowed keys per level.
    """
    data = _good()
    data["source"] = "a"
    data["system"][0]["source"] = "b"
    data["system"][1]["statement"][0]["source"] = "c"
    findings = [f for f in _write(tmp_path, data) if "'source'" in f]
    assert len(findings) == 3, findings


def test_the_guard_cases_and_the_file_name_the_same_systems_both_ways(tmp_path):
    """Property: a system with no guard case, and a guard case naming no system, are each named.

    Class: a guard file missing one of the file's slugs and naming one slug the file lacks. Why: a
    system with no guard case has no selection point, and a guard case for no system selects
    nothing. Killed by: comparing in one direction only. Second correct implementation: the
    symmetric difference of the two slug sets.
    """
    _write(tmp_path, _good())
    guard = tmp_path / "guard.py"
    guard.write_text(
        "import pytest\n\n\n@pytest.mark.guard(system='alpha-system')\ndef test_a():\n    pass\n\n\n"
        "@pytest.mark.guard(system='gamma-system')\ndef test_g():\n    pass\n",
        encoding="utf-8",
    )
    findings = system_scope.guard_case_findings(guard, tmp_path)
    assert any(_names(f, "beta-system") for f in findings), findings
    assert any(_names(f, "gamma-system") for f in findings), findings
    assert len(findings) == 2, findings


def test_the_real_file_passes_all_seven_checks_and_states_its_counts(capsys):
    """Property: the committed file, with the real guard cases, has no finding and prints its counts.

    Class: the real tree. Why: the line is what a reader of a run sees, so it must state how many
    systems and statements were checked, not a bare zero. Killed by: printing the findings count
    alone. Second correct implementation: a two-line summary with the counts on the first line.
    """
    assert system_scope.main() == 0
    assert capsys.readouterr().out.splitlines()[0] == LANDING
