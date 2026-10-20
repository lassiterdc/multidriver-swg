"""Tests of the triage log: the real log is clean and prints its landing line, and one violating arm per refusal.

Each arm writes a log into `tmp_path` holding five well-formed rows — every finding, with and
without a part, a split unit key, both fourth-finding states — and then the one violating row,
sixth. The arm asserts that exactly one finding comes back and that it names the sixth row and
the field as whole words, so the well-formed rows are the arm's satisfying inputs and a refusal
that fires on them reds the arm. Beside each such log the arm writes a divergence ledger in which
every well-formed pointer of those rows resolves — two through departure entries and one through
a pair entry that replaced a departure — so pointer resolution runs on every arm and fires on
none of them except where an arm's own sixth row is meant to dangle.
"""

from pathlib import Path

import pytest

from tests.correspondence import triage_log

pytestmark = pytest.mark.state_check

HEADER = f'rule = "{triage_log.RULE}"\nhalt_condition = "{triage_log.HALT_CONDITION}"\nstatement = "a statement"\n'
WELL_FORMED_ROWS = """
[[row]]
test = "equivalence:_utils.py:1991-2075"
finding = 1
artifact = "the new code"
commit = "0123456789abcdef0123456789abcdef01234567"

[[row]]
test = "equivalence:_utils.py:1991-2075:peaks"
finding = 2
artifact = "the stored reference value"
commit = "89abcdef0123456789abcdef0123456789abcdef"

[[row]]
test = "equivalence:_d_build_water_level_generator.py:117-127,131-136"
finding = 3
artifact = "the stored reference value"
commit = "fedcba9876543210fedcba9876543210fedcba98"
departure = { unit = "_d_build_water_level_generator.py:117-127,131-136", site = 120 }
ruled_outcome = "The old code counted the buffer twice; the reference value now counts it once."

[[row]]
test = "refactor_reminder:_utils.py:1-18:storm"
finding = 4
artifact = "the new code"
commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
departure = { unit = "_utils.py:1-18", site = 1 }
state = "fork ruled, method not built"

[[row]]
test = "equivalence:_inputs.py:30-41"
finding = 4
artifact = "the new code"
commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
departure = { unit = "_inputs.py:30-41", site = 41 }
state = "pair registered"
"""
LANDING_LINE = (
    "triage log: 0 rows; halt condition: work on a failing equivalence test stops and goes to the"
    " developer when the failure's finding is undecided, or when the test cannot pass against"
    " correctly written new code; departure pointers: 0 resolved of 0"
)
LABEL = "canon-ast:1:0000000000000000000000000000000000000000000000000000000000000000"
LEDGER_HEADER = 'rule = "divergence-ledger:1"\nstatement = "a statement"\n'
# A departure entry for each well-formed pointer row: the third finding at site 120 and the
# fourth finding whose method is not built. The sixth-row pointer `_utils.py:1991-2075` site 2000,
# which two arms use as a well-formed pointer while testing another field, resolves here too.
LEDGER_DEPARTURES = {
    "row-3": (
        '[[departure]]\nunit = "_d_build_water_level_generator.py:117-127,131-136"\nsite = 120\nkind = "finding-3"\n'
        'grounds = { triage = "equivalence:_d_build_water_level_generator.py:117-127,131-136" }\n'
        'new_code = "not yet written"\nwhy = "w"\njudged_correct_when = "j"\n'
    ),
    "row-4": (
        '[[departure]]\nunit = "_utils.py:1-18"\nsite = 1\nkind = "finding-4"\n'
        'grounds = { triage = "refactor_reminder:_utils.py:1-18:storm" }\n'
        'new_code = "not yet written"\nwhy = "w"\njudged_correct_when = "j"\nfuture_pair = "storm"\n'
    ),
    "sixth": (
        '[[departure]]\nunit = "_utils.py:1991-2075"\nsite = 2000\nkind = "finding-3"\n'
        'grounds = { triage = "equivalence:_utils.py:1991-2075" }\n'
        'new_code = "not yet written"\nwhy = "w"\njudged_correct_when = "j"\n'
    ),
}
SIDE = f'{{ function = "m:f", combined = "{LABEL}", functions = {{ "m:f" = "{LABEL}" }} }}'
# The pair entry that replaced the fifth row's fourth-finding departure once its pair was registered. A fourth
# finding is a desired deviation, so that pair diverges and carries its three reasons.
LEDGER_PAIR = (
    f'[[pair]]\nid = "inputs"\npre_refactor = {SIDE}\ncurrent = {SIDE}\ndiverges = true\n'
    'changed = "c"\nwhy = "w"\njudged_correct_when = "j"\n'
    'departure = { unit = "_inputs.py:30-41", site = 41 }\n'
)


def _ledger(root: Path, departures: tuple[str, ...] = tuple(LEDGER_DEPARTURES), pair: bool = True) -> None:
    """Write a divergence ledger under `root` holding the named departure entries and, when `pair`, the pair entry."""
    path = root / triage_log.LEDGER_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    body = LEDGER_HEADER + "".join("\n" + LEDGER_DEPARTURES[name] for name in departures)
    path.write_text(body + ("\n" + LEDGER_PAIR if pair else ""), encoding="utf-8")


def _root(tmp_path: Path, sixth_row: str, header: str = HEADER) -> Path:
    """Write a log of the five well-formed rows plus `sixth_row`, and a ledger resolving their pointers, under `tmp_path`."""
    path = tmp_path / triage_log.LOG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + WELL_FORMED_ROWS + "\n[[row]]\n" + sixth_row, encoding="utf-8")
    _ledger(tmp_path)
    return tmp_path


def _names(finding: str, *tokens: str) -> bool:
    """Whether the finding names every token as a whole word, never as a substring.

    Row numbers and unit keys contain one another (`1` inside `16`, `:1-1` inside `:1-16`), so a
    substring test would pass on a finding that names a different row or unit.
    """
    words = {word.strip("'\":,(){}") for word in finding.split()}
    return set(tokens) <= words


def _only_finding(root: Path, *tokens: str) -> None:
    """Assert the log under `root` yields exactly one finding, naming the sixth row and every token."""
    _rows, findings = triage_log.load(root)
    assert len(findings) == 1 and _names(findings[0], "row", "6", *tokens), findings


ROW_TAIL = 'artifact = "the new code"\ncommit = "0123456789abcdef0123456789abcdef01234567"\n'


def test_the_real_triage_log_has_no_finding():
    """Property: the committed log loads with no header or row finding.

    Class: the log at every stage — no row at landing, rows appended as failures close. Why: the
    State check job reaches this module only through this test, so a row written in a wrong form
    would otherwise fail nothing. Killed by: any change that reports a finding on a well-formed
    log, for example refusing a log with no row. Second correct implementation: a reader that
    validates each row against a schema object before any cross-field rule.
    """
    _rows, findings = triage_log.load()
    assert not findings, "\n".join(findings)


def test_the_script_prints_the_landing_line(capsys):
    """Property: the script prints exactly the row count, the halt condition and how many pointers resolve, and exits 0.

    Class: the real log as landed, with no row and so no pointer. Why: a zero must read as a
    stated zero, and the line is the one place a reader learns the halt condition and how many of
    the log's departure pointers the divergence ledger resolves; this test is a deliberate
    tripwire that the first appended row must update. Killed by: printing the count alone, or
    dropping the pointers' count from the line. Not established: that the halt condition is read
    from the log's header rather than from the module's constant — on the landed log the two are
    equal, so this test cannot tell them apart. Second correct implementation: a `main` that
    formats the line from `load()`'s rows and a header reader of its own.
    """
    assert triage_log.main() == 0
    assert capsys.readouterr().out == LANDING_LINE + "\n"


def test_a_top_level_key_outside_the_form_is_named(tmp_path):
    """Property: a log key other than rule, halt_condition, statement and row is named, so misfiled rows are never read as an empty log.

    Class: any top-level key the log's form does not name — a misspelt table such as `[[rows]]`,
    or a stray value. Why: rows filed under `[[rows]]` leave `row` absent, and the log would read
    as holding no row with every other check green, losing every closing written there. Killed by:
    accepting any top-level key. Second correct implementation: comparing the set of top-level
    keys with the allowed set and naming the difference.
    """
    path = tmp_path / triage_log.LOG_PATH
    path.parent.mkdir(parents=True)
    path.write_text(HEADER + '\n[[rows]]\ntest = "equivalence:_utils.py:1-18"\nfinding = 1\n' + ROW_TAIL)
    rows, findings = triage_log.load(tmp_path)
    assert rows == [] and len(findings) == 1 and _names(findings[0], "header", "key", "rows"), findings


def test_a_row_key_outside_the_form_is_named(tmp_path):
    """Property: a row key other than the seven of the row form is named with the row and the key.

    Class: any key a row carries that the form does not name — a stray note, or a misspelt
    optional field. Why: a misspelt optional field, such as `ruled_outcom` on a third finding or
    `state` written as `stat`, would otherwise be ignored while the row reads as complete. Killed
    by: accepting any row key. Second correct implementation: building each row into a dataclass
    whose constructor refuses unexpected keyword arguments.
    """
    row = 'test = "equivalence:_utils.py:1-18"\nfinding = 1\nnote = 3\n' + ROW_TAIL
    _only_finding(_root(tmp_path, row), "key", "note")


def test_a_finding_outside_the_four_is_named(tmp_path):
    """Property: a row whose finding is not 1, 2, 3 or 4 is named with the value.

    Class: any integer outside 1 to 4, including 0 and negatives. Why: only four kinds of finding
    are defined, so any other number records nothing a reader can interpret. Killed by: accepting
    any integer. Second correct implementation: a lookup in a mapping from finding number to its
    name, refusing a missing key.
    """
    _only_finding(_root(tmp_path, 'test = "equivalence:_utils.py:1991-2075"\nfinding = 5\n' + ROW_TAIL), "finding", "5")


def test_a_third_finding_without_a_ruled_outcome_is_named(tmp_path):
    """Property: a third finding (the old code was wrong) that states no ruled outcome is named.

    Class: a third finding whose `ruled_outcome` is absent or empty. Why: once the old code is
    judged wrong, the reference value no longer means what the old code computed, so a reader of
    the row needs, in words, where the old code went wrong and what the expected value now stands for. Killed by: dropping
    the ruled-outcome check. Second correct implementation: a per-finding table of required
    fields, with `ruled_outcome` required for 3.
    """
    row = (
        'test = "equivalence:_utils.py:1991-2075"\nfinding = 3\n'
        'departure = { unit = "_utils.py:1991-2075", site = 2000 }\n' + ROW_TAIL
    )
    _only_finding(_root(tmp_path, row), "ruled_outcome")


def test_a_departure_pointer_is_required_on_findings_3_and_4_and_refused_on_1_and_2(tmp_path):
    """Property: a fourth finding with no departure is named, and so is a first finding carrying one.

    Class: both directions of the pointer rule — a third or fourth finding with no `departure`, a
    first or second finding with any. Why: a departure entry explains a place where the new code is
    meant to behave differently from the old code; without the pointer that explanation cannot be
    found from the row, and with a pointer on a first or second finding the log claims such a
    place where none was found. Killed by: dropping the pointer check. Second correct
    implementation: comparing `"departure" in row` with `finding in (3, 4)` and naming any
    disagreement.
    """
    missing = 'test = "equivalence:_utils.py:1991-2075"\nfinding = 4\nstate = "pair registered"\n' + ROW_TAIL
    _only_finding(_root(tmp_path / "missing", missing), "departure", "4")
    extra = (
        'test = "equivalence:_utils.py:1991-2075"\nfinding = 1\n'
        'departure = { unit = "_utils.py:1991-2075", site = 2000 }\n' + ROW_TAIL
    )
    _only_finding(_root(tmp_path / "extra", extra), "departure", "1")


def test_a_fourth_finding_state_outside_the_two_is_named(tmp_path):
    """Property: a fourth finding whose state is neither of its two states is named with the value.

    Class: any state string other than the two. Why: the state says whether the old behaviour's
    separate implementation still has to be written, which a free word cannot say reliably. Killed
    by: accepting any string. Second correct implementation: an enum of the two states looked up
    by value.
    """
    row = (
        'test = "equivalence:_utils.py:1991-2075"\nfinding = 4\nstate = "done"\n'
        'departure = { unit = "_utils.py:1991-2075", site = 2000 }\n' + ROW_TAIL
    )
    _only_finding(_root(tmp_path, row), "state", "done")


@pytest.mark.parametrize(
    "test",
    [
        pytest.param("the peaks test", id="prose"),
        pytest.param("equivalence:_utils.py:18-1", id="descending-range"),
        pytest.param("equivalence:_utils.py:5-9,1-3", id="ranges-out-of-order"),
        pytest.param("equivalence:_utils.py:1-9,5-12", id="overlapping-ranges"),
    ],
)
def test_a_test_that_is_not_a_test_identifier_is_named(tmp_path, test):
    """Property: a row whose test is not `{family}:{unit key}` or `{family}:{unit key}:{part}` with a canonical key is named.

    Class: any string not of that form — prose, a bare unit key, a family with no key — and a
    key-shaped string whose line ranges are not canonical: a range that runs backwards, ranges out
    of order, or ranges that overlap. Why: a triage row is joined to its test by that identifier,
    so prose cannot be followed back to the test it closed, and a key the disposition ledger would
    refuse as non-canonical names no unit, so no test can carry it. Killed by: accepting any string;
    or testing the key with the key-shaped pattern alone, without checking that its ranges ascend,
    run in order and do not overlap. Second correct implementation: one regular expression over
    the identifier's family and optional part, with the key's ranges parsed and each start compared
    with the previous end.
    """
    _only_finding(_root(tmp_path, f'test = "{test}"\nfinding = 1\n' + ROW_TAIL), "test")


GOOD_COMMIT = "0123456789abcdef0123456789abcdef01234567"


def _findings(tmp_path: Path, row: str) -> list[str]:
    """Write a log holding `row` as its only row under `tmp_path` and return the check's findings."""
    path = tmp_path / triage_log.LOG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER + "\n[[row]]\n" + row, encoding="utf-8")
    return triage_log.load(tmp_path)[1]


@pytest.mark.parametrize(
    "commit",
    [
        pytest.param(GOOD_COMMIT.upper(), id="upper-case"),
        pytest.param("0123456789ABCDEF0123456789abcdef01234567", id="mixed-case"),
    ],
)
def test_a_forty_digit_hex_commit_of_any_case_is_accepted(tmp_path, commit):
    """Property: a row whose commit is forty hexadecimal digits yields no finding, whatever the digits' case.

    Class: every forty-character string of the digits 0-9, a-f and A-F, here all upper-case and
    mixed case, in an otherwise well-formed first-finding row.

    Why: `git rev-parse --verify {hash}^{commit}` resolves an upper-case full hash to the same
    commit as its lower-case spelling, so a reader can follow such a row to its commit; a check
    refusing it fails the State check job on a row nothing is wrong with and obliges every row
    writer to a spelling the row's stated form (`{40 hex}`) does not ask for.

    Killed by: a commit pattern that admits only the lower-case digits, `[0-9a-f]{40}`.

    Second correct implementation, under which this still passes: `len(commit) == 40 and
    set(commit) <= set(string.hexdigits)`, which shares no pattern with a regular-expression form.
    """
    row = f'test = "equivalence:_utils.py:1-18"\nfinding = 1\nartifact = "the new code"\ncommit = "{commit}"\n'
    findings = _findings(tmp_path, row)
    assert not findings, "\n".join(findings)


@pytest.mark.parametrize(
    ("field", "line"),
    [
        ("rule", 'rule = "triage-log:2"\n'),
        ("halt_condition", 'halt_condition = "the developer decides"\n'),
        ("statement", 'statement = " "\n'),
    ],
)
def test_a_header_field_that_is_wrong_is_named(tmp_path, field, line):
    """Property: a header whose rule or halt condition is not the expected text, or whose statement is blank, is named.

    Class: each of the three header fields, wrong while the other two are right. Why: the header is
    where a reader learns when to stop work on a failing test, so a header stating another
    condition, or a different rule version, misdirects every reader. Killed by: skipping that
    field's comparison. Second correct implementation: comparing the header as one table against
    an expected table, naming each differing key.
    """
    header = "".join(line if part.startswith(field + " ") else part for part in HEADER.splitlines(keepends=True))
    _rows, findings = triage_log.load(
        _root(tmp_path, 'test = "equivalence:_utils.py:1-18"\nfinding = 1\n' + ROW_TAIL, header)
    )
    assert len(findings) == 1 and _names(findings[0], "header", field), findings


@pytest.mark.parametrize(
    ("field", "tail"),
    [
        ("artifact", 'artifact = ""\ncommit = "0123456789abcdef0123456789abcdef01234567"\n'),
        ("commit", 'artifact = "the new code"\ncommit = "0123456789ABCDEF"\n'),
        ("commit", 'artifact = "the new code"\ncommit = "0123456789abcdef0123456789abcdef0123456g"\n'),
    ],
    ids=["artifact-empty", "commit-short", "commit-not-hex"],
)
def test_an_artifact_or_commit_of_the_wrong_form_is_named(tmp_path, field, tail):
    """Property: a row whose artifact is empty, or whose commit is not forty hexadecimal digits, is named.

    Class: an empty or absent artifact; a commit shorter or longer than forty characters, or one
    holding a character that is not a hexadecimal digit of either case. Why: the row records which
    artifact changed and in which commit, and a reader resolves the commit with git, which an
    abbreviated or mistyped hash may not resolve. Killed by: skipping that field's check. Second
    correct implementation, for the commit: `len(commit) == 40 and set(commit) <=
    set(string.hexdigits)`.
    """
    _only_finding(_root(tmp_path, 'test = "equivalence:_utils.py:1-18"\nfinding = 1\n' + tail), field)


@pytest.mark.parametrize(
    "departure",
    [
        '{ unit = "_utils.py:1-18" }',
        '{ unit = "_utils.py:1-18", site = 0 }',
        '{ unit = "the storm unit", site = 3 }',
        '"_utils.py:1-18 line 3"',
        '{ unit = "_utils.py:18-1", site = 2 }',
        '{ unit = "_utils.py:5-9,1-3", site = 2 }',
        '{ unit = "_utils.py:1-9,5-12", site = 2 }',
    ],
)
def test_a_departure_not_of_the_key_form_is_named(tmp_path, departure):
    """Property: a departure that is not `{ unit = {unit key}, site = {line} }` with a line of 1 or more is named.

    Class: a missing site, a site below 1, a unit that is not a unit key, a pointer written as
    free text, and a key-shaped unit whose line ranges are not canonical: a range that runs
    backwards, ranges out of order, or ranges that overlap. Why: a departure entry is found by its
    unit's key together with a line number within that unit, so only a pointer carrying both can
    later be followed to the entry, and a non-canonical key names no unit. Killed by: accepting any
    departure value; or testing the unit with the key-shaped pattern alone, without checking its
    ranges. Second correct implementation: a dataclass constructed from the table that raises on a
    missing or ill-typed field, with the unit's ranges parsed and compared range by range.
    """
    row = (
        f'test = "equivalence:_utils.py:1-18"\nfinding = 4\nstate = "pair registered"\n'
        f"departure = {departure}\n" + ROW_TAIL
    )
    _only_finding(_root(tmp_path, row), "departure")


def test_a_departure_pointer_that_names_no_ledger_entry_is_named(tmp_path):
    """Property: a well-formed pointer of a third or fourth finding that no departure entry or pair entry carries is named.

    Class: every well-formed pointer whose unit and site match no departure entry's unit and site
    and no pair entry's `departure`, here a known unit at a site no entry names. Why: a pointer
    is how a reader finds the explanation of a deliberate departure, so one that leads nowhere
    leaves the departure unexplained while the row reads as complete. Killed by: not resolving
    pointers, or resolving by unit alone. Second correct implementation: building a mapping from
    `(unit, site)` to the entry that carries it and naming each pointer the mapping lacks.
    """
    row = (
        'test = "equivalence:_utils.py:1-18"\nfinding = 3\n'
        'departure = { unit = "_utils.py:1-18", site = 5 }\nruled_outcome = "r"\n' + ROW_TAIL
    )
    _only_finding(_root(tmp_path, row), "departure", "_utils.py:1-18", "5")


def test_pointers_resolve_through_a_departure_entry_and_through_a_pair_entry_that_replaced_one(tmp_path):
    """Property: a pointer resolves through a departure entry, or through a pair entry carrying it as its `departure`.

    Class: the well-formed rows' three pointers — two to departure entries, one to the pair entry
    that replaced a fourth finding's departure once its pair was registered. With all three
    entries in the ledger nothing is named; removing the pair entry, or one departure entry,
    names exactly the row that pointed at it. Why: a fourth finding's departure entry becomes a
    pair entry when its pair is registered, so a resolution reading departure entries alone would
    name a closed departure as dangling. Killed by: resolving through departure entries only, or
    through pair entries only. Second correct implementation: one pass over both entry lists
    that yields each entry's `(unit, site)`.
    """
    benign = 'test = "equivalence:_utils.py:1-18"\nfinding = 1\n' + ROW_TAIL
    root = _root(tmp_path, benign)
    assert triage_log.load(root)[1] == []
    _ledger(root, pair=False)
    _only = triage_log.load(root)[1]
    assert len(_only) == 1 and _names(_only[0], "row", "5", "departure"), _only
    _ledger(root, departures=("row-4", "sixth"))
    _only = triage_log.load(root)[1]
    assert len(_only) == 1 and _names(_only[0], "row", "3", "departure"), _only


def test_the_summary_line_counts_the_pointers_that_resolve(tmp_path, capsys):
    """Property: the summary line reads `departure pointers: {resolved} resolved of {total}` over the log's well-formed pointers.

    Class: logs whose pointers all resolve and logs where some dangle. Why: the line is how a
    reader sees, without the findings, how much of the log is backed by the ledger; a constant
    or a count of rows instead of pointers would misstate it. Killed by: printing a fixed phrase,
    or counting rows. Second correct implementation: counting resolved pointers in `main` from
    `load()`'s findings instead of from the resolution itself.
    """
    root = _root(tmp_path, 'test = "equivalence:_utils.py:1-18"\nfinding = 1\n' + ROW_TAIL)
    assert triage_log.main(root) == 0
    assert "; departure pointers: 3 resolved of 3\n" in capsys.readouterr().out
    _ledger(root, departures=("row-3",), pair=False)
    assert triage_log.main(root) == 1
    assert "; departure pointers: 1 resolved of 3\n" in capsys.readouterr().out


def test_a_pointer_with_no_ledger_beside_the_log_is_named(tmp_path):
    """Property: a log holding a well-formed pointer and no divergence ledger under its root yields one finding saying so.

    Class: any root whose log carries at least one pointer and whose ledger file is absent. Why:
    an absent ledger leaves every pointer unresolvable, and reading that as "nothing dangles"
    would hide it; reading it as a crash would stop every other check. Killed by: skipping
    resolution when the ledger is absent. Second correct implementation: naming each pointer as
    dangling instead of naming the absent ledger once.
    """
    root = _root(tmp_path, 'test = "equivalence:_utils.py:1-18"\nfinding = 1\n' + ROW_TAIL)
    (root / triage_log.LEDGER_PATH).unlink()
    findings = triage_log.load(root)[1]
    assert len(findings) == 1 and "divergence ledger" in findings[0], findings


def test_the_ledger_the_fixture_writes_has_the_ledger_form(tmp_path):
    """Property: `validate` returns nothing on the divergence ledger `_root` writes beside every log.

    Class: the fixture ledger every `_root` arm reads, with all its departure entries and its pair
    entry. Why: the triage log reads that ledger with `load` only and never validates it, so a
    fixture that breaks the ledger's form would stay green on every resolution arm while proving
    resolution against a ledger no real ledger could be. Killed by: a fixture entry outside the
    form, such as its pair entry marked `diverges = false` while carrying a `departure`. Second
    correct implementation: validating the fixture ledger inside `_root` itself and raising there.
    """
    from tests.correspondence import divergence_ledger_load

    reader = divergence_ledger_load.reader()
    root = _root(tmp_path, 'test = "equivalence:_utils.py:1-18"\nfinding = 1\n' + ROW_TAIL)
    assert reader.validate(reader.load(root / triage_log.LEDGER_PATH)) == []
