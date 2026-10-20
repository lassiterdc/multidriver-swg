"""Tests of the detector: the real tree reports nothing, each case fires alone on its own input, and the gate stops the cases.

Every synthetic tree has the toolkit's layout under `tmp_path`: a package `src/multidriver_swg`
with one module declaring pair `p`, a registration module `tests/registrations/x.py`, and a
divergence ledger beside the package. The SATISFYING tree records labels computed from that same
source, so the matched pair (marked `diverges = false`), the departure entry whose fingerprint
matches, and the departure entry for an unregistered unit with no fingerprint all report nothing.
Each case's test then breaks exactly one thing, and asserts that exactly that case fires and that
its finding names the entry. Nothing synthetic is imported: the detector reads source text.
"""

from pathlib import Path

import pytest

from tests.correspondence import divergence, fingerprint, pair_scan

pytestmark = pytest.mark.state_check

PKG = "multidriver_swg"
SIDES_SOURCE = """from multidriver_swg.pairs import pair_side


def helper(x):
    return x + 1


def target(x):
    return x * 2


@pair_side(pair="p", side="pre_refactor")
def old(x):
    return helper(x)


@pair_side(pair="p", side="current")
def new(x):
    return x + 1
"""
REGISTRATION_SOURCE = """from multidriver_swg.sides import target
from tests.correspondence.binding import registers


@registers(unit="_u.py:1-2")
def reg(x):
    return target(x)
"""
DEPARTURE_TAIL = 'kind = "finding-3"\ngrounds = { triage = "equivalence:_u.py:1-2" }\nnew_code = "n"\nwhy = "w"\njudged_correct_when = "j"\n'


def _table(closure, function: str) -> str:
    """A TOML inline table recording `closure` for `function`: its function name, combined label and per-function labels."""
    functions = ", ".join(f'"{k}" = "{v}"' for k, v in sorted(closure.functions.items()))
    return f'{{ function = "{function}", combined = "{closure.combined}", functions = {{ {functions} }} }}'


def _tree(
    tmp_path: Path, pair_entry: bool = True, extra_entries: str = "", registered_fingerprint: bool = True
) -> Path:
    """Write the synthetic tree and a ledger whose labels match its source; return the tree's root."""
    package = tmp_path / "src" / PKG
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sides.py").write_text(SIDES_SOURCE, encoding="utf-8")
    registrations = tmp_path / "tests" / "registrations"
    registrations.mkdir(parents=True, exist_ok=True)
    (registrations / "x.py").write_text(REGISTRATION_SOURCE, encoding="utf-8")
    old = pair_scan.closure(package / "sides.py", "old", package, PKG)
    new = pair_scan.closure(package / "sides.py", "new", package, PKG)
    reg = pair_scan.closure(registrations / "x.py", "reg", package, PKG)
    body = 'rule = "divergence-ledger:1"\nstatement = "s"\n'
    if pair_entry:
        body += (
            f'\n[[pair]]\nid = "p"\npre_refactor = {_table(old, "multidriver_swg.sides:old")}\n'
            f"current = {_table(new, 'multidriver_swg.sides:new')}\ndiverges = false\n"
        )
    body += '\n[[departure]]\nunit = "_u.py:1-2"\nsite = 1\n' + DEPARTURE_TAIL
    if registered_fingerprint:
        body += f"fingerprint = {_table(reg, 'tests.registrations.x:reg').replace('function =', 'callable =', 1)}\n"
    body += '\n[[departure]]\nunit = "_v.py:1-2"\nsite = 1\n' + DEPARTURE_TAIL + extra_entries
    (package / "divergence_ledger.toml").write_text(body, encoding="utf-8")
    return tmp_path


def _edit(root: Path, old: str, new: str, module: str = "src/multidriver_swg/sides.py") -> None:
    """Replace `old` with `new` once in one synthetic module, after the ledger was recorded."""
    path = root / module
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, old
    path.write_text(text.replace(old, new), encoding="utf-8")


def _only(root: Path, case: str, *tokens: str) -> None:
    """Assert the detector reports exactly one finding, of `case`, naming every token."""
    findings = divergence.detect(root).findings
    assert len(findings) == 1 and findings[0].startswith(f"{case}:") and all(t in findings[0] for t in tokens), findings


def test_this_tree_has_no_divergence_finding():
    """Property: on the toolkit's own tree the detector reports no finding.

    Class: the real tree at any number of pairs and ledger entries. Why: this is the detector's
    first mount; a finding here means a ledger record and the code it describes disagree. It
    asserts the absence of findings, a relation that holds at any count, never a count of pairs.
    Killed by: a detector that reports on a consistent tree. Second correct implementation: the
    same comparison computed per ledger entry rather than per scanned pair.
    """
    report = divergence.detect()
    assert report.findings == [], report.findings


def test_a_ledger_that_matches_its_code_reports_nothing(tmp_path):
    """Property: the satisfying tree — matched pair, matched departure fingerprint, unregistered unit without one — reports nothing.

    Class: every case's satisfying input at once: a `diverges = false` pair whose labels match (P1,
    P2), a pair both scanned and recorded (P3, P4), a departure whose fingerprint matches its
    callable (D1), a registered unit whose entry records its fingerprint and an unregistered unit
    whose entry records none (D2). Why: every case test below rests on this tree being clean.
    Killed by: any case firing on a consistent record. Second correct implementation: comparing
    printed labels as whole strings rather than per function.
    """
    report = divergence.detect(_tree(tmp_path))
    assert report.findings == [], report.findings


def test_p1_a_changed_current_side_names_the_moved_function(tmp_path):
    """Property: a change to the current side's code after it was recorded fires P1, naming the entry and the moved function.

    Class: any edit that moves a function in the current side's closure. Why: the pair entry
    vouches for the current sister as it was; once its code moves, the entry's reasons may no
    longer be true. Killed by: not comparing the current side. Second correct implementation:
    comparing only the combined label and then naming the differing functions.
    """
    root = _tree(tmp_path)
    _edit(root, "def new(x):\n    return x + 1", "def new(x):\n    return x + 2")
    _only(root, "P1", "pair entry p", "multidriver_swg.sides:new")


def test_p2_a_changed_pre_refactor_side_names_the_moved_function(tmp_path):
    """Property: a change to a function the pre-refactor side reaches fires P2, naming the entry and that function.

    Class: an edit to a function reached through a call from the pre-refactor side, here `helper`.
    Why: the pre-refactor side is the reference the pair is compared against, so its move is as
    stale as the current side's. Killed by: comparing only the side's own function. Second correct
    implementation: one comparison loop over both sides with the case named by side.
    """
    root = _tree(tmp_path)
    _edit(root, "def helper(x):\n    return x + 1", "def helper(x):\n    return x + 3")
    _only(root, "P2", "pair entry p", "multidriver_swg.sides:helper")


GHOST = (
    '\n[[pair]]\nid = "ghost"\n'
    f'pre_refactor = {{ function = "m:f", combined = "canon-ast:1:{"a" * 64}", functions = {{ "m:f" = "canon-ast:1:{"a" * 64}" }} }}\n'
    f'current = {{ function = "m:g", combined = "canon-ast:1:{"a" * 64}", functions = {{ "m:g" = "canon-ast:1:{"a" * 64}" }} }}\n'
    "diverges = false\n"
)


def test_p3_a_pair_entry_naming_no_scanned_pair_is_named(tmp_path):
    """Property: a pair entry whose id is no scanned pair fires P3, naming the entry.

    Class: any pair entry left behind after its pair was removed or renamed. Why: a record of a
    pair that no longer exists describes nothing and hides that the pair went away. Killed by: not
    checking entry ids against the scan. Second correct implementation: the set difference of
    recorded and scanned ids.
    """
    _only(_tree(tmp_path, extra_entries=GHOST), "P3", "ghost")


def test_p4_a_scanned_pair_without_an_entry_names_both_sites(tmp_path):
    """Property: a scanned pair with no pair entry fires P4, naming both of its sites.

    Class: any registered pair the ledger does not record. Why: every pair needs a pair entry or
    an explicit no-divergence marker, so an unrecorded pair is a departure nobody explained.
    Killed by: not checking scanned pairs against the entries. Second correct implementation:
    the set difference of scanned and recorded ids, naming each site from the scan.
    """
    _only(_tree(tmp_path, pair_entry=False), "P4", "scanned pair p", "sides.py:12", "sides.py:17")


def test_d1_a_changed_callable_closure_names_the_moved_function(tmp_path):
    """Property: a change to the code a departure entry's callable reaches fires D1, naming the entry and the moved function.

    Class: an edit to any function in the recorded callable's closure, here the package function
    the registration callable calls. Why: the departure entry vouches for the new code as it was
    when registered. Killed by: not comparing departure fingerprints. Second correct
    implementation: comparing the recorded combined label first and naming functions only on a
    mismatch.
    """
    root = _tree(tmp_path)
    _edit(root, "def target(x):\n    return x * 2", "def target(x):\n    return x * 4")
    _only(root, "D1", "_u.py:1-2", "multidriver_swg.sides:target")


def test_d2_a_registered_unit_whose_entry_records_no_fingerprint_is_named(tmp_path):
    """Property: a departure entry for a registered unit that records no fingerprint fires D2, naming the entry and the module.

    Class: every unit an owning system has registered whose departure entry still lacks a
    fingerprint; and, in the same tree, an unregistered unit without one, which does not fire.
    Why: once new code is registered, its departure entry must fingerprint it, or a later change
    to that code moves no record. Killed by: not consulting the registration modules. Second
    correct implementation: reading registered units from the registry at import, in a child.
    """
    _only(_tree(tmp_path, registered_fingerprint=False), "D2", "_u.py:1-2", "tests.registrations.x")


def test_the_printer_gate_reports_once_and_measures_no_case(tmp_path, monkeypatch):
    """Property: when the printer check fails, the detector reports the single gate finding and no case, on a tree where P3 fires.

    Class: any interpreter that prints the fixed function differently. Why: every fingerprint
    would then read as moved, so the cases would report the printer, not the code. The control,
    in the same test, shows P3 firing with the gate passing, so the gate's silence on P3 is a
    suppression and not an absence. Killed by: running the cases after a failed gate. Second
    correct implementation: running the cases and discarding their findings when the gate fails.
    """
    root = _tree(tmp_path, extra_entries=GHOST)
    _only(root, "P3", "ghost")
    monkeypatch.setattr(fingerprint, "printer_check", lambda: "printer check failed under Python 0.0")
    findings = divergence.detect(root).findings
    assert len(findings) == 1 and findings[0].startswith("G:") and "Python 0.0" in findings[0], findings


def test_a_one_sided_pair_in_the_package_surfaces_as_a_scan_finding(tmp_path):
    """Property: a pair declared with one side only appears among the detector's findings as a scan finding.

    Class: any pair id with a single declared side. Why: the scan's structural findings are the
    detector's findings, evaluated before the cases, so a malformed declaration stops the build.
    Killed by: dropping the scan's findings from the report. Second correct implementation:
    reporting scan findings after the cases.
    """
    root = _tree(tmp_path)
    (root / "src" / PKG / "solo.py").write_text(
        'from multidriver_swg.pairs import pair_side\n\n\n@pair_side(pair="solo", side="current")\ndef f(x):\n    return x\n',
        encoding="utf-8",
    )
    findings = divergence.detect(root).findings
    assert any(f.startswith("scan:") and "solo" in f for f in findings), findings
