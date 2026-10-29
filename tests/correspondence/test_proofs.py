"""Tests of the proof recorder and of the tracked proof records' format check.

The recorder tests drive in-process `pytester` sessions. The inner session loads the recorder
with `-p tests.correspondence.proofs`, and the binding as well where its inner test requests
`new_side`; a small conftest in the inner tree adds the recorder's two options, which the toolkit's
root configuration adds in the outer session. A synthetic registration module is written under
the inner tree and made importable as `tests.registrations.{name}` for the test's duration. Every
record file is written under the inner tree, by a path the inner run is given.
"""

import json
import sys

import pytest

import tests.registrations as registrations
from tests.correspondence import binding, proofs, write_root

pytestmark = pytest.mark.state_check

UNIT = "a.py:1-2"
INI = (
    "[pytest]\nmarkers =\n    equivalence(unit, system, part): a synthetic equivalence test\n"
    "    refactor_reminder(unit, system, part): a synthetic reminder\n"
)
INNER_CONFTEST = (
    'pytest_plugins = ["tests.correspondence.record"]\n\n\n'
    "def pytest_addoption(parser):\n    from tests.correspondence import proofs, record\n\n"
    "    proofs.add_options(parser)\n    record.add_options(parser)\n"
)
LICENCE = (
    "class Plus:\n"
    "    def admits(self, reference, value):\n"
    "        return value == reference\n"
    "\n"
    "    def outside(self, reference):\n"
    "        return reference + 1\n"
)


def _registrations(pytester, monkeypatch, request, modules):
    """Write `modules` as synthetic registration modules under the inner tree, importable for this test only."""
    place = pytester.path / "registrations_fixture"
    place.mkdir()
    for name, source in modules.items():
        (place / f"{name}.py").write_text(source)
        request.addfinalizer(lambda name=name: sys.modules.pop(f"{binding.REGISTRATIONS}.{name}", None))
    monkeypatch.setattr(registrations, "__path__", [*registrations.__path__, str(place)])


def _inner_run(pytester, body, *args, with_binding=False):
    """Run `body` as the inner tree's one test file with the recorder loaded, passing `args` to the inner run."""
    pytester.makeini(INI)
    pytester.makeconftest(INNER_CONFTEST)
    pytester.makepyfile(test_inner=body)
    plugins = ["-p", "tests.correspondence.proofs"] + (["-p", "tests.correspondence.binding"] if with_binding else [])
    return pytester.inline_run("-p", "no:cacheprovider", *plugins, *args)


def _lines(path):
    """The JSON lines of a record file."""
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_a_failure_not_raised_by_the_tests_own_comparison_is_not_an_assertion(pytester, monkeypatch, request):
    """Property: a failure that the test's own comparison did not raise is recorded as not an assertion, and never stops the run.

    Class: any failed call of an equivalence test whose last frame is not the test's own
    comparison — an `assert` raised inside registered new code; and a call that fails with no
    exception at all, as a test marked `xfail(strict=True)` that passes does, whose run must still
    record its other failures. Why: such a failure says nothing about whether the test's comparison
    catches a wrong value, so it proves nothing about the test; and a recorder that crashes on it
    ends the session with an internal error, losing every other record of the run. Killed by:
    classifying by exception type; and by reading the traceback of a failure that carries no
    exception. Second correct implementation: classifying by the frame's code object compared with
    the test function's, rather than by file path, beside a recorder that leaves an exception-free
    failure out of the record instead of recording it as not an assertion.
    """
    _registrations(
        pytester,
        monkeypatch,
        request,
        {
            "fixture_asserts": "from tests.correspondence.binding import registers\n\n\n"
            f"@registers(unit='{UNIT}')\ndef side(x):\n    assert False\n"
        },
    )
    body = (
        f"import pytest\n{LICENCE}\n\n@pytest.mark.equivalence(unit='{UNIT}', system='fixture-asserts')\n"
        "def test_one(new_side):\n    assert Plus().admits(4, new_side(2, reference=4, licence=Plus()))\n"
    )
    record = pytester.path / "proofs.jsonl"
    _inner_run(pytester, body, f"--proof-record={record}", with_binding=True)
    lines = _lines(record)
    assert [(line["assertion"], line["site"]) for line in lines] == [
        (False, "registrations_fixture/fixture_asserts.py:6")
    ], lines
    strict_xpass = (
        "import pytest\n\n\n@pytest.mark.xfail(strict=True)\n"
        f"@pytest.mark.equivalence(unit='{UNIT}', system='a-system')\ndef test_xpass():\n    pass\n\n\n"
        "@pytest.mark.equivalence(unit='b.py:1-2', system='a-system')\ndef test_fails():\n    assert 1 == 2\n"
    )
    second = pytester.path / "strict_xpass.jsonl"
    result = _inner_run(pytester, strict_xpass, f"--proof-record={second}")
    assert result.ret == 1, result.ret
    lines = _lines(second)
    assert [line["unit"] for line in lines if line["unit"] == "b.py:1-2"] == ["b.py:1-2"], lines
    assert not [line for line in lines if line["unit"] == UNIT and line["assertion"]], lines


COMPARE_HELPER = "def assert_equal(value, reference):\n    assert value == reference\n"


def test_a_failure_at_the_tests_own_comparison_is_an_assertion(pytester):
    """Property: a failure at the test's own comparison is recorded as an assertion, at the line that failed.

    Class: any equivalence test whose comparison fails where the test makes it — an `assert` in the
    test's own body, and an `assert` inside the shared comparison helper the test delegates to,
    `tests/correspondence/compare.py` under the session's root (here the inner tree carries its own
    copy, loaded by file path). Why: that is the authoring red a test written before its new code
    takes as its proof, so it must be recorded as one wherever the comparison is made. Killed by:
    recording every failure as not an assertion; and by accepting only the test's own file. Second
    correct implementation: comparing the last frame's module name with the test's module and the
    helper's, instead of their files.
    """
    body = f"import pytest\n\n\n@pytest.mark.equivalence(unit='{UNIT}', system='a-system')\ndef test_one():\n    assert 1 == 2\n"
    record = pytester.path / "proofs.jsonl"
    _inner_run(pytester, body, f"--proof-record={record}")
    lines = _lines(record)
    assert [(line["assertion"], line["site"]) for line in lines] == [(True, "test_inner.py:6")], lines
    helper = pytester.path / "tests" / "correspondence"
    helper.mkdir(parents=True)  # plain directories: the outer session already holds a `tests` package
    (helper / "compare.py").write_text(COMPARE_HELPER)
    body = (
        "import importlib.util\nfrom pathlib import Path\n\nimport pytest\n\n"
        "spec = importlib.util.spec_from_file_location(\n"
        "    'inner_compare', Path(__file__).parent / 'tests' / 'correspondence' / 'compare.py'\n)\n"
        "compare = importlib.util.module_from_spec(spec)\nspec.loader.exec_module(compare)\n\n\n"
        f"@pytest.mark.equivalence(unit='{UNIT}', system='a-system')\n"
        "def test_one():\n    compare.assert_equal(1, 2)\n"
    )
    record = pytester.path / "helper.jsonl"
    _inner_run(pytester, body, f"--proof-record={record}")
    lines = _lines(record)
    assert [(line["assertion"], line["site"]) for line in lines] == [(True, "tests/correspondence/compare.py:2")], lines


def test_nothing_is_written_without_an_explicit_record_path(pytester):
    """Property: a run given neither record option leaves no file behind, even when an equivalence test fails.

    Class: any run of the suite without `--proof-record` or `--run-record`. Why: an ordinary run
    must not write into the checkout, and a record nobody asked for would be read as evidence.
    Killed by: writing to a default path. Second correct implementation: holding the records in
    memory and writing them only at session end when a path was given.
    """
    body = f"import pytest\n\n\n@pytest.mark.equivalence(unit='{UNIT}', system='a-system')\ndef test_one():\n    assert 1 == 2\n"
    pytester.makeini(INI)
    pytester.makeconftest(INNER_CONFTEST)
    pytester.makepyfile(test_inner=body)
    before = sorted(p.relative_to(pytester.path) for p in pytester.path.rglob("*") if "__pycache__" not in p.parts)
    pytester.inline_run("-p", "no:cacheprovider", "-p", "tests.correspondence.proofs")
    after = sorted(p.relative_to(pytester.path) for p in pytester.path.rglob("*") if "__pycache__" not in p.parts)
    assert after == before, sorted(set(after) - set(before))


def _records_file(tmp_path, rows):
    """A synthetic root holding one equivalence test of `UNIT` and a proof-record file with `rows`."""
    (tmp_path / "tests" / "correspondence").mkdir(parents=True)
    (tmp_path / "tests" / "test_x.py").write_text(
        f"import pytest\n\n\n@pytest.mark.equivalence(unit='{UNIT}', system='a-system')\ndef test_one():\n    pass\n"
    )
    (tmp_path / "tests" / "correspondence" / "proofs.toml").write_text('rule = "proof-record:1"\n' + rows)
    return tmp_path


def test_a_proof_rows_route_is_one_of_two(tmp_path):
    """Property: a proof row whose route is neither `authoring-red` nor `mutation` is a finding naming the row and `route`.

    Class: any route value outside the two ways a test is proven. Why: the port-state count reads
    a row as a proof, so a row recording an unknown route would count a test proven by nothing
    stated. Killed by: accepting any string. Second correct implementation: a schema check of the
    whole row that rejects the field among others.
    """
    root = _records_file(tmp_path, f'[[proof]]\nfamily = "equivalence"\nunit = "{UNIT}"\nroute = "luck"\n')
    _rows, findings = proofs.load(root)
    assert any({"1:", "route"} <= set(f.split()) for f in findings), findings


def test_a_proof_row_names_an_existing_test(tmp_path):
    """Property: a proof row for a test the inventory does not list is a finding naming the row.

    Class: any proof row whose (family, unit, part) matches no listed test. Why: a proof of a test
    that does not exist inflates the proven count. Killed by: skipping the inventory lookup.
    Second correct implementation: collecting the tree with pytest and matching node markers.
    """
    root = _records_file(tmp_path, '[[proof]]\nfamily = "equivalence"\nunit = "x.py:1-2"\nroute = "mutation"\n')
    _rows, findings = proofs.load(root)
    assert any({"1:", "equivalence:x.py:1-2"} <= set(f.split()) for f in findings), findings


def test_the_run_record_carries_whether_each_test_was_bound(pytester, monkeypatch, request):
    """Property: the run record states, per family test, whether `new_side` reached a registered callable.

    Class: any run mixing registered and unregistered units. Why: a unit counts as ported only
    when its tests passed against registered code, so a passing run against a placeholder must not
    read as bound. Killed by: writing `bound = true` always. Second correct implementation: deriving
    `bound` afterwards from the registration modules the static reader lists.
    """
    _registrations(
        pytester,
        monkeypatch,
        request,
        {
            "fixture_bound": "from tests.correspondence.binding import registers\n\n\n"
            f"@registers(unit='{UNIT}')\ndef side(x):\n    return 2 * x\n"
        },
    )
    body = f"import pytest\n{LICENCE}\n\n"
    for name, system in (("test_registered", "fixture-bound"), ("test_unregistered", "unregistered-system")):
        body += (
            f"@pytest.mark.equivalence(unit='{UNIT}', system='{system}')\n"
            f"def {name}(new_side):\n    assert Plus().admits(4, new_side(2, reference=4, licence=Plus()))\n\n\n"
        )
    record = pytester.path / "run.jsonl"
    _inner_run(pytester, body, f"--run-record={record}", with_binding=True)
    lines = _lines(record)
    assert [(line["outcome"], line["bound"]) for line in lines] == [("passed", True), ("failed", False)], lines


RECORD_PAIRS = (
    ("--proof-record", "--run-record"),
    ("--proof-record", "--result-record"),
    ("--run-record", "--result-record"),
)


def _refusal_run(pytester, body, *args):
    """Run `body` as the inner tree's one test file with the recorder loaded, keeping both output streams.

    `runpytest` rather than `inline_run`: a refusal prints to the terminal and to stderr, and only
    the in-process `RunResult` keeps both for assertion. The inner conftest declares all three record
    options, so an arm may pass any of them without the inner run refusing the argument itself.
    """
    pytester.makeini(INI)
    pytester.makeconftest(INNER_CONFTEST)
    pytester.makepyfile(test_inner=body)
    return pytester.runpytest("-p", "no:cacheprovider", "-p", "tests.correspondence.proofs", *args)


def _output(result):
    """Everything a run printed, stdout first and stderr after, so one substring check covers both."""
    return "\n".join(result.outlines) + "\n" + "\n".join(result.stderr.lines)


@pytest.mark.parametrize(("first", "second"), RECORD_PAIRS)
def test_a_pair_of_record_options_naming_one_path_is_refused(pytester, first, second):
    """Property: any pair among the three record options that resolves to one path is refused, naming both flags and the path.

    Class: every pair formed from the three record channels, which is the finite population the
    refusal ranges over -- each pair nameable here is one member, so a refusal wired to only the
    proof/run pair fails this test on the two members it does not cover. Why: the channels write
    differently, so one path serving two of them means one channel's write destroys another's
    record, and which survives depends on hook order. Killed by: refusing only the pair the decision
    text names (proof/run); resolving the options nowhere, so two spellings of one file are not
    caught; and asserting on the exit code alone, which an undeclared option also produces. Second
    correct implementation: one that compares `os.path.realpath` of the option values instead of
    `Path.resolve` refuses the same pairs, provided the message names both flags.
    """
    record = pytester.path / "one.jsonl"
    result = _refusal_run(pytester, "def test_ok():\n    pass\n", f"{first}={record}", f"{second}={record}")
    out = _output(result)
    assert result.ret == 4, out
    assert "INTERNALERROR" not in out, out
    assert first in out and second in out, out
    assert str(record.resolve()) in out, out
    assert "unrecognized arguments" not in out, out
    assert not record.exists(), record


def test_distinct_record_paths_are_not_refused(pytester):
    """Property: three record options naming three different paths run normally.

    Class: the satisfying input for the refusal -- every pair distinct, which is the configuration
    an invoker actually uses. Why: a refusal that fires on distinct paths makes every recorded run
    fail, so the arm is what separates a pairwise check from a "more than one record option" check.
    Killed by: refusing whenever two record options are set; comparing option VALUES without
    resolving, which refuses two spellings of one path but also nothing else -- the negative
    direction is what this arm holds. Second correct implementation: one that refuses only on a
    collision found by set cardinality over resolved paths behaves identically here.
    """
    paths = [pytester.path / name for name in ("proofs.jsonl", "run.jsonl", "result.json")]
    result = _refusal_run(
        pytester,
        "def test_ok():\n    pass\n",
        f"--proof-record={paths[0]}",
        f"--run-record={paths[1]}",
        f"--result-record={paths[2]}",
    )
    out = _output(result)
    assert result.ret == 0, out
    assert "name one resolved path" not in out, out


def test_two_spellings_of_one_record_path_are_refused(pytester):
    """Property: a pair whose values are two spellings of one record file is refused, so the check compares resolved paths and not the text typed.

    Class: any pair among the three record options whose values differ as text and resolve to one
    file -- here a symlink and its target, the spelling difference only resolution can see, beside
    the same-literal colliding input and the distinct-path input the arms above already carry.
    Why: two channels must not share a file, and an invocation that reaches one file through a link
    collides exactly as one that types the same path twice; a check comparing the values as typed
    admits it, and the truncating channel then destroys the appending channel's record. The link's
    target is asserted absent because that is what distinguishes a refusal from a run that wrote.
    Killed by: comparing the option values as typed (`Path(value)` in place of
    `Path(value).resolve()`). Second correct implementation: comparing `os.path.realpath` of both
    values refuses the same pairs, provided the message names both flags.
    """
    target = pytester.path / "one.jsonl"
    link = pytester.path / "link.jsonl"
    link.symlink_to(target)
    result = _refusal_run(pytester, "def test_ok():\n    pass\n", f"--proof-record={link}", f"--run-record={target}")
    out = _output(result)
    assert result.ret == 4, out
    assert "--proof-record" in out and "--run-record" in out, out
    assert "unrecognized arguments" not in out, out
    assert not target.exists(), out


def test_the_admission_gate_classifies_the_resolved_proof_record_path(pytester, monkeypatch):
    """Property: the admission gate reads the RESOLVED proof-record path, so a link into a refused mount is refused as its target is.

    Class: a proof-record option whose value is a symlink under an admitted mount pointing into a
    mount the predicate refuses -- the spelling difference is the whole input, and the arm beside it
    (a plain path on a refused mount) cannot reach it, because there the raw and the resolved
    spelling answers agree. Why: the bytes land on the target's filesystem, not on the link's, so a
    gate classifying the text typed admits a shared root it cannot make safe; the target is asserted
    absent because a refusal and a run that wrote are otherwise indistinguishable in the output.
    Killed by: calling `admitted()` with the raw option value instead of the resolved one. Second
    correct implementation: one that resolves inside `admitted()` before classifying answers the
    same and refuses the same path.
    """
    portal = pytester.path / "portal"
    portal.mkdir()
    target = portal / "proofs.jsonl"
    link = pytester.path / "link.jsonl"
    link.symlink_to(target)
    fixture_table = pytester.path / "mounts"
    fixture_table.write_text(
        f"/dev/root / ext4 rw,relatime 0 0\nportaldev {portal} fuse.portal rw,nosuid 0 0\n", encoding="utf-8"
    )
    measured = write_root.filesystem_class

    def _fixture_class(path, table=None):
        """Read the fixture table whatever table `admitted` passes, which is its import-time default."""
        return measured(path, fixture_table)

    monkeypatch.setattr(write_root, "filesystem_class", _fixture_class)
    result = _refusal_run(pytester, "def test_ok():\n    pass\n", f"--proof-record={link}")
    out = _output(result)
    assert result.ret == 4, out
    assert "--proof-record" in out, out
    assert "fuse.portal" in out, out
    assert not target.exists(), out


def test_an_inadmissible_proof_record_path_is_refused_naming_the_measured_type(pytester, monkeypatch):
    """Property: a proof-record path on a network mount is refused at configure time, and the refusal names the type measured.

    Class: a proof path the write-root predicate refuses, injected through a fixture mount table
    reporting `nfs4`, with the distinct-path arm above as the satisfying input on an admitted mount.
    Why: the proof record is the one shared root this tree declares, so a path the predicate will not
    admit must stop the run rather than be written; and the reason must name the type, or an invoker
    cannot tell a network mount from a typo. Killed by: skipping the admission gate; applying it to
    the RAW option value rather than the resolved one; and refusing with a message that omits the
    measured type. Second correct implementation: one that calls the predicate at `pytest_configure`
    with the resolved path and formats the same refusal answers identically.
    """
    nfs = pytester.path / "nfs"
    fixture_table = pytester.path / "mounts"
    fixture_table.write_text(
        f"/dev/root / ext4 rw,relatime 0 0\nhost:/export {nfs} nfs4 rw,vers=4 0 0\n", encoding="utf-8"
    )
    measured = write_root.filesystem_class

    def _fixture_class(path, table=None):
        """Read the fixture table whatever table `admitted` passes, which is its import-time default."""
        return measured(path, fixture_table)

    monkeypatch.setattr(write_root, "filesystem_class", _fixture_class)
    result = _refusal_run(pytester, "def test_ok():\n    pass\n", f"--proof-record={nfs / 'proofs.jsonl'}")
    out = _output(result)
    assert result.ret == 4, out
    assert "INTERNALERROR" not in out, out
    assert "--proof-record" in out, out
    assert "nfs4" in out, out
    assert not (nfs / "proofs.jsonl").exists(), out


def test_an_over_bound_record_line_is_refused_through_the_usage_channel(pytester):
    """Property: a record line the bound refuses ends the run with a named error, and appends no line.

    Class: one over-long record line -- here a unit literal longer than the 4096-byte bound, which
    reaches the recorder through the marker the inventory reads; the satisfying input is any unit
    under the bound, which every other arm in this file exercises. Why: an untranslated raise from a
    hook is an INTERNALERROR with a traceback that abandons the rest of the run, while the usage
    channel names the option and the observed length. Killed by: letting `RecordLineTooLong` escape
    untranslated; translating it into a plain `ValueError`; and appending a truncated line instead of
    refusing. Second correct implementation: one that catches the error at the hook and raises the
    same usage error there, rather than inside `_append`, matches the exit code and the message.
    """
    record = pytester.path / "proofs.jsonl"
    body = (
        "import pytest\n\n\n"
        f"@pytest.mark.equivalence(unit={'u' * 4100!r}, system='a-system')\n"
        "def test_one():\n    assert 1 == 2\n"
    )
    result = _refusal_run(pytester, body, f"--proof-record={record}")
    out = _output(result)
    assert result.ret == 4, out
    assert "INTERNALERROR" not in out, out
    assert "--proof-record" in out, out
    assert "exceeds the 4096-byte bound" in out, out
    assert not record.exists(), record
