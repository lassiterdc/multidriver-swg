"""Tests of the extraction generator: its population, its stored texts, its record, its removals and its refusal.

Every test builds a git repository in `tmp_path` holding a two-section corpus record, one tracked
file per section, a ledger of three rows, and an `extractions/LICENSE`; files are staged and
never committed. The git location variables a hook exports are removed first so a lookup reads
the temporary repository. The real run of the generator is not a test.

Every test class here is a NEW CAPABILITY: no generator existed, so a failure is shown by the
wrong implementation a docstring names.
"""

import tomllib
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, extraction, generate_extractions

pytestmark = pytest.mark.state_check

GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")
SOURCE_A = b"def f():\n    return 1\n\n\ndef g():\n    return 2\n"
SOURCE_B = b"def h():\n    return 3\n"
LICENSE_TEXT = b"MIT notice, untouched\n"
RECORD = """rule = "corpus-record:1"

[[section]]
name = "alpha"
directory = "legacy_a"

[[section.file]]
path = "mod_a.py"
group = "retained"

[[section]]
name = "beta"
directory = "legacy_b"

[[section.file]]
path = "mod_b.py"
group = "retained"
"""


def _keys() -> dict[str, str]:
    """Return the canonical keys of the two functions of `mod_a.py`, computed by the extent rule itself."""
    found, _ = extent.extents("mod_a.py", SOURCE_A.decode())
    return {f["name"]: f["key"] for f in found}


def _row(key: str, disposition: str) -> str:
    return f'[[row]]\nkey = "{key}"\ndisposition = "{disposition}"\nreason = "r"\n\n'


def _tree(tmp_path: Path, monkeypatch, dispositions=(("f", "scope-in"), ("g", "scope-in")), reverse=False) -> Path:
    """Build the temporary repository; `reverse` creates the two ledger files in the opposite order."""
    import subprocess

    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    for directory, name, data in (("legacy_a", "mod_a.py", SOURCE_A), ("legacy_b", "mod_b.py", SOURCE_B)):
        (tmp_path / directory).mkdir()
        (tmp_path / directory / name).write_bytes(data)
    keys = _keys()
    ledger_dir = tmp_path / "tests" / "correspondence" / "ledger"
    ledger_dir.mkdir(parents=True)
    first = 'legacy_file = "mod_a.py"\n\n' + "".join(_row(keys[n], d) for n, d in dispositions)
    second = 'legacy_file = "mod_z.py"\n\n' + _row("mod_z.py:1-2", "scope-in")
    files = [("mod_a.toml", first), ("mod_z.toml", second)]
    for name, text in reversed(files) if reverse else files:
        (ledger_dir / name).write_text(text, encoding="utf-8")
    (tmp_path / extraction.STORED_DIR).mkdir(parents=True)
    (tmp_path / extraction.STORED_DIR / "LICENSE").write_bytes(LICENSE_TEXT)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _tree_snapshot(root: Path) -> dict[str, bytes]:
    out = {}
    for sub in (extraction.STORED_DIR, generate_extractions.RECORD_PATH):
        base = root / sub
        paths = [base] if base.is_file() else sorted(p for p in base.rglob("*") if p.is_file())
        out.update({p.relative_to(root).as_posix(): p.read_bytes() for p in paths})
    return out


def _drop_mod_z(root: Path) -> None:
    """Remove the third ledger row's file so that the temporary ledger holds the rows a test names."""
    (root / "tests" / "correspondence" / "ledger" / "mod_z.toml").unlink()


def test_a_scope_in_range_is_written_and_nothing_else(tmp_path, monkeypatch):
    """Property: one stored text per range of each scope-in key, equal to the cut, and none for a discard row.

    Class: a ledger with two scope-in rows and one discard row in the first section and a
    second section whose file is never read. Why it is required: the stored texts are the
    population the fidelity check ranges over. Killed by: writing a text for every row, or
    slicing the file instead of calling `cut` (the CRLF-free sources here would hide the
    second, which the reviewer's cut tests cover). Second correct implementation: collecting
    ranges into a set before writing passes.
    """
    root = _tree(tmp_path, monkeypatch, (("f", "scope-in"), ("g", "discard")))
    _drop_mod_z(root)
    figures = generate_extractions.generate(root)
    keys = _keys()
    first, last = (int(x) for x in keys["f"].split(":")[1].split("-"))
    expected = extraction.cut(root, "alpha", "mod_a.py", first, last)
    written = _tree_snapshot(root)
    path = extraction.stored_path("alpha", "mod_a.py", first, last)
    assert written[path] == expected
    texts = [p for p in written if p.endswith(".txt")]
    assert texts == [path]
    assert figures == {"keys": 1, "ranges": 1, "lines": len(expected.splitlines()), "removed": 0}
    assert not any("beta" in p for p in written)


def test_the_record_holds_both_labelled_digests_and_the_regenerating_command(tmp_path, monkeypatch):
    """Property: each record row names its section, file, range, path and the two labelled digests, and the header names the command.

    Class: every written text. Why it is required: the check compares three values and the
    record is the third. Killed by: a missing label, a digest of the wrong bytes, or a header
    without the command. Second correct implementation: computing the stored digest from the
    in-memory bytes passes, since the bytes are equal on a faithful write. Not established: that
    the stored digest is taken from the file as read back, which no faithful write can show.
    """
    root = _tree(tmp_path, monkeypatch)
    _drop_mod_z(root)
    generate_extractions.generate(root)
    text = (root / generate_extractions.RECORD_PATH).read_text(encoding="utf-8")
    assert f"#   {generate_extractions.COMMAND}" in text
    rows = tomllib.loads(text)["extraction"]
    assert len(rows) == 2
    for row in rows:
        data = (root / row["path"]).read_bytes()
        assert row["cut"] == extraction.label("cut-lines", data)
        assert row["stored"] == extraction.label("stored-text", data)
        assert row["path"] == extraction.stored_path(row["section"], row["file"], row["first"], row["last"])


def test_two_runs_with_ledger_files_created_in_opposite_orders_are_byte_identical(tmp_path, monkeypatch):
    """Property: the texts and the record do not depend on the order the ledger files were created in, and a re-run changes nothing.

    Class: two roots built identically except for the creation order of their ledger files, and a
    second run over one root. Why it is required: a record that moved with directory order would
    show as a diff on every regeneration. Killed by: iterating the ledger directory unsorted
    (the creation order is the iteration order on many filesystems), or emitting rows in key
    order of discovery. Second correct implementation: sorting the final rows only passes.
    """
    one = _tree(tmp_path / "one", monkeypatch)
    two = _tree(tmp_path / "two", monkeypatch, reverse=True)
    for root in (one, two):
        _drop_mod_z(root)
        generate_extractions.generate(root)
    snapshot = _tree_snapshot(one)
    assert snapshot == _tree_snapshot(two)
    generate_extractions.generate(one)
    assert _tree_snapshot(one) == snapshot
    assert len(snapshot) > 2


def test_two_processes_with_different_hash_seeds_write_identical_bytes(tmp_path, monkeypatch):
    """Property: the run's output does not depend on the interpreter's string-hash seed.

    Class: two separate interpreters run as the command line does, with `PYTHONHASHSEED` 1 and
    2, over two identical roots. Why it is required: a set iterated in hash order would reorder
    the record between runs, and an in-process comparison cannot see that because both runs
    share one seed. Killed by: iterating the set of ranges unsorted. Second correct
    implementation: building the rows in a sorted list from the start passes.
    """
    import os
    import subprocess
    import sys

    outputs = []
    for seed in ("1", "2"):
        root = _tree(tmp_path / seed, monkeypatch)
        _drop_mod_z(root)
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONDONTWRITEBYTECODE": "1"}
        run = subprocess.run(
            [sys.executable, "-m", "tests.correspondence.generate_extractions", "--root", str(root)],
            cwd=generate_extractions.REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
        )
        assert run.returncode == 0, run.stderr
        outputs.append(_tree_snapshot(root))
    assert outputs[0] == outputs[1]
    assert len(outputs[0]) > 2


def test_license_is_never_touched_and_a_stale_text_is_removed_and_named(tmp_path, monkeypatch, capsys):
    """Property: `LICENSE` keeps its bytes, a `.txt` whose range is no longer cited is deleted and named, and an orphan non-text file is left.

    Class: a stale stored text in its own directory, an extra file of another suffix, and the
    licence. Why it is required: the notice must stay beside the copies, and a removed range must
    not leave a copy that the check would then call an orphan. Killed by: clearing the whole
    directory, or never deleting. Second correct implementation: deleting every `.txt` under
    the directory that is not in the freshly computed population passes. Not established: a
    deletion keyed on the previous record's rows, which this input does not admit, because the
    stale text is placed by hand and no earlier record row names it.
    """
    root = _tree(tmp_path, monkeypatch)
    _drop_mod_z(root)
    stale = root / extraction.STORED_DIR / "alpha" / "mod_a" / "900-901.txt"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"old\n")
    keep = root / extraction.STORED_DIR / "NOTE.md"
    keep.write_bytes(b"not a stored text\n")
    figures = generate_extractions.generate(root)
    assert figures["removed"] == 1
    assert not stale.exists()
    assert "alpha/mod_a/900-901.txt" in capsys.readouterr().err
    assert keep.read_bytes() == b"not a stored text\n"
    assert (root / extraction.STORED_DIR / "LICENSE").read_bytes() == LICENSE_TEXT


def test_an_unresolvable_key_is_refused_by_name_and_nothing_is_written(tmp_path, monkeypatch):
    """Property: a scope-in key the extent rule does not know is refused naming the key, and no text or record is written.

    Class: the third ledger row (a key of a file that is not retained), with the first section's
    good rows beside it as the control. Why it is required: a half-written population would look
    complete. Killed by: skipping unresolved keys, or writing before resolving. Second correct
    implementation: resolving all keys first, then cutting, passes.
    """
    root = _tree(tmp_path, monkeypatch)
    before = _tree_snapshot(root)
    with pytest.raises(extraction.ExtractionError, match="mod_z.py:1-2"):
        generate_extractions.generate(root)
    assert _tree_snapshot(root) == before
    assert generate_extractions.main(root) == 1


def test_main_prints_the_one_summary_line(tmp_path, monkeypatch, capsys):
    """Property: the run prints exactly one line, naming keys, ranges, lines written and texts removed, and returns 0.

    Class: a successful run on the two-key ledger. Why it is required: the line is what the
    coordinator reports. Killed by: printing per-file lines. Second correct implementation:
    formatting the line from the returned figures passes.
    """
    root = _tree(tmp_path, monkeypatch)
    _drop_mod_z(root)
    assert generate_extractions.main(root) == 0
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert out.startswith("extractions: keys 2, ranges 2, lines written ")
    assert out.endswith("texts removed 0\n")


def test_a_second_sections_stored_copy_is_swept_as_stale_and_named(tmp_path, monkeypatch, capsys):
    """Property: a `.txt` under another section's directory of `extractions/` is deleted and named, like any stale text.

    Class: a stored copy under the second section, which the population never cites. Why it is
    required: the module docstring states that the sweep covers every `.txt` there and not the
    first section alone, and the shape row says a text whose range is no longer in the
    population is deleted and said. Killed by: limiting the sweep to the first section's
    directory. Second correct implementation: deleting every `.txt` under the
    directory that is not in the freshly computed population passes. Not established: a deletion
    keyed on the previous record's rows, which this input does not admit, because the second
    section's copy is placed by hand and no earlier record row names it.
    Not established: that a later corpus section should be swept by this run.
    """
    root = _tree(tmp_path, monkeypatch)
    _drop_mod_z(root)
    other = root / extraction.STORED_DIR / "beta" / "mod_b" / "1-2.txt"
    other.parent.mkdir(parents=True)
    other.write_bytes(b"def h():\n")
    figures = generate_extractions.generate(root)
    assert figures["removed"] == 1
    assert not other.exists()
    assert "beta/mod_b/1-2.txt" in capsys.readouterr().err
