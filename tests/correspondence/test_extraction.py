"""Tests of the cutting function, the labelled digest and the stored path of an extraction.

The failing-case tests build a git repository inside `tmp_path` holding a one-section corpus
record and the files a test names, staged and never committed, so no git identity is needed.
The git location variables a hook exports are removed first, because a lookup that inherits
`GIT_DIR` reads the repository that exported it and not the temporary one.

Every test class here is a NEW CAPABILITY: no cut existed before this module, so a test's
failure is shown by the wrong implementation its docstring names, not by a pre-change run.
"""

import hashlib
import re
import subprocess
import tomllib
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extent, extraction

pytestmark = pytest.mark.state_check

SECTION = "temp"
DIRECTORY = "legacy_dir"
RECORD = f'rule = "corpus-record:1"\n\n[[section]]\nname = "{SECTION}"\ndirectory = "{DIRECTORY}"\n'
GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")


def _tree(tmp_path: Path, monkeypatch, files: dict[str, bytes], untracked: tuple[str, ...] = ()) -> Path:
    """Build a repository at `tmp_path` holding the record and `files`, staging all but `untracked`."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    (tmp_path / DIRECTORY).mkdir()
    for name, data in files.items():
        (tmp_path / DIRECTORY / name).write_bytes(data)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    tracked = [corpus_record.RECORD_NAME] + [f"{DIRECTORY}/{n}" for n in files if n not in untracked]
    subprocess.run(["git", "add", "--", *tracked], cwd=tmp_path, check=True)
    return tmp_path


def test_cut_returns_the_cited_lines_with_their_terminators(tmp_path, monkeypatch):
    """Property: the cut of lines A to B is exactly those lines' bytes, whichever lines they are.

    Class: any range of a file whose lines all end in a line feed, at the start, in the middle,
    at the end and one line wide. Why it is required: every stored copy and both of its digests
    are this cut. Killed by: counting lines from 0, an exclusive upper bound, or returning the
    range without its last terminator. Second correct implementation: a regular
    expression matching `\\r\\n|\\r|\\n` line ends and slicing its matches passes. Not established: a
    split on line feeds alone is not one, because it mishandles a CR or CRLF file.
    """
    root = _tree(tmp_path, monkeypatch, {"a.py": b"one\ntwo\nthree\nfour\n"})
    assert extraction.cut(root, SECTION, "a.py", 1, 2) == b"one\ntwo\n"
    assert extraction.cut(root, SECTION, "a.py", 2, 3) == b"two\nthree\n"
    assert extraction.cut(root, SECTION, "a.py", 4, 4) == b"four\n"
    assert extraction.cut(root, SECTION, "a.py", 1, 4) == b"one\ntwo\nthree\nfour\n"


def test_cut_keeps_a_last_line_without_a_terminator_as_it_is(tmp_path, monkeypatch):
    """Property: a range ending at a file's unterminated last line ends without a terminator.

    Class: a file whose final line has none, cut with the final line alone and with others
    before it. Why it is required: 8 of the 15 legacy `.py` files end this way, and an added
    newline changes the cut's digest against the corpus. Killed by: appending a line feed to
    every line, as `"\\n".join(text.splitlines()) + "\\n"` does. Control: the same file with its
    terminator present returns the terminator, so the two inputs differ only in that byte.
    Second correct implementation: slicing `data.splitlines(keepends=True)` passes.
    """
    root = _tree(tmp_path, monkeypatch, {"open.py": b"one\ntwo\nthree", "closed.py": b"one\ntwo\nthree\n"})
    assert extraction.cut(root, SECTION, "open.py", 3, 3) == b"three"
    assert extraction.cut(root, SECTION, "open.py", 2, 3) == b"two\nthree"
    assert extraction.cut(root, SECTION, "closed.py", 3, 3) == b"three\n"


@pytest.mark.parametrize(
    ("data", "first", "last", "expected"),
    [
        (b"a\r\nb\r\nc\r\n", 2, 2, b"b\r\n"),
        (b"a\rb\rc\r", 1, 2, b"a\rb\r"),
        (b"a\r\nb\rc\nd", 1, 3, b"a\r\nb\rc\n"),
        (b"a\r\nb\rc\nd", 3, 4, b"c\nd"),
    ],
    ids=["crlf", "cr", "mixed-first-three", "mixed-ending-unterminated"],
)
def test_cut_keeps_each_lines_own_terminator(tmp_path, monkeypatch, data, first, last, expected):
    """Property: a line keeps the terminator the file gives it, whether CRLF, CR or LF.

    Class: files using carriage-return line ends, and a file that mixes all three. Why it is
    required: a cited range's digest is over the file's own bytes. Killed by: normalising line
    ends to a line feed, as reading in text mode does. Second correct implementation: slicing
    `data.splitlines(keepends=True)` passes. Not established: a terminator other than these
    three, which the form-feed test below refuses instead of cutting.
    """
    root = _tree(tmp_path, monkeypatch, {"a.py": data})
    assert extraction.cut(root, SECTION, "a.py", first, last) == expected


def test_cut_does_not_decode_or_strip(tmp_path, monkeypatch):
    """Property: leading and trailing whitespace, tabs, a byte-order mark and non-ASCII bytes come back untouched.

    Class: lines whose bytes a decode-then-encode or a strip would alter. Why it is required:
    the stored copy must hash equal to the corpus bytes. Killed by: `.strip()` on a line,
    decoding with `utf-8-sig` (drops the mark), or re-encoding with a different error handler.
    Second correct implementation: returning a `memoryview` slice converted to bytes passes.
    """
    data = b"\xef\xbb\xbf  lead\t \n\ttab\xc3\xa9  \nlast  "
    root = _tree(tmp_path, monkeypatch, {"a.py": data})
    assert extraction.cut(root, SECTION, "a.py", 1, 3) == data
    assert extraction.cut(root, SECTION, "a.py", 1, 1) == b"\xef\xbb\xbf  lead\t \n"
    assert extraction.cut(root, SECTION, "a.py", 2, 2) == b"\ttab\xc3\xa9  \n"


def test_cut_refuses_an_untracked_file_in_the_section_directory(tmp_path, monkeypatch):
    """Property: a file lying in the section directory that git does not track is refused, naming it.

    Class: any file present on disk and absent from the index, and, as the control, the same
    bytes under another name that is tracked. Why it is required: a stray script or a leftover
    cache must never enter a digest; the corpus record reads only tracked files for the same
    reason. Killed by: checking `Path.is_file()` instead of git's tracked list. Second correct
    implementation: asking `git ls-files --error-unmatch` for the one path passes.
    """
    root = _tree(tmp_path, monkeypatch, {"a.py": b"x\n", "stray.py": b"x\n"}, untracked=("stray.py",))
    assert extraction.cut(root, SECTION, "a.py", 1, 1) == b"x\n"
    with pytest.raises(extraction.ExtractionError, match="stray.py"):
        extraction.cut(root, SECTION, "stray.py", 1, 1)


def test_cut_refuses_a_tracked_file_missing_from_the_working_tree(tmp_path, monkeypatch):
    """Property: a file git lists but the working tree lacks is a refusal naming it, not an OSError.

    Class: a deleted tracked file. Why it is required: a generator run on a half-checked-out
    tree must stop with a message that names the file. Killed by: letting `read_bytes` raise.
    Second correct implementation: checking existence before reading passes.
    """
    root = _tree(tmp_path, monkeypatch, {"a.py": b"x\n"})
    (root / DIRECTORY / "a.py").unlink()
    with pytest.raises(extraction.ExtractionError, match="a.py"):
        extraction.cut(root, SECTION, "a.py", 1, 1)


@pytest.mark.parametrize("separator", ["\x0b", "\x0c", "\x1c", "\x1d", "\x1e"], ids=["vt", "ff", "fs", "gs", "rs"])
def test_cut_refuses_a_file_whose_two_line_counts_disagree(tmp_path, monkeypatch, separator):
    """Property: a file where `str.splitlines()` and `bytes.splitlines()` give different counts is refused, naming it.

    Class: a file holding a character that the text method splits on and the bytes method
    does not (the property under suspicion is that disagreement, so the control varies
    exactly that character). Why it is required: a unit key's line numbers are the text
    method's, and a cut on the other reading would hand back different lines. Killed by:
    dropping the count comparison, which returns bytes for a range whose numbers mean other
    lines. Second correct implementation: computing the whole file's two counts once per file
    and caching the verdict passes. Not established: comparing the counts of the cut's own text
    is not one, because a disagreement outside the range then goes unrefused. The control, the same text with the character replaced by a
    letter, is cut without a refusal.
    """
    with_separator = f"first{separator}second\nthird\n".encode()
    replaced = b"firstXsecond\nthird\n"
    root = _tree(tmp_path, monkeypatch, {"bad.py": with_separator, "good.py": replaced})
    assert extraction.cut(root, SECTION, "good.py", 1, 2) == replaced
    with pytest.raises(extraction.ExtractionError, match="bad.py"):
        extraction.cut(root, SECTION, "bad.py", 1, 2)


def test_cut_refuses_a_unicode_line_separator_and_a_file_that_is_not_utf8(tmp_path, monkeypatch):
    """Property: U+2028 and U+0085, which only the text method splits on, and non-UTF-8 bytes are refused by file name.

    Class: the two non-ASCII separators, which a byte-level check on ASCII control characters
    would miss, and a file the text method cannot decode. Why it is required: both break the
    line count the extent rule uses. Killed by: restricting the comparison to ASCII control
    characters, or decoding with `errors="replace"`. Second correct implementation: any check
    that decodes strictly and compares counts passes.
    """
    files = {
        "ls.py": "a b\n".encode(),
        "nel.py": "a\u0085b\n".encode(),
        "latin.py": b"caf\xe9\n",
    }
    root = _tree(tmp_path, monkeypatch, files)
    for name in files:
        with pytest.raises(extraction.ExtractionError, match=re.escape(name)):
            extraction.cut(root, SECTION, name, 1, 1)


@pytest.mark.parametrize(("first", "last"), [(0, 1), (-1, 2), (3, 2), (1, 4), (4, 4), (2, 9)])
def test_cut_refuses_a_range_outside_the_file(tmp_path, monkeypatch, first, last):
    """Property: a range starting below 1, ending before it starts, or ending past the file's last line is refused.

    Class: both edges of the file and an inverted range, against a 3-line file whose own
    full range is accepted (the control). Why it is required: Python slicing would return a
    short or empty cut for these and a digest of nothing would be recorded. Killed by: slicing
    without bounds checks. Second correct implementation: clamping is not one; any version
    that raises for these ranges passes.
    """
    root = _tree(tmp_path, monkeypatch, {"a.py": b"1\n2\n3\n"})
    assert extraction.cut(root, SECTION, "a.py", 1, 3) == b"1\n2\n3\n"
    with pytest.raises(extraction.ExtractionError, match="a.py"):
        extraction.cut(root, SECTION, "a.py", first, last)


def test_cut_refuses_an_unknown_section_and_a_missing_record(tmp_path, monkeypatch):
    """Property: a section the record does not list, and a root with no record, are refusals naming what is missing.

    Class: a section name that is not in the record, and a repository with no record file.
    Why it is required: a cut must not guess a directory. Killed by: falling back to a default
    directory. Second correct implementation: looking the section up in a parsed mapping passes.
    """
    root = _tree(tmp_path, monkeypatch, {"a.py": b"x\n"})
    with pytest.raises(extraction.ExtractionError, match="nonesuch"):
        extraction.cut(root, "nonesuch", "a.py", 1, 1)
    (root / corpus_record.RECORD_NAME).unlink()
    with pytest.raises(extraction.ExtractionError, match=re.escape(corpus_record.RECORD_NAME)):
        extraction.cut(root, SECTION, "a.py", 1, 1)


def test_label_is_the_rule_the_version_and_the_sha256_of_the_bytes():
    """Property: a label reads `{rule}:1:{sha256 hex}` for both rules, and the digest follows the bytes.

    Class: the two rules, and byte strings differing in one bit. Why it is required: the
    recorded digests are compared with these labels. Killed by: hashing text instead of bytes,
    a different digest function, a version that is not the rule's own, or ignoring the rule.
    Second correct implementation: building the digest with `hashlib.new("sha256")` passes.
    """
    data = b"some\r\nbytes"
    for rule in ("cut-lines", "stored-text"):
        assert extraction.label(rule, data) == f"{rule}:1:{hashlib.sha256(data).hexdigest()}"
    assert extraction.label("cut-lines", b"\x00") != extraction.label("cut-lines", b"\x01")
    assert extraction.label("cut-lines", data) != extraction.label("stored-text", data)
    assert re.fullmatch(r"cut-lines:1:[0-9a-f]{64}", extraction.label("cut-lines", b""))


def test_label_refuses_a_rule_it_does_not_know():
    """Property: a rule outside `RULE_VERSIONS` is a refusal naming the rule.

    Class: any misspelt or new rule name. Why it is required: a label under a rule with no
    version could never be checked against a current rule. Killed by: defaulting the version.
    Second correct implementation: a lookup that raises on a missing key, converted to the
    refusal, passes.
    """
    with pytest.raises(extraction.ExtractionError, match="cut-line"):
        extraction.label("cut-line", b"x")


@pytest.mark.parametrize(
    ("section", "file", "first", "last", "expected"),
    [
        ("stormy", "_utils.py", 1991, 2075, "tests/correspondence/extractions/stormy/_utils/1991-2075.txt"),
        (
            "stormy",
            "_b7.1_qaqc_simulated_events.py",
            1,
            1,
            "tests/correspondence/extractions/stormy/_b7.1_qaqc_simulated_events/1-1.txt",
        ),
        ("later", "x.py", 10, 12, "tests/correspondence/extractions/later/x/10-12.txt"),
    ],
)
def test_stored_path_is_a_function_of_section_file_stem_and_range(section, file, first, last, expected):
    """Property: the path is `tests/correspondence/extractions/{section}/{stem}/{first}-{last}.txt`, with the stem and not the `.py` name.

    Class: a plain name, a name holding a dot, and a second section. Why it is required: the
    guard's acceptance of a finding is by path, so the path must not move. Killed by: keeping
    the suffix, a flat layout, or omitting the section. Second correct implementation:
    building the string with `PurePosixPath` joins passes.
    """
    assert extraction.stored_path(section, file, first, last) == expected


@pytest.mark.parametrize(
    ("section", "file", "first", "last"),
    [
        ("a/b", "x.py", 1, 1),
        ("s", "d/x.py", 1, 1),
        ("s", "", 1, 1),
        ("", "x.py", 1, 1),
        ("s", "x.py", 0, 1),
        ("s", "x.py", 5, 4),
    ],
)
def test_stored_path_refuses_what_is_not_one_path_level_or_a_range(section, file, first, last):
    """Property: a separator in the section or file, an empty one, and a bad range are refused.

    Class: each of the six, against the accepted values of the test above. Why it is required:
    a path with an extra level would store a copy where nothing looks for it. Killed by:
    formatting the string without checks. Second correct implementation: validating with a
    regular expression instead passes.
    """
    with pytest.raises(extraction.ExtractionError):
        extraction.stored_path(section, file, first, last)


def _real_candidates() -> list[dict]:
    record = tomllib.loads((corpus_record.REPO_ROOT / corpus_record.RECORD_NAME).read_text(encoding="utf-8"))
    return [e for e in record["section"][0]["file"] if e["role"] == "candidate"]


def test_cut_of_a_whole_real_file_is_the_file_and_its_line_count_is_the_extent_rules():
    """Property: for each of the real corpus's candidate files, lines 1 to N cut is the file's bytes, N is the extent rule's count, and cuts partition.

    Class: all 15 real candidate files, of which 8 end without a terminator. Why it is
    required: the stored copies are cuts of these files, so the cut must agree with the
    file and with the line count `extent` and the corpus record use. The property under
    suspicion is that count, so it is read from `extent.extents` and from the record's
    own `lines` field, two counts the cut does not use. Killed by: any cut that adds or
    drops a terminator on a real file, or counts a different way (measured: 0 of 15 files
    disagree between the two splitlines). Second correct implementation: reading the file
    with `io.BytesIO(...).readlines()` gives the same bytes for these files. Not established:
    a corpus other than this pinned one.
    """
    candidates = _real_candidates()
    assert len(candidates) == 15
    for entry in candidates:
        name = entry["path"]
        data = (corpus_record.REPO_ROOT / "_old_code_to_refactor" / name).read_bytes()
        n = entry["lines"]
        assert extent.extents(name, data.decode("utf-8"))[1]["lines"] == n, name
        assert extraction.cut(corpus_record.REPO_ROOT, "stormy", name, 1, n) == data, name
        half = n // 2
        two_parts = extraction.cut(corpus_record.REPO_ROOT, "stormy", name, 1, half)
        two_parts += extraction.cut(corpus_record.REPO_ROOT, "stormy", name, half + 1, n)
        assert two_parts == data, name
