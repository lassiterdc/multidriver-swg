"""Reviewer's tests of the cutting function: where in a file the two line counts disagree, and which directory's tracked file is cut.

The coder's tests of `extraction.cut` place every line-count disagreement INSIDE the range they
cut, and every untracked file under a name git tracks nowhere. Both positions leave a wrong
implementation passing: a count comparison made over the cut's own bytes rather than over the
whole file, and a tracked-file check that matches a file's name anywhere in the repository rather
than its path in the section's directory. These tests move the input to the positions that
separate the right implementation from those two.

Each test builds a git repository inside `tmp_path` holding a one-section corpus record and the
files it names, staged and never committed, so no git identity is needed. The git location
variables a hook exports are removed first, because a lookup that inherits `GIT_DIR` reads the
repository that exported it and not the temporary one.
"""

import subprocess
from pathlib import Path

import pytest

from tests.correspondence import corpus_record, extraction

pytestmark = pytest.mark.state_check

SECTION = "temp"
DIRECTORY = "legacy_dir"
RECORD = f'rule = "corpus-record:1"\n\n[[section]]\nname = "{SECTION}"\ndirectory = "{DIRECTORY}"\n'
GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_PREFIX")


def _repository(tmp_path: Path, monkeypatch, files: dict[str, bytes], untracked: tuple[str, ...] = ()) -> Path:
    """Build a repository at `tmp_path` holding the record and `files` (paths relative to it), staging all but `untracked`."""
    for var in GIT_LOCATION_VARS:
        monkeypatch.delenv(var, raising=False)
    (tmp_path / corpus_record.RECORD_NAME).write_text(RECORD, encoding="utf-8")
    for relative, data in files.items():
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / relative).write_bytes(data)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    tracked = [corpus_record.RECORD_NAME] + [r for r in files if r not in untracked]
    subprocess.run(["git", "add", "--", *tracked], cwd=tmp_path, check=True)
    return tmp_path


@pytest.mark.parametrize(
    ("text", "first", "last", "control"),
    [
        ("a{sep}b\nc\nd\n", 3, 3, b"d\n"),
        ("a{sep}b\nc\nd\n", 2, 3, b"c\nd\n"),
        ("a\nb\nc{sep}d\n", 1, 1, b"a\n"),
    ],
    ids=["disagreement-before-the-range", "disagreement-before-a-two-line-range", "disagreement-after-the-range"],
)
def test_cut_refuses_a_disagreeing_file_wherever_the_disagreement_lies(
    tmp_path, monkeypatch, text, first, last, control
):
    """Property: a file whose two line counts disagree is refused whatever range is asked for, not only a range spanning the disagreement.

    Class: a form feed placed on a line before the requested range, and on a line after it, so
    the cut's own bytes hold no disagreement in any case. Why it is required: in the first two
    positions the bytes-counted line 3 of `a<FF>b / c / d` is `d`, while the line the text count
    numbers 3 is `c`, so a cut returned there hands back a different line from the one a unit
    key's number names; the third position shows the refusal is of the file, not of the range.
    Killed by: comparing the two counts over the cut's own bytes instead of over the whole file,
    which accepts all three inputs and returns the wrong line for the first two. Second correct
    implementation: computing both whole-file counts once per file and caching the verdict passes.
    The control, the same text with the form feed replaced by a letter, is cut to the expected
    bytes, so the refusal turns on the separator and not on the range or the file's shape.
    """
    root = _repository(
        tmp_path,
        monkeypatch,
        {
            f"{DIRECTORY}/bad.py": text.format(sep="\x0c").encode(),
            f"{DIRECTORY}/good.py": text.format(sep="X").encode(),
        },
    )
    assert extraction.cut(root, SECTION, "good.py", first, last) == control
    with pytest.raises(extraction.ExtractionError, match="bad.py"):
        extraction.cut(root, SECTION, "bad.py", first, last)


def test_cut_refuses_an_untracked_section_file_whose_name_git_tracks_in_another_directory(tmp_path, monkeypatch):
    """Property: only the file at the section directory's own path counts as tracked; the same name tracked elsewhere does not admit it.

    Class: an untracked file in the section directory whose name git tracks under a different
    directory of the same repository, against a tracked file of the section directory as the
    control. Why it is required: the cut must read the corpus file the record certifies, and an
    untracked file shadowing it would enter a digest; a check by name alone cannot tell the two
    apart. Killed by: testing the file's base name against the tracked list, or testing whether
    any tracked path ends with the name, either of which admits the untracked copy. Second
    correct implementation: asking `git ls-files --error-unmatch -- {directory}/{file}` passes.
    """
    root = _repository(
        tmp_path,
        monkeypatch,
        {"other/a.py": b"tracked elsewhere\n", f"{DIRECTORY}/a.py": b"untracked here\n", f"{DIRECTORY}/b.py": b"b\n"},
        untracked=(f"{DIRECTORY}/a.py",),
    )
    assert extraction.cut(root, SECTION, "b.py", 1, 1) == b"b\n"
    with pytest.raises(extraction.ExtractionError, match="a.py"):
        extraction.cut(root, SECTION, "a.py", 1, 1)
