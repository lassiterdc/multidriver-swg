"""Cut a cited range of a legacy file into the bytes a stored extraction holds, label its digest, and name where it is stored.

The disposition ledger cites legacy code by file and line range. A stored extraction is a plain
text copy of one cited range, and two digests are recorded for it: the digest of the range as the
corpus holds it now, and the digest of the stored copy. Those two digests, and the comparison of
both with the corpus and with the copy, mean something only if every party that needs "the bytes of
lines A to B" gets the same bytes. This module is that one cut: the generator that writes the
copies, the check that compares them and the key of a stored reference value all call `cut` and
none slices a file itself, so a second reading of "line A to line B" cannot arise.

The cut is lines `first` to `last` inclusive, counted from 1, each line with the terminator it has
in the file, with no decoding of the result and no stripping. A file's last line may have no
terminator, and then the cut ends without one rather than gaining one. The only decoding done
is to count lines the way the extent rule does (`str.splitlines()`), and that count must equal the
cut's own (`bytes.splitlines()`): the two disagree on a file holding a form feed, a vertical tab, a
file or record separator, or a Unicode line separator, and in that case a unit key's line numbers
are not the cut's, so the file is refused rather than cut on one reading.

A digest is labelled `{rule}:{version}:{sha256}`, where the rule names how the digested bytes were
produced (the cut of a cited range, or the stored copy as it sits on disk). Changing what a rule
does means raising its version in `RULE_VERSIONS`; a label recorded under an older version then no
longer matches the current rule, which is how a changed cutting rule is told apart from an edited
copy.
"""

from __future__ import annotations

import hashlib
import tomllib
from pathlib import Path
from types import MappingProxyType

from tests.correspondence import corpus_record

REPO_ROOT = corpus_record.REPO_ROOT

# Repository-relative home of the stored copies. Deliberately NOT a Python-importable name or a
# `.py` suffix on any file below it: a directory or file that looks like a module is picked up by
# tools that classify paths by suffix, and a stored copy of legacy code must stay inert text.
STORED_DIR = "tests/correspondence/extractions"

CUT_RULE = "cut-lines"
STORED_RULE = "stored-text"
RULE_VERSIONS = MappingProxyType({CUT_RULE: 1, STORED_RULE: 1})


class ExtractionError(ValueError):
    """Raised, naming the file or section at fault, when a range cannot be cut or a label or path cannot be made."""


def _section_directory(root: Path, section: str) -> str:
    """Return the directory of the corpus-record section named `section`, relative to `root`."""
    record_path = root / corpus_record.RECORD_NAME
    if not record_path.is_file():
        raise ExtractionError(
            f"{corpus_record.RECORD_NAME}: absent under {root}, so section {section!r} has no directory"
        )
    record = tomllib.loads(record_path.read_text(encoding="utf-8"))
    for entry in record.get("section", []):
        if entry.get("name") == section:
            directory = entry.get("directory")
            if not isinstance(directory, str) or not directory:
                raise ExtractionError(f"section {section!r}: the record gives it no directory")
            return directory.rstrip("/")
    raise ExtractionError(f"section {section!r}: not a section of {corpus_record.RECORD_NAME}")


def _check_range(first: int, last: int, what: str) -> None:
    """Refuse a range that is not whole numbers with 1 <= first <= last, naming `what` it was for."""
    ok = all(isinstance(n, int) and not isinstance(n, bool) for n in (first, last))
    if not ok or not 1 <= first <= last:
        raise ExtractionError(f"{what}: the range {first!r}-{last!r} is not whole line numbers with 1 <= first <= last")


def cut(root: Path, section: str, file: str, first: int, last: int) -> bytes:
    """Return the bytes of lines `first` to `last` inclusive of `file` in the corpus-record section `section`.

    `file` is the name as the corpus record lists it, relative to the section's directory. A caller
    needs from this that the bytes are exactly the file's own for those lines: each line keeps its
    terminator, a last line with none stays without, and nothing is decoded or stripped.

    The file is read only if git tracks it in that section's directory, so an untracked or ignored
    file lying there is refused instead of cut. A file whose line count under `str.splitlines()`
    differs from its count under `bytes.splitlines()`, a file that is not UTF-8, and a range outside
    the file are refused, each naming the file. A tracked file missing from the working tree is
    refused too, so the failure reads as a refusal and not as an `OSError`.
    """
    _check_range(first, last, file)
    directory = _section_directory(root, section)
    relative = f"{directory}/{file}"
    if relative not in set(corpus_record.tracked_files(root)):
        raise ExtractionError(
            f"{file}: git does not track {relative}, and only a tracked file of section {section!r} is cut"
        )
    try:
        data = (root / relative).read_bytes()
    except OSError as exc:
        raise ExtractionError(f"{file}: tracked and not readable from the working tree ({type(exc).__name__})") from exc
    try:
        extent_count = len(data.decode("utf-8").splitlines())
    except UnicodeDecodeError as exc:
        raise ExtractionError(
            f"{file}: not UTF-8, so its lines cannot be counted the way the extent rule counts them"
        ) from exc
    lines = data.splitlines(keepends=True)
    if extent_count != len(lines):
        raise ExtractionError(
            f"{file}: {extent_count} lines under str.splitlines() and {len(lines)} under bytes.splitlines(), "
            "so a unit key's line numbers would not be this cut's"
        )
    if last > len(lines):
        raise ExtractionError(f"{file}: the range {first}-{last} runs past the file's {len(lines)} lines")
    return b"".join(lines[first - 1 : last])


def label(rule: str, data: bytes) -> str:
    """Return `{rule}:{version}:{sha256 of data}`, refusing a rule that is not in `RULE_VERSIONS`."""
    if rule not in RULE_VERSIONS:
        raise ExtractionError(f"rule {rule!r}: not one of {sorted(RULE_VERSIONS)}")
    return f"{rule}:{RULE_VERSIONS[rule]}:{hashlib.sha256(data).hexdigest()}"


def stored_path(section: str, file: str, first: int, last: int) -> str:
    """Return the repository-relative, slash-separated path of the stored copy of one cited range.

    The path is `tests/correspondence/extractions/{section}/{file stem}/{first}-{last}.txt` and is a
    function of those four values alone, so it does not move while the corpus is frozen and a
    later corpus section lands in a directory of its own. The file's stem is used, not its `.py`
    name. A `section` or `file` carrying a path separator, or an empty one, is refused, because it
    would make the path name more than one level.
    """
    for what, value in (("section", section), ("file", file)):
        if not value or "/" in value or "\\" in value:
            raise ExtractionError(
                f"{what} {value!r}: empty or carrying a path separator, so it cannot be one path level"
            )
    _check_range(first, last, file)
    return f"{STORED_DIR}/{section}/{Path(file).stem}/{first}-{last}.txt"
