"""Write the stored copy of every cited legacy range, and the record of the digests that describe them.

An equivalence test cites a range of a scoped-in unit's legacy code, and its check compares the
stored copy of that range with the corpus and with the digests recorded for it. Those copies and
that record must therefore be one product of one run, derived from the disposition ledger and the
corpus and from nothing written by hand, so that the copies cannot drift from the ledger's idea
of which ranges are cited. This script is that run: it reads each `scope-in` row of the ledger,
resolves the row's key through the extent rule, and writes one plain text file per range of the
key through `extraction.cut`, the one function that cuts, so no second reading of "lines A to B"
exists here.

Run as `python -m tests.correspondence.generate_extractions [--root PATH]` it writes under the
repository root (or PATH) and prints one line: keys, ranges, lines written, texts removed. A key that does not
resolve is refused, naming it, before anything is written, so a refusal leaves the tree as it was.
A stored text whose range is no longer cited is deleted and named on standard error. The file
`extractions/LICENSE`, the MIT notice that sits beside the copies, is never written or deleted.

Only the first corpus-record section is read, because `extent.resolve` resolves keys in that
section alone, so a second section's directory is never read and no copy of it is written. The
stale-text sweep covers every `.txt` under `extractions/` and not that section alone, so a
second section's stored copies, having no cited range in the population, are deleted and named
like any other stale text.
"""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

from tests.correspondence import extent, extraction, ledger

REPO_ROOT = extraction.REPO_ROOT
RECORD_PATH = "tests/correspondence/extraction_record.toml"
RECORD_RULE = "extraction-record:1"
COMMAND = "python -m tests.correspondence.generate_extractions"


def _scoped_in_keys(root: Path) -> list[str]:
    """Return the key of every `scope-in` row of every per-script ledger file under `root`, sorted.

    The ledger files are read directly and not through `ledger.load`, because that function also
    validates every row's form and a malformed row elsewhere must not stop this run; a key this
    run cannot use is refused when it is resolved. The per-script files are visited in sorted
    order so the result does not depend on the order the files were created in.
    """
    folder = root / ledger.LEDGER_DIR
    keys: list[str] = []
    for path in sorted(p for p in folder.glob("*.toml") if p.name != ledger.CEDED_FILE):
        for row in tomllib.loads(path.read_text(encoding="utf-8")).get("row", []):
            if row.get("disposition") == "scope-in":
                keys.append(row.get("key"))
    # Ordered by text form, because a row with no key or a non-string key would otherwise make the sort
    # compare incomparable types and raise before `population` can refuse the key by name.
    return sorted(keys, key=str)


def _ranges(key: str) -> list[tuple[str, int, int]]:
    """Split a canonical key `file:a-b[,c-d]` into one (file, first, last) per range."""
    name, spans = key.split(":", 1)
    return [(name, *map(int, span.split("-"))) for span in spans.split(",")]


def population(root: Path) -> tuple[list[str], list[tuple[str, int, int]]]:
    """Return the scoped-in keys and the sorted, distinct ranges they cite, refusing an unresolvable key.

    A key is unresolvable when `extent.resolve` returns None for it, or when it is not a string
    of the canonical form; the refusal names the key.
    """
    keys = _scoped_in_keys(root)
    cited: set[tuple[str, int, int]] = set()
    for key in keys:
        if not isinstance(key, str) or not ledger.KEY_RE.match(key) or extent.resolve(key, root) is None:
            raise extraction.ExtractionError(f"{key!r}: a scope-in key that does not resolve through the extent rule")
        cited.update(_ranges(key))
    return keys, sorted(cited)


def _record_text(rows: list[dict]) -> str:
    """Render the record: a header naming the command that regenerates it, then one table per stored text."""
    out = [
        "# The record of the stored copies of cited legacy ranges: for each, the digest of the range cut from the corpus",
        "# and the digest of the stored copy, each labelled {rule}:{version}:{sha256}.",
        "# Generated; do not edit by hand. Regenerate with:",
        f"#   {COMMAND}",
        "",
        f"rule = {json.dumps(RECORD_RULE)}",
    ]
    for row in rows:
        out += [
            "",
            "[[extraction]]",
            f"section = {json.dumps(row['section'])}",
            f"file = {json.dumps(row['file'])}",
            f"first = {row['first']}",
            f"last = {row['last']}",
            f"path = {json.dumps(row['path'])}",
            f"cut = {json.dumps(row['cut'])}",
            f"stored = {json.dumps(row['stored'])}",
        ]
    return "\n".join(out) + "\n"


def generate(root: Path = REPO_ROOT) -> dict:
    """Write the stored copies and the record under `root`; return the figures the summary line prints.

    Every range is cut before any file is written, so a refusal from `cut` or from `population`
    leaves the tree untouched. The stored digest is taken from the file as read back from disk.
    Texts under `extractions/` ending in `.txt` whose range is not cited are deleted; any other
    file there, `LICENSE` included, is left alone.
    """
    keys, ranges = population(root)
    section = extent._section(root)["name"]
    cuts = [(file, first, last, extraction.cut(root, section, file, first, last)) for file, first, last in ranges]
    rows: list[dict] = []
    wanted: set[Path] = set()
    lines_written = 0
    for file, first, last, data in cuts:
        relative = extraction.stored_path(section, file, first, last)
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        wanted.add(target)
        lines_written += len(data.splitlines())
        rows.append(
            {
                "section": section,
                "file": file,
                "first": first,
                "last": last,
                "path": relative,
                "cut": extraction.label(extraction.CUT_RULE, data),
                "stored": extraction.label(extraction.STORED_RULE, target.read_bytes()),
            }
        )
    stored_root = root / extraction.STORED_DIR
    removed = sorted(p for p in stored_root.rglob("*.txt") if p not in wanted) if stored_root.is_dir() else []
    for stale in removed:
        stale.unlink()
        print(f"removed {stale.relative_to(root).as_posix()}", file=sys.stderr)
    for folder in (
        sorted((p for p in stored_root.rglob("*") if p.is_dir()), reverse=True) if stored_root.is_dir() else []
    ):
        if not any(folder.iterdir()):
            folder.rmdir()
    (root / RECORD_PATH).write_text(_record_text(rows), encoding="utf-8")
    return {"keys": len(keys), "ranges": len(ranges), "lines": lines_written, "removed": len(removed)}


def main(root: Path = REPO_ROOT) -> int:
    """Run `generate`, print the one summary line, and return 0; print a refusal to standard error and return 1."""
    try:
        figures = generate(root)
    except extraction.ExtractionError as exc:
        print(f"generate_extractions: refused: {exc}", file=sys.stderr)
        return 1
    print(
        f"extractions: keys {figures['keys']}, ranges {figures['ranges']}, "
        f"lines written {figures['lines']}, texts removed {figures['removed']}"
    )
    return 0


if __name__ == "__main__":
    # `--root {path}` names the repository to write under; the default is the checkout this file is in.
    sys.exit(main(Path(sys.argv[2]) if sys.argv[1:2] == ["--root"] and len(sys.argv) > 2 else REPO_ROOT))
