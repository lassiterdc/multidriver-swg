"""Keep a value a lift computed once, say what built it, and report how it compares with the value computed now.

An equivalence test decides on the value its lift recomputes in the same run, so a library difference
cannot explain a mismatch between the two sides. A value stored earlier is a second opinion only: it says
what the lift gave on some run, and it is trusted for nothing unless the inputs it was built from are the
inputs of this run. This module stores such a value beside the key it was built from, and compares it with
the recomputed value only when every part of the key still matches, as a report that is never `pass` or `fail`.

A stored value is `tests/correspondence/reference_values/{system}/{unit key slug}[__{part}].toml` beside its
codec payload (`.npz`). The key is the cut digests of the unit's stored texts, the lift digest, the digests
of the lift's fixtures and the environment identity, which is `UNCHARACTERIZED` until a container image and a
lockfile digest both exist. The diagnostics (interpreter and library versions) are recorded and no rule reads
them. `store` is the separate act of regeneration and nothing here calls it while comparing.

`compare_stored` returns exactly one report and raises nothing. It never calls a `require_*` gate, because
each skips by raising and it runs inside an equivalence test, where a skip would suppress the verdict that
the recomputed value is given. It returns `NOT MEASURED {cause}` instead when the corpus record, the key
resolution or this unit's own stored texts report a finding. The stored-value state check, which decides no
verdict, calls `require_extractions` first.

Run as `python -m tests.correspondence.reference_values` it prints one line and exits 0.
"""

from __future__ import annotations

import json
import re
import sys
import tomllib
import warnings
from pathlib import Path
from typing import Any

from tests.correspondence import (
    compare,
    corpus_record,
    extent,
    extraction,
    extraction_check,
    fixtures,
    lift,
    value_codec,
)

REPO_ROOT = extraction.REPO_ROOT
STORE = Path("tests/correspondence/reference_values")
UNCHARACTERIZED = "UNCHARACTERIZED"
FIELDS = ("cuts", "lift", "fixtures", "environment")


class ReferenceValueError(Exception):
    """A stored value that cannot be written, with the reason in the message."""


class ReferenceValueReport(UserWarning):
    """The report of a comparison with a stored value; a warning, never a pass or a fail."""


def environment_identity(container_digest: str | None = None, lockfile_digest: str | None = None) -> str:
    """Return `UNCHARACTERIZED` unless both a container digest and a lockfile digest are given, else their joined text."""
    if container_digest and lockfile_digest:
        return f"container={container_digest};lockfile={lockfile_digest}"
    return UNCHARACTERIZED


def _slug(unit: str, part: str | None) -> str:
    """The file stem of a stored value: the unit key with every character outside letters, digits, `.` and `-` made `_`."""
    stem = re.sub(r"[^A-Za-z0-9.-]", "_", unit)
    return f"{stem}__{part}" if part else stem


def _directory(declaration: lift.LiftDeclaration) -> Path:
    return declaration.root / STORE / fixtures.system_directory_name(declaration.system)


def _paths(declaration: lift.LiftDeclaration) -> tuple[Path, Path]:
    stem = _slug(declaration.unit, declaration.part)
    return _directory(declaration) / f"{stem}.toml", _directory(declaration) / f"{stem}.npz"


def key_of(declaration: lift.LiftDeclaration, environment: str = UNCHARACTERIZED) -> dict[str, Any]:
    """Build the key from the inputs of this run: the cut digests, the lift digest, the fixtures' digests, the environment.

    Refuses by raising when an input cannot be read (a missing stored text or an unrecorded fixture), so
    that the caller names the cause.
    """
    name, _ = lift._section(declaration.root)
    cuts = [
        extraction.label(extraction.CUT_RULE, extraction.cut(declaration.root, name, file, first, last))
        for file, first, last in lift._ranges(declaration)
    ]
    rows = fixtures._read_record(
        declaration.root / fixtures.FIXTURES / fixtures.system_directory_name(declaration.system)
    )
    recorded = []
    for fixture in declaration.fixtures:
        if fixture not in rows:
            raise ReferenceValueError(f"the fixture {fixture!r} of system {declaration.system!r} is not recorded")
        recorded.append(f"{fixture}={rows[fixture]['digest']}")
    return {"cuts": cuts, "lift": lift.lift_digest(declaration), "fixtures": recorded, "environment": environment}


def _diagnostics() -> dict[str, str]:
    """The interpreter and library versions recorded beside a stored value; no rule reads them."""
    import numpy
    import pandas
    import xarray

    found = {
        "python": ".".join(map(str, sys.version_info[:3])),
        "numpy": numpy.__version__,
        "pandas": pandas.__version__,
        "xarray": xarray.__version__,
    }
    try:
        import scipy

        found["scipy"] = scipy.__version__
    except ImportError:
        pass
    return found


def _quote(text: str) -> str:
    return json.dumps(text)


def _render(declaration: lift.LiftDeclaration, key: dict[str, Any], payload_name: str, payload_digest: str) -> str:
    """The text of a stored value's descriptor."""
    lines = [f"unit = {_quote(declaration.unit)}"]
    if declaration.part:
        lines.append(f"part = {_quote(declaration.part)}")
    lines += ["", "[key]"]
    for field in ("cuts", "fixtures"):
        lines.append(f"{field} = [{', '.join(_quote(item) for item in key[field])}]")
    lines += [f"lift = {_quote(key['lift'])}", f"environment = {_quote(key['environment'])}", "", "[diagnostics]"]
    lines += [f"{name} = {_quote(version)}" for name, version in _diagnostics().items()]
    lines += ["", "[payload]", f"file = {_quote(payload_name)}", f"digest = {_quote(payload_digest)}", ""]
    return "\n".join(lines)


def store(declaration: lift.LiftDeclaration, value: Any, environment: str = UNCHARACTERIZED) -> Path:
    """Write `value` and its descriptor for the declared lift, replacing any earlier ones; return the descriptor's path.

    This is the separate act of regeneration. The comparison never calls it, and no test run does.
    """
    data = value_codec.encode(value)
    descriptor, payload = _paths(declaration)
    descriptor.parent.mkdir(parents=True, exist_ok=True)
    payload.write_bytes(data)
    descriptor.write_text(
        _render(declaration, key_of(declaration, environment), payload.name, value_codec.label(data)), encoding="utf-8"
    )
    return descriptor


def _read(declaration: lift.LiftDeclaration) -> dict | None:
    """The parsed descriptor of the declared lift's stored value, or None when there is none."""
    descriptor, _ = _paths(declaration)
    return tomllib.loads(descriptor.read_text(encoding="utf-8")) if descriptor.is_file() else None


def _moved_field(stored: dict, current: dict[str, Any]) -> str | None:
    """The first key field whose stored value is not the current one, or None when every field matches."""
    recorded = stored.get("key", {})
    return next((field for field in FIELDS if recorded.get(field) != current[field]), None)


def _not_measured(declaration: lift.LiftDeclaration) -> str | None:
    """The cause when the inputs of a comparison cannot be trusted: the corpus record, the key resolution or this unit's texts."""
    root = declaration.root
    if corpus_record.record_findings(root):
        return corpus_record.NOT_MEASURED
    if extent.check(root)[0]:
        return extent.NOT_MEASURED
    if extraction_check.extraction_findings(root, declaration.unit):
        return extraction_check.NOT_MEASURED
    return None


def _report(declaration: lift.LiftDeclaration, recomputed: Any) -> str:
    """Decide the one report, in the order: NOT MEASURED, EXCLUDED, ABSENT, STALE, then the comparison."""
    cause = _not_measured(declaration)
    if cause:
        return f"NOT MEASURED {cause}"
    if declaration.draws == "unseeded":
        return "EXCLUDED unseeded draw"
    stored = _read(declaration)
    if stored is None:
        return "ABSENT"
    moved = _moved_field(stored, key_of(declaration, environment_identity()))
    if moved:
        return f"STALE {moved}"
    _, payload = _paths(declaration)
    if not payload.is_file() or value_codec.label(payload.read_bytes()) != stored.get("payload", {}).get("digest"):
        return "STALE payload"
    agrees = compare.Exact().admits(value_codec.decode(payload.read_bytes()), recomputed)
    return "CHARACTERIZED DIVERGENCE: agrees" if agrees else "CHARACTERIZED DIVERGENCE: moved environment"


def compare_stored(declaration: lift.LiftDeclaration, recomputed: Any) -> str:
    """Return exactly one report about the stored value, warn it as a `ReferenceValueReport`, and raise nothing.

    The reports are `ABSENT`, `STALE {field}`, `EXCLUDED unseeded draw`, `NOT MEASURED {cause}`, and, with every
    key part matching, `CHARACTERIZED DIVERGENCE: agrees` or `: moved environment` under `compare.Exact`. None is
    `pass` or `fail`: the verdict belongs to the recomputed value.
    """
    try:
        report = _report(declaration, recomputed)
    except Exception as exc:  # the report must never replace the recomputed value's verdict with an error
        report = f"NOT MEASURED {type(exc).__name__}: {exc}"
    warnings.warn(ReferenceValueReport(f"{declaration.unit}: {report}"), stacklevel=2)
    return report


def _stored_descriptors(root: Path) -> list[Path]:
    base = Path(root) / STORE
    return sorted(base.glob("*/*.toml")) if base.is_dir() else []


def _match(root: Path, descriptor: Path, declarations: list[lift.LiftDeclaration]) -> lift.LiftDeclaration | None:
    """The declaration that a descriptor belongs to, by its system directory, unit and part."""
    data = tomllib.loads(descriptor.read_text(encoding="utf-8"))
    return next(
        (
            d
            for d in declarations
            if fixtures.system_directory_name(d.system) == descriptor.parent.name
            and d.unit == data.get("unit")
            and (d.part or None) == data.get("part")
        ),
        None,
    )


def stored_value_findings(root: Path = REPO_ROOT) -> list[str]:
    """Return one finding per stored value that is stale, has no declared lift, or whose payload is not its record's."""
    declarations = lift.discover(root)
    findings = []
    for descriptor in _stored_descriptors(root):
        name = descriptor.relative_to(root).as_posix()
        stored = tomllib.loads(descriptor.read_text(encoding="utf-8"))
        declaration = _match(root, descriptor, declarations)
        if declaration is None:
            findings.append(f"{name}: STALE lift; no declared lift has this unit and part")
            continue
        try:
            moved = _moved_field(stored, key_of(declaration, environment_identity()))
        except Exception as exc:
            findings.append(f"{name}: STALE inputs; the key cannot be built ({type(exc).__name__}: {exc})")
            continue
        if moved:
            findings.append(f"{name}: STALE {moved}")
        payload = descriptor.with_name(stored.get("payload", {}).get("file", "?"))
        if not payload.is_file() or value_codec.label(payload.read_bytes()) != stored.get("payload", {}).get("digest"):
            findings.append(f"{name}: payload {payload.name} is not the one its record names (missing or edited)")
    return findings


def summary(root: Path = REPO_ROOT) -> dict[str, int]:
    """Count stored values, the uncharacterized ones, the stale ones, and declared lifts with no stored value."""
    declarations = lift.discover(root)
    descriptors = _stored_descriptors(root)
    uncharacterized = sum(
        1
        for d in descriptors
        if tomllib.loads(d.read_text(encoding="utf-8")).get("key", {}).get("environment") == UNCHARACTERIZED
    )
    stale = sum(1 for finding in stored_value_findings(root) if ": STALE" in finding)
    stored_units = {
        (
            d.parent.name,
            tomllib.loads(d.read_text(encoding="utf-8")).get("unit"),
            tomllib.loads(d.read_text(encoding="utf-8")).get("part"),
        )
        for d in descriptors
    }
    missing = sum(
        1
        for d in declarations
        if (fixtures.system_directory_name(d.system), d.unit, d.part or None) not in stored_units
    )
    return {"stored": len(descriptors), "uncharacterized": uncharacterized, "stale": stale, "no_stored_value": missing}


def main(root: Path = REPO_ROOT) -> int:
    """Print the one summary line and return 0; the summary decides nothing."""
    figures = summary(root)
    print(
        f"reference values: {figures['stored']} stored, {figures['uncharacterized']} {UNCHARACTERIZED}, "
        f"{figures['stale']} stale, {figures['no_stored_value']} declared lift(s) with no stored value"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
