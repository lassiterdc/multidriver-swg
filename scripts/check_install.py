#!/usr/bin/env python3
"""Check that each built distribution installs alone, imports every module, and declares what it uses.

The declaration in `pyproject.toml` and the imports in `src/` are two statements about the same thing,
and nothing compares them: a module may import a library the declaration never names, the declaration
may carry a library no module imports, or a bound may be stated so loosely that the floor it promises
is one the code was never run against. Each of those ships, and a caller meets it as an `ImportError`
or a silent version skew at their own call site rather than as the push that promised otherwise. This
gate compares the two statements by MEASURING one of them: it installs the built artifact into a fresh
environment holding nothing else, imports every module the artifact provides, and reads every import
statement the installed files carry, so what it checks is what an installer would get.

Both artifacts are checked on one invocation, and so is every ground: each refusal is one stdout line
`refused: {id}: {detail}`, the closing line reads `{dir}: {n} refusal(s)` or `{dir}: conforms`, and the
exit status is 0 when nothing was refused, 1 when anything was, and 2 on a usage error.

The six grounds, and the refusal set is CLOSED at these six:

- `wrong-artifacts`: the directory does not hold exactly one `.tar.gz` and one `.whl`. A release ships
  both, so a directory holding another count is not the output of one build, and comparing an artifact
  against a declaration is only meaningful for the artifact the build wrote.
- `install-failed`: installing the artifact into its fresh environment exits non-zero, and the detail
  names the artifact and the installer's last line. The artifact's own dependencies arrive here the way
  an installer would fetch them, so an unresolvable declaration is this refusing rather than a mystery
  at a user's install.
- `unconstrained`: a declared requirement carries no version specifier. An unconstrained declaration
  installs whatever is newest at the user's moment, which is a marker of a moving environment.
- `import-failed`: importing a module the artifact provides raises, and the detail names the module and
  the exception type. This is the ground a caller would otherwise reach first.
- `undeclared`: an imported name maps to no installed distribution, or to one the artifact's
  `Requires-Dist` does not name. The first case is an import of a library nothing declared; the second
  is a library that arrived as another package's dependency and that this package's declaration must
  therefore name itself.
- `unimported`: a declared requirement names a distribution no module of the artifact imports. This is
  the arm that keeps the declaration equal to what the scan decides: without it a declaration can only
  grow, and every growth is an install a user pays for.

`--resolution` is passed to the installer and decides which satisfying version arrives: `highest` (the
default) is what a user's install gets today, and `lowest-direct` is the floor the declaration promises,
made real. Under `lowest-direct` the installer resolves only the requirements named on its command
line, so every `Requires-Dist` entry this check does not skip is passed beside the artifact as well --
with the artifact alone, `lowest-direct` installs the highest version the transitive closure allows,
which is the resolution it is named to avoid.

A requirement entry is SKIPPED when, and only when, it carries an extra MARKER (`; extra == "docs"`):
that declares a library for an optional install rather than one this package imports, and judging it
would refuse a declaration for shipping an optional environment. An extras BRACKET is not that marker
and is not skipped: `dep[fast]>=1` is a mandatory dependency on `dep`, installed with `dep`'s `fast`
extra, and dropping it would refuse an import the declaration covers and never pass its floor to the
installer. A platform-marked entry is not skipped either: the marker names where the dependency is
needed, and a declaration that installs nothing on this platform is exactly what an `unimported`
refusal reports.

Deliberately NOT a static read of `pyproject.toml`: the wheel's `METADATA` and the sdist's `PKG-INFO`
are what an installer reads, and a declaration that never reached the built artifact would pass a read
of the source file. Deliberately NOT `importlib.metadata` from this interpreter: the environment that
matters is the one the artifact made, and this script therefore runs its scan inside that environment.
The scan itself is a stdlib program this file embeds, so the gate needs no package of its own beyond
the installer.

The install runs with this process's environment, so an offline run passes uv's own variables --
`UV_NO_INDEX`, `UV_FIND_LINKS`, `UV_CACHE_DIR` -- exactly as uv documents them, and the gate adds no
index option of its own. The environment it creates is a temporary directory and is removed with it, so
nothing here is reused between artifacts or between runs: the point is a fresh environment each time.

Not established: that importing a module exercises what a caller does with it. This gate proves the
module imports in an environment holding only the artifact's declaration; whether the module's own
behaviour is correct is the suite's question.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from email.message import Message
from email.parser import Parser
from pathlib import Path

# The floor of `requires-python`, and the interpreter the artifacts are installed against. The job that
# runs this check selects the same version, so a module that needs a newer interpreter than the package
# promises fails here rather than at a user's install.
ARTIFACT_PYTHON = "3.11"
RESOLUTIONS = ("highest", "lowest-direct")
SDIST_SUFFIX = ".tar.gz"
WHEEL_SUFFIX = ".whl"
UV = "uv"
EXIT_USAGE = 2

# A requirement's distribution name and the rest of the entry. The marker is cut off before this
# pattern is applied, so the rest is the entry's extras bracket, if any, followed by its version
# specifier: `dep[fast]>=1` gives the name `dep` and the rest `[fast]>=1`. The bracket is kept in the
# rest rather than matched away because the full entry is what `lowest-direct` passes to the installer,
# and an extra's own dependencies are part of what the declaration promises.
_REQUIREMENT = re.compile(r"^\s*(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\s*(?P<rest>.*)$")
# The operators a version specifier carries. An extras bracket carries none of them, so this is what
# separates "bounded" from "declared with no bound" without parsing the bracket out first.
_SPECIFIER = re.compile(r"[<>=!~]")
# The name normalisation a distribution name and a `Requires-Dist` name are equal under.
_NORMALIZE = re.compile(r"[-_.]+")

# The scan, run by the environment's own interpreter so that "installed" means what it means to a user.
# It imports every module the artifact's distribution carries, reads every import statement in the
# installed files, and resolves each third-party name to the distribution that provides it. The
# distribution is named by its metadata `Name`, derived from the artifact rather than assumed, so this
# program reads a synthetic package's wheel exactly as it reads this package's.
_SCAN_PROGRAM = """\
import ast
import importlib
import importlib.metadata
import json
import pkgutil
import sys
from pathlib import Path

report = {"error": "", "failures": {}, "imports": [], "modules": [], "providers": {}}
report_path = Path(sys.argv[2])


def write():
    report_path.write_text(json.dumps(report), encoding="utf-8")


try:
    distribution = importlib.metadata.distribution(sys.argv[1])
except Exception as error:
    report["error"] = f"{type(error).__name__}: {error}"
    write()
    raise SystemExit(0)

# The top-level import names the distribution installed: a package directory or a single module file,
# with the metadata and data directories of the installation left out.
roots = set()
for entry in distribution.files or []:
    parts = entry.parts
    if not parts:
        continue
    first = parts[0]
    if first == ".." or first.endswith(".dist-info") or first.endswith(".data") or first.endswith(".egg-info"):
        continue
    roots.add(first[:-3] if first.endswith(".py") else first)

standard = set(sys.stdlib_module_names) | {"__future__"}
imported = set()
for root in sorted(roots):
    try:
        importlib.import_module(root)
    except BaseException as error:
        report["failures"][root] = f"{type(error).__name__}: {error}"
        continue
    path = getattr(sys.modules.get(root), "__path__", None)
    names = [root]
    if path is not None:
        names.extend(
            found.name for found in pkgutil.walk_packages(path, prefix=root + ".", onerror=lambda _: None)
        )
    for name in names:
        if name not in report["modules"]:
            report["modules"].append(name)
        try:
            importlib.import_module(name)
        except BaseException as error:
            report["failures"][name] = f"{type(error).__name__}: {error}"

for entry in distribution.files or []:
    if not entry.name.endswith(".py"):
        continue
    try:
        tree = ast.parse(Path(distribution.locate_file(entry)).read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeDecodeError) as error:
        report["failures"].setdefault(str(entry), f"{type(error).__name__}: {error}")
        continue
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".", 1)[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                imported.add(node.module.split(".", 1)[0])

providers = importlib.metadata.packages_distributions()
outside = sorted(name for name in imported if name not in standard and name not in roots)
report["imports"] = outside
report["providers"] = {name: providers.get(name, []) for name in outside}
write()
"""


def _normalized(name: str) -> str:
    """Two distribution names that name the same distribution compare equal under this form."""
    return _NORMALIZE.sub("-", name).strip().lower()


def _metadata(artifact: Path) -> Message:
    """The artifact's core metadata: the wheel's `METADATA` or the sdist's shallowest `PKG-INFO`."""
    if artifact.suffix == WHEEL_SUFFIX:
        with zipfile.ZipFile(artifact) as archive:
            carried = sorted(name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
            if not carried:
                raise ValueError(f"{artifact.name} carries no .dist-info/METADATA")
            return Parser().parsestr(archive.read(carried[0]).decode("utf-8"))
    with tarfile.open(artifact, "r:gz") as archive:
        carried = [member for member in archive.getmembers() if member.name.endswith("/PKG-INFO")]
        if not carried:
            raise ValueError(f"{artifact.name} carries no PKG-INFO")
        member = min(carried, key=lambda found: (found.name.count("/"), found.name))
        handle = archive.extractfile(member)
        if handle is None:
            raise ValueError(f"{artifact.name} carries a PKG-INFO that cannot be read")
        return Parser().parsestr(handle.read().decode("utf-8"))


def _requirements(metadata: Message) -> list[tuple[str, str]]:
    """Each declared requirement this check judges, as its distribution name and the rest of its entry.

    The rest is the entry's extras bracket, if any, followed by its version specifier: `dep[fast]>=1`
    gives `dep` and `[fast]>=1`, and the full entry is what `lowest-direct` passes to the installer.

    An extra-MARKED entry is the only kind left out: `; extra == "docs"` declares a library for an
    optional install, which this package does not import and therefore cannot be refused for. A
    bracket is NOT that marker and is not left out -- `dep[fast]>=1` is a mandatory dependency on
    `dep`, installed with `dep`'s `fast` extra, and it is how a package whose own code needs that
    extra spells it. Dropping it would empty the names an import may be provided by, so an import the
    declaration plainly covers would be refused as `undeclared`, and its floor would never be passed
    to the installer under `lowest-direct`.
    """
    declared: list[tuple[str, str]] = []
    for raw in metadata.get_all("Requires-Dist") or []:
        requirement, _, marker = str(raw).partition(";")
        if "extra" in marker:
            continue
        matched = _REQUIREMENT.match(requirement)
        if matched is None:
            continue
        declared.append((matched.group("name"), matched.group("rest").strip()))
    return declared


def _artifacts(directory: Path) -> list[tuple[str, Path]] | None:
    """The one sdist and the one wheel in `directory`, sdist first, or None when it holds another count."""
    found: list[tuple[str, Path]] = []
    for label, suffix in (("sdist", SDIST_SUFFIX), ("wheel", WHEEL_SUFFIX)):
        matches = sorted(path for path in directory.glob(f"*{suffix}") if path.is_file())
        if len(matches) != 1:
            return None
        found.append((label, matches[0]))
    return found


def _last_line(result: subprocess.CompletedProcess) -> str:
    """The last non-blank line a command printed, whichever stream carried it."""
    for stream in (result.stderr, result.stdout):
        lines = [line.strip() for line in (stream or "").splitlines() if line.strip()]
        if lines:
            return lines[-1]
    return f"exit status {result.returncode} with no output"


def _installed(
    artifact: Path, resolution: str, declared: list[tuple[str, str]], environment: Path
) -> tuple[Path | None, str]:
    """A fresh environment holding the artifact and the dependencies its own metadata declares.

    The second member is the installer's last line when the environment could not be made, and the
    empty string when it could. Under `lowest-direct` the declared requirements are passed beside the
    artifact because the option governs only the requirements named on the command line.
    """
    created = subprocess.run(
        [UV, "venv", "--quiet", "--python", ARTIFACT_PYTHON, str(environment)],
        capture_output=True,
        text=True,
        check=False,
    )
    if created.returncode != 0:
        return None, _last_line(created)
    parts = ("Scripts", "python.exe") if os.name == "nt" else ("bin", "python")
    python = environment.joinpath(*parts)
    command = [UV, "pip", "install", "--python", str(python), "--resolution", resolution]
    if resolution == "lowest-direct":
        command.extend(f"{name}{rest}" for name, rest in declared)
    command.append(str(artifact))
    installed = subprocess.run(command, capture_output=True, text=True, check=False)
    if installed.returncode != 0:
        return None, _last_line(installed)
    return python, ""


def _scan(python: Path, distribution: str, report_path: Path) -> dict[str, object]:
    """What the fresh environment holds: the modules that imported, the ones that did not, and the imports."""
    result = subprocess.run(
        [str(python), "-c", _SCAN_PROGRAM, distribution, str(report_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not report_path.is_file():
        return {"error": _last_line(result), "failures": {}, "imports": [], "modules": [], "providers": {}}
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        return {"error": f"the scan report is not JSON ({error})", "failures": {}, "imports": [], "providers": {}}


def _artifact_refusals(label: str, artifact: Path, resolution: str) -> list[tuple[str, str]]:
    """Every ground one artifact gives, in the order a reader acts on them: declaration, install, imports."""
    try:
        metadata = _metadata(artifact)
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        return [
            (
                "install-failed",
                f"{label} {artifact.name} cannot be read as a distribution "
                f"({type(error).__name__}: {error}); pass an artifact a build wrote",
            )
        ]
    declared = _requirements(metadata)
    distribution = str(metadata.get("Name") or "")
    refusals: list[tuple[str, str]] = [
        (
            "unconstrained",
            f"{label} {artifact.name} declares {dependency} with no version specifier; "
            "state the range the package is tested against",
        )
        for dependency, rest in declared
        if not _SPECIFIER.search(rest)
    ]
    with tempfile.TemporaryDirectory(prefix="check-install-") as work:
        python, failure = _installed(artifact, resolution, declared, Path(work) / "environment")
        if python is None:
            refusals.append(
                (
                    "install-failed",
                    f"installing {label} {artifact.name} into a fresh {ARTIFACT_PYTHON} environment failed: {failure}",
                )
            )
            return refusals
        report = _scan(python, distribution, Path(work) / "scan.json")
    if report.get("error"):
        refusals.append(
            (
                "import-failed",
                f"the modules of {label} {artifact.name} could not be scanned: {report['error']}",
            )
        )
        return refusals
    failures = report.get("failures") or {}
    for module in sorted(failures):
        refusals.append(("import-failed", f"{module} does not import in the {label} environment ({failures[module]})"))
    named = {_normalized(dependency) for dependency, _ in declared}
    provided: set[str] = set()
    providers = report.get("providers") or {}
    for imported in report.get("imports") or []:
        owners = [_normalized(owner) for owner in providers.get(imported, [])]
        if not owners:
            refusals.append(
                (
                    "undeclared",
                    f"{imported} maps to no installed distribution: the {label} {artifact.name} imports it "
                    "and nothing declares it",
                )
            )
            continue
        declared_owner = named.intersection(owners)
        if not declared_owner:
            refusals.append(
                (
                    "undeclared",
                    f"{imported} is provided by {', '.join(sorted(owners))}, which {artifact.name} does not declare",
                )
            )
            continue
        provided.update(declared_owner)
    for dependency, _ in declared:
        if _normalized(dependency) not in provided:
            refusals.append(
                (
                    "unimported",
                    f"{artifact.name} declares {dependency} and no module of the {label} imports it; "
                    "drop it from the declaration or import it",
                )
            )
    return refusals


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """The directory of built artifacts, and the resolution the installer resolves at."""
    parser = argparse.ArgumentParser(
        description="Check that each built distribution installs alone, imports, and declares what it uses."
    )
    parser.add_argument(
        "--dist",
        required=True,
        help="the directory holding the one sdist and the one wheel a build wrote, for example dist",
    )
    parser.add_argument(
        "--resolution",
        default=RESOLUTIONS[0],
        choices=RESOLUTIONS,
        help=(
            "the resolution the installer uses: 'highest' for what a user's install gets today, "
            "'lowest-direct' for the floor the declaration promises"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Print one line per refusal and one summary line, and return the exit status."""
    arguments = _parse_arguments(argv)
    directory = Path(arguments.dist)
    if shutil.which(UV) is None:
        print(f"{UV} is not on PATH; install uv, or run this check where it is", file=sys.stderr)
        return EXIT_USAGE
    artifacts = _artifacts(directory)
    if artifacts is None:
        refusals = [
            (
                "wrong-artifacts",
                f"{directory} does not hold exactly one {SDIST_SUFFIX} and one {WHEEL_SUFFIX}; "
                "pass the directory one build of one release wrote",
            )
        ]
    else:
        refusals = []
        for label, artifact in artifacts:
            refusals.extend(_artifact_refusals(label, artifact, arguments.resolution))
    for name, detail in refusals:
        print(f"refused: {name}: {detail}")
    print(f"{directory}: " + (f"{len(refusals)} refusal(s)" if refusals else "conforms"))
    return 1 if refusals else 0


if __name__ == "__main__":
    sys.exit(main())
