"""Review tests for the run-configuration reading surface: the docs, the exports and the no-machine-value rule.

These checks exist because each one crosses an artifact boundary the unit tests cannot see from inside
a module. The field table an operator reads is a different file from the loader that refuses its
fields, the navigation entry is another file again, and the package's re-export list is a fourth. A
change that keeps every unit test green and still leaves the page, the navigation, the exports or the
no-machine-value rule behind is the failure these tests are written against.

They read the artifacts as TEXT — the Markdown table, the navigation file, the module source — rather
than importing a rendered form, because the defect is a disagreement between what was written and what
runs, and a rendered page would hide the wording an operator actually reads.
"""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

import pytest

import multidriver_swg.run_configuration as run_configuration
import multidriver_swg.run_configuration.main_configuration as main_configuration
import multidriver_swg.run_configuration.revision_gate as revision_gate
from multidriver_swg.run_configuration.main_configuration import MainConfiguration, MainConfigurationError
from multidriver_swg.run_configuration.provenance import ProvenanceError, read_identity
from multidriver_swg.run_configuration.revision_gate import REVISION_RULE

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_PAGE = REPO_ROOT / "docs" / "main-configuration.md"
MKDOCS = REPO_ROOT / "mkdocs.yml"

# The names the package must re-export, and the constants it must not: a constant a caller compares
# against is the module's own, and a package-level alias would invite a comparison against a name the
# package file could change. Both halves of that sentence are asserted below.
EXPECTED_EXPORTS = {
    "DeclaredRevision",
    "JudgementConfiguration",
    "LocalConfiguration",
    "MainConfiguration",
    "MainConfigurationError",
    "RevisionGateError",
    "SlurmConfiguration",
    "load_declared_revision",
    "persist_identity",
    "read_identity",
    "require_clean_checkout",
    "require_declared_revision",
    "require_package_checkout",
    "require_revision_gate",
    "running_package_directory",
    "running_revision",
}
NOT_RE_EXPORTED = ("MAIN_CONFIGURATION_RULE", "REVISION_RULE", "IDENTITY_RULE", "MODES", "PATH_KEY_KINDS")

# A path that only one machine could hold. Neither the modules nor the page may contain one as a
# literal: every machine value belongs in the estate's configuration file.
_HOST_PATH = re.compile(r"(?<![\w.])/(?:home|Users|tmp|mnt|scratch|opt|srv)/")


def _section(text: str, heading: str) -> list[str]:
    """Return the lines of the `## {heading}` section, up to the next second-level heading."""
    lines = text.splitlines()
    start = next(index for index, line in enumerate(lines) if line.strip() == f"## {heading}")
    body: list[str] = []
    for line in lines[start + 1 :]:
        if line.startswith("## "):
            break
        body.append(line)
    return body


def _documented_field_labels(text: str, heading: str = "The fields") -> set[str]:
    """Return the fully qualified field labels a section's Markdown table names in its first column.

    A cell is read as the concatenation of its backticked spellings, so `` `[slurm] partition` `` becomes
    `slurm.partition` and a bare `` `rule` `` stays `rule`. The separator row and the header row carry no
    backticked spelling and are skipped, which is why the extraction keys on the backticks rather than on
    the row's position.
    """
    labels: set[str] = set()
    for line in _section(text, heading):
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not cells:
            continue
        spellings = re.findall(r"`([^`]+)`", cells[0])
        if not spellings:
            continue
        label = spellings[0]
        match = re.fullmatch(r"\[([a-z]+)\] (.+)", label)
        labels.add(f"{match.group(1)}.{match.group(2)}" if match else label)
    return labels


def _code_field_labels() -> set[str]:
    """Return the field labels the loader accepts, built from the module's closed key sets."""
    return (
        {"rule", "estate_root", "bindings", "project", "mode"}
        | {f"judgement.{key}" for key in main_configuration._JUDGEMENT_KEYS}
        | {f"local.{key}" for key in main_configuration._LOCAL_KEYS}
        | {f"slurm.{key}" for key in main_configuration._SLURM_KEYS}
    )


def _absolute_literals(path: Path) -> list[str]:
    """Return every absolute-path string literal in `path`'s source, read as a syntax tree."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and (node.value.startswith("/") or re.match(r"[A-Za-z]:\\\\", node.value))
    ]


def test_the_documented_field_table_names_exactly_the_fields_the_loader_reads():
    """Property: the page's field table and the loader's closed key sets name the same fields, and the table names no field the loader does not read.

    Class: every documented field row and every key in the four closed sets.
    Why it is required: the page IS the operator's contract. A field added to the loader and omitted
    from the page is undocumented configuration; a field documented and not read is a value an operator
    writes and the run silently ignores — which is the defect the loader's unnamed-key refusal exists
    to prevent at the file, reappearing at the page.
    Killed by: adding a key to one of the four sets without a row on the page, adding a row for a field
    no set names, or listing a table (`[slurm]`) rather than its fields.
    Second correct implementation: a page whose table keeps one row per fully qualified field; the
    extraction is order-blind and spelling-normalised.
    """
    documented = _documented_field_labels(DOC_PAGE.read_text(encoding="utf-8"))

    assert documented == _code_field_labels()


def test_the_documented_revision_table_names_exactly_the_declaration_fields():
    """Property: the revision section's table names exactly the three keys the declaration form accepts.

    Class: the declaration's closed key set and the rows under the revision heading.
    Why it is required: the declaration is read from a separate file with its own refusals, and an
    operator who cannot see its three fields on the page will write a fourth that nothing reads.
    Killed by: adding a key to the declaration without a row, or documenting a `branch` key the loader
    refuses.
    Second correct implementation: one row per fully qualified field; the extraction keys on backticks.
    """
    documented = _documented_field_labels(DOC_PAGE.read_text(encoding="utf-8"), "The declared revision")

    assert documented == set(revision_gate._DOCUMENT_KEYS)


def test_the_page_states_the_version_literals_the_modes_and_the_three_path_keys():
    """Property: the page states both version literals, all three mode names and all three fixed path keys.

    Class: the constants the loader and the gate compare against, plus the mode and key-kind tuples.
    Why it is required: these are the values an operator must write verbatim. A page that described the
    modes in prose but never spelled one, or omitted a fixed key, would leave the operator guessing at a
    value the loader compares as a literal.
    Killed by: dropping a mode from the modes section, or a key kind from the path-key table.
    Second correct implementation: the same names in any order and any section; containment is all that
    is asserted.
    """
    text = DOC_PAGE.read_text(encoding="utf-8")

    assert main_configuration.MAIN_CONFIGURATION_RULE in text
    assert REVISION_RULE in text
    for mode in main_configuration.MODES:
        assert mode in text
    for kind in main_configuration.PATH_KEY_KINDS:
        assert kind in text


def test_every_refusal_names_the_field_it_refuses_and_the_file_it_read(tmp_path: Path):
    """Property: each documented refusal's message carries the field's configuration spelling and the configuration file's path.

    Class: one file per refusal row of the page's table — a wrong header, a relative root, an absolute
    binding path, a one-segment project, an unknown mode, a missing `[local]`, a non-positive core count,
    an unnamed top-level key, an incomplete `[slurm]` table and a batch run with no `jobs`.
    Why it is required: an operator repairs a field of an estate-side file, so a message naming only the
    defect strands them; naming only the field leaves them unsure which machine's file was read.
    Killed by: raising a bare `ValueError`, prefixing the file at only some call sites, or spelling a
    nested field as `partition` rather than `slurm.partition`.
    Second correct implementation: attaching the path once in `load` and the label once per check; the
    same ten files produce ten messages with both facts.
    """
    base = (
        'rule = "main-configuration:1"\n'
        'estate_root = "/estate"\n'
        'bindings = "config/paths_this_machine.toml"\n'
        'project = "somewhere.river"\n'
    )
    cases = [
        (
            "rule.toml",
            "rule",
            'rule = "main-configuration:2"\nestate_root = "/estate"\n'
            'bindings = "config/paths_this_machine.toml"\nproject = "somewhere.river"\n'
            'mode = "local"\n[local]\ncores = 4\n',
        ),
        (
            "estate_root.toml",
            "estate_root",
            'rule = "main-configuration:1"\nestate_root = "estate"\n'
            'bindings = "config/paths_this_machine.toml"\nproject = "somewhere.river"\n'
            'mode = "local"\n[local]\ncores = 4\n',
        ),
        (
            "bindings.toml",
            "bindings",
            'rule = "main-configuration:1"\nestate_root = "/estate"\n'
            'bindings = "/config/paths_this_machine.toml"\nproject = "somewhere.river"\n'
            'mode = "local"\n[local]\ncores = 4\n',
        ),
        (
            "project.toml",
            "project",
            'rule = "main-configuration:1"\nestate_root = "/estate"\n'
            'bindings = "config/paths.toml"\nproject = "somewhere"\n'
            'mode = "local"\n[local]\ncores = 4\n',
        ),
        ("mode.toml", "mode", base + 'mode = "distributed"\n[local]\ncores = 4\n'),
        ("local_table.toml", "local.cores", base + 'mode = "local"\n'),
        ("local_cores.toml", "local.cores", base + 'mode = "local"\n[local]\ncores = 0\n'),
        (
            "threads.toml",
            "threads",
            'rule = "main-configuration:1"\nestate_root = "/estate"\n'
            'bindings = "config/paths.toml"\nproject = "a.b"\nmode = "local"\nthreads = 8\n'
            "[local]\ncores = 4\n",
        ),
        (
            "slurm_account.toml",
            "slurm.account",
            base + 'mode = "batch_job"\n[slurm]\npartition = "compute"\ntime_minutes = 60\njobs = 4\n',
        ),
        (
            "slurm_jobs.toml",
            "slurm.jobs",
            base + 'mode = "batch_job"\n[slurm]\npartition = "compute"\naccount = "allocation"\ntime_minutes = 60\n',
        ),
    ]

    for filename, label, text in cases:
        path = tmp_path / filename
        path.write_text(text, encoding="utf-8")
        with pytest.raises(MainConfigurationError) as caught:
            MainConfiguration.load(path)
        message = str(caught.value)
        assert str(path) in message, label
        assert f"'{label}'" in message, label


def test_the_page_is_navigable_and_exists_where_the_navigation_says():
    """Property: the navigation file lists the page under its title, and the listed file exists.

    Class: the navigation entry and the file it names.
    Why it is required: a page outside the navigation is reachable only by a reader who already knows
    its path — which the operator creating a first configuration does not.
    Killed by: adding the page without a nav line, adding a nav line with a filename that does not exist,
    or naming the page something else in the two places.
    Second correct implementation: an entry at another position in the navigation; the check reads the
    line, not its index.
    """
    navigation = MKDOCS.read_text(encoding="utf-8")

    assert "  - Main Configuration: main-configuration.md" in navigation
    assert (REPO_ROOT / "docs" / "main-configuration.md").is_file()


def test_the_package_reexports_the_surface_and_keeps_the_constants_in_their_modules():
    """Property: every new public name resolves through the package and appears in `__all__`, and the version literals, modes and key kinds do not.

    Class: the re-export names and the constants beside them.
    Why it is required: a sibling imports from the package, so a name missing there is a second import
    route a reader has to discover; and a re-exported constant invites a comparison against a name the
    package file could change instead of the module that owns it.
    Killed by: adding an import without an `__all__` entry, or re-exporting `MAIN_CONFIGURATION_RULE`.
    Second correct implementation: any import order; the checks are membership assertions.
    """
    for name in EXPECTED_EXPORTS:
        assert name in run_configuration.__all__, name
        assert hasattr(run_configuration, name), name

    for name in NOT_RE_EXPORTED:
        assert name not in run_configuration.__all__, name
        assert not hasattr(run_configuration, name), name


def test_the_surface_modules_and_the_page_carry_no_machine_specific_path():
    """Property: no module of the new surface holds an absolute-path string literal, and the page holds no host path.

    Class: the three modules' string constants and the page's text.
    Why it is required: the whole point of the configuration file is that a machine value never lands in
    a tracked toolkit file; a default estate root or a default document path in code is exactly that
    leak, and it is silent because the value works on the machine that wrote it.
    Killed by: adding a default home-directory path, a documented example with a real host path, or a
    fallback document location.
    Second correct implementation: placeholders and relative paths only; the same three modules and the
    page carry none.
    """
    for module_path in (
        REPO_ROOT / "src" / "multidriver_swg" / "run_configuration" / "main_configuration.py",
        REPO_ROOT / "src" / "multidriver_swg" / "run_configuration" / "revision_gate.py",
        REPO_ROOT / "src" / "multidriver_swg" / "run_configuration" / "provenance.py",
    ):
        assert _absolute_literals(module_path) == [], module_path.name

    assert _HOST_PATH.findall(DOC_PAGE.read_text(encoding="utf-8")) == []


def test_the_page_documents_the_three_execution_checks_the_gate_performs():
    """Property: the page names the three checks a run performs before computing: the package's checkout, the commit comparison, and the clean tree.

    Class: the page's declared-revision section.
    Why it is required: the gate can stop a run an operator believes is configured correctly, and the
    page is the only place that operator learns why — an undocumented refusal reads as a broken run.
    Killed by: documenting the declaration's fields and not the three checks that read them.
    Second correct implementation: the three facts in any order and wording that names each; the check
    asserts the three distinguishing phrases.
    """
    section = " ".join(" ".join(_section(DOC_PAGE.read_text(encoding="utf-8"), "The declared revision")).split())

    assert "source tree is the directory the package was imported from" in section
    assert "commit equals the declared" in section
    assert "working tree is clean" in section


# ---------------------------------------------------------------------------------------------------
# Findings raised against the commit these tests were written against. Each test below is red at that
# commit and states the input class it ranges over. They are not to be weakened, skipped or deleted:
# the repair is to the code or the file the test names.
# ---------------------------------------------------------------------------------------------------

# The ten files this chunk writes, as its write-set declaration lists them.
CHUNK_WRITE_SET = (
    "src/multidriver_swg/run_configuration/main_configuration.py",
    "src/multidriver_swg/run_configuration/revision_gate.py",
    "src/multidriver_swg/run_configuration/provenance.py",
    "src/multidriver_swg/run_configuration/__init__.py",
    "tests/test_main_configuration.py",
    "tests/test_revision_gate.py",
    "tests/test_provenance.py",
    "tests/test_main_configuration_review.py",
    "docs/main-configuration.md",
    "mkdocs.yml",
)

ANONYMIZATION_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "anonymization-guard.yml"


def _workflow_shell_literal(workflow_text: str, variable: str) -> str:
    """Return the single-quoted value the anonymization workflow assigns to the shell variable `variable`."""
    match = re.search(rf"^\s*{variable}='(.+)'\s*$", workflow_text, flags=re.MULTILINE)
    assert match is not None, f"the workflow no longer assigns {variable}"
    return match.group(1)


def test_no_file_the_chunk_writes_matches_the_anonymization_guards_local_path_pattern():
    """Property: the public-surface check of the anonymization guard, applied to every file this chunk writes, reports no finding.

    Required by: the repository's own guard over the whole tree (the workflow's first check), applied
    to the ten files this chunk adds.
    Class: the ten files of the write-set declaration, each read as text, against the pattern and the
    allowlist the workflow itself carries.
    Why it is required: the workflow's check one fails the job on ANY tracked file outside its
    two-file allowlist that matches its local-path pattern, and that pattern's first alternative is a
    home-directory prefix followed by a run of letters, digits, `.`, `_` and `-`. A prose ellipsis
    after a `home` segment is a run of dots, so it matches; the file that carries one reddens the guard
    on the first push of the branch although it holds no machine path.
    Killed by: any of the ten files carrying a home-directory-shaped, user-directory-shaped,
    drive-letter-shaped or synced-folder token, including one written as an ellipsis in a docstring.
    Second correct implementation: the same file with the offending example reworded so that no
    alternative of the pattern matches; the pattern is read from the workflow, not restated here, so a
    later change to the guard changes this check with it.
    """
    workflow = ANONYMIZATION_WORKFLOW.read_text(encoding="utf-8")
    pattern = re.compile(_workflow_shell_literal(workflow, "pat"))
    allowlist = re.compile(_workflow_shell_literal(workflow, "allow"))

    offenders = [
        relative
        for relative in CHUNK_WRITE_SET
        if not allowlist.search(relative)
        and pattern.search((REPO_ROOT / relative).read_bytes().decode("utf-8", errors="replace"))
    ]

    assert offenders == []


def _committed_checkout(tmp_path: Path, name: str = "toolkit") -> Path:
    """A repository named `name` with one commit, whose message is `name`, and whose source tree holds a `multidriver_swg` package."""
    repository = tmp_path / name
    package = repository / "src" / "multidriver_swg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    for arguments in (
        ("init", "-q"),
        ("add", "-A"),
        ("-c", "user.name=review", "-c", "user.email=review@example.invalid", "commit", "-q", "-m", name),
    ):
        subprocess.run(["git", *arguments], cwd=repository, check=True, capture_output=True, text=True)
    return repository


def test_ambient_git_location_variables_do_not_redirect_the_gate_to_another_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Property: the commit `running_revision` reports and the verdict the composed gate gives describe the checkout they are pointed at, even when the process environment names a different repository through `GIT_DIR` and `GIT_WORK_TREE`.

    Shape row: DR-4 refusals 2 to 4 compare "the running commit" and "that checkout" with the project's
    declaration, and DR-5 records `running_revision()` as every stub's `code_version`.
    Class: one process environment that names a second, unrelated repository, read by the two public
    entry points that run git: `running_revision` and `require_revision_gate`.
    Why it is required: git sets exactly these two variables for every process a hook or a
    `rebase --exec` starts, and a command run from one inherits them. Git then answers every
    `rev-parse` from the named repository whatever the working directory is, so the gate reports the
    OTHER checkout's commit while the code that runs is this one — the silent attribution of output to
    the wrong code that the module's own opening paragraph says the gate exists to prevent. A conforming
    checkout is also refused, naming a package path that is in fact the package of the checkout.
    Killed by: passing the parent environment to `subprocess.run` unchanged.
    Second correct implementation: removing the repository-location variables from the child's
    environment, or addressing the repository with `--git-dir` and `--work-tree` taken from the
    directory itself; the first checkout's commit is returned and the gate admits it either way.
    """
    first = _committed_checkout(tmp_path, "first")
    second = _committed_checkout(tmp_path, "second")
    first_head = revision_gate.running_revision(first)
    second_head = revision_gate.running_revision(second)
    assert first_head != second_head, "the two repositories must hold different commits for this test to mean anything"

    monkeypatch.setenv("GIT_DIR", str(second / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(second))

    assert revision_gate.running_revision(first) == first_head
    assert (
        revision_gate.require_revision_gate(
            first / "src" / "multidriver_swg", revision_gate.DeclaredRevision(commit=first_head)
        )
        == first_head
    )


@pytest.mark.parametrize("gate", ["clean_checkout", "composed_gate"])
def test_a_user_git_setting_that_hides_untracked_files_does_not_hide_one_from_the_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, gate: str
):
    """Property: an untracked file in the checkout is refused even when the user's own git configuration tells `git status` not to list untracked files.

    Shape row: DR-4 refusal 4 — `git status --porcelain` of the checkout is not empty, with "tracked
    changes and untracked files that `.gitignore` does not cover both count, because an untracked
    module can change what runs".
    Class: both public entry points that run the status check, under one user-level setting.
    Why it is required: `status.showUntrackedFiles = no` is a documented, common setting for large
    working trees, and it changes the output of `git status --porcelain` itself. A gate that inherits
    the ambient configuration therefore admits exactly the case the shape names as a reason for the
    check — an untracked module — on the machines of whoever set it, and says nothing. The control
    below shows the setting is live: git itself reports the tree clean.
    Killed by: running `git status --porcelain` with no override of the untracked-files mode.
    Second correct implementation: passing the untracked-files mode on the command line, or reading
    `--porcelain=v1 -z` under it; the untracked module is refused either way.
    """
    repository = _committed_checkout(tmp_path)
    head = revision_gate.running_revision(repository)

    user_configuration = tmp_path / "user.gitconfig"
    user_configuration.write_text("[status]\n\tshowUntrackedFiles = no\n", encoding="utf-8")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(user_configuration))

    (repository / "src" / "multidriver_swg" / "added_module.py").write_text("x = 1\n", encoding="utf-8")

    control = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repository, check=True, capture_output=True, text=True
    )
    assert control.stdout == "", "the user setting must be live for this test to mean anything"

    with pytest.raises(revision_gate.RevisionGateError) as caught:
        if gate == "clean_checkout":
            revision_gate.require_clean_checkout(repository)
        else:
            revision_gate.require_revision_gate(
                repository / "src" / "multidriver_swg", revision_gate.DeclaredRevision(commit=head)
            )

    assert "added_module.py" in str(caught.value)


@pytest.mark.parametrize(
    ("reader", "refusal", "document_name"),
    [
        (MainConfiguration.load, MainConfigurationError, "run_configuration.toml"),
        (revision_gate.load_declared_revision, revision_gate.RevisionGateError, "toolkit_revision.toml"),
        (read_identity, ProvenanceError, "implementation_identity.json"),
    ],
)
def test_a_file_that_is_not_utf8_text_is_refused_by_the_surfaces_own_class_naming_the_file(
    tmp_path: Path, reader, refusal: type[Exception], document_name: str
):
    """Property: a file whose bytes are not UTF-8 is refused with the surface's own refusal class and a message that names the file.

    Shape row: DR-6 — `MainConfiguration.load(path)` "refuses, naming the field and the file, anything
    outside this table"; DR-5 — the identity file is "refused on read" when it is not the form; the
    declaration is the third file the same surfaces read (DR-6, the project's declared revision).
    Class: one document per reader, each holding bytes that no UTF-8 reading decodes.
    Why it is required: each reader decodes the file before it parses it, and a decoding failure is
    the interpreter's `UnicodeDecodeError`, whose message names a byte offset and a codec and no file.
    The reader's own docstring promises that an operator holding a refused file can tell WHICH file
    failed; a file saved in a legacy single-byte encoding by an editor is the ordinary way to reach
    this, and it arrives as an exception no caller catching the surface's refusal class will catch.
    Killed by: calling `Path.read_text(encoding="utf-8")` outside the `try` that translates parse
    failures.
    Second correct implementation: reading bytes and decoding inside the translating `try`; the same
    three files are refused with their path in the message.
    """
    path = tmp_path / document_name
    path.write_bytes(b"rule = \xff\xfe\n")

    with pytest.raises(refusal) as caught:
        reader(path)

    assert str(path) in str(caught.value)
