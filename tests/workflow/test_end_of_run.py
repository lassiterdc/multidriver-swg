"""Check the end-of-run step: the record directory, the conditional divergence report, and the final rule.

The step is two facts in one artifact: a run always leaves a record directory, and it leaves the divergence
report in it only when some pair ran its pre-refactor side. A check that could not tell those apart would
pass a writer that always writes the report (a report about a comparison the run never made) or one that
never writes it (a run that used legacy code and said nothing). The unit checks drive both records
directly; the engine checks run the real workflow over a committed copy and the record the pre-run step
writes, because the rule's input list, the directory output, the report flag and the second invocation's
behaviour are what a run depends on and a mocked engine would let a wrong one pass.

Every check names the wrong implementation it is red under, because a check that cannot fail for the reason
it claims constrains nothing.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

import multidriver_swg.workflow.pre_run as pre_run
from multidriver_swg.run_configuration.main_configuration import MainConfiguration
from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.end_of_run import (
    DIVERGENCE_HTML,
    DIVERGENCE_TEXT,
    END_OF_RUN_RECORD,
    END_OF_RUN_RULE,
    render_divergence_html,
    write_end_of_run_record,
)
from multidriver_swg.workflow.pre_run import prepare_run
from multidriver_swg.workflow.report_items import CITATION, DIVERGENCE
from tests.workflow.conftest import ToolkitCopy

PROJECT = "smoke.run"

# The text a report is rendered from, carrying the three characters the renderer must escape and a `pre`
# block's own delimiters, so a renderer that embeds the text unescaped breaks the page rather than passing.
SPECIMEN_TEXT = 'Pairs covered: 1\nPair a diverges: <depth> & <surge> (reason: "why")\n</pre><b>not markup</b>\n'


@dataclass(frozen=True)
class _Estate:
    """The three keys' directories and files, the declaration, and the configuration."""

    root: Path
    project_directory: Path
    configuration: Path
    bindings: Path
    run_output: Path
    selection: Path


def _estate(tmp_path: Path, toolkit: ToolkitCopy) -> _Estate:
    """Write a full estate: a binding document for all three keys, the declaration, and the configuration.

    The selection key is bound and its file is absent, which is the branch every run takes until a selection
    exists: the record is then written with the shortened target list, and the end-of-run record is its last
    target.
    """
    root = tmp_path / "estate"
    project_directory = root / "projects" / "smoke"
    project_directory.mkdir(parents=True)
    (root / "config").mkdir(parents=True)
    run_output = tmp_path / "run"
    run_output.mkdir()
    selection = project_directory / "selection.toml"

    bindings = root / "config" / "paths_test.toml"
    bindings.write_text(
        "\n".join(
            [
                'rule = "path-bindings:1"',
                "",
                "[paths]",
                f'"{PROJECT}.project_directory" = "{project_directory}"',
                f'"{PROJECT}.run_output" = "{run_output}"',
                f'"{PROJECT}.selection" = "{selection}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (project_directory / "toolkit_revision.toml").write_text(
        f'rule = "toolkit-revision:1"\ncommit = "{toolkit.commit}"\n', encoding="utf-8"
    )

    configuration = root / "config" / "run.toml"
    configuration.write_text(
        'rule = "main-configuration:1"\n'
        f'estate_root = "{root}"\n'
        'bindings = "config/paths_test.toml"\n'
        f'project = "{PROJECT}"\n'
        'mode = "local"\n\n[local]\ncores = 1\n',
        encoding="utf-8",
    )

    return _Estate(
        root=root,
        project_directory=project_directory,
        configuration=configuration,
        bindings=bindings,
        run_output=run_output,
        selection=selection,
    )


def _prepare(monkeypatch: pytest.MonkeyPatch, toolkit: ToolkitCopy, estate: _Estate):
    """Run the pre-run step as the command would, with the revision gate's package pointed at the copy."""
    monkeypatch.setattr(pre_run, "running_package_directory", lambda: toolkit.package)
    return prepare_run(MainConfiguration.load(estate.configuration), estate.configuration)


def _pre_text(document: str) -> str:
    """Return the content of the document's one `pre` element, which is the escape of the text it renders."""
    matches = re.findall(r"<pre>(.*?)</pre>", document, re.S)
    assert len(matches) == 1, document
    return matches[0]


def _report_categories(report: Path) -> dict[str, dict[str, list[str]]]:
    """Return the category table the report page carries, read from the page's own data."""
    text = report.read_text(encoding="utf-8")
    prefix = "var categories = "
    line = next(candidate for candidate in text.splitlines() if candidate.strip().startswith(prefix))
    return json.loads(line.strip().removeprefix(prefix).removesuffix(";"))


def test_a_pre_refactor_record_writes_both_report_files(tmp_path: Path) -> None:
    """Property: a record with one pre-refactor side creates the directory, writes the text report, and writes the HTML rendering of that text.

    Class: a record naming one pair on its pre-refactor side; the satisfying input is the all-current
    record of the next test.
    Why it is required: this is the whole of the conditional — the run used a legacy function, so its reader
    has to be told where the legacy and current code differ, in a form a browser can open as well as one a
    terminal can.
    What kills it: writing neither file, writing the text and not the HTML, or rendering the HTML from
    anything other than the text that was written.
    Second correct implementation: rendering the HTML from the report structure before serialising the
    text; both files then carry the same statements and the unescape check holds.
    """
    record_directory = tmp_path / "end_of_run" / "record"

    write_end_of_run_record(record_directory, side_record=SideRecord({"a": "pre_refactor"}))

    text_path = record_directory / DIVERGENCE_TEXT
    html_path = record_directory / DIVERGENCE_HTML
    assert record_directory.is_dir()
    assert text_path.is_file()
    assert html_path.is_file()

    text = text_path.read_text(encoding="utf-8")
    assert text.startswith("Pairs covered: "), text
    assert html.unescape(_pre_text(html_path.read_text(encoding="utf-8"))) == text


def test_an_all_current_record_creates_the_directory_and_writes_neither_file(tmp_path: Path) -> None:
    """Property: a record whose every side is current, and a record with no pairs, each create the directory and write nothing into it.

    Class: an all-current record with one entry, and the empty record a run with no registered pair builds.
    Why it is required: a run that used no legacy function has no divergence to report, and a report it did
    not make the comparison for would be confident and wrong; the empty directory is the true statement that
    the run finished.
    What kills it: writing the report whenever the directory is created, or reading `ran_pre_refactor` from
    the presence of entries rather than from their sides.
    Second correct implementation: branching on the set of recorded sides; the same two records write
    neither file.
    """
    for sides in ({"a": "current"}, {}):
        record_directory = tmp_path / "end_of_run" / f"record_{len(sides)}"
        write_end_of_run_record(record_directory, side_record=SideRecord(sides))

        assert record_directory.is_dir()
        assert list(record_directory.iterdir()) == []


def test_the_html_escapes_the_text_and_declares_its_charset() -> None:
    """Property: `<`, `>` and `&` in the text appear escaped in the document, the document declares `utf-8`, and its title names the report.

    Class: a text carrying all three characters plus a closing `pre` tag and an element of its own.
    Why it is required: the text is a report of what the code does, so a character that reaches the markup
    unescaped either disappears into it or breaks the page, and a reader would then be looking at something
    the run did not write.
    What kills it: interpolating the text without escaping, or escaping only some of the three.
    Second correct implementation: any escaping that is its own inverse; the unescape check and the
    absence of the raw sequence both decide.
    """
    document = render_divergence_html(SPECIMEN_TEXT)

    assert '<meta charset="utf-8">' in document
    assert f"<title>{DIVERGENCE}</title>" in document
    assert "&lt;depth&gt;" in document
    assert "&amp;" in document
    assert "<depth>" not in document
    assert "</pre><b>" not in document
    assert _pre_text(document) != SPECIMEN_TEXT
    assert html.unescape(_pre_text(document)) == SPECIMEN_TEXT


def test_a_prepared_run_builds_the_record_and_leaves_no_divergence_file(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, engine
) -> None:
    """Property: an engine run over the record the pre-run step writes builds `end_of_run/record`, and the record holds neither report file.

    Class: the shortened target list, which ends with the end-of-run record, over a clean committed copy.
    Why it is required: the record is the run's closing artifact and its only place in the target list is the
    last entry; a list that omitted it would build every step and produce no record, and a rule reading its
    own output would be a cycle the engine refuses.
    What kills it: leaving the record out of the computed list, giving the rule the record among its inputs,
    or writing the report on a run whose every pair ran its current side.
    Second correct implementation: any rule whose input is the list minus its own output; the same directory
    and the same absent files result.
    """
    estate = _estate(tmp_path, toolkit_copy)
    record = _prepare(monkeypatch, toolkit_copy, estate)
    assert record.targets[-1] == END_OF_RUN_RECORD

    completed = engine(toolkit_copy, estate)

    assert completed.returncode == 0, completed.stderr
    record_directory = estate.run_output / END_OF_RUN_RECORD
    assert record_directory.is_dir()
    assert not (record_directory / DIVERGENCE_TEXT).exists()
    assert not (record_directory / DIVERGENCE_HTML).exists()


def test_a_second_invocation_finds_the_record_up_to_date_and_does_not_run_the_rule(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, engine
) -> None:
    """Property: a second direct invocation of the same run exits 0 with nothing to do, so the final rule does not run again.

    Class: the same estate run twice, the second time with every output present.
    Why it is required: the final rule is output-bearing, and an output-bearing rule that ran on every
    invocation would rewrite the run's record on a re-run that changed nothing — which a reader of the
    record would reasonably read as a second run.
    What kills it: an output the engine does not see (a directory it does not track, or a rule with no
    declared output), so the rule is scheduled every time.
    Second correct implementation: any output the engine records; the second run's own report is the check.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare(monkeypatch, toolkit_copy, estate)
    first = engine(toolkit_copy, estate)
    assert first.returncode == 0, first.stderr

    second = engine(toolkit_copy, estate)

    assert second.returncode == 0, second.stderr
    assert "Nothing to be done" in second.stderr, second.stderr
    assert (estate.run_output / END_OF_RUN_RECORD).is_dir()


def test_the_report_shows_the_divergence_only_after_the_record_holds_one(
    tmp_path: Path, toolkit_copy: ToolkitCopy, monkeypatch: pytest.MonkeyPatch, engine
) -> None:
    """Property: a run's report carries the citation section and no divergence item, and a report built after a divergence file is placed in the record carries it under its own category.

    Class: the run's own report over the shortened list, then a report-only invocation over the same tree
    with one `divergence_report.html` placed inside the record directory.
    Why it is required: the record directory is what the report flags, and the file inside it appears only
    when a run selected a pre-refactor side — which no configuration can do yet — so this is the only way to
    show that the page presents the divergence under its own category rather than as a download or not at
    all. It is also what shows the directory flag works with the single-file report.
    What kills it: flagging the record without the patterns form (the file is then unregistered, or the
    report build refuses the directory), flagging it under the citation heading, or omitting the flag.
    Second correct implementation: the htmlindex form of the same flag over a directory holding an
    `index.html`; the category and the item are what the check reads.
    """
    estate = _estate(tmp_path, toolkit_copy)
    _prepare(monkeypatch, toolkit_copy, estate)
    report = estate.run_output / "report.html"

    completed = engine(
        toolkit_copy,
        estate,
        extra_arguments=("--report", str(report), "--report-after-run"),
    )

    assert completed.returncode == 0, completed.stderr
    record_directory = estate.run_output / END_OF_RUN_RECORD
    assert record_directory.is_dir()
    before = _report_categories(report)
    assert set(before) == {CITATION}

    (record_directory / DIVERGENCE_HTML).write_text(render_divergence_html(SPECIMEN_TEXT), encoding="utf-8")

    rebuilt = engine(toolkit_copy, estate, extra_arguments=("--report", str(report)))

    assert rebuilt.returncode == 0, rebuilt.stderr
    after = _report_categories(report)
    assert set(after) == {CITATION, DIVERGENCE}
    shown = [path for paths in after[DIVERGENCE].values() for path in paths]
    assert shown == [f"{END_OF_RUN_RECORD}/{DIVERGENCE_HTML}"]


def test_the_module_names_the_rule_and_its_one_output() -> None:
    """Property: the rule's name and its record path are the constants the workflow file and the target list read, and the record path is under the rule's name.

    Class: the module's two constants.
    Why it is required: the workflow file, the target list and the report all name the same rule; a record
    path that did not sit under the rule's own directory would break the run output's one-directory-per-step
    form that a reader navigates by.
    What kills it: renaming either constant, or moving the record out of the rule's directory.
    Second correct implementation: the path composed from the rule's name, as here; the same two values
    result.
    """
    assert END_OF_RUN_RULE == "end_of_run"
    assert END_OF_RUN_RECORD == "end_of_run/record"
    assert f"{END_OF_RUN_RULE}/record" == END_OF_RUN_RECORD
