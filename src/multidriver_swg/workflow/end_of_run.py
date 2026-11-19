"""The end-of-run step: the run's one final rule, the record directory it writes, and the divergence report.

A run's last act is to produce one artifact that says what the run was: the end-of-run record. It is a
DIRECTORY rather than a file because the record is a place — the divergence report lives inside it when
the run used a pre-refactor side, and a later step writes more into it — and because a directory output is
what lets the rule be the last rule with no other rule waiting on it.

`END_OF_RUN_RECORD` is a path relative to the engine's working directory, and that is the whole of what
this module says about location: the command resolves the working directory through the estate, so naming
an absolute place here would be a second statement of a resolution another system owns.

The divergence report appears only when the run selected a pre-refactor side of some pair. A run whose
every pair ran its current side used no legacy function, so a report of divergences would be a report
about a comparison the run never made; the record directory is created empty in that case, which is still
a statement — the run finished and it has a record.

The HTML file is a rendering of the text, not a second report: the text is written first and the HTML is
escaped from the text that was written, so the two cannot disagree about what the run diverged from. The
report is not built by this step — the engine builds it after a successful run from the artifacts the
rules flag — and a failed run therefore has no report.

Not established: that the divergence ledger is valid (`divergence_report.write_report` refuses one that is
not), that the record directory is writable, or that a run under a scheduler reaches this rule on the
driver. The rule is declared local so it runs where the engine's metadata store is, and the cluster
measurement is what shows whether that holds.
"""

from __future__ import annotations

import html
from pathlib import Path

from multidriver_swg import divergence_report
from multidriver_swg.dispatch import SideSelection
from multidriver_swg.side_record import SideRecord, side_record
from multidriver_swg.workflow.report_items import DIVERGENCE

# The rule's name. The workflow file writes this rule by hand rather than rendering it from the table,
# because the table holds compute systems and this rule computes nothing: it is the run's closing step.
END_OF_RUN_RULE = "end_of_run"

# The rule's one declared output: the record directory, relative to the engine's working directory. Every
# other artifact of the run lives under its own rule's directory, and this one keeps that form so a reader
# of the run output sees one directory per step and no special case.
END_OF_RUN_RECORD = f"{END_OF_RUN_RULE}/record"

# The divergence report's two files, inside the record directory. The names are stated once here because
# the writer and every reader of the record must agree on them, and the HTML file is the one the run's
# report shows.
DIVERGENCE_TEXT = "divergence_report.txt"

DIVERGENCE_HTML = "divergence_report.html"


def render_divergence_html(text: str) -> str:
    """Return `text` as one HTML document: the report's title, and the text escaped inside one `pre` block.

    Escaped rather than embedded, because the text is a report of the code's departures and any `<`, `>` or
    `&` it carries is content: an unescaped one would either vanish into the page's markup or break it, and
    a reader of the page would be looking at something the run did not write.

    The document carries its charset in a `meta` element rather than in a byte-order mark, so the file's
    bytes and its declared encoding are the same fact however it is opened. The text sits in one `pre` block
    so the line structure the text report has — one statement to a line — survives into the page.
    """
    return (
        "<!DOCTYPE html>\n"
        "<html>\n"
        "<head>\n"
        '<meta charset="utf-8">\n'
        f"<title>{DIVERGENCE}</title>\n"
        "</head>\n"
        "<body>\n"
        f"<pre>{html.escape(text)}</pre>\n"
        "</body>\n"
        "</html>\n"
    )


def run_side_record() -> SideRecord:
    """Return the record of this run's side selection: the current side of every registered pair.

    A run built through this command selects no override, because the main configuration carries no
    pre-refactor setting: its fields are the rule, the estate root, the bindings, the project, the mode, the
    judgement section and the two mode sections, and the selection value's default side is the current side
    of every pair. So the record this function builds is the record of that default, and a run it describes
    diverges from nothing.

    This function is the ONE site a per-run setting replaces. A setting that names the pre-refactor side,
    per run or per pair, arrives here and nowhere else: the step below takes the record as a parameter, so a
    record naming a pre-refactor side drives the divergence branch with no other change, and the tests drive
    that branch with such a record rather than with a setting no configuration can carry yet.
    """
    return side_record(SideSelection())


def write_end_of_run_record(record_directory: Path, *, side_record: SideRecord) -> None:
    """Create `record_directory` and, when the run selected a pre-refactor side, write the divergence report into it.

    The directory is created unconditionally: a run that finished has a record, and a step that produced
    nothing at all would be indistinguishable from a step that never ran. When
    `side_record.ran_pre_refactor` is false, nothing else is written, and the empty directory is the true
    statement that the run used no legacy function.

    The text report is written through `divergence_report.write_report`, which reads the ledger and the pair
    registry and refuses a ledger of the wrong form; the HTML file is then rendered from the text that was
    written, so the page and the text cannot say different things.
    """
    record_directory.mkdir(parents=True, exist_ok=True)
    if not side_record.ran_pre_refactor:
        return

    text_path = record_directory / DIVERGENCE_TEXT
    with text_path.open("w", encoding="utf-8") as stream:
        divergence_report.write_report(stream)
    (record_directory / DIVERGENCE_HTML).write_text(
        render_divergence_html(text_path.read_text(encoding="utf-8")), encoding="utf-8"
    )
