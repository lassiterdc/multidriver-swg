"""Review the final record's selection boundary and its mixed-side branch.

An empty registry cannot distinguish a live side record from a constant empty record. A child
interpreter registers a pair through the public API, keeping the parent test registry unchanged.
A separate mixed-side case ensures that one legacy side, not every side, triggers both reports.
"""

from __future__ import annotations

import html
import json
import re
import subprocess
import sys
from pathlib import Path

from multidriver_swg.side_record import SideRecord
from multidriver_swg.workflow.end_of_run import DIVERGENCE_HTML, DIVERGENCE_TEXT, write_end_of_run_record


def test_the_run_side_record_tracks_pairs_registered_at_call_time() -> None:
    """Property: a newly registered pair appears as current in the next run-side record.

    Class: registries changing between two calls through public pair registration.
    Why it is required: a captured empty or stale mapping omits pairs the run can select, so the final
    record no longer describes the run's default selection.
    What kills it: returning an empty SideRecord or caching the registry on the first call.
    Second correct implementation: rebuilding the mapping directly from the registry on each call,
    rather than delegating through the selection helper, preserves the same observable mapping.
    """
    probe = "\n".join(
        [
            "import json",
            "from multidriver_swg.pairs import pair_side",
            "from multidriver_swg.workflow.end_of_run import run_side_record",
            "before = run_side_record().as_mapping()",
            "pair_side(pair='review_pair', side='current')(len)",
            "pair_side(pair='review_pair', side='pre_refactor')(len)",
            "after = run_side_record().as_mapping()",
            "print(json.dumps([before, after]))",
        ]
    )
    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    before, after = json.loads(completed.stdout)

    assert all(side == "current" for side in before.values())
    assert after == {**before, "review_pair": "current"}


def test_one_legacy_side_among_current_sides_writes_matching_reports(tmp_path: Path) -> None:
    """Property: one legacy entry among current entries writes both reports with identical decoded text.

    Class: mixed side records with the legacy entry ordered after a current one.
    Why it is required: the reporting predicate is whether any pair selected legacy, not whether the
    first pair did or all pairs did; the HTML must preserve the text actually written.
    What kills it: replacing the any-side predicate with an all-side or first-entry predicate, or
    rendering unrelated text into the HTML file.
    Second correct implementation: testing membership in the set of selected side values and rendering
    the text through a template produces the same two files and decoded content.
    """
    destination = tmp_path / "nested" / "record"
    write_end_of_run_record(destination, side_record=SideRecord({"a": "current", "z": "pre_refactor"}))

    text = (destination / DIVERGENCE_TEXT).read_text(encoding="utf-8")
    document = (destination / DIVERGENCE_HTML).read_text(encoding="utf-8")
    pre_blocks = re.findall(r"<pre>(.*?)</pre>", document, re.DOTALL)

    assert text.startswith("Pairs covered: ")
    assert len(pre_blocks) == 1
    assert html.unescape(pre_blocks[0]) == text
