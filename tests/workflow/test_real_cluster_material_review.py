"""Review the collector against the engine's complete job-header class.

Checkpoint jobs have their own header token; a reader that handles only ordinary rules silently
loses the licence job from the measurement that is meant to explain checkpoint execution.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.workflow.real_cluster.collect_measurement import collect


@pytest.mark.parametrize("header", ["rule", "localrule", "checkpoint", "localcheckpoint"])
def test_the_job_map_keeps_every_engine_job_header_kind(tmp_path: Path, header: str) -> None:
    """Property: every engine job-header kind retains the rule-to-job association in the reading.

    Class: ordinary and checkpoint jobs, each scheduled or local, followed by their engine job id.
    Why it is required: the checkpoint measurement must identify the licence job just as the ordinary
    probe identifies compute jobs; a missing map row prevents matching that job to execution evidence.
    What kills it: recognising only rule/localrule headers, or discarding the header before jobid.
    Second correct implementation: tokenize headers with a state machine instead of a regular
    expression; the same Markdown rule/job association is emitted for all four forms.
    """
    log_directory = tmp_path / ".snakemake" / "log"
    log_directory.mkdir(parents=True)
    (log_directory / "review.snakemake.log").write_text(
        f"{header} review_measurement_licence:\n"
        "    output: licence/licence.json\n"
        "    jobid: 17\n"
        "rule review_measurement_consumer:\n"
        "    jobid: 18\n",
        encoding="utf-8",
    )

    reading = collect(tmp_path)
    associations = {
        name: int(identifier)
        for name, identifier in re.findall(r"\|\s*(review_measurement_\w+)\s*\|\s*(\d+)\s*\|", reading)
    }

    assert associations == {"review_measurement_licence": 17, "review_measurement_consumer": 18}
