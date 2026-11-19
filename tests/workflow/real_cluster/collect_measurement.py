"""Print, as Markdown, what one run directory holds: the job map, the scheduler's view, where things ran, and the record.

A measurement is a run on a machine and then a reading of what the run left. This script is the reading, and
it only reads: it changes nothing in the run directory. It prints five sections.

1. **The engine log's map of rule to job.** Every rule block's `jobid` line, and the scheduler plugin's
   `Job N has been submitted with SLURM jobid M` lines, per log file under `.snakemake/log`.
2. **The scheduler's accounting** for each scheduler job id the log names, from `sacct -X -P`, quoted as the
   command printed it. A machine with no `sacct`, or a log with no scheduler job, is said so.
3. **Where things ran.** Every line of the probe's measurement files, with its host, and the parse counts per
   host with the median and the maximum of the graph-start check's seconds.
4. **The targets**: each target of the run-start record, whether it exists, and the rule its metadata record
   names, read through the toolkit's own reader.
5. **The provenance record** the end-of-run rule wrote, read through the toolkit's own reader.

It uses the standard library and the toolkit only.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from pathlib import Path

from multidriver_swg.workflow.end_of_run import END_OF_RUN_RECORD
from multidriver_swg.workflow.exercised_set import (
    PROVENANCE_FILE,
    ExercisedSetError,
    producing_rules,
    read_provenance,
)
from multidriver_swg.workflow.run_start import RUN_START_FILE, RunStartError, read_run_start

# The probe's measurement files, in the order they are printed. Each is a JSON-lines file under `measurement`.
MEASUREMENT_FILES = (
    "parses.jsonl",
    "handlers.jsonl",
    "checkpoint_parses.jsonl",
    "checkpoint_evaluations.jsonl",
)

# The accounting columns, the ones a measurement quotes.
SACCT_FORMAT = "JobID,JobName,State,ExitCode,NodeList,Start,End"

_RULE_BLOCK = re.compile(r"^(?:local)?rule (\w+):\s*$")
_JOB_ID = re.compile(r"^\s+jobid:\s*(\d+)\s*$")
_SUBMITTED = re.compile(r"Job (\d+) has been submitted with SLURM jobid (\d+)")


def _table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    """Return a Markdown table, or the line `none` when there are no rows."""
    body = [list(row) for row in rows]
    if not body:
        return "none\n"
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join("" if cell is None else str(cell) for cell in row) + " |" for row in body]
    return "\n".join(lines) + "\n"


def _log_map(log_text: str) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Return the log's `(rule, job)` pairs and its `(job, scheduler job)` pairs, each once and in order of appearance."""
    jobs: list[tuple[str, str]] = []
    submitted: list[tuple[str, str]] = []
    rule: str | None = None
    for line in log_text.splitlines():
        block = _RULE_BLOCK.match(line)
        if block:
            rule = block.group(1)
            continue
        job = _JOB_ID.match(line)
        if job and rule is not None:
            pair = (rule, job.group(1))
            if pair not in jobs:
                jobs.append(pair)
            rule = None
            continue
        match = _SUBMITTED.search(line)
        if match and (match.group(1), match.group(2)) not in submitted:
            submitted.append((match.group(1), match.group(2)))
    return jobs, submitted


def _job_map_section(run_directory: Path) -> tuple[str, list[str]]:
    """Return the job-map section and the scheduler job ids every log names, in order."""
    logs = sorted((run_directory / ".snakemake" / "log").glob("*.snakemake.log"))
    lines = ["## Rule to job map", ""]
    scheduler_ids: list[str] = []
    if not logs:
        lines += ["no engine log under `.snakemake/log`", ""]
    for log in logs:
        jobs, submitted = _log_map(log.read_text(encoding="utf-8", errors="replace"))
        scheduler_by_job = dict(submitted)
        lines += [f"### {log.name}", ""]
        lines.append(_table(("rule", "job", "scheduler job"), ((r, j, scheduler_by_job.get(j)) for r, j in jobs)))
        scheduler_ids += [identifier for _, identifier in submitted if identifier not in scheduler_ids]
    return "\n".join(lines) + "\n", scheduler_ids


def _accounting_section(scheduler_ids: Sequence[str]) -> str:
    """Return the accounting section: `sacct`'s own rows for each scheduler job id, or the reason there are none.

    One query per job id, each quoted as the command printed it: a query that names several ids prints their
    rows in the scheduler's own order, and one id per query keeps each row beside the command that asked for it.
    """
    lines = ["## Scheduler accounting", ""]
    if not scheduler_ids:
        return "\n".join([*lines, "the engine log names no scheduler job", ""]) + "\n"
    for identifier in scheduler_ids:
        command = ["sacct", "-X", "-P", "-j", identifier, f"--format={SACCT_FORMAT}"]
        lines += [f"`{' '.join(command)}`", ""]
        try:
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
        except FileNotFoundError:
            return "\n".join([*lines, "`sacct` is not on this machine's path", ""]) + "\n"
        lines += ["```text", completed.stdout.rstrip("\n"), "```", ""]
        if completed.returncode != 0:
            lines += [f"`sacct` exited {completed.returncode}: {completed.stderr.strip()}", ""]
    return "\n".join(lines) + "\n"


def _read_lines(path: Path) -> list[dict[str, object]]:
    """Return the JSON object on each line of `path`, or nothing when the file is absent."""
    if not path.is_file():
        return []
    records: list[dict[str, object]] = []
    for text in path.read_text(encoding="utf-8").splitlines():
        if text.strip():
            records.append(json.loads(text))
    return records


def _where_section(run_directory: Path) -> str:
    """Return the section that lists every measurement line with its host and the parse statistics."""
    lines = ["## Where things ran", ""]
    for name in MEASUREMENT_FILES:
        records = _read_lines(run_directory / "measurement" / name)
        lines += [f"### {name}", ""]
        if not records:
            lines += ["no lines", ""]
            continue
        keys = list(dict.fromkeys(key for record in records for key in record))
        lines.append(_table(keys, ([record.get(key) for key in keys] for record in records)))
        per_host = Counter(str(record.get("host")) for record in records)
        lines.append(_table(("host", "lines"), sorted(per_host.items())))
        seconds = [float(record["seconds"]) for record in records if isinstance(record.get("seconds"), int | float)]
        if seconds:
            lines += [f"seconds: median {statistics.median(seconds):.6f}, maximum {max(seconds):.6f}", ""]
    return "\n".join(lines) + "\n"


def _targets_section(run_directory: Path) -> str:
    """Return the section that lists each target of the run-start record, whether it exists and its producing rule."""
    lines = ["## Targets", ""]
    try:
        record = read_run_start(run_directory / RUN_START_FILE)
    except (OSError, RunStartError) as error:
        return "\n".join([*lines, f"no usable run-start record: {error}", ""]) + "\n"
    rows = []
    for target in record.targets:
        if target == END_OF_RUN_RECORD:
            rows.append((target, (run_directory / target).exists(), "the end-of-run rule's own output"))
            continue
        try:
            producer = producing_rules(run_directory, [target])[target]
        except ExercisedSetError as error:
            producer = f"no record: {error}"
        rows.append((target, (run_directory / target).exists(), producer))
    lines.append(_table(("target", "exists", "rule named by its metadata record"), rows))
    return "\n".join(lines) + "\n"


def _provenance_section(run_directory: Path) -> str:
    """Return the section that summarizes the provenance file, or says it is absent or refused."""
    lines = ["## Provenance record", ""]
    path = run_directory / END_OF_RUN_RECORD / PROVENANCE_FILE
    if not path.is_file():
        return "\n".join([*lines, f"no `{END_OF_RUN_RECORD}/{PROVENANCE_FILE}`", ""]) + "\n"
    try:
        document = read_provenance(path)
    except ExercisedSetError as error:
        return "\n".join([*lines, f"refused by the reader: {error}", ""]) + "\n"
    identities = document["identities"]
    assert isinstance(identities, dict)
    exercised = document["exercised_systems"]
    assert isinstance(exercised, list)
    lines += [
        f"exercised systems ({len(exercised)}): {', '.join(exercised)}",
        "",
        f"implementations: {', '.join(sorted({entry['implementation'] for entry in identities.values()}))}",
        "",
        f"code versions: {', '.join(sorted({entry['code_version'] for entry in identities.values()}))}",
        "",
        f"rules that ran in the last invocation: {', '.join(document['invocation_rules']) or 'none'}",  # type: ignore[arg-type]
        "",
        f"side record: {json.dumps(document['side_record'], sort_keys=True)}",
        "",
    ]
    return "\n".join(lines) + "\n"


def collect(run_directory: Path) -> str:
    """Return the Markdown reading of `run_directory`: the five sections, in order."""
    job_map, scheduler_ids = _job_map_section(run_directory)
    return "\n".join(
        [
            f"# Measurement reading of the run directory `{run_directory.name}`\n",
            job_map,
            _accounting_section(scheduler_ids),
            _where_section(run_directory),
            _targets_section(run_directory),
            _provenance_section(run_directory),
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Print the reading of the run directory named on the command line."""
    parser = argparse.ArgumentParser(description="Print what a measured run directory holds, as Markdown.")
    parser.add_argument("run_directory", type=Path)
    arguments = parser.parse_args(argv)
    if not arguments.run_directory.is_dir():
        print(f"{arguments.run_directory} is not a directory", file=sys.stderr)
        return 2
    print(collect(arguments.run_directory))
    return 0


if __name__ == "__main__":
    sys.exit(main())
