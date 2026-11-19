"""The checkpoint test workflow: a small standalone workflow whose licence rule is a checkpoint.

This is NOT the toolkit's graph. It has the layout a later stage of the toolkit will give the observed-record
assembly: a pre-selection rule, one selection rule per data product, a licence rule declared as a checkpoint,
a merge rule, a consumer of the merge's output, and an output-bearing final rule. It exists to measure the
ENGINE's behaviour at a checkpoint, on a machine where that behaviour matters, and to be a permanent local
check of that behaviour, without waiting for the real rules to exist.

The question it answers: when the licence finds a statistic it rejected pooling for, and the run's selection
fixture holds no pooling entry for that statistic, does the run end after the licence with exit 0, its report
and its final output; and when the entry is added and the run is launched again on the same output, does it
run the merge, the consumer and the final rule without running the licence again?

The selection fixture is `selection_fixture.json` in the engine's working directory, one JSON object with
two keys: `rejected`, the statistics the licence rejects pooling for, and `pooling`, the statistics that
have a pooling entry. The launcher writes it. The licence copies `rejected` into its own output, and what
runs after the licence is decided by reading that output and the fixture AFTER the checkpoint: the inputs of
`all` and of the final rule are functions, because a fixed list would build the merge whether or not the
licence permits it, and the run would run through the checkpoint instead of stopping at it.

The licence's and the merge's HTML files are report items, so a report built after a stopped run holds the
licence item and no merge item.

Every parse appends a line to `measurement/checkpoint_parses.jsonl`, and every evaluation of the inputs
after the checkpoint appends a line, with its seconds, to `measurement/checkpoint_evaluations.jsonl`, so the
re-evaluation is counted and timed. The `measurement` directory is relative to the engine's working
directory.
"""

import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

_MEASUREMENT = Path("measurement")

_FIXTURE = Path("selection_fixture.json")

# The data products that each get a selection rule.
_PRODUCTS = ("radar", "gauge")

LICENCE_DOCUMENT = "licence/licence.json"
LICENCE_ITEM = "licence/licence.html"
MERGE_DOCUMENT = "merge/merge.json"
MERGE_ITEM = "merge/merge.html"
CONSUMER_DOCUMENT = "consumer/consumer.json"
FINAL_OUTPUT = "closing/record.txt"


def _where():
    """Return the fields every line carries: the host, the process, the scheduler job (or none) and the time."""
    return {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "utc": datetime.now(timezone.utc).isoformat(),
    }


def _append(name, record):
    """Append `record` as one JSON line to `measurement/{name}`, creating the directory on first use."""
    _MEASUREMENT.mkdir(exist_ok=True)
    with (_MEASUREMENT / name).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record) + "\n")


def _page(title):
    """Return a one-element HTML document, the form a report item must have to be shown in the page."""
    return f'<!DOCTYPE html>\n<html>\n<head>\n<meta charset="utf-8">\n<title>{title}</title>\n</head>\n<body>{title}</body>\n</html>\n'


def _after_licence(caller):
    """Return the targets that follow the licence, deciding by the licence's output and the fixture.

    `checkpoints.licence.get()` is what makes the engine re-evaluate this function when the licence has run:
    before then it raises and the engine builds the part of the graph that exists. After the licence the
    function reads the rejected statistics from the licence's own output and the pooling entries from the
    fixture. A statistic with no pooling entry leaves the merge, the consumer and their outputs out of the
    list, so the run ends after the licence.

    Each call that gets past the checkpoint appends one line to `measurement/checkpoint_evaluations.jsonl`
    with the caller's name and its seconds.
    """
    started = time.perf_counter()
    licence_document = checkpoints.licence.get().output.document
    rejected = json.loads(Path(licence_document).read_text(encoding="utf-8"))["rejected"]
    pooling = json.loads(_FIXTURE.read_text(encoding="utf-8"))["pooling"]
    covered = all(statistic in pooling for statistic in rejected)
    targets = [LICENCE_DOCUMENT, LICENCE_ITEM]
    if covered:
        targets += [MERGE_DOCUMENT, MERGE_ITEM, CONSUMER_DOCUMENT]
    _append(
        "checkpoint_evaluations.jsonl",
        {**_where(), "caller": caller, "covered": covered, "seconds": time.perf_counter() - started},
    )
    return targets


def _all_inputs(wildcards):
    """Return everything the run builds: what follows the licence, and the final output."""
    return [*_after_licence("all"), FINAL_OUTPUT]


def _final_inputs(wildcards):
    """Return what the final rule waits on: what follows the licence."""
    return _after_licence("closing")


_append("checkpoint_parses.jsonl", _where())


rule all:
    input:
        _all_inputs
    localrule: True


rule pre_selection:
    output:
        "pre_selection/pre_selection.json"
    run:
        Path(output[0]).write_text(json.dumps({"stage": "pre_selection"}) + "\n", encoding="utf-8")


rule per_source_selection:
    input:
        "pre_selection/pre_selection.json"
    output:
        "per_source/{product}.json"
    run:
        Path(output[0]).write_text(json.dumps({"product": wildcards.product}) + "\n", encoding="utf-8")


checkpoint licence:
    input:
        expand("per_source/{product}.json", product=_PRODUCTS)
    output:
        document=LICENCE_DOCUMENT,
        item=report(LICENCE_ITEM, category="Licence"),
    run:
        rejected = json.loads(_FIXTURE.read_text(encoding="utf-8"))["rejected"]
        Path(output.document).write_text(json.dumps({"rejected": rejected}) + "\n", encoding="utf-8")
        Path(output.item).write_text(_page("licence"), encoding="utf-8")


rule merge:
    input:
        LICENCE_DOCUMENT
    output:
        document=MERGE_DOCUMENT,
        item=report(MERGE_ITEM, category="Merge"),
    run:
        Path(output.document).write_text(json.dumps({"stage": "merge"}) + "\n", encoding="utf-8")
        Path(output.item).write_text(_page("merge"), encoding="utf-8")


rule merge_consumer:
    input:
        MERGE_DOCUMENT
    output:
        CONSUMER_DOCUMENT
    run:
        Path(output[0]).write_text(json.dumps({"stage": "consumer"}) + "\n", encoding="utf-8")


rule closing:
    input:
        _final_inputs
    output:
        FINAL_OUTPUT
    localrule: True
    run:
        Path(output[0]).write_text("\n".join(input) + "\n", encoding="utf-8")
