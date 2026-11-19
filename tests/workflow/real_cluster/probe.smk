"""The probe workflow: the toolkit's own workflow file, included unchanged, with every parse and handler logged.

A measurement of the toolkit's command needs two facts the command's own output cannot give: how many times
the workflow file is parsed and what one graph-start check costs there, and which host each handler runs on.
This file answers both without editing the toolkit's workflow file. It puts the checkout's `src` at the head
of the import path, times ONE call of the graph-start check, appends one line to `measurement/parses.jsonl`,
and then includes the toolkit's workflow file. The line is written after the check returned, so a parse line
from a job is itself evidence that the check can run inside that job.

It declares no rule, so the included file's `all` is the engine's default target; the file includes through
the engine's own include call with the flag that keeps it so, because the `include:` directive would leave
the default target unset. Its three handlers each append one line to `measurement/handlers.jsonl`.

One effect is known: the included file computes its source root from the main workflow file's directory, so
it puts this directory's grandparent, `tests`, at the head of the import path. `tests` holds no
`multidriver_swg` package, and the package is already imported by the time that line runs, so the import
still resolves through the root this file inserted.

The `measurement` directory is relative to the engine's working directory, which is the run output.
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_SOURCE_ROOT = str(Path(workflow.basedir).resolve().parents[2] / "src")
# Moved to the head unconditionally, for the reason the included file states: a membership test would leave
# another copy of the toolkit ahead of this checkout.
while _SOURCE_ROOT in sys.path:
    sys.path.remove(_SOURCE_ROOT)
sys.path.insert(0, _SOURCE_ROOT)

from multidriver_swg.workflow.graph_start import require_graph_start  # noqa: E402

_MEASUREMENT = Path("measurement")


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


_started = time.perf_counter()
require_graph_start()
_seconds = time.perf_counter() - _started
_append("parses.jsonl", {**_where(), "seconds": _seconds})


def _handler(name):
    """Append the line a handler writes: which handler, and where it ran."""
    record = _where()
    _append("handlers.jsonl", {"handler": name, **record})


onstart:
    _handler("onstart")


onsuccess:
    _handler("onsuccess")


onerror:
    _handler("onerror")


# The `include:` directive leaves the default target as it was before the included file, so the included
# file's `all` would not be the engine's default and a launch with no target would build nothing. The engine's
# own include call takes the flag that keeps the included file's first rule as the default target.
workflow.include(
    str(Path(_SOURCE_ROOT) / "multidriver_swg" / "workflow" / "Snakefile"),
    overwrite_default_target=True,
)
