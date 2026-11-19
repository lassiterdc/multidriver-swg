"""Reviewer tests of the licence wiring: a refusal that cannot refuse, and a deposit step nobody ran.

`tests/test_publish_workflow.py` and `tests/test_record_identifiers_workflow.py` pin the two new steps
as TEXT: the first that the flags are spelled, the second that fragments such as `sys.exit(1)` appear
in the step's command. Neither runs a step's command, and neither reads whether the step is allowed to
fail the job. A `continue-on-error: true` on the licence step keeps every one of those tests green
while the release uploads under terms the check refused; a deposit script whose heredoc did not parse,
or whose comparison was inverted, keeps the fragment test green and is first run at the release.

Module guarded: `.github/workflows/publish.yml` (the `Check the licence statements` step of
`post-build-check`) and `.github/workflows/record-identifiers.yml` (the
`Check the deposit states the release's licence` step), judged by running the commands they carry.
One-time test: it proves the wiring's change once and is deleted at the landing unless promoted.

Rules, each a function of the workflow text and of what the command does when run:

1. neither licence step can fail without failing its job: no `if:`, no `continue-on-error`;
2. the licence step's own command, with `$RUNNER_TEMP` resolved, run over a release whose citation
   states MIT exits 1 naming `citation-licence`, and over a conforming release exits 0 — the
   specification's "uploads nothing" arm;
3. the licence step's `--dist` is the directory the job's download fills, and the script accepts every
   flag the step passes;
4. the deposit step's heredoc script, extracted and run against a local server standing in for the
   deposit host, exits 0 on a matching licence in either case or either record shape, 1 on a different
   or absent one, 2 when no record or no input exists, and queries the version DOI the citation's
   `doi` identifier carries rather than the concept DOI or a software-heritage value.
"""

from __future__ import annotations

import http.server
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import urllib.parse
from pathlib import Path

import pytest

from tests.test_check_licences import _release
from tests.test_publish_workflow import DISTRIBUTION_CHECK
from tests.test_publish_workflow import LICENCE_STEP as PUBLISH_LICENCE_STEP
from tests.test_publish_workflow import _job as _publish_job
from tests.test_publish_workflow import _step as _publish_step
from tests.test_record_identifiers_workflow import LICENCE_STEP as DEPOSIT_STEP
from tests.test_record_identifiers_workflow import _job as _record_job
from tests.test_record_identifiers_workflow import _step as _record_step

REPO_ROOT = Path(__file__).resolve().parents[1]
RECORD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "record-identifiers.yml"
LICENCE_SCRIPT = REPO_ROOT / "scripts" / "check_licences.py"

VERSION_DOI = "10.5281/zenodo.2222222"
CONCEPT_DOI = "10.5281/zenodo.1111111"
TABLE = '[[class]]\nname = "release-metadata"\nsystem = "p"\nstate = "released"\nlicence = "AGPL-3.0-or-later"\npaths = ["PKG-INFO"]\n'
CITATION = (
    "cff-version: 1.2.0\n"
    "title: x\n"
    "license: AGPL-3.0-or-later\n"
    f'doi: "{CONCEPT_DOI}"\n'
    "identifiers:\n"
    "  - type: swh\n"
    '    value: "swh:1:rev:0123456789012345678901234567890123456789"\n'
    '    description: "first"\n'
    "  - type: doi\n"
    f'    value: "{VERSION_DOI}"\n'
    '    description: "the version this release published"\n'
    "  - type: swh\n"
    '    value: "swh:1:dir:0123456789012345678901234567890123456789"\n'
)


@pytest.mark.parametrize(
    ("job", "step"),
    [
        (_publish_job(DISTRIBUTION_CHECK), _publish_step(_publish_job(DISTRIBUTION_CHECK), PUBLISH_LICENCE_STEP)),
        (_record_job(), _record_step(_record_job(), DEPOSIT_STEP)),
    ],
    ids=["publish-licence-step", "deposit-licence-step"],
)
def test_a_licence_step_can_fail_its_job(job, step):
    """Property: the licence step carries no `if:` and no `continue-on-error`, and neither does its job.

    Class: NEW CAPABILITY — the plausible wrong implementation is the step with `continue-on-error:
    true` (or `if: always()` / `if: false`), which the author's text tests do not read: the step's
    name, flags and position survive, the job stays green, and the uploads that `needs` it run. Red
    under it.
    """
    for key in ("if", "continue-on-error"):
        assert key not in step, (key, step)
        assert key not in job, (key, job)


def _expand(command: str, runner_temp: Path) -> str:
    return command.replace("$RUNNER_TEMP", str(runner_temp)).replace("${{ runner.temp }}", str(runner_temp))


def _run_publish_licence_step(tmp_path: Path, *, citation: str | None) -> subprocess.CompletedProcess:
    release = _release(tmp_path)
    if citation is not None:
        release.citation.write_text(citation, encoding="utf-8")
    runner_temp = tmp_path / "runner"
    (runner_temp / "release-enumeration").mkdir(parents=True)
    shutil.copy(release.enumeration, runner_temp / "release-enumeration" / "release-enumeration.json")
    (tmp_path / "scripts").symlink_to(REPO_ROOT / "scripts")
    command = str(_publish_step(_publish_job(DISTRIBUTION_CHECK), PUBLISH_LICENCE_STEP)["run"])
    command = re.sub(r"^python ", f'"{sys.executable}" ', command.strip())
    return subprocess.run(
        ["bash", "-c", _expand(command, runner_temp)],
        cwd=tmp_path,
        env={**os.environ, "RUNNER_TEMP": str(runner_temp)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_licence_step_command_conforms_a_conforming_release_and_refuses_an_mit_citation(tmp_path):
    """Property: the step's own command, run from a workspace laid out as the job lays it out, exits 0
    on a release whose four statements agree and exits 1 naming `citation-licence` when the citation
    states MIT.

    Class: NEW CAPABILITY — the plausible wrong implementation is a command whose paths are
    spelled consistently with the author's test and wrong against the job's layout (`--dist` at the
    workspace root, `--readme` at a path the checkout does not have, the enumeration under a name the
    enumeration step does not write): it exits 2 on every release. The conforming arm is the control
    that separates a refusal from an unreadable input.
    """
    (tmp_path / "ok").mkdir()
    ok = _run_publish_licence_step(tmp_path / "ok", citation=None)
    assert ok.returncode == 0, (ok.stdout, ok.stderr)
    assert "conforms" in ok.stdout, ok.stdout

    (tmp_path / "mit").mkdir()
    bad = _run_publish_licence_step(tmp_path / "mit", citation="cff-version: 1.2.0\nlicense: MIT\n")
    assert bad.returncode == 1, (bad.stdout, bad.stderr)
    assert "refused: citation-licence" in bad.stdout, bad.stdout


# ---------------------------------------------------------------------------------------------
# The deposit step's script, extracted from the workflow and run.
# ---------------------------------------------------------------------------------------------


def _deposit_script() -> str:
    """The heredoc body of the deposit step, dedented by the indentation of its `python -` line."""
    lines = RECORD_WORKFLOW.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == f"- name: {DEPOSIT_STEP}")
    opener = next(i for i in range(start, len(lines)) if lines[i].strip() == "python - <<'PY'")
    indent = len(lines[opener]) - len(lines[opener].lstrip())
    body: list[str] = []
    for line in lines[opener + 1 :]:
        if line.strip() == "PY":
            break
        body.append(line[indent:] if line.startswith(" " * indent) else line.lstrip())
    else:
        pytest.fail("the heredoc is not closed by a line reading PY")
    return "\n".join(body) + "\n"


class _Deposit:
    """A local HTTP server standing in for the deposit host; records each request's query string."""

    def __init__(self, payload: object, status: int = 200) -> None:
        outer = self
        self.queries: list[str] = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                outer.queries.append(urllib.parse.unquote_plus(self.path))
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                return

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> _Deposit:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/api"


def _hit(license_value: object) -> dict:
    metadata: dict[str, object] = {}
    if license_value is not None:
        metadata["license"] = license_value
    return {"hits": {"hits": [{"metadata": metadata}]}}


def _run_deposit(tmp_path: Path, deposit: _Deposit, *, table: str = TABLE, citation: str = CITATION):
    (tmp_path / "release_state.toml").write_text(table, encoding="utf-8")
    (tmp_path / "CITATION.cff").write_text(citation, encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-"],
        input=_deposit_script(),
        cwd=tmp_path,
        env={**os.environ, "ZENODO_API": deposit.url},
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


@pytest.mark.parametrize(
    "license_value",
    [{"id": "agpl-3.0-or-later"}, {"id": "AGPL-3.0-or-later"}, "AGPL-3.0-or-later", " agpl-3.0-or-later "],
    ids=["dict-lower", "dict-exact", "string", "string-padded"],
)
def test_the_deposit_step_passes_a_matching_licence_in_either_case_and_shape(tmp_path, license_value):
    """Property: a deposit stating the table's licence — lower- or exact-case, as an object with `id`
    or as a bare string — exits 0, and the query carries the version DOI.

    Class: NEW CAPABILITY — the plausible wrong implementation compares case-sensitively (the deposit
    host lower-cases identifiers) or reads only one record shape; it fails every release's run. The
    query assertion kills a step that asks for the concept DOI or a software-heritage `value:`.
    """
    with _Deposit(_hit(license_value)) as deposit:
        result = _run_deposit(tmp_path, deposit)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert deposit.queries and VERSION_DOI in deposit.queries[0], deposit.queries
    assert CONCEPT_DOI not in deposit.queries[0], deposit.queries
    assert "swh:" not in deposit.queries[0], deposit.queries


@pytest.mark.parametrize(
    ("license_value", "named"),
    [({"id": "mit"}, "mit"), ("CC-BY-4.0", "CC-BY-4.0"), (None, "no licence")],
    ids=["other-dict", "other-string", "absent"],
)
def test_the_deposit_step_fails_on_another_licence_or_none(tmp_path, license_value, named):
    """Property: a deposit stating another licence, or none, exits 1 with a `deposit-licence` line.

    Class: NEW CAPABILITY — the plausible wrong implementation passes an absent field (`None !=
    wanted` skipped), or reports the mismatch and exits 0; the first leaves a deposit with no licence
    green. Red under each.
    """
    with _Deposit(_hit(license_value)) as deposit:
        result = _run_deposit(tmp_path, deposit)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "refused: deposit-licence" in result.stdout, result.stdout
    del named


def test_the_deposit_step_exits_two_when_the_deposit_holds_no_record(tmp_path):
    """Property: an empty search answer is an unreadable input (exit 2), not a pass and not a refusal.

    Class: NEW CAPABILITY — the plausible wrong implementation indexes `hits[0]` unguarded and dies
    with a traceback (exit 1, indistinguishable from a licence refusal), or treats no record as
    agreement (exit 0).
    """
    with _Deposit({"hits": {"hits": []}}) as deposit:
        result = _run_deposit(tmp_path, deposit)
    assert result.returncode == 2, (result.returncode, result.stdout, result.stderr)
    assert "unreadable-input" in result.stdout, result.stdout


@pytest.mark.parametrize(
    "arm",
    ["no-table-class", "no-citation-doi"],
)
def test_the_deposit_step_exits_two_when_an_input_it_reads_is_absent(tmp_path, arm):
    """Property: a table with no `release-metadata` licence, or a citation with no `doi` identifier,
    exits 2 before any request is made.

    Class: NEW CAPABILITY — the plausible wrong implementation queries with `doi:"None"` or compares
    against `None`, both of which can fail or pass for the wrong reason.
    """
    table = TABLE if arm != "no-table-class" else TABLE.replace('licence = "AGPL-3.0-or-later"\n', "")
    citation = CITATION if arm != "no-citation-doi" else "cff-version: 1.2.0\ntitle: x\nlicense: AGPL-3.0-or-later\n"
    with _Deposit(_hit({"id": "agpl-3.0-or-later"})) as deposit:
        result = _run_deposit(tmp_path, deposit, table=table, citation=citation)
    assert result.returncode == 2, (result.returncode, result.stdout, result.stderr)
    assert deposit.queries == [], deposit.queries
