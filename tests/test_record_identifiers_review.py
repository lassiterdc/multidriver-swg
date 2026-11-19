"""Probes of the identifiers recorder, each reading one stated contract of the script.

Each test states the contract it reads from the script's own docstring or the release design, and
fails where the script does not honour it. The fixtures are the recorder tests' own, so a probe
differs from a passing run in one property only.
"""

from __future__ import annotations

import os

import pytest

from tests.test_record_identifiers import (
    ASSET,
    ASSET_NAME,
    CITATION,
    DIRECTORY,
    ORIGIN,
    RECORDS_PATH,
    TAG,
    _accepted,
    _citation,
    _digest,
    _record,
    _refusals,
    _run,
)
from tests.test_record_identifiers import (
    service as _service_fixture,
)

service = _service_fixture


def test_a_record_naming_another_repository_with_the_same_prefix_is_not_this_origin(service, tmp_path):
    """The docstring's `zenodo-record` ground is a record that names THIS origin; a longer repository name is not it."""
    _accepted(service)
    other = _record(metadata={"related_identifiers": [{"identifier": f"{ORIGIN}-fork/tree/{TAG}"}]})
    service.route("GET", RECORDS_PATH, 200, {"hits": {"hits": [other]}})
    _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1, completed.stdout
    assert "zenodo-record" in [name for name, _ in _refusals(completed.stdout)]
    assert not (tmp_path / "releases").exists()


def test_a_record_naming_this_origin_at_another_tag_is_not_this_tag(service, tmp_path):
    """The docstring says the record must name this origin AT THIS TAG; the tag is read nowhere in the script."""
    _accepted(service)
    older = _record(
        metadata={"related_identifiers": [{"identifier": f"{ORIGIN}/tree/v0.1.0"}], "version": "v0.1.0"},
        doi="10.5281/zenodo.9999999",
    )
    service.route("GET", RECORDS_PATH, 200, {"hits": {"hits": [older]}})
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1, completed.stdout
    assert "zenodo-record" in [name for name, _ in _refusals(completed.stdout)]
    assert citation.read_text(encoding="utf-8") == CITATION
    assert "9999999" not in citation.read_text(encoding="utf-8")


def test_a_digest_inside_a_longer_run_of_hex_is_not_the_digest_the_notes_state(service, tmp_path):
    """`asset-digest` reads the digest the notes STATE; 64 characters inside a 128-character digest state a different one."""
    digest = _digest(ASSET)
    longer = "a" * 32 + digest + "b" * 32
    _accepted(service, notes=f"sha512 of something else: {longer}")
    _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1, completed.stdout
    assert "asset-digest" in [name for name, _ in _refusals(completed.stdout)]


def test_a_locator_that_is_not_a_hash_is_refused_and_never_written(service, tmp_path):
    """A `swh:1:rev:` locator carries 40 hex characters; the citation must not carry one the archive did not give."""
    _accepted(service, revision={"id": "not-a-hash", "directory": DIRECTORY})
    citation = _citation(tmp_path)

    completed = _run(service, tmp_path)

    assert completed.returncode == 1, completed.stdout
    assert "archive-locator" in [name for name, _ in _refusals(completed.stdout)]
    assert citation.read_text(encoding="utf-8") == CITATION


def test_a_comment_after_a_key_the_step_owns_is_kept(service, tmp_path):
    """Only the step's two keys change: a comment line sitting after an existing `doi` is not part of that key."""
    _accepted(service)
    text = CITATION.replace(
        'repository-code: "https://github.com/example/multidriver-swg.git"\n',
        'doi: "10.5281/zenodo.1"\n# kept for the maintainers\nrepository-code: "https://github.com/example/multidriver-swg.git"\n',
    )
    citation = tmp_path / "CITATION.cff"
    citation.write_text(text, encoding="utf-8")

    completed = _run(service, tmp_path)

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "# kept for the maintainers" in citation.read_text(encoding="utf-8").splitlines()


def test_a_second_run_over_its_own_output_changes_nothing(service, tmp_path):
    """The docstring calls the archival request idempotent; the files it writes must be too."""
    _accepted(service)
    citation = _citation(tmp_path)

    first = _run(service, tmp_path)
    once = citation.read_text(encoding="utf-8")
    second = _run(service, tmp_path)

    assert first.returncode == 0 and second.returncode == 0, second.stderr + second.stdout
    assert citation.read_text(encoding="utf-8") == once
    assert once.count("identifiers:") == 1


def test_a_failed_write_of_the_citation_leaves_no_record_behind(service, tmp_path):
    """A run that cannot write the citation's replacement leaves no record behind either."""
    if os.geteuid() == 0:
        pytest.skip("a privileged user writes into a read-only directory, so the fault cannot be injected")
    _accepted(service)
    _citation(tmp_path)
    (tmp_path / "releases" / TAG).mkdir(parents=True)
    tmp_path.chmod(0o555)
    try:
        completed = _run(service, tmp_path)
    finally:
        tmp_path.chmod(0o755)

    assert completed.returncode == 2, completed.stdout + completed.stderr
    assert not (tmp_path / "releases" / TAG / ASSET_NAME).exists()
    assert (tmp_path / "CITATION.cff").read_text(encoding="utf-8") == CITATION
