---
type: testing
title: Verification environments and available evidence
description: Declared CI responsibilities, pinned environments, source-import bootstrap, and the missing test implementation boundary in this checkout.
tags: [testing, ci, environments, evidence]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-4cd1f7cdaad0608a7dcfb6b8
    resource: repo://.github/workflows/anonymization-guard.yml
  - id: openwiki-source-4b463a05e77f74e7ad649366
    resource: repo://.github/workflows/install-check.yml
  - id: openwiki-source-191a678c740b0d4e6eb5b163
    resource: repo://.github/workflows/state-check.yml
  - id: openwiki-source-a55b1eaa43892421d129f18d
    resource: repo://.github/workflows/whole-suite.yml
  - id: openwiki-source-4d1645cb6317345817452838
    resource: repo://.pre-commit-config.yaml
  - id: openwiki-source-5fd879e8b1a9ca380f062f2d
    resource: repo://conftest.py
  - id: openwiki-source-05ccef8d4cf1698187f20464
    resource: repo://pyproject.toml
  - id: openwiki-source-7e5fb20040f8993f9cc07e0c
    resource: repo://scripts/check_release_tag.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Verification environments and available evidence

## Available evidence versus intended checks

This checkout's `tests/` hierarchy contains directories for correspondence, equivalence, reminders, registrations, lifts, conformance, fixtures, and workflow checks, but file discovery found no Python test implementations. Directory names are not coverage. No suite was run for this wiki, and no passing results or live-cluster verification are asserted.

The whole-suite workflow's commentary refers to `tests/correspondence/record.py` and root plugin registration. That recorder is not present here; the actual root `conftest.py` only bootstraps imports and does not register `--result-record`. Therefore the configured whole-suite command is not evidence that a usable recorder or complete suite exists in this tree. The pre-commit divergence hook likewise names `tests.correspondence.divergence`, absent from the discovered files.

## Environment declaration

Python support starts at 3.11. Runtime dependencies are NumPy `>=2,<3` and Snakemake `==9.15.0`; the optional HPC extra pins the SLURM executor plugin `2.8.0`. The test dependency group pins the scientific stack and tooling, including NumPy, pandas, xarray, SciPy, pytest, plotting and copula libraries, `uv`, build, and setuptools. These pins are a declared reproduction environment, not stored reference results in this checkout.

The whole-suite job installs hashed `requirements.txt`, installs the project editable without dependency resolution, then installs the test group. State check installs editable source with the test group directly. They share declarations but not identical installation procedures. Lock generation and release-byte checks are described in [release verification](../operations/release-verification.md).

## Source import bootstrap

`conftest.py` resolves whether `multidriver_swg` already comes from this checkout's `src/`. If source exists, imports do not resolve here, and `PYTHONPATH` does not explicitly name another package copy, it prepends this `src/` to `sys.path` and child-process `PYTHONPATH`. An explicit alternate copy is intentionally not overridden. This supports isolated runners without silently changing a caller's explicit subject selection; it is not itself the missing subject-check or recorder plugin.

## CI responsibility map

| Workflow | Configured invocation or check | Evidence boundary |
|---|---|---|
| `state-check.yml` | `python -m pytest -m "state_check and not equivalence and not refactor_reminder" -p no:cacheprovider -ra` on branch pushes and PRs to main/develop | A selected state subset, not whole-suite release evidence. |
| `whole-suite.yml` | `python -m pytest -p no:cacheprovider -ra --result-record "$RUNNER_TEMP/result-record.json"` on branches, non-release tags, and reusable release calls | No marker/path filtering; artifact upload runs even on failure and only warns if missing. Recorder unavailable in this checkout. |
| `install-check.yml` | Build sdist/wheel; `check_install.py --dist dist` and `--resolution lowest-direct`; highest-resolution job also runs `check_public_api.py` on wheel | Distribution installation/import/declaration checks, not scientific-method verification. |
| `anonymization-guard.yml` | Public control-file and machine-path checks, synthetic count-only self-test, private identity scan, secret count reporting | Working-tree/tracked-content policy, not Git history remediation. Missing carrier fails rather than skipping. |

Workflow comments describe branch protection policy, but local YAML does not verify remote ruleset settings. The release workflow explicitly depends on its own whole-suite call and admission gate regardless of those comments.

## Markers and result-record contract

`pyproject.toml` declares `state_check` for pre-refactor state checks and `conformance(unit, system, part)` for publication-math tests. Its marker list does not declare the `equivalence` or `refactor_reminder` names used in the state workflow expression. It also contains `strict_markers = true`; that configuration text alone is not proof of runtime marker enforcement or installed plugin behavior.

The release tag checker requires the result record to report `pass`, `complete`, no exclusions, and a null collection scope. A passing subset cannot meet that policy. The checker reads these fields but does not compare an embedded record commit against `--commit`; the release workflow separately checks enumeration identity against gate outputs. Never infer whole-suite success from an uploaded artifact name alone.

## Choosing a verification route

For distribution usability, inspect or run the present install scripts with built artifacts. For a scientific or workflow change, first establish that the corresponding executable tests and fixtures actually exist; the directory skeleton cannot supply them. For release admission, use the exact unfiltered whole-suite protocol only after its recorder and suite are available. No source changes or synthetic tests were introduced during this documentation pass.

## Related pages

- [Legacy correspondence](../concepts/legacy-correspondence.md)
- [Execution and recovery](../operations/execution-and-recovery.md)
- [Release verification](../operations/release-verification.md)
- [Publication conformance](publication-conformance.md)
