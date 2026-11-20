---
type: operations
title: Release gates, distribution bytes, licensing, and identifiers
description: Ordered tag and artifact gates, reproducible lock checks, release-state ownership, and delayed DOI and archive records.
tags: [release, packaging, licensing, provenance]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-3c099d9af7cba6b30d6eaf07
    resource: repo://.gitattributes
  - id: openwiki-source-f2608d0d515da097485b6ec5
    resource: repo://.github/workflows/publish.yml
  - id: openwiki-source-dea36368944d31a97e0bd9c9
    resource: repo://.github/workflows/record-identifiers.yml
  - id: openwiki-source-d6a9eb56960e984a3cd0cce3
    resource: repo://CITATION.cff
  - id: openwiki-source-140abf15242885583cc66fba
    resource: repo://MANIFEST.in
  - id: openwiki-source-05ccef8d4cf1698187f20464
    resource: repo://pyproject.toml
  - id: openwiki-source-23775c3de52f3ab95a13cb8b
    resource: repo://README.md
  - id: openwiki-source-18bae7dbf60d58f4ae444d70
    resource: repo://release_state.toml
  - id: openwiki-source-7e5fb20040f8993f9cc07e0c
    resource: repo://scripts/check_release_tag.py
  - id: openwiki-source-40ce2dc233ee4f0625f46ba4
    resource: repo://scripts/record_identifiers.py
  - id: openwiki-source-d52826cef5388d209134146c
    resource: repo://scripts/render_lock.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Release gates, distribution bytes, licensing, and identifiers

## Tag admission before building

A pushed `v*` tag starts `publish.yml`. Its reusable whole-suite job runs first; the release gate then runs unless cancelled, even when the suite fails. Download failure for the result record is tolerated only so the gate can issue an explicit refusal.

`check_release_tag.py` requires:

- Tag exactly `v` plus the version read at the tagged commit.
- Exactly one top-level citation version matching the project version.
- Tagged commit an ancestor of `origin/main`.
- No tracked files beneath `_old_code_to_refactor/` at that commit.
- A result record with `verdict = pass`, `completeness = complete`, empty exclusions, and null collection scope.

The record checker validates those fields; despite its docstring it does not compare a commit identity inside that result record. Workflow provenance supplies additional context, not an extra check hidden in the script. Archive exclusion of legacy files is insufficient for tag admission: the gate checks the Git tree itself.

## Artifact gates before publication

After admission, build creates wheel and sdist. `post-build-check` checks out the tagged source before downloading artifacts and performs, in order:

1. Fresh-environment install/import checks via `check_install.py`.
2. Re-render comparison of committed locks using pinned `uv==0.11.31`.
3. Inspection that the sdist actually contains root `pylock.toml` and `requirements.txt`.
4. Enumeration of sdist, wheel and `git archive` source bytes with member digests and whole-suite run identity.
5. Comparison of enumeration run ID, attempt, and commit against gate outputs, and its source commit against `GITHUB_SHA`.
6. Released-byte content scanning, release-state classification, and licence agreement checks.
7. Retention of the successful enumeration as an artifact.

Each ordinary failed step stops later steps. Only then does TestPyPI upload run, followed by PyPI upload and creation of a GitHub Release. Release notes state the attached enumeration's SHA-256, so the later identifier job can verify the same byte record. This describes configured policy, not evidence that this checkout has passed a release.

## Byte ownership and differing distributions

`release_state.toml` declares classes owned by a named system, path patterns, and one of `released`, `excluded`, or `estate-resident`. Released rows carry a licence; overlapping classes and shipped excluded/estate-resident entries are refusal conditions. Unclassified entries are reported but pass: recall of the classification is explicitly unmeasured. The currently visible table contains packaging metadata and release-script classes; do not infer full ownership coverage from its presence.

Source archives follow committed `.gitattributes` export exclusions, including tests, CI, legacy sources, corpus records, and refactor tools. These rules do not govern an sdist built from a checkout. `MANIFEST.in` separately prunes tests and includes both lock forms. Wheel runtime package data explicitly includes the divergence ledger and Snakefile. The three byte sets therefore cannot be treated as interchangeable copies of the tree.

## Resolution and licensing

The declared resolution cutoff is `2026-11-16T00:00:00Z`. The renderer resolves universally for Python 3.11, constrains against test-group pins, generates hashes, and produces PEP 751 `pylock.toml` plus `requirements.txt` into fresh temporary targets. `--check` compares bytes rather than rewriting committed locks. Moving the cutoff requires intentional regeneration.

Project metadata and citation declare `AGPL-3.0-or-later`. The repository also holds MIT legacy material and MIT-licensed test extractions, documented in README; do not flatten that into one repository-wide licence. The release licence gate compares artifact `License-Expression`, covering licences, README distribution statements, and citation terms against declared release classes. Source-tree licensing and admissible release contents are distinct questions.

## Identifiers after deposit

`record-identifiers.yml` is manually dispatched after deposit and archival services have seen the release. It resolves the tag's commit, not the dispatch branch head. The script requests archival saving, looks up Software Heritage revision/directory locators and Zenodo version/concept DOIs, and retrieves the Release enumeration whose digest must appear in its notes. Missing remote prerequisites refuse before file writing.

The resulting citation identifiers and `releases/<tag>/release-enumeration.json` are committed to `develop`, never inserted into the already published tag. A subsequent deposit-licence check can fail after this commit; repair the external deposit and rerun rather than assuming a failed workflow rolled back its identifier record. The current citation contains version and licence but no DOI identifiers.

## Navigation

- [Legacy correspondence](../concepts/legacy-correspondence.md)
- [Verification map](../testing/verification-map.md)
- [Quickstart](../quickstart.md)
