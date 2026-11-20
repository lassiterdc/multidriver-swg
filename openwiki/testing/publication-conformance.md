---
type: concept
title: Publication-math status and divergence diagnosis
description: The independent conformance vocabulary, authored nine-class diagnosis, status invariants, and blocking predicates.
tags: [conformance, mathematics, diagnosis, testing]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-20T07:54:33.693Z
sources:
  - id: openwiki-source-05ccef8d4cf1698187f20464
    resource: repo://pyproject.toml
  - id: openwiki-source-777dd677da0d7e04e1d4042a
    resource: repo://src/multidriver_swg/conformance/status.py
  - id: openwiki-source-fc587e523cfacf97fdfbe1ab
    resource: repo://src/multidriver_swg/conformance/taxonomy.py
generated: { by: "omp", at: "2026-11-20T07:54:33.693Z" }
---

# Publication-math status and divergence diagnosis

## What this interface decides

The `multidriver_swg.conformance` package publishes a vocabulary for manuscript/code conformance, not a numeric comparison engine. `ConformanceFailure` carries an equation identifier, observed and expected objects, recorded binding, reachability, and the caller's `diverged` boolean. Observed and expected may be arrays; `classify` never compares them.

A `Diagnosis` records a selected divergence class, evidence, equation identifier, and optional ruling party. `classify(failure, diagnosis)` validates that authored record and returns its class. It cannot decide which scientific authority is right. This is separate from legacy equivalence or a deliberately registered legacy departure: preserving an old result is not proof that a manuscript equation is correct.

## Nine classes and repair destinations

| Class | Meaning | Repair destination described by the taxonomy |
|---|---|---|
| D1 | Manuscript transcription error | Manuscript |
| D2 | Faithfully described but flawed methodology | Methodology |
| D3 | Correct paper, defective code | Code |
| D4 | Substantive code method absent from paper | Manuscript |
| D5 | Paper method absent from code | Code |
| D6 | Parameter-value drift | Value's single home |
| D7 | Different estimator substituted | Estimator on one side |
| D8 | Cross-repository implementation | Binding |
| D9 | Unreachable implementation | Reachability |

`AUTHOR_JUDGED` contains D1 and D2: each requires a nonblank `ruled_by`, rather than an automated distinction between mistaken text and mistaken method. `DERIVED_IN_STATUS` contains D8 and D9, represented by binding/verdict rather than duplicated in published defect lists.

## Diagnosis refusal order

The classifier first rejects a failure reporting agreement (`NotAFailure`), then equation mismatch (`DiagnosisMismatch`), then derived-fact contradictions (`ClassConflict`), then missing D1/D2 author attestation (`AuthorJudgementMissing`), and finally blank/non-string evidence (`UnevidencedDiagnosis`).

D8 requires `CROSS_REPO`, and `CROSS_REPO` requires D8. D9 requires an unreachable implementation, and an unreachable implementation requires D9. A single diagnosis cannot encode both cross-repository and unreachable: those facts demand two different classes, so that combination is refused rather than arbitrarily prioritized. Evidence is required as a nonblank string; this function does not verify its scientific sufficiency.

## Published status and blocking

`Status` is a frozen per-equation record with three distinct dimensions:

- Verdict: `CONFORMING`, `DIVERGENT`, or `UNREACHABLE`.
- Binding: `BOUND`, `CROSS-REPO`, or `NO-MANUSCRIPT-TEXT`.
- Defect tuple: zero or more actual `DivergenceClass` enum members, not raw strings.

Construction materializes the defect iterable once, preventing an exhausted generator from bypassing checks. It rejects D8/D9 in the defect list, a conforming verdict with defects, and a divergent verdict without defects. An unreachable verdict may have an empty list. A diagnosis selects one class; a status may carry multiple non-derived defects. These are different granularities, not conflicting encodings.

`is_blocking` reads declared sets: divergent or unreachable verdicts block, as does cross-repository binding regardless of verdict. Thus a conforming cross-repository entry still blocks. `NO-MANUSCRIPT-TEXT` alone is not in the blocking binding set.

`manuscript_binding` derives `NO-MANUSCRIPT-TEXT` only from the exact empty string. Any nonempty span returns `None`, not `BOUND`: text presence does not establish repository ownership. Callers must obtain the missing binding evidence elsewhere; whitespace is not normalized by this helper.

## Coverage boundary

The project registers a `conformance(unit, system, part)` pytest marker. A marker declaration and these vocabulary types do not establish executed equation coverage. No publication equation register or numerical conformance results are asserted by this page.

## Related pages

- [System boundaries](../architecture/system-boundaries.md)
- [Legacy correspondence](../concepts/legacy-correspondence.md)
- [Verification map](verification-map.md)
