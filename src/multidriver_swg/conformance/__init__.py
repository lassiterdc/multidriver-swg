"""The conformance vocabulary: one import surface for the divergence taxonomy and what it publishes.

A consumer of the conformance package reaches the class vocabulary, the classifier and — as this
package grows — the `Status` shape through one import statement rather than through module paths.
That matters because the class vocabulary and the status vocabulary are two faces of ONE vocabulary:
the classes a status publishes in its defect list are the classes this package defines, so a consumer
that had to reach them by module path could quietly diverge from the ones the producer means.

This file carries no logic of its own. It re-exports the names `taxonomy` defines, following the
re-export pattern the run-configuration package already uses, and declares `__all__` so the surface
is enumerable rather than inferred from what happens to be imported.

Implements the `nine-class-classifier` scope item's publishing half.
"""

from __future__ import annotations

from multidriver_swg.conformance.status import (
    BLOCKING_BINDINGS,
    BLOCKING_VERDICTS,
    ConformingWithDefects,
    DerivedClassInDefectList,
    NotADivergenceClass,
    Status,
    StatusVocabularyError,
    UndiagnosedDivergence,
    is_blocking,
    manuscript_binding,
)
from multidriver_swg.conformance.taxonomy import (
    AUTHOR_JUDGED,
    DERIVED_IN_STATUS,
    AuthorJudgementMissing,
    Binding,
    ClassConflict,
    ConformanceFailure,
    ConformanceTaxonomyError,
    Diagnosis,
    DiagnosisMismatch,
    DivergenceClass,
    NotAFailure,
    UnevidencedDiagnosis,
    Verdict,
    classify,
)

__all__ = [
    "AUTHOR_JUDGED",
    "BLOCKING_BINDINGS",
    "BLOCKING_VERDICTS",
    "AuthorJudgementMissing",
    "Binding",
    "ClassConflict",
    "ConformanceFailure",
    "ConformanceTaxonomyError",
    "ConformingWithDefects",
    "DERIVED_IN_STATUS",
    "DerivedClassInDefectList",
    "Diagnosis",
    "DiagnosisMismatch",
    "DivergenceClass",
    "NotADivergenceClass",
    "NotAFailure",
    "Status",
    "StatusVocabularyError",
    "UndiagnosedDivergence",
    "UnevidencedDiagnosis",
    "Verdict",
    "classify",
    "is_blocking",
    "manuscript_binding",
]
