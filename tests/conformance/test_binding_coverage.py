r"""Assert on the real tree that every register equation carries one binding and every bound test exists.

`tests/conformance/test_bindings.py` drives `bindings.coverage_findings` over synthetic `pytester`
trees, one clean arm and one defect arm per check, so each check is proved to fire. This module makes
that same function's EMPTY finding set a claim about this repository: the checked-in ledger, the
checked-in register and the checked-in conformance markers are the inputs, and no fixture stands
between them and the assertion.

The finding set and the partition are asserted together because neither alone states the obligation. A
row moved off `TEST` takes its own test with it: the row now owes nothing, so a ledger that retired
both the binding and the function in one edit draws no finding at all — and the partition is exactly
what notices that a published obligation was retired. In the other direction, a ledger whose partition
still reads 21/6/6/3 can have lost a bound test name to a rename nobody re-listed, which only the
finding set reports.

What this does NOT establish: that the 36 binding assignments are the RIGHT ones. `coverage_findings`
reads whether each row is well formed and whether the tests it names exist, exactly once each; whether
`E-06` is genuinely unreachable is a judgement about the paper and the code, carried by each row's
`ground` and reviewed against § B2.
"""

from __future__ import annotations

from collections import Counter

import pytest

from tests.conformance import bindings
from tests.conformance.register import EXPECTED_IDS
from tests.correspondence import corpus_record, inventory

# Derived from `__file__` rather than from the working directory, so a run started anywhere reads the
# checkout this test lives in and never the tree above whatever directory pytest was invoked from.
REPO_ROOT = corpus_record.REPO_ROOT


@pytest.mark.conformance(
    unit="binding-coverage",
    part="test_the_real_tree_binds_every_equation_and_covers_every_bound_test",
    system="publication-math-conformance",
)
def test_the_real_tree_binds_every_equation_and_covers_every_bound_test() -> None:
    """Property: over the real ledger and the real conformance listing, `coverage_findings` reports no
    defect at all, and the ledger partitions 21 `TEST`, 6 `CROSS-REPO`, 6 `UNREACHABLE`,
    3 `NO-MANUSCRIPT-TEXT`.

    Class: NEW CAPABILITY — the plausible wrong implementation is a ledger written from § B2's Status
    column that names the tests § B2 PROPOSES: every row is well formed, the partition is right, and
    21 of the names point at functions that were never written. No arm of the equation suite can see
    that, because an equation nobody tested and an equation that owes no test are the same green.
    This assertion is red under it with one finding per missing test.

    Why: the ledger's whole purpose is that an untested equation stops reading like a text-less one,
    and that purpose survives only if the names in it are checked against the tree on every run rather
    than once by the author who wrote both.

    Reachability: the real ledger, the real register and the real listing are the inputs, and the
    listing is read statically out of `REPO_ROOT`, so a row added, dropped or reclassified, a `TEST`
    row whose named function was renamed, and a conformance marker naming an equation-like unit that
    no `TEST` row carries all arrive at these assertions.

    Invocation isolated: `REPO_ROOT` is derived from `__file__`, and `bindings.load()` finds the
    ledger from its own module path, so neither the working directory nor the caller's environment
    changes the result. The listing's own unreadable-marker findings travel in the pair and are
    reported through the fifth coverage check, so a marker written with a module constant rather than
    a string literal is a finding here too.

    Kills: a `TEST` row pointing at a function that does not exist; a row whose binding word
    contradicts the register's verbatim span; a bound test that was renamed without the row; a
    conformance marker carrying an equation-like unit with no `TEST` row; a reclassification that
    keeps the finding set empty and moves the partition.

    Second correct implementation: a coverage check that reports its findings in a different order, or
    that adds detail to each finding, passes — the assertion is over the empty set, and the partition
    is compared as a mapping.
    """
    ledger = bindings.load()
    findings = bindings.coverage_findings(ledger, EXPECTED_IDS, inventory.listing(REPO_ROOT))
    message = f"the real tree's binding coverage reports {len(findings)} defect(s):\n" + "\n".join(findings)
    assert findings == [], message

    partition = Counter(row["binding"] for row in ledger.values())
    assert dict(partition) == {"TEST": 21, "CROSS-REPO": 6, "UNREACHABLE": 6, "NO-MANUSCRIPT-TEXT": 3}
