"""Assert every register span still occurs exactly once, at its recorded place, in the manuscript.

The register is a copy of the manuscript's equations taken at one commit, and nothing in this
repository keeps the two in step: the manuscript lives in a sibling clone, so a line inserted
above an equation moves every range below it while no committed artifact changes. This module is
the only thing that notices. When it fails it names the entry whose span no longer occurs exactly
once, or whose recorded line range no longer contains it — and that range is the pointer the user
acts on when they transcribe a correction themselves, so a stale pointer is a wrong instruction
rather than untidy metadata.

The bookkeeping is split from the assertions on purpose. The manuscript is resolved and read
ONCE per session, and the two independent properties are two test functions over the same
33-entry domain, so a duplicated span and a misplaced span report separately instead of one
masking the other.

What this check does NOT establish: that a span is the RIGHT equation. A span occurring exactly
once certifies that item 1 disambiguated it, never that it mirrors the equation its entry claims —
completeness and correct transcription are carried by item 1's review against § B2. That limit is
stated because this module's green is easy to read as "the register is correct", which it is not.

When the manuscript cannot be resolved the whole module SKIPS, naming every path it tried. A skip
is not a pass: it says the check did not run, which is the only honest verdict for a check whose
subject is absent. The session fixture below is autouse for exactly that reason — without it, the
arm that does not read the manuscript would pass on its own and the module would report a partial
success over an absent subject.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from tests.conformance.register import entries

MANUSCRIPT_ENV_VAR = "MULTIDRIVER_SWG_PAPER"
MANUSCRIPT_FILENAME = "swg_supplement.tex"
MANUSCRIPT_REPO_DIRNAME = "multidriver-swg-paper"

REPO_ROOT = Path(__file__).resolve().parents[2]

REGISTER = entries()
SPANNED_EQUATION_IDS = tuple(equation_id for equation_id in sorted(REGISTER) if REGISTER[equation_id]["verbatim_span"])
IMPLICIT_EQUATION_IDS = tuple(
    equation_id for equation_id in sorted(REGISTER) if not REGISTER[equation_id]["verbatim_span"]
)


def candidate_manuscript_paths() -> tuple[Path, ...]:
    """Every place decision 3.3 looks for the manuscript, in resolution order.

    The environment variable names the CLONE (`multidriver-swg-paper`), so `swg_supplement.tex` is
    appended to it; a value that names the file itself is accepted too, because a caller holding
    the file has already resolved what the clause wants and refusing it would fail a correct
    configuration. The fallback is the sibling-directory default the decision fixes exactly:
    `{repo_root}/../multidriver-swg-paper/swg_supplement.tex`.
    """
    attempts: list[Path] = []
    configured = os.environ.get(MANUSCRIPT_ENV_VAR)
    if configured:
        from_environment = Path(configured).expanduser()
        attempts.append(from_environment if from_environment.is_file() else from_environment / MANUSCRIPT_FILENAME)
    attempts.append(REPO_ROOT.parent / MANUSCRIPT_REPO_DIRNAME / MANUSCRIPT_FILENAME)
    return tuple(attempts)


def resolve_manuscript() -> Path:
    """The first readable candidate, or `pytest.skip` naming every attempt.

    Skip rather than fail: an absent manuscript is an environment the decision names explicitly,
    and a CI box without the sibling clone must not read as a register defect. Never `return` a
    non-existent path — a caller that then reads it would raise a `FileNotFoundError` dressed as a
    test failure.

    The message names BOTH resolution arms on every skip, including the arm that could not be
    tried because its variable is unset: an unset variable and a variable pointing at nothing are
    different configurations, and a reader diagnosing a CI skip needs to know which one they have.
    """
    attempts = candidate_manuscript_paths()
    for candidate in attempts:
        if candidate.is_file():
            return candidate
    configured = os.environ.get(MANUSCRIPT_ENV_VAR)
    environment_arm = (
        f"{MANUSCRIPT_ENV_VAR} -> {attempts[0]} (readable: {attempts[0].is_file()})"
        if configured
        else f"{MANUSCRIPT_ENV_VAR} is not set"
    )
    sibling_arm = f"sibling default -> {attempts[-1]} (readable: {attempts[-1].is_file()})"
    pytest.skip(
        f"register drift check skipped: {MANUSCRIPT_FILENAME} is unresolvable; tried {environment_arm}; {sibling_arm}"
    )


@pytest.fixture(scope="session")
def manuscript_path() -> Path:
    """The resolved manuscript path for this session, or a skip naming every attempt."""
    return resolve_manuscript()


@pytest.fixture(scope="session")
def manuscript_text(manuscript_path: Path) -> str:
    """The manuscript's full text, read once for every assertion in this module."""
    return manuscript_path.read_text(encoding="utf-8")


@pytest.fixture(scope="session", autouse=True)
def _manuscript_must_resolve(manuscript_text: str) -> None:
    """Make an unresolvable manuscript skip the WHOLE module, not just the arms that read it.

    Without this, the implicit-equation arm would pass while the span arms skipped, and the run
    would report a partial success over an absent subject. `manuscript_text` is the dependency and
    not a value: the point is to pull the resolution into session setup.
    """
    assert manuscript_text is not None


@pytest.mark.parametrize("equation_id", SPANNED_EQUATION_IDS)
def test_every_non_empty_span_occurs_exactly_once(manuscript_text: str, equation_id: str) -> None:
    """Property: each spanned entry's `verbatim_span` occurs exactly once in the manuscript.

    Class: NEW CAPABILITY — the plausible wrong implementation is a register whose duplicate
    spans were never disambiguated, which is the state item 1 had before the shape's round-1
    RAISE: the Kendall-tau test statistic, null distribution and rejection region each occur
    twice (406/564, 411/569, 413/571). Under that register this test is red with `occurs 2
    times`, measured below in the build return.
    Reachability: any manuscript edit that repeats a span, and any register entry left
    un-disambiguated, reaches this assertion; the real register and the real manuscript are the
    input. Both-states anchor: the assertion counts occurrences in the manuscript, a property
    that exists whether or not this module changes.
    Invocation isolation (B4-4, arm 1): with `MULTIDRIVER_SWG_PAPER` set, the same invocation
    reproduces the verdict from any checkout, because neither `__file__` nor the working
    directory enters the resolution; the fallback arm is tree-relative by decision 3.3 and is
    exercised by `test_register_drift.py`'s skip path rather than by this assertion.
    Kills: a register span that was copied from § B2's prose cell instead of re-extracted, or a
    manuscript edit that duplicates an equation.
    A second correct implementation: a checker that normalises nothing and counts with
    `re.finditer(re.escape(span))` still passes, because the count it reports is the same for a
    unique literal span.
    """
    span = REGISTER[equation_id]["verbatim_span"]
    occurrences = manuscript_text.count(span)
    assert occurrences == 1, (
        f"register entry {equation_id}: its verbatim span occurs {occurrences} times in the manuscript, "
        "not once; a span that is not unique cannot certify which equation the entry mirrors"
    )


@pytest.mark.parametrize("equation_id", SPANNED_EQUATION_IDS)
def test_every_non_empty_span_lies_in_its_recorded_line_range(manuscript_text: str, equation_id: str) -> None:
    """Property: each spanned entry's `verbatim_span` is a substring of its recorded range's slice.

    Class: NEW CAPABILITY — the plausible wrong implementation is a register carrying § B2's
    pointers unchanged, which the shape documents as running one line early from line 86 onward;
    under it this test is red for most entries with `not inside its recorded range`, measured in
    the build return against E-01's stale pointer.
    Reachability: any line inserted or removed above an equation shifts its range and reaches
    this assertion, and that is the only failure mode it is for. Both-states anchor: the
    assertion compares the span with the manuscript's own slice of lines, not with any range this
    module computes.
    Invocation isolation (B4-4, arm 1): as above — with the environment variable set the verdict
    is independent of which checkout the run resolves from.
    Kills: a stale or hand-edited `line_range`, which is exactly the off-by-one pointer class the
    user acts on when transcribing a fix.
    A second correct implementation: a checker that compares `span in "\n".join(lines[first - 1:last + 1])`
    — an inclusive upper bound — still passes for every entry whose span ends before the last
    line of its range, because the slice only differs when the span ends on the range's final
    line.
    """
    entry = REGISTER[equation_id]
    first, last = entry["line_range"]
    lines = manuscript_text.splitlines()
    assert 1 <= first <= last <= len(lines), (
        f"register entry {equation_id}: line_range {first}..{last} is not a well-ordered range within the "
        f"manuscript's {len(lines)} lines"
    )
    recorded_slice = "\n".join(lines[first - 1 : last])
    assert entry["verbatim_span"] in recorded_slice, (
        f"register entry {equation_id}: its verbatim span is not inside its recorded line_range "
        f"{first}..{last}; the pointer is stale or the span belongs elsewhere"
    )


@pytest.mark.parametrize("equation_id", IMPLICIT_EQUATION_IDS)
def test_every_implicit_equation_records_why_the_manuscript_states_none(manuscript_text: str, equation_id: str) -> None:
    """Property: an entry with no span records `line_range` null and a non-empty defect list.

    Class: NEW CAPABILITY — the plausible wrong implementation carries an implicit equation with
    a manufactured line range, or with an empty note, either of which reads downstream as "the
    manuscript states this" or as "nothing is wrong here".
    Reachability: the three implicit rows (E-34, E-35, E-36) are the input, and any regression
    that gives one a span, a range or an empty note reaches this assertion.
    Both-states anchor: the assertion reads the entry's own recorded fields, and it does not
    depend on the manuscript's text — `manuscript_text` is here so this arm skips with the rest
    of the module when the manuscript is absent.
    Invocation isolation (B4-4, arm 1): the fields come from the in-repo register, so no
    invocation change reaches them; the skip coupling is what makes an absent manuscript report
    SKIPPED rather than a partial pass.
    Kills: an implicit equation recorded as if the manuscript stated it.
    A second correct implementation: a checker that asserts `defect_list != []` before checking
    `line_range is None` still passes, because only one property is broken at a time.
    """
    entry = REGISTER[equation_id]
    assert entry["verbatim_span"] == "", f"register entry {equation_id}: an implicit equation must carry an empty span"
    assert entry["line_range"] is None, (
        f"register entry {equation_id}: an implicit equation must carry a null line_range, found {entry['line_range']!r}"
    )
    assert entry["defect_list"], (
        f"register entry {equation_id}: an implicit equation must record why the manuscript states none; "
        "found an empty defect list"
    )


def test_the_environment_variable_wins_over_the_sibling_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with `MULTIDRIVER_SWG_PAPER` naming a readable file, `resolve_manuscript` returns
    that file's path, ahead of the sibling-directory default.

    Class: NEW CAPABILITY — the plausible wrong implementation is a resolver that implements only
    the sibling-directory arm, or that appends the environment arm behind it; under either, a
    caller who has configured a manuscript gets the default instead, or a skip. Red under it
    because the returned path is not the configured one.
    Reachability: every caller that sets `MULTIDRIVER_SWG_PAPER` reaches this branch, which is the
    first arm of decision 3.3 and the arm the run on this machine actually uses; the input is a
    readable file inside this repository, so the test writes nothing outside it.
    Why required: nothing else in this module exercises the environment arm — a suite run where
    the variable is exported passes whether or not the resolver consults it, so without this test
    the first arm of the settled order is unpinned and a reader cannot tell it from dead code.
    The path's CONTENT is irrelevant here and deliberately so: the property is precedence in the
    resolution order, not whether the file is a manuscript.
    Both-states anchor: the assertion compares the returned `Path` with the configured one, never
    with a message or helper this change introduces.
    Invocation isolation (B4-4, arm 2): the input is an environment variable plus an in-repo file,
    so the verdict reproduces from any checkout; discrimination shown instead — a resolver that
    consults only the sibling default returns a different path, which reddens the first assertion.
    A skip is converted to an explicit failure rather than left to skip, because in a worktree the
    sibling fallback does not resolve and the wrong implementation would otherwise report SKIPPED
    — which D-3 says is not a pass, but which also stops discriminating.
    Kills: a resolver that ignores the environment variable.
    A second correct implementation: a resolver that collects candidates and returns the first
    whose `.exists()` is true still passes, because the configured file exists.
    """
    configured = REPO_ROOT / "tests" / "conformance" / "register" / "register.json"
    assert configured.is_file(), "this test's own input must be readable"
    monkeypatch.setenv(MANUSCRIPT_ENV_VAR, str(configured))

    try:
        resolved = resolve_manuscript()
    except pytest.skip.Exception as exc:
        pytest.fail(f"resolution skipped although a readable manuscript was configured: {exc}")

    assert resolved == configured, f"the configured manuscript was not honoured; resolved {resolved}"
    sibling_default = REPO_ROOT.parent / MANUSCRIPT_REPO_DIRNAME / MANUSCRIPT_FILENAME
    assert resolved != sibling_default, "resolution fell through to the sibling default"


def test_an_unresolvable_manuscript_skips_naming_both_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Property: with the variable unset and the sibling default absent, `resolve_manuscript`
    raises `pytest.skip` whose message names BOTH resolution arms.

    Class: NEW CAPABILITY — two plausible wrong implementations, each red here. A resolver that
    returns a path it has not checked makes the caller raise `FileNotFoundError` dressed as a test
    failure, which is the outcome decision 3.3 names as forbidden; a resolver that skips with a
    message naming only the sibling default leaves the reader of a CI skip unable to tell an
    unset variable from one pointing at nothing.
    Reachability: an unset variable plus a repository root whose sibling default is absent is
    exactly the configuration D-3 describes, and both inputs are real conditions rather than
    manufactured ones — the monkeypatched root is a path that exists in this repository, and the
    default under it is a path that does not.
    Why required: §4 item 3 requires that an unresolvable manuscript SKIP rather than pass, and
    D-3 requires that this be shown rather than asserted; no other test in this module reaches
    the skip arm, because the module's own fixtures skip before any of them can run.
    Both-states anchor: the assertions are the raised exception type and two substrings of the
    message, all of which exist independently of this test.
    Invocation isolation (B4-4, arm 2): the inputs are an environment variable and a module
    constant, so no change to the tree the run resolves from reaches them; discrimination shown
    instead — returning the absent path instead of skipping fails the `else` arm.
    Kills: a resolver that returns an unchecked path, or a skip that names one arm only.
    A second correct implementation: a resolver that gathers the attempts first and then calls
    `pytest.skip` on the collected list still passes, because the assertions are the type and the
    presence of both names, not the sentence around them.
    """
    monkeypatch.delenv(MANUSCRIPT_ENV_VAR, raising=False)
    patched_root = REPO_ROOT / "src"
    monkeypatch.setattr(sys.modules[__name__], "REPO_ROOT", patched_root)
    absent_default = patched_root.parent / MANUSCRIPT_REPO_DIRNAME / MANUSCRIPT_FILENAME
    assert not absent_default.exists(), "this test needs the sibling default to be absent under the patched root"

    try:
        resolve_manuscript()
    except pytest.skip.Exception as exc:
        message = str(exc)
    else:
        pytest.fail("resolve_manuscript returned a path instead of skipping on an absent manuscript")

    assert MANUSCRIPT_ENV_VAR in message, f"the skip message omits the environment arm: {message}"
    assert str(absent_default) in message, f"the skip message omits the sibling arm: {message}"
