"""Reviewer tests of the test-environment rule: the cases the author's module does not reach.

`tests/test_test_environment.py` holds the real files to the rule and drives its reader over one
synthetic descriptor. What it does not reach is the input space the rule has to decide: a pin that
moves, a pin relaxed to a range, a member that disappears, a group that includes another group, a
store that agrees with no pin, stores that disagree with each other, an empty store, and a
`python-version` on another interpreter. A rule that admits one of those measures nothing, and the
suite's whole red would be read as the code's. Every case below is one of them, and the first five
are correct variants, so the rule is held to admitting a workflow and a group that satisfy it in a
spelling other than the one the tree happens to carry.

The rule is imported from the module under review rather than copied: the rule decides a set of
versions, so a reader that silently returns nothing reports every store as recording no version and
every pin as disagreeing, which reds here rather than passing. The reader itself is held to an
independent parse on the real corpus below, which is the case the author's one synthetic descriptor
cannot make.
"""

from __future__ import annotations

import tomllib

import pytest

from tests import test_test_environment as subject

REAL_PYPROJECT = subject.PYPROJECT.read_text(encoding="utf-8")
REAL_WORKFLOW = subject.WHOLE_SUITE.read_text(encoding="utf-8")
REAL_DESCRIPTORS = subject._recorded()


def _swap(text: str, old: str, new: str) -> str:
    """`text` with the one occurrence of `old` replaced; an anchor absent or repeated is a bug in the case."""
    assert text.count(old) == 1, f"the anchor {old!r} must occur once"
    return text.replace(old, new)


def _store(*, numpy: str | None = None, pandas: str | None = None) -> list[tuple[str, dict[str, str]]]:
    """The real stores with the one named version replaced, or with that key removed when the value is None."""
    changed = []
    for path, diagnostics in REAL_DESCRIPTORS:
        edited = dict(diagnostics)
        for name, version in (("numpy", numpy), ("pandas", pandas)):
            if version is None:
                continue
            if version == "":
                edited.pop(name, None)
            else:
                edited[name] = version
        changed.append((path, edited))
    return changed


def _case(pyproject: str = REAL_PYPROJECT, descriptors=None, workflow: str = REAL_WORKFLOW):
    """The rule's three inputs, each defaulting to the real file or store."""
    return (pyproject, REAL_DESCRIPTORS if descriptors is None else descriptors, workflow)


@pytest.mark.parametrize(
    ("case", "refused"),
    [
        pytest.param(_case(), False, id="the real project"),
        pytest.param(
            _case(_swap(REAL_PYPROJECT, '    "tqdm==4.70.1",\n', '    "tqdm==4.70.1",\n    "joblib==1.4.2",\n')),
            False,
            id="correct: another pinned distribution in the group",
        ),
        pytest.param(
            _case(
                _swap(
                    REAL_PYPROJECT,
                    '    "numpy==2.4.6",\n    "pandas==2.3.3",\n',
                    '    "pandas==2.3.3",\n    "numpy==2.4.6",\n',
                )
            ),
            False,
            id="correct: the members in another order",
        ),
        pytest.param(
            _case(workflow=_swap(REAL_WORKFLOW, "python-version: '3.11'", 'python-version: "3.11"')),
            False,
            id="correct: the interpreter in double quotes",
        ),
        pytest.param(
            _case(descriptors=[(path, dict(diagnostics)) for path, diagnostics in REAL_DESCRIPTORS]),
            False,
            id="correct: a store rebuilt from the same diagnostics",
        ),
        pytest.param(
            _case(_swap(REAL_PYPROJECT, '"numpy==2.4.6"', '"numpy==2.4.5"')),
            True,
            id="wrong: a pin moved off the recorded version",
        ),
        pytest.param(
            _case(_swap(REAL_PYPROJECT, '"pandas==2.3.3"', '"pandas>=2.3.3"')),
            True,
            id="wrong: a pin relaxed to a range",
        ),
        pytest.param(
            _case(_swap(REAL_PYPROJECT, '    "xarray==2026.9.0",\n', "")),
            True,
            id="wrong: a reference distribution absent from the group",
        ),
        pytest.param(
            _case(_swap(REAL_PYPROJECT, '    "pytest==9.1.1",\n', '    "pytest==9.1.1",\n    "pytest-cov==7.0.0",\n')),
            True,
            id="wrong: a plugin named `pytest-…` in the group",
        ),
        pytest.param(
            _case(_swap(REAL_PYPROJECT, '    "scipy==1.17.1",\n', '    {include-group = "docs"},\n')),
            True,
            id="wrong: a member that includes another group",
        ),
        pytest.param(_case(descriptors=_store(pandas="3.1.1")), True, id="wrong: the stores record another pandas"),
        pytest.param(_case(descriptors=_store(numpy="")), True, id="wrong: a store records no numpy"),
        pytest.param(_case(descriptors=[]), True, id="wrong: no stored value at all"),
        pytest.param(
            _case(workflow=_swap(REAL_WORKFLOW, "python-version: '3.11'", "python-version: '3.12'")),
            True,
            id="wrong: the job runs another interpreter",
        ),
    ],
)
def test_the_environment_rule_admits_a_matching_declaration_and_refuses_any_drift(case, refused):
    """Property: the rule's verdict is the agreement of the group, the stores and the job's interpreter.

    Class: NEW CAPABILITY — the group's members, the stores' `[diagnostics]` and the workflow's
    `python-version`. The plausible wrong implementations are the thirteen cases above, five correct
    and eight refused; each of the eight is a declaration that has stopped being the environment the
    stored values were computed in, and each of the five is a correct declaration in a spelling other
    than the tree's.

    Why: the suite's red is only the code's while the environment is the stores'. A rule that admits a
    relaxed pin, a missing distribution, a plugin or a moved interpreter leaves the Whole suite job
    free to install an environment that turns eleven environment failures back into failures of the
    code under test, which is the defect this declaration exists to remove.

    Killed by: a rule that reads only the group (every store case passes), only the stores (every pin
    case passes), or only `numpy` (the pandas and xarray cases pass). Second correct implementation:
    the rule stated over a `tomllib` parse of the descriptors rather than over their text, which
    agrees on every store the harness writes.

    Not established: what pip installs from the group's transitive closure, which no file here
    records until the lock (DR-7); and whether a `pytest-…` plugin is ever wanted, which would need
    the pin rule widened rather than bypassed.
    """
    problems = subject._environment_problems(*case)
    assert bool(problems) is refused, problems


def test_the_text_reader_agrees_with_a_parse_on_every_real_descriptor():
    """Property: over every store in the descriptor directory, the text scan reads exactly the `[diagnostics]` table a TOML parse reads.

    Class: NEW CAPABILITY — the real corpus, which the author's module reaches only through the rule's
    verdict and so would read as conforming even if the reader dropped one distribution per store. The
    plausible wrong implementation is a scan that reads a `[diagnostics]` line of the wrong table, or
    that stops at a table boundary it states wrongly: every store still agrees with every pin while one
    of the four is never compared.

    Why: the rule's verdict is only as wide as what the reader returns, and the corpus is the one input
    whose shape the author's synthetic descriptor does not pin.

    Killed by: a reader that drops a key, keeps a comment in a value, or reads past `[diagnostics]`, and
    a read that walks a narrower set of descriptors than the store holds. Second correct
    implementation: the rule itself stated over `tomllib`, which this test uses as the second opinion.

    Not established: that every future descriptor carries all five keys; this test asserts the four
    reference distributions and the interpreter are present, and a store missing one reds instead. The
    corpus's SIZE is not pinned either: the store is meant to grow once the port lands, so the figure
    is compared with a fresh glob of the store rather than with today's count, and only the
    non-emptiness is asserted as a floor.
    """
    stored = sorted(path.relative_to(subject.REPO_ROOT).as_posix() for path in subject.STORE.glob("*/*.toml"))
    assert stored, f"no stored value under {subject.STORE}"
    assert [path for path, _ in REAL_DESCRIPTORS] == stored, (stored, [path for path, _ in REAL_DESCRIPTORS])
    for path, diagnostics in REAL_DESCRIPTORS:
        text = (subject.REPO_ROOT / path).read_text(encoding="utf-8")
        assert diagnostics == tomllib.loads(text)["diagnostics"], path
        for name in (*subject.REFERENCE_DISTRIBUTIONS, "python"):
            assert name in diagnostics, (path, name)


@pytest.mark.parametrize(
    ("old", "new", "id_"),
    [
        (
            '    "pandas==2.3.3",\n',
            '    "pandas==3.1.1",\n    "pandas==2.3.3",\n',
            "the moved pin first, the recorded pin last",
        ),
        (
            '    "pandas==2.3.3",\n',
            '    "pandas==2.3.3",\n    "pandas==3.1.1",\n',
            "the recorded pin first, the moved pin last",
        ),
    ],
    ids=lambda value: value if isinstance(value, str) and " " in value and "\n" not in value else None,
)
def test_the_environment_rule_refuses_a_group_that_pins_one_distribution_at_two_versions(old, new, id_):
    """Property: a group naming one reference distribution at two versions is refused whichever pin is written last.

    Class: NEW CAPABILITY — the group's membership. The plausible wrong implementation is the rule as
    written, which folds the members into a name-to-version dict so the last pin of a name replaces
    the earlier one; a stale pin written before the recorded one is then never compared with the
    stores. pip refuses such a group at install time, so the defect is a late and loud one rather
    than a silent one, but the rule's own claim is that the declaration is one environment.

    Killed by: a rule that compares every pin of a name, or that refuses a repeated name.
    Second correct implementation: none; one name at two versions is no environment.
    """
    problems = subject._environment_problems(*_case(_swap(REAL_PYPROJECT, old, new)))
    assert problems, id_
