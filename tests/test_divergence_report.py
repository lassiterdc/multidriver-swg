"""Tests of the divergence report: what it counts, what it names, what it leaves out, and what it refuses.

The report reads the divergence ledger and the pair registry. Every test builds its own ledger
through `well_formed_ledger`, because the report validates a ledger before reading it and a
ledger without its header or its side tables is refused before the form under test is reached.
The real registry lives for the whole interpreter and other tests compare it with a scan of the
package, so an autouse fixture swaps in an empty registry for each test and restores the real
one after it; no test here registers anything in the real registry. Each test names the wrong
implementation it is red under, because a check that cannot fail for the reason it claims
constrains nothing.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import itertools
import re

import pytest

from multidriver_swg import divergence_ledger
from multidriver_swg import pairs as pair_registry
from multidriver_swg.divergence_ledger import Ledger, validate
from multidriver_swg.divergence_report import (
    DivergenceReport,
    LedgerInvalidError,
    build_report,
    render_report,
    write_report,
)

pytestmark = pytest.mark.state_check

PRE = pair_registry.PRE_REFACTOR
CUR = pair_registry.CURRENT

NO_PAIR_DIVERGES = "No pair diverges."
NO_DEPARTURE = "No departure entry is recorded."

# The seven whole-line forms a report may contain; each statement is exactly one of them.
FORMS = {
    "covered": re.compile(r"Pairs covered: \d+"),
    "registered": re.compile(r"Pairs registered: \d+"),
    "no divergence": re.compile(r"No pair diverges\."),
    "divergent pair": re.compile(r"Pair \S+ diverges: .+ \(reason: .+\)"),
    "departure statement": re.compile(r"The run did not reproduce the legacy code at these sites \(\d+\):"),
    "departure": re.compile(r"\S+ line \d+: .+"),
    "no departure": re.compile(r"No departure entry is recorded\."),
}


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    """Run every test of this module against an empty registry that is discarded with the test."""
    monkeypatch.setattr(pair_registry, "_REGISTRY", {})


def _label(text: str) -> str:
    """A fingerprint label of the form the ledger requires, derived from `text`."""
    return "canon-ast:1:" + hashlib.sha256(text.encode()).hexdigest()


def _side(pair: str, side: str) -> dict:
    """A side table with every key the ledger requires, naming a function after the pair and the side."""
    function = f"constructed.module:{pair}_{side}"
    return {"function": function, "combined": _label(function), "functions": {function: _label(function + "!")}}


def _pair_entry(description: dict) -> dict:
    """One pair entry from a short description: `id`, optional `diverges`, and the reasons a divergence needs."""
    pair = description["id"]
    diverges = description.get("diverges", False)
    entry = {"id": pair, "pre_refactor": _side(pair, PRE), "current": _side(pair, CUR), "diverges": diverges}
    if diverges:
        entry["changed"] = description.get("changed", f"what {pair} changed")
        entry["why"] = description.get("why", f"why {pair} changed")
        entry["judged_correct_when"] = description.get("judged_correct_when", f"{pair} is judged correct when it is")
    if "departure" in description:
        entry["departure"] = description["departure"]
    return entry


def _departure_entry(description: dict) -> dict:
    """One departure entry from a short description: `unit`, `site`, optional `kind`, `why` and `future_pair`."""
    unit, site = description["unit"], description["site"]
    kind = description.get("kind", "licensed")
    entry = {
        "unit": unit,
        "site": site,
        "kind": kind,
        "grounds": {"triage": "a constructed ground"},
        "new_code": f"constructed new code for {unit}",
        "why": description.get("why", f"why {unit} line {site} departs"),
        "judged_correct_when": f"{unit} line {site} is judged correct when it is",
    }
    if kind == "finding-4":
        entry["future_pair"] = description["future_pair"]
    return entry


def well_formed_ledger(pairs: list[dict], departures: list[dict]) -> Ledger:
    """A ledger that `validate` accepts, filled from short descriptions of its entries.

    A pair is described by `id`, an optional `diverges` (default false), optional `changed`,
    `why` and `judged_correct_when` (filled when it diverges), and an optional `departure`
    pointer. A departure is described by `unit`, `site`, an optional `kind` (default
    `licensed`), an optional `why`, and a `future_pair` for kind `finding-4`. The header and
    every other required key are filled with constructed values.
    """
    header = {"rule": divergence_ledger.RULE, "statement": "a ledger constructed for a test"}
    return Ledger([_pair_entry(p) for p in pairs], [_departure_entry(d) for d in departures], header)


def _register(pair: str, *sides: str) -> None:
    """Register one stand-in function per side of `pair` in the swapped-in registry."""
    for side in sides:

        def stand_in(x):
            """A stand-in whose identity is all a test reads."""
            return x

        pair_registry.pair_side(pair=pair, side=side)(stand_in)


def _lines(report: DivergenceReport) -> list[str]:
    """The rendered report as whole lines, which is the unit every wording test matches on."""
    return render_report(report).splitlines()


def test_the_helper_builds_a_ledger_the_validator_accepts():
    """Property: a helper-built ledger has the form, so a refusal in any later test is about the report.

    Class: a ledger with pairs of both kinds and departures of all three kinds. Red under a
    helper that omits a required key or a header, which would make every test below pass or fail
    for the wrong reason.
    """
    ledger = well_formed_ledger(
        [{"id": "a", "diverges": True}, {"id": "b"}],
        [
            {"unit": "u", "site": 3},
            {"unit": "u", "site": 4, "kind": "finding-3"},
            {"unit": "v", "site": 5, "kind": "finding-4", "future_pair": "c"},
        ],
    )
    assert validate(ledger) == []


def test_divergent_pairs_are_named_in_identifier_order_with_their_reasons():
    """Property: a divergent pair is named with its changed text and reason, a non-divergent pair is not.

    Class: pairs a (diverges), b (does not), c (diverges), all registered, given as c, a, b. Red
    under a report that lists every pair, that lists none, or that keeps the ledger file's order.
    """
    for pair in ("a", "b", "c"):
        _register(pair, PRE, CUR)
    ledger = well_formed_ledger(
        [
            {"id": "c", "diverges": True, "changed": "c now sums", "why": "c summed wrongly"},
            {"id": "a", "diverges": True, "changed": "a now sorts", "why": "a sorted wrongly"},
            {"id": "b"},
        ],
        [],
    )
    report = build_report(ledger)
    assert report.divergent == (("a", "a now sorts", "a sorted wrongly"), ("c", "c now sums", "c summed wrongly"))
    assert (report.pairs_covered, report.pairs_registered) == (3, 3)
    lines = _lines(report)
    assert lines[:2] == ["Pairs covered: 3", "Pairs registered: 3"]
    assert "Pair a diverges: a now sorts (reason: a sorted wrongly)" in lines
    assert "Pair c diverges: c now sums (reason: c summed wrongly)" in lines
    assert lines.index("Pair a diverges: a now sorts (reason: a sorted wrongly)") < lines.index(
        "Pair c diverges: c now sums (reason: c summed wrongly)"
    )
    assert not any(line.startswith("Pair b ") for line in lines)
    assert NO_PAIR_DIVERGES not in lines


@pytest.mark.parametrize("entries", [2, 0])
def test_a_report_with_no_divergent_pair_says_so_and_still_states_the_covered_count(entries):
    """Property: the explicit zero carries the covered count, so zero divergences and zero coverage differ.

    Class: two pair entries none of which diverges, and an empty ledger. Red under a report that
    prints nothing for no divergence, or one that drops the covered line when it prints the zero.
    """
    ledger = well_formed_ledger([{"id": f"p{n}"} for n in range(entries)], [])
    lines = _lines(build_report(ledger))
    assert NO_PAIR_DIVERGES in lines
    assert f"Pairs covered: {entries}" in lines
    assert not any(" diverges: " in line for line in lines)


def test_the_no_divergence_statement_is_absent_when_any_pair_diverges():
    """Property: the explicit zero is printed exactly when no pair diverges.

    Class: one divergent pair among two. Red under a report that prints the zero statement
    unconditionally.
    """
    ledger = well_formed_ledger([{"id": "a", "diverges": True}, {"id": "b"}], [])
    assert NO_PAIR_DIVERGES not in _lines(build_report(ledger))


def test_departure_entries_are_listed_with_site_and_reason_under_the_statement_and_count():
    """Property: each departure entry of any kind is named with its site and reason, under the not-reproduced statement.

    Class: a licensed entry and a finding-3 entry. Red under a report that omits departures, that
    omits their reasons, or that states a count other than the number listed.
    """
    ledger = well_formed_ledger(
        [],
        [
            {"unit": "u2", "site": 7, "kind": "finding-3", "why": "the second reason"},
            {"unit": "u1", "site": 4, "why": "the first reason"},
        ],
    )
    report = build_report(ledger)
    assert report.departures == (("u1", 4, "the first reason"), ("u2", 7, "the second reason"))
    lines = _lines(report)
    assert "The run did not reproduce the legacy code at these sites (2):" in lines
    assert "u1 line 4: the first reason" in lines
    assert "u2 line 7: the second reason" in lines
    assert NO_DEPARTURE not in lines


def test_a_report_with_no_departure_entry_says_so_and_counts_zero():
    """Property: an absent departure section is an explicit statement, not a silence.

    Class: a ledger with no departure entries. Red under a report that prints the not-reproduced
    statement with a count of zero, or prints nothing for the section.
    """
    report = build_report(well_formed_ledger([{"id": "a"}], []))
    assert report.departures == ()
    lines = _lines(report)
    assert NO_DEPARTURE in lines
    assert not any(line.startswith("The run did not reproduce") for line in lines)


def test_a_fourth_finding_departure_is_left_out_once_its_pair_has_both_sides_and_the_pair_is_named_instead():
    """Property: the departure entry and the pair entry that replaced it are not both reported as separate changes.

    Class: a finding-4 departure whose recorded pair has both sides registered and a divergent
    pair entry of that identifier. Red under a report that always lists the departure, and under
    one that drops the pair entry along with it.
    """
    _register("later", PRE, CUR)
    ledger = well_formed_ledger(
        [{"id": "later", "diverges": True, "changed": "later changed", "why": "later had a reason"}],
        [{"unit": "u", "site": 9, "kind": "finding-4", "future_pair": "later"}],
    )
    report = build_report(ledger)
    assert report.departures == ()
    assert [pair for pair, _, _ in report.divergent] == ["later"]
    lines = _lines(report)
    assert "Pair later diverges: later changed (reason: later had a reason)" in lines
    assert NO_DEPARTURE in lines


@pytest.mark.parametrize(
    "registered_sides",
    [(), (PRE,), (CUR,)],
    ids=["unregistered", "legacy side only", "current side only"],
)
def test_a_fourth_finding_departure_stays_listed_until_its_pair_has_both_sides(registered_sides):
    """Property: "registered" means both sides present, so a pair being built does not remove its departure entry.

    Class: the same finding-4 departure with its pair unregistered, and registered on one side
    only, each side in turn. Red under a rule that leaves the entry out when either side, or
    any registry key, is present.
    """
    if registered_sides:
        _register("later", *registered_sides)
    ledger = well_formed_ledger(
        [{"id": "later", "diverges": True}],
        [{"unit": "u", "site": 9, "kind": "finding-4", "future_pair": "later"}],
    )
    report = build_report(ledger)
    assert report.departures == (("u", 9, "why u line 9 departs"),)
    assert "u line 9: why u line 9 departs" in _lines(report)


def test_registered_can_be_given_and_replaces_the_registry_reading():
    """Property: an explicit `registered` collection governs both the printed count and the left-out rule.

    Class: an empty registry and an explicit collection naming the finding-4 pair. Red under a
    report that ignores the argument and reads the registry.
    """
    ledger = well_formed_ledger([], [{"unit": "u", "site": 1, "kind": "finding-4", "future_pair": "later"}])
    assert build_report(ledger, registered={"later"}).departures == ()
    assert build_report(ledger, registered=set()).departures != ()
    assert build_report(ledger, registered=["x", "y", "later"]).pairs_registered == 3


def test_the_default_registered_count_reads_the_registry_at_the_call_and_counts_only_both_sided_pairs():
    """Property: the default registered count is the pairs with both sides in the registry as it is now.

    Class: one pair with both sides, one with a single side, read after registration. Red under
    a count of registry keys, and under an import-time copy of the registry.
    """
    ledger = well_formed_ledger([], [])
    assert build_report(ledger).pairs_registered == 0
    _register("whole", PRE, CUR)
    _register("half", CUR)
    assert build_report(ledger).pairs_registered == 1


def test_a_short_ledger_is_visible_because_covered_and_registered_are_both_printed():
    """Property: a ledger that covers fewer pairs than the code registers shows both numbers.

    Class: two pair entries and three registered pairs. Red under a report that prints one of the
    two numbers, or that prints the same number twice.
    """
    for pair in ("a", "b", "c"):
        _register(pair, PRE, CUR)
    lines = _lines(build_report(well_formed_ledger([{"id": "a"}, {"id": "b"}], [])))
    assert "Pairs covered: 2" in lines
    assert "Pairs registered: 3" in lines


def test_a_divergent_pair_without_a_reason_is_refused_with_the_validator_findings():
    """Property: a report is not built from a ledger without its form, and the refusal carries the findings.

    Class: a divergent pair entry whose reason was removed. Red under a report that reads the
    entry anyway, or a refusal that drops the findings.
    """
    ledger = well_formed_ledger([{"id": "a", "diverges": True}], [])
    del ledger.pairs[0]["why"]
    expected = validate(ledger)
    assert expected
    with pytest.raises(LedgerInvalidError) as raised:
        build_report(ledger)
    assert raised.value.findings == expected
    assert isinstance(raised.value, ValueError)
    with pytest.raises(LedgerInvalidError):
        write_report(io.StringIO(), ledger)


def test_a_ledger_without_its_header_is_refused_before_anything_is_read():
    """Property: the refusal covers a header defect as well as an entry defect.

    Class: a ledger built without a header. Red under a validator call placed only over entries.
    """
    with pytest.raises(LedgerInvalidError) as raised:
        build_report(Ledger(pairs=[], departures=[]))
    assert len(raised.value.findings) == 2


def test_the_text_does_not_depend_on_the_order_of_the_ledger_file():
    """Property: pair entries order by identifier and departure entries by unit then numeric line.

    Class: every ordering of three pair entries and of three departure entries, two of one unit
    at lines 10 and 9. Red under file order, and under a line order that compares text (10 before 9).
    """
    pair_descriptions = [{"id": "b", "diverges": True}, {"id": "a", "diverges": True}, {"id": "c"}]
    departure_descriptions = [
        {"unit": "u", "site": 10},
        {"unit": "u", "site": 9},
        {"unit": "t", "site": 20},
    ]
    texts = {
        render_report(build_report(well_formed_ledger(list(p), list(d))))
        for p in itertools.permutations(pair_descriptions)
        for d in itertools.permutations(departure_descriptions)
    }
    assert len(texts) == 1
    lines = texts.pop().splitlines()
    sites = [line for line in lines if " line " in line]
    assert [line.split(":")[0] for line in sites] == ["t line 20", "u line 9", "u line 10"]


def test_a_reason_with_line_breaks_stays_on_one_line():
    """Property: every statement is its own whole line, whatever the ledger text holds.

    Class: a changed text, a reason and a departure reason that each contain a line break. Red
    under a report that writes the ledger text as it stands.
    """
    ledger = well_formed_ledger(
        [{"id": "a", "diverges": True, "changed": "first\nsecond", "why": "one\n\ntwo"}],
        [{"unit": "u", "site": 1, "why": "x\ny"}],
    )
    lines = _lines(build_report(ledger))
    assert "Pair a diverges: first second (reason: one two)" in lines
    assert "u line 1: x y" in lines


def test_the_same_report_renders_the_same_text_and_the_text_ends_in_a_line_break():
    """Property: rendering is deterministic and does not alter the report.

    Class: one report rendered twice. Red under a renderer that carries state between calls.
    """
    report = build_report(well_formed_ledger([{"id": "a", "diverges": True}], [{"unit": "u", "site": 1}]))
    first = render_report(report)
    assert render_report(report) == first
    assert first.endswith("\n")


def test_write_report_writes_the_rendered_report_and_returns_the_structure():
    """Property: the stream receives exactly the rendered text and the caller receives the structure.

    Class: a ledger with one divergent pair and one departure entry. Red under a writer that
    writes a different form or returns nothing.
    """
    ledger = well_formed_ledger([{"id": "a", "diverges": True}], [{"unit": "u", "site": 1}])
    stream = io.StringIO()
    returned = write_report(stream, ledger)
    assert stream.getvalue() == render_report(build_report(ledger))
    assert returned == build_report(ledger)


def test_the_packaged_ledger_reports_relationally_to_its_own_entries():
    """Property: with the packaged ledger and an empty registry the report counts the ledger's own entries.

    Class: the packaged ledger, which holds no pair entry, read with the swapped-in empty
    registry. Red under a report that ignores the ledger it is given, and under a count written
    as a literal rather than read from the ledger.
    """
    ledger = divergence_ledger.load()
    report = build_report()
    assert report.pairs_covered == len(ledger.pairs)
    assert report.pairs_registered == 0
    assert len(report.departures) == len(ledger.departures)
    if not ledger.pairs:
        assert NO_PAIR_DIVERGES in _lines(report)


def _ledger_lines(ledger, registered=None) -> list[str]:
    """The rendered report of `ledger` as whole lines."""
    return render_report(build_report(ledger, registered)).splitlines()


def test_the_statements_come_in_the_specified_order():
    """Property: counts first, then the pair statements, then the departure section.

    Class: a ledger with a divergent pair and departure entries, and one with neither a divergent
    pair nor a departure entry. Red under a report that prints the departure section before the
    pair statements, or the counts after them. Still green under a report with no blank lines
    between sections, or with several, since only the order of statements is read.
    """
    _register("a", PRE, CUR)
    with_both = _ledger_lines(
        well_formed_ledger([{"id": "a", "diverges": True}], [{"unit": "u", "site": 3}]),
    )
    covered = with_both.index("Pairs covered: 1")
    registered = with_both.index("Pairs registered: 1")
    pair = next(i for i, line in enumerate(with_both) if line.startswith("Pair a diverges: "))
    statement = next(i for i, line in enumerate(with_both) if line.startswith("The run did not reproduce"))
    departure = with_both.index("u line 3: why u line 3 departs")
    assert covered < registered < pair < statement < departure

    with_neither = _ledger_lines(well_formed_ledger([{"id": "a"}], []))
    assert with_neither.index("Pairs registered: 1") < with_neither.index("No pair diverges.")
    assert with_neither.index("No pair diverges.") < with_neither.index("No departure entry is recorded.")


def test_every_nonblank_line_is_exactly_one_of_the_seven_forms():
    """Property: a report holds no line outside the seven statement forms, and no line fits two.

    Class: a report with divergent pairs and departures, one with the two explicit zeros, and one
    with a pair statement but no departure. Red under a report that adds a heading, a bullet
    marker or an indentation to any line. Still green under a report with no blank lines at all.
    """
    _register("a", PRE, CUR)
    ledgers = [
        well_formed_ledger(
            [{"id": "a", "diverges": True}, {"id": "b", "diverges": True}, {"id": "c"}],
            [{"unit": "u", "site": 3}, {"unit": "v", "site": 12, "kind": "finding-3"}],
        ),
        well_formed_ledger([], []),
        well_formed_ledger([{"id": "a", "diverges": True}], []),
    ]
    for ledger in ledgers:
        for line in _ledger_lines(ledger):
            if not line:
                continue
            matched = [name for name, form in FORMS.items() if form.fullmatch(line)]
            assert len(matched) == 1, (line, matched)


def test_the_departure_count_is_the_number_listed_after_the_left_out_entries_go():
    """Property: the count in the departure statement is the number of departure lines, left-out entries excluded.

    Class: three departure entries of which one fourth-finding entry has its pair registered with
    both sides, a second fourth-finding entry has a different, unregistered pair, and one is
    licensed. Red under a count of every ledger departure entry (3 where 2 are listed), and under
    a rule that leaves out every fourth-finding entry once any pair is registered.
    """
    _register("done", PRE, CUR)
    ledger = well_formed_ledger(
        [{"id": "done", "diverges": True}],
        [
            {"unit": "u1", "site": 1, "kind": "finding-4", "future_pair": "done"},
            {"unit": "u2", "site": 2, "kind": "finding-4", "future_pair": "pending"},
            {"unit": "u3", "site": 3},
        ],
    )
    report = build_report(ledger)
    assert [(unit, site) for unit, site, _ in report.departures] == [("u2", 2), ("u3", 3)]
    lines = _ledger_lines(ledger)
    assert "The run did not reproduce the legacy code at these sites (2):" in lines
    assert not any(line.startswith("u1 line ") for line in lines)
    assert sum(1 for line in lines if re.fullmatch(r"\S+ line \d+: .+", line)) == 2


def test_a_fourth_finding_departure_is_left_out_by_the_registry_and_not_by_a_pair_entry_pointing_at_it():
    """Property: only a registered pair removes a departure entry; a ledger pair entry that points at it does not.

    Class: a fourth-finding departure entry and a divergent pair entry whose `departure` pointer
    names that entry, with the pair not registered. Red under a rule that leaves the entry out
    because the ledger holds a pointing pair entry. Still green under a rule that reads the
    registry through any accessor, since only the registered set is observed.
    """
    ledger = well_formed_ledger(
        [{"id": "later", "diverges": True, "departure": {"unit": "u", "site": 9}}],
        [{"unit": "u", "site": 9, "kind": "finding-4", "future_pair": "later"}],
    )
    assert [(unit, site) for unit, site, _ in build_report(ledger).departures] == [("u", 9)]
    _register("later", PRE, CUR)
    assert build_report(ledger).departures == ()


def test_write_report_forwards_the_registered_collection_to_the_report():
    """Property: the registered collection given to `write_report` governs the text it writes.

    Class: a fourth-finding entry whose pair is named only in the collection handed to
    `write_report`, passed positionally and by keyword. Red under a writer that builds its report
    from the ledger alone and so ignores the collection. Still green under a writer that calls
    `build_report` or builds the report itself.
    """
    ledger = well_formed_ledger([], [{"unit": "u", "site": 1, "kind": "finding-4", "future_pair": "later"}])
    for call in (
        lambda stream: write_report(stream, ledger, {"later"}),
        lambda stream: write_report(stream, ledger, registered={"later"}),
    ):
        stream = io.StringIO()
        returned = call(stream)
        assert returned.departures == ()
        assert returned.pairs_registered == 1
        assert stream.getvalue() == render_report(build_report(ledger, {"later"}))
        assert "u line 1" not in stream.getvalue()


def test_write_report_with_only_a_stream_reports_the_packaged_ledger_against_the_registry():
    """Property: a bare `write_report(stream)` writes what `render_report(build_report())` returns.

    Class: the packaged ledger and the swapped-in empty registry, no ledger and no collection
    given. Red under a writer that needs a ledger argument, or that writes anything other than
    the rendered default report. Still green under a writer that loads the packaged ledger itself.
    """
    stream = io.StringIO()
    returned = write_report(stream)
    assert stream.getvalue() == render_report(build_report())
    assert returned == build_report()


def test_a_report_cannot_be_changed_after_it_is_built():
    """Property: the structure a report is built into is read-only, so what was rendered can be rebuilt unchanged.

    Class: each field of a built report. Red under a mutable structure, where a caller changing
    a count after building would make the written text and the returned structure disagree. Still
    green under any frozen representation, since only the refusal to assign is observed.
    """
    report = build_report(well_formed_ledger([{"id": "a", "diverges": True}], [{"unit": "u", "site": 1}]))
    for name in ("pairs_covered", "pairs_registered", "divergent", "departures"):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(report, name, getattr(report, name))
