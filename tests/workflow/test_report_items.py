"""Check the report-item conventions: what an item may name, what the graph declares, and what the report shows.

Each check here is written against a form the defect takes when the conventions stop being one statement:
a heading that renders as a blank group, an output the page can only offer as a download, an item naming a
file its rule never declares, two items claiming one artifact, and a report whose one computed section is
absent because the workflow file never flagged it. The last check runs the real engine with its report
option, because the flag travels from the row through the rendered output directive into the engine's own
report, and only a run shows all three agree.

The stub body writes empty artifacts, so the report's content is empty as well; what is asserted is where
the citation section APPEARS, which is the convention this module exists to hold.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from multidriver_swg.workflow.graph import GRAPH, GraphLookupError, RuleSpec
from multidriver_swg.workflow.report_items import (
    CITATION,
    DIVERGENCE,
    IN_PAGE_SUFFIXES,
    ReportItem,
    ReportItemError,
)
from tests.workflow.conftest import Estate, ToolkitCopy, write_record

# Every artifact the graph declares, in table order: the full target list a conforming run builds.
ALL_TARGETS = tuple(f"{rule.name}/{output}" for rule in GRAPH for output in rule.outputs)

# The one item the graph declares, as the table states it: the rule, the declared output and the heading.
EXPECTED_ITEMS = (("method_attribution", "citation_section.html", "Citation"),)


def _report_categories(report: Path) -> dict[str, dict[str, list[str]]]:
    """Return the category table the report page carries, read from the page's own data."""
    text = report.read_text(encoding="utf-8")
    prefix = "var categories = "
    line = next(candidate for candidate in text.splitlines() if candidate.strip().startswith(prefix))
    return json.loads(line.strip().removeprefix(prefix).removesuffix(";"))


@pytest.mark.parametrize("category", ["", "   ", "\n\t"], ids=["empty", "spaces", "whitespace"])
def test_an_item_with_no_heading_is_refused(category: str) -> None:
    """Property: a report item whose category is empty or blank raises `ReportItemError` naming the item's output.

    Class: an empty heading, spaces and a tab: the three ways a heading is written and read as absent.
    Why it is required: the category is the heading the page groups an item under, so an empty one is an
    item shown under no name — present in the report and unfindable in it.
    What kills it: accepting an empty heading, or checking truth instead of non-whitespace text.
    Second correct implementation: a validator that normalises the heading first and refuses the empty
    result; the same three values are refused.
    """
    with pytest.raises(ReportItemError) as caught:
        ReportItem("citation_section.html", category)

    assert "citation_section.html" in str(caught.value)


@pytest.mark.parametrize(
    ("output", "suffix"),
    [
        ("citation_section.txt", ".txt"),
        ("table.csv", ".csv"),
        ("observed_record.json", ".json"),
        ("notes", ""),
        ("archive.tar.gz", ".gz"),
        (".hidden", ""),
    ],
)
def test_an_item_whose_output_the_page_cannot_show_is_refused(output: str, suffix: str) -> None:
    """Property: an output whose suffix is outside the admitted set raises `ReportItemError` naming the output, its suffix and every admitted suffix.

    Class: a text file, a table, a JSON record, an extensionless name, a double extension and a name whose
    only dot is its first character.
    Why it is required: a file the page cannot display appears as a download button, which is the second
    surface the convention exists to refuse; and the message is the repair, so it must name what it read
    and what it accepts.
    What kills it: admitting every suffix, reading the FIRST dot instead of the last, or a message that
    withholds the admitted set.
    Second correct implementation: a table of extensions read from the name's tail; the same six names are
    refused with the same facts in the message.
    """
    with pytest.raises(ReportItemError) as caught:
        ReportItem(output, CITATION)

    message = str(caught.value)
    assert repr(suffix) in message
    assert output in message
    for admitted in IN_PAGE_SUFFIXES:
        assert admitted in message


def test_every_admitted_suffix_is_accepted() -> None:
    """Property: one item per admitted suffix is built and keeps its output and heading unchanged.

    Class: the admitted set, one name per member.
    Why it is required: the set is what a figure or an HTML page is written as; a refusal that also
    rejected an admitted form would leave no way to publish the items the convention is FOR.
    What kills it: a suffix list that disagrees with the check, or a check that compares the wrong slice.
    Second correct implementation: any membership test over the same tuple; the same items are admitted.
    """
    for suffix in IN_PAGE_SUFFIXES:
        item = ReportItem(f"figure{suffix}", CITATION)
        assert item.output == f"figure{suffix}"
        assert item.category == CITATION


def test_a_report_item_is_frozen() -> None:
    """Property: assigning to an item's field raises `FrozenInstanceError`.

    Class: the item record.
    Why it is required: the item is held in a frozen row of the table and read when the workflow file is
    rendered, so a mutable item would let one reader change the heading another renders.
    What kills it: dropping `frozen=True` from the dataclass.
    Second correct implementation: a named tuple; the same assignment is refused.
    """
    item = ReportItem("citation_section.html", CITATION)

    with pytest.raises(AttributeError):
        item.category = DIVERGENCE  # type: ignore[misc]


def test_a_row_refuses_an_item_for_an_output_it_does_not_declare() -> None:
    """Property: a row carrying an item for a file outside its `outputs` raises `GraphLookupError` naming both.

    Class: a row whose one reported file it does not declare.
    Why it is required: the engine flags one of a rule's outputs; an item naming anything else is a report
    entry the engine can only fail on, and it would be discovered when the report is built rather than
    where the row was written.
    What kills it: dropping the membership check, or checking the item's own output against nothing.
    Second correct implementation: an explicit resolution of every item to the row's output tuple; the same
    row is refused.
    """
    with pytest.raises(GraphLookupError) as caught:
        RuleSpec(
            name="reports_a_file_it_lacks",
            system="reporting",
            outputs=("declared.html",),
            report_items=(ReportItem("undeclared.html", CITATION),),
        )

    message = str(caught.value)
    assert "undeclared.html" in message
    assert "reports_a_file_it_lacks" in message


def test_a_row_refuses_two_items_for_one_output() -> None:
    """Property: a row carrying two items that name one output raises `GraphLookupError`.

    Class: a row with two headings claimed for the same declared output.
    Why it is required: an artifact appears in the report once, so a second item is a second statement of
    where it appears and the page would show it twice under two groups.
    What kills it: dropping the duplicate check, or comparing item objects rather than their outputs.
    Second correct implementation: building the listing and comparing its length with a set's; the same row
    is refused.
    """
    with pytest.raises(GraphLookupError):
        RuleSpec(
            name="reports_one_output_twice",
            system="reporting",
            outputs=("declared.html",),
            report_items=(
                ReportItem("declared.html", CITATION),
                ReportItem("declared.html", DIVERGENCE),
            ),
        )


@pytest.mark.parametrize("value", [{"item": "declared.html"}, "declared.html", 7], ids=["mapping", "text", "number"])
def test_a_row_refuses_a_report_item_collection_of_another_kind(value: object) -> None:
    """Property: a `report_items` value that is a mapping, a bare non-item or a number raises `GraphLookupError`.

    Class: the three kinds a hand edit produces that are not a collection of items.
    Why it is required: a mapping's values are not items and a number iterates over nothing, so a row built
    from either would silently publish nothing while its writer believed it published something.
    What kills it: storing the value as given, or iterating it without a kind check.
    Second correct implementation: a kind check before normalisation, with any accepted kind handed to the
    same per-item check; the same three values are refused.
    """
    with pytest.raises(GraphLookupError):
        RuleSpec(name="reports", system="reporting", outputs=("declared.html",), report_items=value)


def test_a_row_stores_a_bare_item_and_a_list_as_tuples() -> None:
    """Property: one item written without its trailing comma, and a list of items, both become tuples of the same items.

    Class: the two forms a one-item or hand-written collection takes.
    Why it is required: the row is frozen and rendered later, so a list stored as given would be mutable
    inside a record this module calls immutable, and a bare item would be a value no reader could iterate.
    What kills it: storing either value unchanged.
    Second correct implementation: refusing both instead of normalising; this test pins the normalising
    branch because that is the one a compute system's later edit reaches.
    """
    bare = RuleSpec(
        name="bare", system="reporting", outputs=("declared.html",), report_items=ReportItem("declared.html", CITATION)
    )
    listed = RuleSpec(
        name="listed",
        system="reporting",
        outputs=("declared.html",),
        report_items=[ReportItem("declared.html", CITATION)],
    )

    assert bare.report_items == (ReportItem("declared.html", CITATION),)
    assert listed.report_items == bare.report_items
    assert isinstance(bare.report_items, tuple) and isinstance(listed.report_items, tuple)


def test_the_graphs_one_report_item_is_the_citation_section() -> None:
    """Property: the table declares exactly one report item — the attribution row's citation section, under the citation heading.

    Class: every row of the table and every item it declares.
    Why it is required: the citation section is what the run publishes in the page today, and a second item
    would be a claim that some system publishes a figure it does not write; the two headings are constants
    because a caller groups items by them.
    What kills it: an item added to another row, the citation item removed, or either heading reworded.
    Second correct implementation: a listing computed per row, as here; the same three values result.
    """
    items = tuple((rule.name, item.output, item.category) for rule in GRAPH for item in rule.report_items)

    assert items == EXPECTED_ITEMS
    assert CITATION == "Citation"
    assert DIVERGENCE == "Divergence report"
    assert IN_PAGE_SUFFIXES == (".html", ".svg", ".png", ".jpg", ".jpeg", ".gif")


def test_the_engine_report_shows_the_citation_section_under_its_category(
    toolkit_copy: ToolkitCopy, estate: Estate, engine
) -> None:
    """Property: a full run whose engine is asked for its report produces one whose category table holds the citation section under `Citation` and nothing else.

    Class: one committed toolkit copy, its full target list, and the report the engine builds after the run.
    Why it is required: the flag travels from the row through the rendered output directive into the
    engine's report, so this is the only check that the three agree — a row declaring an item the file
    never flags, or a file flagging something no row declares, leaves the page without the section the run
    is supposed to publish.
    What kills it: rendering the outputs as bare paths, flagging every output, or naming the heading from
    the file rather than the row.
    Second correct implementation: any rendering that reads the row's items; the same one category and the
    same one path result. The engine's report build is what reads them, so the check is on its output.
    """
    write_record(estate, ALL_TARGETS)
    report = estate.run_output / "report.html"

    completed = engine(
        toolkit_copy,
        estate,
        extra_arguments=("--report", str(report), "--report-after-run"),
    )

    assert completed.returncode == 0, completed.stderr
    assert report.is_file(), completed.stdout
    categories = _report_categories(report)

    assert set(categories) == {CITATION}
    shown = [path for subcategories in categories.values() for paths in subcategories.values() for path in paths]
    assert shown == ["method_attribution/citation_section.html"]
    assert (estate.run_output / "method_attribution" / "citation_section.html").is_file()
