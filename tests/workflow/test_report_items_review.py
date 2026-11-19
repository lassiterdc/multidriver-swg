"""Check that a report-item collection cannot smuggle an invalid member into a frozen rule.

A collection can have the right outer kind while containing something other than a report item.
Such a value must be refused at construction, before the workflow renderer reads its attributes.
"""

from __future__ import annotations

import pytest

from multidriver_swg.workflow.graph import GraphLookupError, RuleSpec
from multidriver_swg.workflow.report_items import CITATION, ReportItem


@pytest.mark.parametrize("invalid", [None, "figure.html", {"output": "figure.html", "category": CITATION}])
@pytest.mark.parametrize("collection_type", [tuple, list])
def test_a_valid_collection_refuses_a_non_item_member(invalid: object, collection_type: type) -> None:
    """Property: a tuple or list containing a non-item raises the graph's own refusal.

    Class: ordered report-item collections with one valid member followed by a non-item: a missing
    value, a filename, or a mapping that resembles the record but is not one.
    Why it is required: the renderer reads each member's output and category; accepting these values
    defers the error until rendering or lets an unvalidated mapping bypass the item conventions.
    What kills it: dropping the member type check, or checking only the collection's first member.
    Second correct implementation: validate all members before tuple conversion, rather than while
    collecting them; the same invalid collections raise GraphLookupError.
    """
    valid = ReportItem("figure.html", CITATION)
    members = collection_type((valid, invalid))

    with pytest.raises(GraphLookupError):
        RuleSpec(name="figure", system="reporting", outputs=("figure.html",), report_items=members)

    control = RuleSpec(
        name="figure", system="reporting", outputs=("figure.html",), report_items=collection_type((valid,))
    )
    assert control.report_items == (valid,)
