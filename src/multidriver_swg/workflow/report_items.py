"""The report-item conventions: which declared artifacts the run's report shows, and under what heading.

A report is only as good as the set of artifacts its rules mark for it, and a marking made at each call
site would be a list of what the run publishes in as many places as the run has rules. This module states
the conventions once: a `ReportItem` names a DECLARED OUTPUT of a rule and the heading the report shows it
under, the two headings this run publishes are constants here, and the one requirement an item must satisfy
to appear in the page — that its file is one a browser can display — is refused when the item is built
rather than discovered when the report is written.

The admitted suffixes are the page-native forms. An artifact meant to be DOWNLOADED rather than read in the
page is therefore refused instead of appearing as a link nobody can render, and a caller that has such an
artifact changes this list deliberately: a special case at one call site would be the second convention
this module exists to prevent.

The module imports nothing from the rule table, so the table can import it and the dependency runs one way:
the row carries its items, and this module states what an item is.
"""

from __future__ import annotations

from dataclasses import dataclass

# The heading the citation section appears under: the run's account of the methods it used.
CITATION = "Citation"

# The heading a divergence report appears under: the record a pre-refactor side leaves when a pair is
# carried only to show the two sides agree.
DIVERGENCE = "Divergence report"

# The suffixes a file can carry and still be shown IN the page: images and HTML. The suffix is compared
# literally, with its case, because the file name a rule declares is the file name the engine records.
IN_PAGE_SUFFIXES = (".html", ".svg", ".png", ".jpg", ".jpeg", ".gif")


class ReportItemError(Exception):
    """A report item the conventions refuse: a heading no reader would see, or an output the page cannot show.

    Deliberately NOT `ValueError`: the message is the repair. It names the offending output, the suffix the
    conventions read from it and the suffixes they admit, because a reader almost always holds a mistyped
    extension or an empty heading and needs the value the convention expects.
    """


@dataclass(frozen=True)
class ReportItem:
    """One declared output the run's report shows, under the heading `category`.

    - `output` — the file name, as the rule declares it in its `outputs`. It is a name rather than a path,
      because the path is the rule's own directory and the engine composes it from the rule the item is
      attached to; an item carrying a path could name a file its rule never declares.
    - `category` — the heading the report groups the item under. A heading is text a reader sees, so an
      empty one is refused rather than rendered as a blank group.

    Frozen, so a table a consumer holds cannot be edited into a report no reader checked, and validated at
    construction, so an item the page cannot display is refused where it is written and not at report time.
    """

    output: str
    category: str

    def __post_init__(self) -> None:
        """Refuse an item with no heading, or whose output is not a page-native file."""
        if not isinstance(self.category, str) or not self.category.strip():
            raise ReportItemError(
                f"report item {self.output!r} carries the category {self.category!r}: an item with no heading is "
                "one the report would show under no name"
            )
        if not isinstance(self.output, str):
            raise ReportItemError(
                f"report item output {self.output!r} is not text: an item names a declared output, so its output "
                "is a file name"
            )
        suffix = _suffix(self.output)
        if suffix not in IN_PAGE_SUFFIXES:
            raise ReportItemError(
                f"report item {self.output!r} has the suffix {suffix!r}, which is not one of "
                + ", ".join(IN_PAGE_SUFFIXES)
                + ": every item meant to be read in the page is an image or an HTML file"
            )


def _suffix(name: str) -> str:
    """Return `name`'s extension, dot included, or the empty text when it carries none.

    String methods rather than a path library: this module imports nothing beyond `dataclasses`, which is
    what keeps reading the table a data-only act. The LAST dot decides, so a stem that carries one keeps its
    extension, and a name whose only dot is its first character has none: `.hidden` is a stem, not a suffix.
    """
    dot = name.rfind(".")
    return name[dot:] if dot > 0 else ""
