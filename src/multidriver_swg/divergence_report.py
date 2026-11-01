"""Report the divergences a run under the pre-refactor setting was subject to.

A run that calls the legacy side of registered pairs should end by telling its reader where the
legacy methods and the current methods differ and why. The divergence ledger holds that
knowledge; this module turns it into a structure (`build_report`), into text (`render_report`)
and onto a stream the caller chooses (`write_report`). It reads the ledger and the pair
registry and nothing else, and it does not decide when a report is written: a caller that wants
one only on a run that selected a legacy side asks the side record whether it did.

The report states four things, in this order:

- how many pairs the ledger covers and how many pairs are registered in code with both sides,
  printed side by side so a ledger that is short of the code can be seen to be short;
- each divergent pair, with the text of what changed and the reason, or an explicit statement
  that no pair diverges, which still carries the covered count so that a report over an empty
  ledger cannot be read as a report over a ledger of pairs that agree;
- the departure entries, which record deliberate departures from the legacy code that have no
  pair, each with its site and reason, under the statement that the run did not reproduce the
  legacy code at these sites; and
- when there is no departure entry, an explicit statement of that.

A departure entry of the kind `finding-4` records the name of the pair that will replace it. Once
that pair is registered with both of its sides, the pair's own ledger entry speaks for the
departure, so the report leaves the departure entry out rather than naming the same change twice.
A pair with one side registered is still being built, and its departure entry stays listed.

Entries are ordered by pair identifier, and departure entries by unit and then by line number, so
the text does not depend on the order of the ledger file. A report is refused when the ledger does
not have its form (`LedgerInvalidError`), because a report built from a malformed entry, such as
a divergent pair with no reason, would be confident and wrong.

Not established: that the registered pairs are the pairs the ledger describes. The report prints
both counts and leaves a difference between them to the divergence detector, which is the check
that stops a build on one.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import TextIO

from multidriver_swg import divergence_ledger, pairs
from multidriver_swg.divergence_ledger import Ledger

FINDING_4 = "finding-4"


class LedgerInvalidError(ValueError):
    """The ledger does not have its form, so no report can be built from it.

    `findings` holds what `divergence_ledger.validate` returned, in its order.
    """

    def __init__(self, findings: list[str]) -> None:
        self.findings = list(findings)
        super().__init__("the divergence ledger is invalid: " + "; ".join(self.findings))


@dataclass(frozen=True)
class DivergenceReport:
    """What a report says, before it is turned into text.

    `pairs_covered` counts the pair entries of the ledger and `pairs_registered` the pairs
    registered in code with both sides. `divergent` holds `(pair, changed, why)` for each pair
    entry that diverges, by pair identifier. `departures` holds `(unit, site, why)` for each
    departure entry that is not left out, by unit and then line.
    """

    pairs_covered: int
    pairs_registered: int
    divergent: tuple[tuple[str, str, str], ...]
    departures: tuple[tuple[str, int, str], ...]


def build_report(ledger: Ledger | None = None, registered: Collection[str] | None = None) -> DivergenceReport:
    """Build the report of `ledger` against the pairs in `registered`.

    A ledger of `None` is the packaged ledger. `registered` of `None` is the identifiers of the
    pairs the registry holds with both sides. Raises `LedgerInvalidError` first when the ledger
    does not have its form.
    """
    if ledger is None:
        ledger = divergence_ledger.load()
    findings = divergence_ledger.validate(ledger)
    if findings:
        raise LedgerInvalidError(findings)
    registered_ids = frozenset(_registered_with_both_sides() if registered is None else registered)
    divergent = tuple(
        (entry["id"], entry["changed"], entry["why"])
        for entry in sorted((e for e in ledger.pairs if e["diverges"]), key=lambda e: e["id"])
    )
    departures = tuple(
        (entry["unit"], entry["site"], entry["why"])
        for entry in sorted(ledger.departures, key=lambda e: (e["unit"], e["site"]))
        if not _left_out(entry, registered_ids)
    )
    return DivergenceReport(len(ledger.pairs), len(registered_ids), divergent, departures)


def render_report(report: DivergenceReport) -> str:
    """The text of `report`, one statement to a line, the same text for the same report."""
    lines = [f"Pairs covered: {report.pairs_covered}", f"Pairs registered: {report.pairs_registered}", ""]
    if report.divergent:
        lines += [
            f"Pair {pair} diverges: {_one_line(changed)} (reason: {_one_line(why)})"
            for pair, changed, why in report.divergent
        ]
    else:
        lines.append("No pair diverges.")
    lines.append("")
    if report.departures:
        lines.append(f"The run did not reproduce the legacy code at these sites ({len(report.departures)}):")
        lines += [f"{unit} line {site}: {_one_line(why)}" for unit, site, why in report.departures]
    else:
        lines.append("No departure entry is recorded.")
    return "\n".join(lines) + "\n"


def write_report(
    stream: TextIO, ledger: Ledger | None = None, registered: Collection[str] | None = None
) -> DivergenceReport:
    """Write the rendered report to `stream` and return the structure it was rendered from."""
    report = build_report(ledger, registered)
    stream.write(render_report(report))
    return report


def _registered_with_both_sides() -> frozenset[str]:
    """The identifiers the registry holds with both a legacy side and a current side, read at the call."""
    return frozenset(
        pair for pair, sides in pairs.registered_pairs().items() if all(side in sides for side in pairs.SIDES)
    )


def _left_out(entry: dict, registered: frozenset[str]) -> bool:
    """Whether a departure entry is left out: a `finding-4` entry whose pair is registered with both sides."""
    return entry["kind"] == FINDING_4 and entry["future_pair"] in registered


def _one_line(text: str) -> str:
    """`text` on one line, so that every statement of the report stays whole on its own line."""
    return " ".join(str(text).split())
