"""The run's stop: what the licence requires, which of it the selection lacks, and what a stopped run still builds.

The run's one checkpoint is the licence row. After it has run, the workflow file asks three questions of
the artifact it wrote and of the run's selection, and this module answers each as a pure function:

- `required_entries` reads the licence document and returns the entries a later stage needs. An entry is
  a fit's event class and statistic, `"{event_class}.{statistic}"`, because that pair is the unit the
  selection artifact names a fit by; the licence's output form beyond that list is the owning system's to
  decide when its bodies are written, and only the stub's form is fixed here.
- `missing_entries` returns the required entries for which the selection holds no fit.
- `stop_targets` returns the run's target list with the outputs of every row downstream of the checkpoint
  removed, which is what a stopped run builds.

The cut uses `downstream_of`, the same ancestry the pre-run step uses for a run with no selection, rather
than "every row at or before the checkpoint in table order". Table order is a proxy for ancestry that
breaks the first time a row independent of the licence is placed early, and it would drop
`method_attribution`, the last row, so a stopped run's report would lack the citation section. That row's
reads are computed from the target list, so `downstream_of` does not reach it and the cut leaves it in.

The module imports only the graph and the selection artifact. It reads no file and writes none: the
workflow file loads the licence and the selection and passes them in, so every function here can be
driven without an estate.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from multidriver_swg.run_configuration.selection_artifact import SelectionArtifact
from multidriver_swg.workflow.graph import GRAPH, downstream_of

# The licence artifact's file name inside its rule's directory. The graph row declares the same name as
# its output; the constant is the file's name for the readers of the document, and a test states that
# the two agree.
LICENCE_FILE = "licence.json"

# The one key a licence document carries.
REQUIRED_ENTRIES_KEY = "required_entries"


class RulingStopError(Exception):
    """The licence document is not the form this toolkit reads: a key set, a value kind or an entry that cannot mean a requirement.

    The message names the key or the entry, because the repair is the licence's body and a message that
    only says the form is wrong sends the reader to search the document.
    """


def required_entries(document: Mapping[str, object]) -> tuple[str, ...]:
    """Return the entries the licence document requires, in the order it lists them.

    The document carries exactly one key, `required_entries`, a list of `"{event_class}.{statistic}"`
    strings. Any other key set, a value that is not a list, or an entry that is not two non-empty
    dot-separated parts is refused with `RulingStopError` naming the key or the entry: a requirement the
    reader cannot parse would otherwise read as "nothing is required" or as an entry no selection can
    satisfy, and either would make the stop's decision about the wrong thing.

    Two parts means exactly one dot. An event class or statistic that itself contains a dot would be read
    as three parts and refused, rather than split at a guessed position.
    """
    if not isinstance(document, Mapping):
        raise RulingStopError(
            f"the licence document is {type(document).__name__}, not a mapping with one key {REQUIRED_ENTRIES_KEY!r}"
        )
    if set(document) != {REQUIRED_ENTRIES_KEY}:
        raise RulingStopError(
            f"the licence document's keys are {sorted(map(str, document))!r}: the form carries exactly one key, {REQUIRED_ENTRIES_KEY!r}"
        )
    entries = document[REQUIRED_ENTRIES_KEY]
    if not isinstance(entries, list):
        raise RulingStopError(
            f"the licence key {REQUIRED_ENTRIES_KEY!r} is {type(entries).__name__}: it is a list of "
            "'{event_class}.{statistic}' strings"
        )
    for entry in entries:
        _require_entry(entry)
    return tuple(entries)


def _require_entry(entry: object) -> None:
    """Refuse an entry that is not two non-empty dot-separated parts of text."""
    if not isinstance(entry, str):
        raise RulingStopError(f"the licence entry {entry!r} is not text: an entry is '{{event_class}}.{{statistic}}'")
    parts = entry.split(".")
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise RulingStopError(
            f"the licence entry {entry!r} is not two non-empty dot-separated parts: an entry is "
            "'{event_class}.{statistic}'"
        )


def missing_entries(required: Iterable[str], selection: SelectionArtifact | None) -> tuple[str, ...]:
    """Return the required entries for which the selection holds no fit, in the order given.

    An entry is met when some `FitIdentifier` of `selection.fits` has that event class and statistic,
    whatever its distribution family or transformation chain: the licence names what a stage needs a fit
    OF, and which family the user chose is the selection's content. With no selection every entry is
    missing, which is what a run that has no recorded selection must report.
    """
    if selection is None:
        return tuple(required)
    held = {f"{fit.event_class}.{fit.statistic}" for fit in selection.fits}
    return tuple(entry for entry in required if entry not in held)


def stop_targets(targets: Iterable[str]) -> tuple[str, ...]:
    """Return `targets` minus every target whose rule directory belongs to a row downstream of the checkpoint row.

    A target is `"{rule}/{output}"`, so the rule directory is the text before the first slash; a target
    with no slash names no rule and is kept. The order of the kept targets is the order given, which
    keeps the end-of-run record last where the pre-run step put it.
    """
    cut = {rule.name for rule in downstream_of(_checkpoint_row())}
    return tuple(target for target in targets if target.split("/", 1)[0] not in cut)


def _checkpoint_row() -> str:
    """Return the name of the table's one checkpoint row.

    Raises `RulingStopError` when the table has no checkpoint row or more than one: the stop is defined
    against exactly one, and a table edited to hold two would otherwise stop at whichever came first.
    """
    names = [rule.name for rule in GRAPH if rule.checkpoint]
    if len(names) != 1:
        raise RulingStopError(
            f"the rule table has {len(names)} checkpoint rows ({names!r}): the stop needs exactly one"
        )
    return names[0]
