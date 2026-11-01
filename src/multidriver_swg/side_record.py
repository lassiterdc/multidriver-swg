"""State, for one run, which side of each registered pair was selected, in a form a run's records can carry.

A run that may call a legacy function instead of its replacement has to say, in what it produces,
which function it chose for every pair. This module defines what that statement contains and
builds it from a side selection and the live pair registry. It stores nothing, writes nothing and
reads no setting; the record that carries the statement belongs to the code that records a run.

The content of a side record is these six rules:

The record maps each pair identifier, exactly as registered, to the side selected for that pair, `pre_refactor` or `current`.

The record has one entry for every registered pair, including each pair selected on the current side; a pair with no entry was not registered.

Entries are ordered by pair identifier, so two records of equal content serialize identically.

A record with no entries is valid and is carried as empty; a run whose provenance carries no side record is a run that selected no sides, which is a different fact from a run with no pairs.

The record declares the side selected for each pair; it does not attest that a pair's function was called.

The provenance record carries the mapping unaltered and infers no side from any other field.

A provenance record that carries this content carries the result of `SideRecord.as_mapping()`
unaltered: the same keys, the same sides, in the same order.

`side_record` builds the record from the live registry at the moment the side is selected, so the
record is captured while the run happens and is never rebuilt later from the setting that chose
the sides. A rebuilt record would describe the registry as it is then, not as it was when the run
chose.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from multidriver_swg import pairs
from multidriver_swg.dispatch import SideSelection, selected_sides


@dataclass(frozen=True)
class SideRecord:
    """The side selected for each registered pair of one run, as a read-only mapping ordered by pair identifier.

    The mapping passed in is copied, sorted by identifier and made read-only, so a caller
    changing its own dict afterwards cannot change a record that a run has already carried.
    The record states which side was selected; it does not check the sides against the pair
    registry, which `side_record` does by building it through `selected_sides`.
    """

    sides: Mapping[str, str]

    def __post_init__(self) -> None:
        """Replace `sides` with a read-only copy of it ordered by pair identifier."""
        object.__setattr__(self, "sides", MappingProxyType(dict(sorted(self.sides.items()))))

    def as_mapping(self) -> dict[str, str]:
        """A fresh plain dict of pair identifier to side, in identifier order, that the caller may change freely.

        This is the value a provenance record carries, and it carries it unaltered.
        """
        return dict(self.sides)

    @property
    def ran_pre_refactor(self) -> bool:
        """True when at least one pair was selected on its `pre_refactor` side.

        A run with no pairs, and a run whose every pair was selected on the current side, both
        read False: neither used a legacy function.
        """
        return any(side == pairs.PRE_REFACTOR for side in self.sides.values())


def side_record(selection: SideSelection) -> SideRecord:
    """Build the record of `selection` over the pairs registered now.

    Refuses what `selected_sides` refuses: an override naming a pair that is not registered, and
    a selected side that has no registered function. A side is therefore never recorded that
    could not have been called.
    """
    return SideRecord(selected_sides(selection))
