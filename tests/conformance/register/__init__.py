"""Load and validate the checked-in equation register mirror.

The register is the executable form of the manuscript's Appendix B § B2: one entry per
load-bearing equation, each carrying the equation's verbatim LaTeX span, the manuscript
section it lives in, the re-derived line range of that span, and the raw divergence notes
§ B2 records for it. Everything downstream reads this package rather than the JSON file
directly, so that the shape of an entry is decided in ONE place: a conformance test that
later trusts `line_range` or `defect_list` is trusting the schema enforced here.

The validation is deliberately strict about the entry's FIELD SET, not only its types.
An entry that gained a field would be an entry whose author believed something the schema
does not carry, and a silent extra key is exactly the drift this package exists to catch.
The reverse direction matters more: an entry that LOST a field would otherwise read as
absent data at every call site instead of as a broken register.

Two rules are not type checks and are enforced here because no call site can enforce them:
an entry whose `verbatim_span` is empty records an equation the manuscript never states, so
its `line_range` must be `null` (there is no span to bound) and its `defect_list` must be
non-empty (an empty note for an absent equation records nothing).
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

REGISTER_PATH = Path(__file__).with_name("register.json")

EXPECTED_EQUATION_COUNT = 36
EXPECTED_IDS = tuple(f"E-{number:02d}" for number in range(1, EXPECTED_EQUATION_COUNT + 1))
EXPECTED_FIELDS = ("section_name", "line_range", "verbatim_span", "defect_list")


class RegisterError(Exception):
    """Base class for every refusal this package raises.

    A named base class rather than a bare exception so a caller can distinguish "the
    register is not usable" from any other failure, including a JSON decoding error.
    """


class RegisterValidationError(RegisterError):
    """The register's content does not satisfy the entry schema.

    The message names the offending entry (its equation ID) or, when the failure is at the
    top level, the ids or count that are wrong. A validation failure that did not name its
    subject would force every reader to bisect a 36-entry file by hand.
    """


class UnknownEquationError(RegisterError):
    """An equation ID outside `E-01`..`E-36` was asked for."""


def validate_register(register: object, *, source: str = "register") -> dict:
    """Validate one parsed register and return it unchanged.

    `source` names the document in every message, because the same schema is applied to the
    file on disk and to the in-memory fixtures the refusal tests drive.

    Deliberately NOT a schema library: the schema is four fields and two cross-field rules,
    and expressing it here keeps the refusal message free to name the entry.
    """
    if not isinstance(register, dict):
        raise RegisterValidationError(
            f"{source}: a register must be a JSON object keyed by equation ID; found {type(register).__name__}"
        )

    missing_ids = [equation_id for equation_id in EXPECTED_IDS if equation_id not in register]
    extra_ids = [str(key) for key in register if key not in EXPECTED_IDS]
    if len(register) != EXPECTED_EQUATION_COUNT or missing_ids or extra_ids:
        raise RegisterValidationError(
            f"{source}: a register must carry exactly {EXPECTED_EQUATION_COUNT} entries keyed "
            f"{EXPECTED_IDS[0]}..{EXPECTED_IDS[-1]}; found {len(register)}"
            + (f"; missing {', '.join(missing_ids)}" if missing_ids else "")
            + (f"; unexpected {', '.join(extra_ids)}" if extra_ids else "")
        )

    for equation_id in EXPECTED_IDS:
        _validate_entry(equation_id, register[equation_id], source=source)
    return register


def entries() -> dict:
    """Return the validated register, keyed by equation ID.

    The result is a deep copy: two callers sharing one mutable dict would make a test's
    in-place edit visible to the next caller, and that failure reads as a register defect
    rather than as aliasing.
    """
    return copy.deepcopy(_load())


def entry(equation_id: str) -> dict:
    """Return one validated register entry, or raise `UnknownEquationError` naming the id."""
    register = _load()
    try:
        found = register[equation_id]
    except KeyError:
        raise UnknownEquationError(
            f"no register entry for {equation_id!r}; the register is keyed {EXPECTED_IDS[0]}..{EXPECTED_IDS[-1]}"
        ) from None
    return copy.deepcopy(found)


def _validate_entry(equation_id: str, entry_value: object, *, source: str) -> None:
    """Validate one entry of an already key-checked register, naming the entry on refusal."""
    where = f"{source}: entry {equation_id}"
    if not isinstance(entry_value, dict):
        raise RegisterValidationError(f"{where} must be a JSON object; found {type(entry_value).__name__}")

    missing_fields = [field for field in EXPECTED_FIELDS if field not in entry_value]
    extra_fields = [str(field) for field in entry_value if field not in EXPECTED_FIELDS]
    if missing_fields or extra_fields:
        raise RegisterValidationError(
            where
            + " must carry exactly "
            + ", ".join(EXPECTED_FIELDS)
            + (f"; missing {', '.join(missing_fields)}" if missing_fields else "")
            + (f"; unexpected {', '.join(extra_fields)}" if extra_fields else "")
        )

    if not isinstance(entry_value["section_name"], str):
        raise RegisterValidationError(
            f"{where} section_name must be a string; found {_type_of(entry_value['section_name'])}"
        )
    if not isinstance(entry_value["verbatim_span"], str):
        raise RegisterValidationError(
            f"{where} verbatim_span must be a string; found {_type_of(entry_value['verbatim_span'])}"
        )
    line_range = entry_value["line_range"]
    if line_range is not None and not _is_line_range(line_range):
        raise RegisterValidationError(f"{where} line_range must be a two-integer list or null; found {line_range!r}")
    defect_list = entry_value["defect_list"]
    if not isinstance(defect_list, list) or not all(isinstance(note, str) for note in defect_list):
        raise RegisterValidationError(f"{where} defect_list must be a list of strings; found {_type_of(defect_list)}")

    if entry_value["verbatim_span"] == "":
        if line_range is not None:
            raise RegisterValidationError(
                f"{where} has an empty verbatim_span, so line_range must be null; found {line_range!r}"
            )
        if not defect_list:
            raise RegisterValidationError(
                f"{where} has an empty verbatim_span, so defect_list must record why the manuscript "
                "states no equation; found an empty list"
            )


def _is_line_range(value: object) -> bool:
    """True when `value` is a two-integer list. `bool` is excluded: it is an `int` subclass."""
    return (
        isinstance(value, list)
        and len(value) == 2
        and all(isinstance(bound, int) and not isinstance(bound, bool) for bound in value)
    )


def _type_of(value: object) -> str:
    """The value's type name, for a refusal message that says what was found."""
    return type(value).__name__


def _load() -> dict:
    """Parse and validate the register once, then reuse the validated result.

    Cached because every conformance test and the drift check read it, and re-parsing a
    20 KB file per call would make the drift check's cost scale with the register's size
    for no gain. A failed load is NOT cached: the first refusal is raised and the next
    caller retries, so a file repaired between calls is seen.
    """
    global _VALIDATED
    if _VALIDATED is None:
        try:
            parsed = json.loads(REGISTER_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RegisterValidationError(f"{REGISTER_PATH.name}: not valid JSON ({exc})") from exc
        _VALIDATED = validate_register(parsed, source=REGISTER_PATH.name)
    return _VALIDATED


_VALIDATED: dict | None = None
