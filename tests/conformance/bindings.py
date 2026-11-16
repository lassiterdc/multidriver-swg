"""Load the binding ledger and report every defect in it and in the conformance tests it binds.

The § B2 equation register states what the manuscript says about 36 load-bearing equations; it does
not state what this repository OWES each one. That obligation is this module's subject, carried by
`bindings.toml`: one row per equation, whose `binding` is `TEST` (a conformance test computes the
equation), `CROSS-REPO` (the implementation lives in a sibling repository), `UNREACHABLE` (the code
exists and the published run cannot reach it), or `NO-MANUSCRIPT-TEXT` (the manuscript states no
equation, so no test can compute one). Without the ledger an equation nobody has tested yet and an
equation that owes no test read the same in a green suite.

WHY THE LANDED `Binding` AND THE REACHABILITY FLAG ARE DERIVED AND NEVER STORED. A row's `binding`
word already fixes both facts: `CROSS-REPO` is the one binding the status vocabulary publishes as
`CROSS_REPO` and `NO-MANUSCRIPT-TEXT` the one it publishes as `NO_MANUSCRIPT_TEXT`, and reachability
is false exactly for `UNREACHABLE`. Writing either one down beside the word creates a second home
for a fact that can then be stated two ways — and the disagreement would surface at a consumer that
believes the copy and not the word. So the ledger refuses a row carrying either key, and
`binding_of`/`reachable_of` compute them from the one place the fact is written.

WHY THE LEDGER IS TOML AND NOT JSON BESIDE `register.json`. The register is generated from the
manuscript and is machine-shaped; the ledger is hand-maintained and every row carries a prose ground
quoting the § B2 Status fragment that decided it. The repository's other hand-maintained ledgers are
TOML, so a reader meets one format rather than two.

Not established: that a row's `binding` is the RIGHT classification. That is a judgement about the
paper and the code, decided by the ground the row quotes and reviewed against § B2; this module only
establishes that the row is well formed and that the tests it names exist, exactly once each.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

from multidriver_swg.conformance.status import manuscript_binding
from multidriver_swg.conformance.taxonomy import Binding
from tests.conformance.register import EXPECTED_IDS
from tests.conformance.register import entries as register_entries

LEDGER_PATH = Path(__file__).with_name("bindings.toml")

# The closed vocabulary of the ledger's own `binding` word. The values are the WIRE spellings and
# not `Binding` members, because two of the four (`TEST` and `UNREACHABLE`) publish a landed binding
# this table derives; a `Binding` here would put the derived value in the ledger and buy the second
# home the module docstring refuses.
BINDING_VALUES = ("TEST", "CROSS-REPO", "UNREACHABLE", "NO-MANUSCRIPT-TEXT")

REQUIRED_FIELDS = ("binding", "ground")
TEST_FIELD = "test"

# `Binding.CROSS_REPO` and `Binding.NO_MANUSCRIPT_TEXT` are the two values the status vocabulary
# publishes for two of the wire words; every other word lands as `BOUND`, which is why the table is
# keyed by the wire word rather than derived by a chain of ifs a reader has to follow.
_LANDED = {
    "TEST": Binding.BOUND,
    "CROSS-REPO": Binding.CROSS_REPO,
    "UNREACHABLE": Binding.BOUND,
    "NO-MANUSCRIPT-TEXT": Binding.NO_MANUSCRIPT_TEXT,
}

# EQUATION-LIKE is a property over the grammar of a unit name, deliberately not a list of the
# equation ids: a typo such as `E4` or `e-04` then draws a finding, and a component unit that a later
# change adds needs no edit here. The pattern admits any case and an optional dash, because the
# defect it exists to catch is a marker that MEANT an equation and spelled it another way.
EQUATION_LIKE = re.compile(r"[Ee]-?\d+")

# Every conformance test lives under this directory, so a `TEST` row's test outside it is a test the
# family's own selection cannot reach.
TEST_FILE_PREFIX = "tests/conformance/"


class LedgerError(Exception):
    """Base class for every refusal this module raises, so a caller can catch the family."""


class LedgerValidationError(LedgerError):
    """The ledger or a derived fact does not satisfy the schema, or a row contradicts the register."""


def load(path: Path = LEDGER_PATH) -> dict[str, dict]:
    """Parse and validate the ledger at `path`, returning its rows keyed by equation id.

    Each returned value holds `binding` and `ground`, plus `test` on a `TEST` row — the ledger's own
    keys and nothing derived, so a caller that wants the landed facts calls `binding_of` or
    `reachable_of` and reads one answer rather than two.

    Deliberately NOT cached. `register` caches because every test and the drift check re-read one
    fixed file; this loader is also driven over synthetic ledgers under `tmp_path`, and a cache
    keyed on the path would keep a fixture's result alive for a later call in the same session. A
    36-table TOML parse is cheap enough that correctness wins outright.
    """
    source = Path(path)
    try:
        parsed = tomllib.loads(source.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise LedgerValidationError(f"{source.name}: not valid TOML ({exc})") from exc

    missing_ids = [equation_id for equation_id in EXPECTED_IDS if equation_id not in parsed]
    extra_ids = [str(key) for key in parsed if key not in EXPECTED_IDS]
    if missing_ids or extra_ids:
        raise LedgerValidationError(
            f"{source.name}: the ledger must carry exactly the register's {len(EXPECTED_IDS)} equations "
            f"{EXPECTED_IDS[0]}..{EXPECTED_IDS[-1]}"
            + (f"; missing {', '.join(missing_ids)}" if missing_ids else "")
            + (f"; unexpected {', '.join(extra_ids)}" if extra_ids else "")
        )

    register = register_entries()
    rows: dict[str, dict] = {}
    for equation_id in EXPECTED_IDS:
        rows[equation_id] = _validate_row(source.name, equation_id, parsed[equation_id], register[equation_id])
    return rows


def binding_of(row: dict) -> Binding:
    """The landed `Binding` one ledger row decides, or a refusal naming the value it cannot place."""
    value = _binding_word(row)
    return _LANDED[value]


def reachable_of(row: dict) -> bool:
    """Whether one ledger row's implementation is reachable: false exactly for `UNREACHABLE`.

    The comparison runs against the WIRE word rather than against `binding_of(row)` because
    `UNREACHABLE` and `TEST` both land as `Binding.BOUND` — the reachability fact lives in the word
    the ledger writes, and reading the derived binding here would answer `True` for every row.
    """
    return _binding_word(row) != "UNREACHABLE"


def coverage_findings(ledger: dict[str, dict], register_ids, entries) -> list[str]:
    """Return one finding per defect, over the ledger, the register's ids and the conformance listing.

    The five checks, in the order below:

    1. the ledger's ids are the register's — every equation carries exactly one binding;
    2. each `TEST` row has exactly one conformance entry whose `unit` is the id and whose `function`
       is the row's `test`, in a file under `tests/conformance/`;
    3. no non-`TEST` row has an entry;
    4. every entry whose `unit` is EQUATION-LIKE is a `TEST` row's id, with that row's `test` name;
    5. every unreadable-marker finding of `inventory.listing` is a finding here.

    `entries` is `inventory.listing`'s return value, the `(entries, findings)` pair, because check 5
    reports the findings that travel in its second element. A caller that has already reduced the
    pair to its entry list is REFUSED rather than silently read: a list carries no unreadable-marker
    findings, and reading it as "there were none" would retire check 5 while leaving its population
    unexamined — the failure this function exists to catch, one level up. Entries of families other
    than `conformance` are ignored here as well, so a caller may pass the pair unfiltered.

    Checks 2 and 4 can both name one defect when a `TEST` row's named test disagrees with the listed
    function. That duplication is deliberate: each check is stated over its own population, and a
    finding suppressed for being a duplicate would make one of the two checks unable to report.
    """
    listed, unreadable = _listing_parts(entries)
    conformance = [entry for entry in listed if entry.get("family") == "conformance"]
    register = list(register_ids)
    register_set = set(register)
    findings: list[str] = []

    for equation_id in register:
        if equation_id not in ledger:
            findings.append(f"{equation_id}: the register carries this equation and the ledger carries no binding")
    for equation_id in ledger:
        if equation_id not in register_set:
            findings.append(f"{equation_id}: the ledger carries this id and the register does not")

    for equation_id, row in ledger.items():
        matches = [entry for entry in conformance if entry["unit"] == equation_id]
        if row["binding"] == "TEST":
            if len(matches) != 1:
                where = ", ".join(f"{entry['file']}::{entry['function']}" for entry in matches) or "none"
                findings.append(
                    f"{equation_id}: binding TEST needs exactly one conformance test carrying unit="
                    f"{equation_id!r}; found {len(matches)} ({where})"
                )
                continue
            entry = matches[0]
            if entry["function"] != row["test"]:
                findings.append(
                    f"{equation_id}: the row names test {row['test']!r} and "
                    f"{entry['file']}::{entry['function']} is listed against it"
                )
            elif not entry["file"].startswith(TEST_FILE_PREFIX):
                findings.append(
                    f"{equation_id}: {entry['file']}::{entry['function']} is listed against it and lives "
                    f"outside {TEST_FILE_PREFIX}"
                )
        elif matches:
            where = ", ".join(f"{entry['file']}::{entry['function']}" for entry in matches)
            findings.append(f"{equation_id}: binding {row['binding']} carries no test and {where} is listed against it")

    for entry in conformance:
        unit = entry["unit"]
        if not isinstance(unit, str) or EQUATION_LIKE.fullmatch(unit) is None:
            continue
        where = f"{entry['file']}::{entry['function']}"
        row = ledger.get(unit)
        if row is None:
            findings.append(f"{where} names unit {unit!r}, which no ledger row carries")
        elif row["binding"] != "TEST":
            findings.append(f"{where} names unit {unit!r}, whose binding is {row['binding']}")
        elif entry["function"] != row.get("test"):
            findings.append(f"{where} names unit {unit!r} and the row names test {row.get('test')!r}")

    findings.extend(unreadable)
    return findings


def _validate_row(source: str, equation_id: str, value: object, register_entry: dict) -> dict:
    """Validate one ledger row against the schema and the register, returning its own keys only.

    The refusals run in a fixed order — shape, field set, field values, manuscript agreement — so a
    caller repairing a row learns the earliest thing wrong with it rather than an arbitrary one.
    """
    where = f"{source}: {equation_id}"
    if not isinstance(value, dict):
        raise LedgerValidationError(f"{where} must be a table; found {type(value).__name__}")

    if "binding" not in value:
        raise LedgerValidationError(
            f"{where} carries no `binding`; every row states one of {', '.join(BINDING_VALUES)}"
        )
    binding = value["binding"]
    if binding not in BINDING_VALUES:
        raise LedgerValidationError(
            f"{where} binding is {binding!r}, which is outside the closed set {', '.join(BINDING_VALUES)}"
        )

    allowed = (*REQUIRED_FIELDS, TEST_FIELD) if binding == "TEST" else REQUIRED_FIELDS
    missing_fields = [field for field in allowed if field not in value]
    extra_fields = [str(field) for field in value if field not in allowed]
    if missing_fields or extra_fields:
        raise LedgerValidationError(
            f"{where} with binding {binding} must carry exactly {', '.join(allowed)}"
            + (f"; missing {', '.join(missing_fields)}" if missing_fields else "")
            + (f"; unexpected {', '.join(extra_fields)}" if extra_fields else "")
            + " (the landed binding and reachability are derived from `binding` and are never written)"
        )

    ground = value["ground"]
    if not isinstance(ground, str) or not ground.strip():
        raise LedgerValidationError(
            f"{where} ground must be the § B2 Status fragment that decides the row; found blank"
        )
    if binding == "TEST":
        test = value[TEST_FIELD]
        if not isinstance(test, str) or not test.strip():
            raise LedgerValidationError(f"{where} names no test function; a TEST row names one")

    # The manuscript span decides one binding and only one, so a row may disagree with the register
    # in two directions and both are refusals: an equation the manuscript never states cannot owe a
    # test, and an equation it states cannot be classified as text-less.
    decided = manuscript_binding(register_entry["verbatim_span"])
    if decided is not None and binding != decided.value:
        raise LedgerValidationError(
            f"{where} binding is {binding!r}, but the register's verbatim_span is empty, which decides "
            f"{decided.value!r} and nothing else"
        )
    if decided is None and binding == Binding.NO_MANUSCRIPT_TEXT.value:
        raise LedgerValidationError(
            f"{where} binding is {Binding.NO_MANUSCRIPT_TEXT.value!r}, but the register carries a verbatim span, "
            "so the manuscript does state this equation"
        )

    row = {"binding": binding, "ground": ground}
    if binding == "TEST":
        row[TEST_FIELD] = value[TEST_FIELD]
    return row


def _binding_word(row: dict) -> str:
    """One row's wire `binding` word, refusing a row that carries none or one outside the closed set."""
    binding = row.get("binding")
    if binding not in BINDING_VALUES:
        raise LedgerValidationError(
            f"a ledger row's binding is {binding!r}, which is outside the closed set {', '.join(BINDING_VALUES)}"
        )
    return binding


def _listing_parts(entries) -> tuple[list[dict], list[str]]:
    """Split `inventory.listing`'s pair into its entries and its unreadable-marker findings.

    A bare entry list is refused with the shape it must be passed in, because the zero the fifth
    check would report over it is indistinguishable from a population nobody read.
    """
    if not isinstance(entries, tuple) or len(entries) != 2:
        raise LedgerValidationError(
            "entries must be `inventory.listing`'s (entries, findings) pair; a bare entry list carries no "
            "unreadable-marker findings and would retire the fifth coverage check silently"
        )
    listed, findings = entries
    return list(listed), [str(finding) for finding in findings]
