"""Append one record line per call, and count the bytes the filesystem actually accepted.

The correspondence harness writes three record channels. Only the proof record is appended to
across runs; the run record and the result record are both truncated at session start. That makes
exactly one of the three a SHARED write root, and this module owns the append so the guarantee
below is stated, and tested, once.

The declaration this module carries: **the proof-record append channel is this member's one shared
write root**; every other root an invocation writes is private to that invocation's context. The
run record and the result record are private because their session-start truncation is not
atomic-safe -- two invocations sharing one would each re-serialize the whole object from their own
partial state, and whichever wrote last would erase the other's without saying so.

The module's second half answers a different question about the same kind of path: which filesystem
class a record path falls in, and whether that class is one the append above may write to at all. A
refused path is REFUSED rather than downgraded -- falling back to a private write, or warning and
proceeding, would each let a run claim a shared root that the predicate has just said it cannot
make safe.

Two properties the append has to have, each stated with the failure it prevents:

- One raw write whose returned count is compared to the encoded payload length. Deliberately NOT a
  buffered text handle: `Path.open("a")` returns a `TextIOWrapper`, whose `write` either accepts
  the whole string or raises, so a short count is not observable through it at all and the
  comparison could not be made. A raw `FileIO` returns the count it landed, which is what makes
  the comparison possible rather than decorative.
- The accepted bytes are rolled back when that count falls short. A short write leaves the accepted
  prefix in the file, and when the withheld byte is the trailing newline the truncated line still
  parses as JSON, so the corruption surfaces only when the NEXT append concatenates onto it.
  Truncating back to the pre-write position is what makes "fails loudly instead of leaving a
  partial line" true of the file rather than only of the exception.

The payload is bounded below `MAX_RECORD_LINE` bytes because a single write is what keeps the
append safe without a lock, and the bound is on the ENCODED length rather than on `len(str)`: a
non-ASCII character encodes to two or more bytes, so a line whose character count is under the
bound can still exceed it in bytes. `ensure_ascii=False` is load-bearing for that reason -- with
the default the JSON text is pure ASCII and the two measures never diverge, so a bound on
characters and a bound on bytes would accept and refuse exactly the same lines and the sentence
above would have no input that tests it.

Deliberately NOT raised as pytest's usage error from here: the usage channel belongs to the caller
that knows which option's path it was handed, and only a payload refusal is an invoker's mistake.
A short count is an environment failure and is never translated into that channel, because the
usage message would blame the invoker for it.
"""

from __future__ import annotations

import json
import os
from io import FileIO
from pathlib import Path

# The largest encoded record line this append writes, in bytes. Measured over the inventory's 128
# units and the longest site the checkout can name -- a 94-character path with a line number -- the
# widest line is 260 bytes, so the bound leaves 15.8x headroom while staying inside what one write
# lands without a partial line. The exception typename is the one field that can raise that figure
# (the line is 246 bytes plus its length), and it is recorded verbatim from whatever a test raised.
MAX_RECORD_LINE = 4096


class WriteRootError(Exception):
    """A record line that could not be written as one complete, bounded write."""


# Deliberately NOT renamed to earn the `Error` suffix ruff's N818 asks for: both names are the
# recorder's specified interface, which its tests and its translator are written against, so the
# suffix would rename an interface to satisfy a style rule rather than a caller.
class RecordLineTooLong(WriteRootError):  # noqa: N818
    """The encoded line is not below the bound, so it was refused before any file was opened."""

    def __init__(self, length: int, bound: int) -> None:
        """Carry the measured length and the bound, so a caller reports them rather than re-deriving them."""
        self.length = length
        self.bound = bound
        super().__init__(f"record line {length} bytes is not below the {bound}-byte bound")


# The same suppression as `RecordLineTooLong`, for the same reason.
class ShortRecordWrite(WriteRootError):  # noqa: N818
    """The filesystem accepted fewer bytes than the encoded line, and the file was rolled back."""

    def __init__(self, written: int, expected: int) -> None:
        """Carry both counts, so a caller reports what landed beside what was asked for."""
        self.written = written
        self.expected = expected
        super().__init__(f"record write accepted {written} of {expected} bytes")


def append_line(path: Path, line: dict) -> None:
    """Append `line` to `path` as one JSON line, or raise without leaving a partial line behind.

    Raises `RecordLineTooLong` before the file is opened when the encoded line reaches the bound,
    so an over-long line leaves no file at the path; and `ShortRecordWrite` after truncating the
    file back to its pre-write length when the filesystem accepts fewer bytes than the line holds.
    A caller that reports an invoker's mistake translates the first and never the second.
    """
    payload = (json.dumps(line, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    if len(payload) >= MAX_RECORD_LINE:
        raise RecordLineTooLong(len(payload), MAX_RECORD_LINE)
    handle = FileIO(path, "ab")  # module-level name: the seam a test stages a short write through
    try:
        position = handle.tell()  # in append mode, this is the current end of the file
        written = handle.write(payload)
        if written != len(payload):
            os.ftruncate(handle.fileno(), position)
            raise ShortRecordWrite(written, len(payload))
    finally:
        handle.close()


# The mount types a record path may be written to with no mutual exclusion. An ALLOWLIST, so a
# filesystem this list has never heard of refuses rather than admits: a gate that guesses "probably
# local" on an unfamiliar mount is the direction this decision cannot fail in.
LOCAL_FSTYPES = frozenset({"ext4", "xfs", "btrfs", "tmpfs", "zfs", "overlay"})

# The table the class is read from; a module-level default so a test points the reader at a fixture
# rather than at this machine's mounts. Field order is mt(5)'s: device, mountpoint, type, options.
MOUNT_TABLE = Path("/proc/self/mounts")

# Cloud-sync directories are ordinary local directories as far as the mount table can see -- the
# mount type of ~/Dropbox and of this checkout are the same ext4 mount -- so "not cloud-synced"
# has to be a declared-root test beside the type test rather than a second row of it. Each root is
# built from Path.home() rather than written as a literal, because a literal home path in a
# committed file is what the toolkit's anonymization guard refuses.
#
# A root is held as it is DECLARED and resolved where it is COMPARED, not here. `admitted` is handed
# an already-resolved record path, so a root left in its own spelling compares a resolved path
# against an unresolved one -- and `is_relative_to` then answers False for every record under a root
# that is a symlink, which reports the path as outside a root it is under. Resolving at the
# comparison keeps that in one place: a root the module did not build, such as a fixture's, is
# interpreted by the same rule as one it did.
CLOUD_SYNC_ROOTS = tuple(
    root
    for root in (Path.home() / name for name in ("Dropbox", "OneDrive", "Google Drive", "iCloud Drive"))
    if root.exists()
)

# mt(5) escapes the three whitespace characters and the backslash; that is the whole set a field
# cannot spell literally, so decoding anything else would be inventing a second grammar.
OCTAL_ESCAPES = {"\\040": " ", "\\011": "\t", "\\012": "\n", "\\134": "\\"}


def _unescape(field: str) -> str:
    """Decode the octal escapes a mount table uses for whitespace and for the backslash itself."""
    for escaped, character in OCTAL_ESCAPES.items():
        field = field.replace(escaped, character)
    return field


def mount_entries(table: Path = MOUNT_TABLE) -> list[tuple[Path, str]]:
    """The `(mountpoint, fstype)` pairs of a mount table, in the table's own order.

    Raises `OSError` when the table cannot be read. `admitted` turns that into a refusal rather than
    letting it escape: a gate that cannot measure must not answer "admitted", and an exception
    raised out of a `pytest_configure` hook reaches the terminal as an internal error instead of as
    the named refusal an invoker's mistake deserves.
    """
    entries = []
    for line in table.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        entries.append((Path(_unescape(fields[1])), fields[2]))
    return entries


def filesystem_class(path: Path, table: Path = MOUNT_TABLE) -> str | None:
    """The fstype of the longest mountpoint in `table` that covers `path`, or None when none does.

    `path` must ALREADY BE RESOLVED: a symlink decides which filesystem the bytes land on, and
    resolving here as well would let this answer about a different path from the one a same-path
    check compared. A path that does not exist yet needs no separate step -- a mountpoint can only
    sit at an existing directory, so the longest match against the path as written is the match
    against its nearest existing ancestor.

    Matching is on PATH COMPONENTS, never on a string prefix, so a mount at `/tmp/a` does not cover
    `/tmp/ab`. When two mountpoints of equal length both cover the path, the later table entry
    wins, which is where a stacked mount appears.
    """
    chosen = None
    for mountpoint, fstype in mount_entries(table):
        if path.is_relative_to(mountpoint) and (chosen is None or len(mountpoint.parts) >= len(chosen[0].parts)):
            chosen = (mountpoint, fstype)
    return None if chosen is None else chosen[1]


def admitted(path: Path, table: Path = MOUNT_TABLE) -> tuple[bool, str]:
    """Whether `path` may be written as a shared record root, and the reason either way.

    `path` must already be resolved, for the reason `filesystem_class` states. The reason is written
    for a person reading a refusal -- it names the measured fstype, the cloud-sync root, or the
    table that could not be read -- because the caller puts it in the error a run ends with.
    Deliberately NOT a warning or a private fallback: both would let the run proceed while claiming
    a shared root the predicate has refused.
    """
    try:
        fstype = filesystem_class(path, table)
    except OSError as exc:
        return False, f"the mount table {table} cannot be read: {exc}"
    if fstype is None:
        return False, f"no mountpoint in {table} covers {path}"
    if fstype not in LOCAL_FSTYPES:
        return False, f"{path} is on a {fstype} mount, which is not one of {sorted(LOCAL_FSTYPES)}"
    for declared in CLOUD_SYNC_ROOTS:
        root = declared.resolve()  # the record path is resolved, so the root must be too
        if path.is_relative_to(root):
            return False, f"{path} lies under the cloud-sync root {root}"
    return True, f"{path} is on a {fstype} mount and outside every declared cloud-sync root"
