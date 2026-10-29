"""Tests of the counted record append: one raw write, its returned count, the bound and the rollback.

Every failing arm stages its short write through the module-level `FileIO` name `write_root`
declares, so the failure is produced deterministically on any machine rather than by filling a
filesystem. The module is new, so each test is a NEW CAPABILITY test rather than a REGRESSION one:
there is no pre-change tree for it to be red on, and the plausible wrong implementation it rules
out is named in its own docstring.
"""

import io
import json
from pathlib import Path

import pytest

from tests.correspondence import write_root

pytestmark = pytest.mark.state_check

MAXIMUM = write_root.MAX_RECORD_LINE
SAMPLE = {"family": "equivalence", "unit": "b.py:1-2", "part": None, "assertion": True}


def _encoded(line: dict) -> int:
    """The byte length of one record line, encoded exactly as the module encodes it."""
    return len((json.dumps(line, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8"))


def _line_of_encoded_length(length: int) -> dict:
    """An ASCII-only record line whose encoded length is exactly `length` bytes."""
    filler = length - _encoded({"k": ""})
    line = {"k": "a" * filler}
    assert _encoded(line) == length, (_encoded(line), length)
    return line


def _lines(path):
    """The JSON lines of a record file."""
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class _StandInFileIO:
    """A stand-in for the module-level `FileIO` name, holding a real handle and answering every call the body makes."""

    def __init__(self, target, mode):
        self._real = io.FileIO(target, mode)

    def tell(self):
        """The real handle's position, which in append mode is the current end of the file."""
        return self._real.tell()

    def fileno(self):
        """The real descriptor, so the body's rollback truncates the file it actually wrote."""
        return self._real.fileno()

    def close(self):
        """Close the real handle."""
        self._real.close()


class _DroppingWrite(_StandInFileIO):
    """Accepts one byte fewer than it is given, and writes none of it."""

    def write(self, data):
        """Report one byte fewer than the payload without landing any of it."""
        return len(data) - 1


class _TruncatingWrite(_StandInFileIO):
    """Writes all but the last byte and reports the shorter count, as a short kernel write does."""

    def write(self, data):
        """Append the payload's first bytes, withholding its final byte."""
        self._real.write(data[:-1])
        return len(data) - 1


def test_a_line_is_appended_as_one_newline_terminated_json_line(tmp_path):
    """Property: each call appends exactly one JSON line that round-trips, leaving earlier lines in place.

    Class: any record line -- two calls here, so append mode is exercised rather than a single
    write into a fresh file. Why: the proof record accumulates across runs, so a second call must
    add a line rather than replace the first, and a consumer reads the file as one JSON document
    per line. Killed by: opening the record for writing instead of appending, which leaves one
    line after two calls; and omitting the trailing newline, which merges two records into one
    line. Second correct implementation: one that opens the path with `os.open` and writes through
    `os.write`, checking the returned count, lands the same bytes and passes this test.
    """
    path = tmp_path / "proofs.jsonl"
    first = {"unit": "b.py:1-2", "assertion": True}
    second = {"unit": "b.py:3-4", "assertion": False}
    write_root.append_line(path, first)
    write_root.append_line(path, second)
    text = path.read_text(encoding="utf-8")
    assert text.count("\n") == 2, text
    assert text.endswith("\n"), text
    assert _lines(path) == [first, second]


def test_a_short_write_raises_carrying_both_counts(tmp_path, monkeypatch):
    """Property: a write that lands fewer bytes than the line raises `ShortRecordWrite` with both counts.

    Class: any short write -- here the stand-in accepts one byte fewer and lands nothing, the
    counterpart of the full write the round-trip test exercises. Why: a short or absent write must
    fail loudly instead of leaving a record whose line count is right and whose content is not, and
    carrying both counts lets a caller report it without depending on a message's wording. Killed
    by: ignoring the returned count and treating the call as complete. Second correct
    implementation: one that holds the payload in a local, compares its length in a named
    condition and raises through a helper, instead of comparing inline after the write.
    """
    monkeypatch.setattr(write_root, "FileIO", _DroppingWrite)
    path = tmp_path / "proofs.jsonl"
    line = dict(SAMPLE)
    encoded = _encoded(line)
    with pytest.raises(write_root.ShortRecordWrite) as raised:
        write_root.append_line(path, line)
    assert (raised.value.written, raised.value.expected) == (encoded - 1, encoded)
    assert path.read_bytes() == b""


def test_a_short_write_leaves_the_file_at_its_pre_write_size(tmp_path, monkeypatch):
    """Property: the prefix a short write accepted is truncated away, so the file keeps its pre-write size.

    Class: any short write that lands a prefix -- here all but the trailing newline, which is the
    case that still parses as JSON and so hides itself from a line-count check; the satisfying
    input is the same call under a stand-in that lands the whole line, which the round-trip test
    covers. Why: a partial line carrying the whole object loads on its own, so without the rollback
    the next append concatenates onto it and corrupts a record that read fine before. Killed by:
    raising the exception without truncating back; and truncating to zero rather than to the
    pre-write position, which discards the lines an earlier run recorded. Second correct
    implementation: one that reads the pre-write size from `os.fstat(fd).st_size` instead of
    `tell()` truncates to the same position and passes this test.
    """
    path = tmp_path / "proofs.jsonl"
    write_root.append_line(path, {"unit": "kept.py:1-2", "assertion": True})
    before = path.stat().st_size
    assert before > 0
    monkeypatch.setattr(write_root, "FileIO", _TruncatingWrite)
    with pytest.raises(write_root.ShortRecordWrite):
        write_root.append_line(path, dict(SAMPLE))
    assert path.stat().st_size == before, (path.stat().st_size, before)
    assert _lines(path) == [{"unit": "kept.py:1-2", "assertion": True}]


def test_the_bound_is_checked_before_the_file_is_opened(tmp_path):
    """Property: a line one byte under the bound writes; a line at the bound raises and leaves no file.

    Class: the two positions around the bound, on an ASCII payload so the byte and character
    measures agree. Why: the bound is what keeps the append one atomic write, and a refusal that
    had already opened the file would create an empty record at a path the invoker then believes
    holds a complete one. Killed by: comparing above the bound rather than at it, which lets the
    at-bound line write; and checking after the open, which leaves the file behind. Second correct
    implementation: one that encodes into a local and tests the remaining headroom before opening
    writes and refuses identically, and passes this test.
    """
    under = _line_of_encoded_length(MAXIMUM - 1)
    path = tmp_path / "under-the-bound.jsonl"
    write_root.append_line(path, under)
    assert path.stat().st_size == MAXIMUM - 1

    absent = tmp_path / "at-the-bound.jsonl"
    with pytest.raises(write_root.RecordLineTooLong) as raised:
        write_root.append_line(absent, _line_of_encoded_length(MAXIMUM))
    assert (raised.value.length, raised.value.bound) == (MAXIMUM, MAXIMUM)
    assert not absent.exists()


def test_the_bound_measures_encoded_bytes_rather_than_characters(tmp_path):
    """Property: a line whose character count is under the bound but whose encoding reaches it is refused.

    Class: non-ASCII content -- here 2043 two-byte characters, which is 2053 characters of JSON
    text and exactly 4096 bytes of encoded line; and a shorter non-ASCII line under the bound on
    both measures, which writes. Why: atomicity is a byte property of the filesystem, so a bound
    measured on `len(str)` would admit a line far longer than the size it believes it admitted.
    Killed by: measuring the bound on the string before encoding, which passes the 2053-character
    line through. Second correct implementation: one that encodes first and compares
    `len(payload) < MAX_RECORD_LINE` refuses and writes identically.
    """
    wide = {"k": "é" * 2043}
    assert len(json.dumps(wide, sort_keys=True, ensure_ascii=False)) + 1 < MAXIMUM
    assert _encoded(wide) == MAXIMUM
    refused = tmp_path / "refused.jsonl"
    with pytest.raises(write_root.RecordLineTooLong):
        write_root.append_line(refused, wide)
    assert not refused.exists()

    narrow = {"k": "é" * 100}
    assert _encoded(narrow) < MAXIMUM
    written = tmp_path / "written.jsonl"
    write_root.append_line(written, narrow)
    assert _lines(written) == [narrow]


class _RecordingClose(_TruncatingWrite):
    """`_TruncatingWrite` that records each `close` call, so a test can see the handle was released."""

    closes = []

    def close(self):
        """Record that this arm reached the close, then close the real handle."""
        _RecordingClose.closes.append(True)
        super().close()


def test_the_line_is_written_in_sorted_key_order(tmp_path):
    """Property: the bytes written are the sorted-key encoding, so one record lands one line whichever order the caller built it in.

    Class: every record line -- here an ASCII mapping whose insertion order is not its sorted
    order, which is the only shape that separates a sorted encoding from an insertion-ordered
    one; the satisfying input is any mapping whose own order already matches its sorted order,
    which the round-trip test above writes. Why: the proof record accumulates across runs and is
    compared line by line, so an encoding whose bytes follow the caller's insertion order makes
    two runs that recorded the same proof differ on disk. Killed by: dropping `sort_keys`, which
    leaves all five arms above green and writes this line in insertion order. Second correct
    implementation: one that builds the same payload and lands it through `os.open`/`os.write`
    writes identical bytes and passes this test.
    """
    line = {"zeta": 1, "alpha": 2}
    path = tmp_path / "ordered.jsonl"
    write_root.append_line(path, line)
    assert path.read_text(encoding="utf-8") == json.dumps(line, sort_keys=True) + "\n"


def test_the_handle_is_closed_when_a_write_falls_short(tmp_path, monkeypatch):
    """Property: the short-write path closes the descriptor rather than leaving it to reference counting.

    Class: any short write -- here the rollback arm's stand-in, which lands all but the final
    byte; the satisfying input is the same call on a full write, which the round-trip test
    covers. Why: the short-write path is the one where the caller is already handling a failure
    and never received an open handle to release, so a descriptor left open there is released
    only when the object is collected -- and remains open for writes until then on any
    implementation that does not count references. Killed by: replacing the `finally` body with
    no close, which leaves all five arms above green. Second correct implementation: one that
    opens the handle as a context manager closes it on the same path and passes this test.
    """
    _RecordingClose.closes.clear()
    monkeypatch.setattr(write_root, "FileIO", _RecordingClose)
    with pytest.raises(write_root.ShortRecordWrite):
        write_root.append_line(tmp_path / "closed.jsonl", dict(SAMPLE))
    assert _RecordingClose.closes == [True], _RecordingClose.closes


ROOT_LINE = "/dev/root / ext4 rw,relatime 0 0"


def _table(tmp_path, *lines):
    """Write a mount table fixture with `lines` in order, and return its path."""
    table = tmp_path / "mounts"
    table.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return table


def test_an_ext4_mount_is_admitted(tmp_path):
    """Property: a record path on a local ext4 mount is admitted, and the reason names the measured type.

    Class: any path whose longest covering mountpoint carries an allowlisted fstype -- here the
    synthetic `/` mount every other arm also sits under, so this is the admitted counterpart the
    refusing arms are read against. Why: the proof record is a shared write root, and the mechanism
    is only safe where the mount is local and not cloud-synced. Killed by: an empty allowlist, or a
    denylist of network types, which admits an unknown mount the way this refuses it. Second
    correct implementation: one that computes the class by calling `statfs` and mapping its type
    number to a name for the allowlisted set answers ext4 here and refuses the same paths.
    """
    table = _table(tmp_path, ROOT_LINE)
    admitted, reason = write_root.admitted(tmp_path / "records.jsonl", table)
    assert admitted is True, reason
    assert "ext4" in reason, reason


def test_a_network_mount_is_refused_naming_the_measured_type(tmp_path):
    """Property: a record path on an nfs mount is refused, and the reason names the type it measured.

    Class: any path whose longest covering mountpoint carries a type outside the allowlist -- here
    the network type the decision exists for, beside the ext4 arm that must stay admitted. Why: a
    network mounted record path needs mutual exclusion this module does not provide, so refusing is
    the only answer that cannot claim a safety it does not have. Killed by: admitting anything not
    on a denylist, and by refusing with a reason that does not say what was measured, which leaves
    the invoker unable to tell a network mount from a typo. Second correct implementation: one that
    reads the same table through `csv`-style field splitting with the same escape set answers nfs4
    here.
    """
    shared = tmp_path / "nfs"
    table = _table(tmp_path, ROOT_LINE, f"host:/export {shared} nfs4 rw,vers=4 0 0")
    admitted, reason = write_root.admitted(shared / "records.jsonl", table)
    assert admitted is False, reason
    assert "nfs4" in reason, reason


def test_two_nested_mountpoints_resolve_to_the_longer_one(tmp_path):
    """Property: a path under two mountpoints answers the longer one, and its sibling answers the outer.

    Class: nested mountpoints -- a path inside both the outer and the inner mount, with a sibling
    path under the outer alone as the differently-positioned satisfying input. Why: a bind or
    overlay mount hides the type of everything beneath it, so the answer must come from the deepest
    covering mountpoint or the wrong filesystem governs the write. Killed by: taking the first
    match in table order, which answers the outer mount for the inner path. Second correct
    implementation: one that sorts the pairs by descending component count and takes the first
    match answers identically.
    """
    outer = tmp_path / "outer"
    inner = outer / "inner"
    table = _table(tmp_path, ROOT_LINE, f"/dev/outer {outer} ext4 rw 0 0", f"/dev/inner {inner} btrfs rw 0 0")
    assert write_root.filesystem_class(inner / "records.jsonl", table) == "btrfs"
    assert write_root.filesystem_class(outer / "records.jsonl", table) == "ext4"


def test_a_mountpoint_with_an_octal_escaped_space_matches_its_directory(tmp_path):
    """Property: a mountpoint spelled with `\\040` covers the directory whose name has a space, and only that one.

    Class: a mountpoint containing a space, which the table format can only spell as an octal escape
    -- with a near-miss directory whose name has no space as the differently-positioned input. Why:
    without unescaping, a path under such a mount resolves to the mount above it, so the type test
    silently answers about the wrong filesystem. Killed by: splitting the line on whitespace and
    keeping the escaped field, and by unescaping into a string that then matches a prefix rather
    than whole components. Second correct implementation: one that decodes every three-digit octal
    sequence rather than the four the format permits answers identically over this fixture.
    """
    spaced = tmp_path / "with space"
    table = _table(tmp_path, ROOT_LINE, f"/dev/space {tmp_path}/with\\040space tmpfs rw 0 0")
    assert write_root.filesystem_class(spaced / "records.jsonl", table) == "tmpfs"
    assert write_root.filesystem_class(tmp_path / "withspace" / "records.jsonl", table) == "ext4"


def test_a_mountpoint_does_not_cover_a_sibling_whose_name_extends_it(tmp_path):
    """Property: a mount at `.../a` leaves a path under `.../ab` on the mount above it.

    Class: two sibling directories whose names share a prefix, with the mount sitting at the shorter
    name -- the only shape that separates a component match from a string-prefix match. Why: a
    string-prefix match reports the wrong filesystem for every sibling whose name extends a
    mountpoint, which silently admits a network or cloud mount's neighbour. Killed by:
    `str(path).startswith(str(mountpoint))`. Second correct implementation: one that compares
    `path.parts[: len(mountpoint.parts)] == mountpoint.parts` answers identically.
    """
    short = tmp_path / "a"
    longer = tmp_path / "ab"
    table = _table(tmp_path, ROOT_LINE, f"host:/export {short} nfs4 rw 0 0")
    assert write_root.filesystem_class(longer / "records.jsonl", table) == "ext4"
    assert write_root.filesystem_class(short / "records.jsonl", table) == "nfs4"


def test_an_unknown_filesystem_type_is_refused(tmp_path):
    """Property: a mount whose type the allowlist has never heard of is refused rather than admitted.

    Class: any fstype outside `LOCAL_FSTYPES` -- here ext2, a real local type that is deliberately
    not on the list, which separates an allowlist from a "not obviously network" test. Why: the
    decision admits only types it names, so an unfamiliar mount must fail closed. Killed by: a
    denylist of network types, or by an `ext2 not in NETWORK_TYPES` shortcut. Second correct
    implementation: one that looks the type up in a dict of admitted types and treats a miss as a
    refusal answers identically.
    """
    odd = tmp_path / "odd"
    table = _table(tmp_path, ROOT_LINE, f"/dev/odd {odd} ext2 rw 0 0")
    assert "ext2" not in write_root.LOCAL_FSTYPES
    admitted, reason = write_root.admitted(odd / "records.jsonl", table)
    assert admitted is False, reason
    assert "ext2" in reason, reason


def test_a_path_under_a_cloud_sync_root_is_refused_even_on_ext4(tmp_path, monkeypatch):
    """Property: a path under a declared cloud-sync root is refused though its mount type is allowed.

    Class: a cloud-sync root on an ordinary local mount -- measured on the machine this predicate
    was decided for, a Dropbox directory is the same ext4 mount as the checkout -- with a sibling
    path outside the root as the differently-positioned satisfying input. Why: the mount type
    cannot see a sync client, so without the declared-root half a record written there would be
    shared with whatever the client replicates it to. Killed by: testing the mount type alone,
    which admits exactly the case the decision was written for. Second correct implementation: one
    that compares the resolved path against `Path.home()`-relative roots by walking its parents
    answers identically.
    """
    sync = tmp_path / "Dropbox"
    sync.mkdir()
    monkeypatch.setattr(write_root, "CLOUD_SYNC_ROOTS", (sync,))
    table = _table(tmp_path, ROOT_LINE)
    admitted, reason = write_root.admitted(sync / "records.jsonl", table)
    assert admitted is False, reason
    assert "cloud-sync" in reason and str(sync) in reason, reason
    outside, outside_reason = write_root.admitted(tmp_path / "records.jsonl", table)
    assert outside is True, outside_reason


def test_a_cloud_sync_root_is_recognised_through_its_own_target(tmp_path, monkeypatch):
    """Property: a declared cloud-sync root refuses a resolved record path under it however that root is spelled.

    Class: declared roots whose own spelling is not their resolved path -- here a root declared as a
    symlink to a directory on the same allowed mount, read against the resolved record path the
    configure hook hands the gate; the first arm below is the same root declared as its own target,
    which is the differently-positioned input that refuses under the current code. Why: the gate is
    only ever given a RESOLVED path, so a root held in its own spelling is compared against a path
    that has already stepped through the link, and every record under that root is then reported as
    outside it -- the drop-box moved to another disk and left behind as a link is refused nowhere,
    though the mount type cannot see it either, so this half of the predicate is the only thing that
    would have caught it. Killed by: comparing the resolved path against the root as declared, which
    is what the module does today, and by resolving the record path but not the roots. Second
    correct implementation: one that resolves each declared root inside the comparison, or once when
    the tuple is built, refuses both arms and passes.
    """
    target = tmp_path / "sync-target"
    target.mkdir()
    declared = tmp_path / "Dropbox"
    declared.symlink_to(target, target_is_directory=True)
    table = _table(tmp_path, ROOT_LINE)
    record = (target / "records.jsonl").resolve()

    # Control arm: the same root, spelled as its own target, is refused under the current code.
    monkeypatch.setattr(write_root, "CLOUD_SYNC_ROOTS", (target,))
    control, control_reason = write_root.admitted(record, table)
    assert control is False, control_reason

    # The arm the gate must refuse as well: the root as the link it is actually spelled with.
    monkeypatch.setattr(write_root, "CLOUD_SYNC_ROOTS", (declared,))
    admitted, reason = write_root.admitted(record, table)
    assert admitted is False, reason
    assert "cloud-sync" in reason, reason


def test_a_path_that_does_not_exist_yet_answers_its_nearest_existing_ancestor(tmp_path):
    """Property: a path with missing components answers the mount its nearest existing ancestor is on.

    Class: a record path under a directory that has never been created, which is the normal case at
    configure time because the record file is created by its first write. Why: a missing path has
    no mount of its own, and answering "no filesystem" would refuse every first run. Killed by:
    requiring the path to exist, which raises or refuses at configure. Second correct
    implementation: one that walks up to the nearest existing ancestor explicitly and matches that
    answers the same type here.
    """
    inner = tmp_path / "outer" / "inner"
    table = _table(tmp_path, ROOT_LINE, f"/dev/inner {inner} btrfs rw 0 0")
    missing = inner / "records" / "deep.jsonl"
    assert not missing.exists()
    assert write_root.filesystem_class(missing, table) == "btrfs"
    assert write_root.admitted(missing, table)[0] is True


def test_a_symlink_is_classified_by_the_spelling_it_is_given(tmp_path):
    """Property: the unresolved spelling answers the link's own mount and the resolved spelling the target's.

    Class: a real symlink pointing from an allowed mount into a mount whose type is refused -- both
    arms are asserted, because the admitted direction alone cannot tell a classifier that resolves
    from one that does not. Why: only the resolved path says where the bytes land, so a caller that
    resolves once must get the target's answer, while the unresolved answer is what proves the
    function is not resolving behind the caller's back. Killed by: resolving inside
    `filesystem_class`, which makes both arms answer the target, and by never resolving at all,
    which makes both answer the link's mount. Second correct implementation: one that compares
    `os.path.realpath` of the target only when the caller passed it answers both arms identically.
    """
    portal = tmp_path / "portal"
    portal.mkdir()
    target = portal / "records.jsonl"
    target.write_text("", encoding="utf-8")
    table = _table(tmp_path, ROOT_LINE, f"/dev/fuse {portal} fuse.portal rw,nosuid,nodev 0 0")
    link = tmp_path / "records.jsonl"
    link.symlink_to(target)
    assert link.is_symlink()
    assert write_root.filesystem_class(link, table) == "ext4"
    assert write_root.admitted(link, table)[0] is True
    assert write_root.filesystem_class(link.resolve(), table) == "fuse.portal"
    admitted, reason = write_root.admitted(link.resolve(), table)
    assert admitted is False, reason
    assert "fuse.portal" in reason, reason


def test_a_mount_table_that_cannot_be_read_is_refused_rather_than_admitted(tmp_path):
    """Property: an unreadable mount table produces a refusal whose reason names the table.

    Class: a table path that does not exist -- the shape a missing or unreadable `/proc` takes, and
    a path the gate cannot measure. Why: a gate that cannot measure must not answer "admitted", and
    an exception raised from a configure hook reaches the terminal as an internal error rather than
    as a named refusal. Killed by: letting the `OSError` escape, and by defaulting to admitted when
    the read fails. Second correct implementation: one that returns the same refusal from a
    `try`/`except FileNotFoundError` narrower than `OSError` answers identically for this input.
    """
    absent = tmp_path / "no-such-table"
    admitted, reason = write_root.admitted(tmp_path / "records.jsonl", absent)
    assert admitted is False, reason
    assert "cannot be read" in reason, reason


def test_the_checkout_it_runs_from_is_admitted_on_the_real_mount_table():
    """Property: the checkout's own path is admitted by this machine's real table, whose type is named on failure.

    Class: the real `/proc/self/mounts` and a path under the checkout this test file lives in --
    the population the predicate will actually run over, as against every synthetic table above.
    Why: the synthetic arms pin the logic and say nothing about whether the logic admits the tree
    the harness runs from; a predicate that refuses its own checkout would fail every run. Killed
    by: an allowlist missing the checkout's real type, and by a parser that cannot read the real
    table's field count. Second correct implementation: one that calls `findmnt` for the same path
    answers the same type on a machine where both are available.
    """
    checkout = Path(__file__).resolve().parents[2]
    admitted, reason = write_root.admitted(checkout / "records.jsonl")
    assert admitted is True, reason
