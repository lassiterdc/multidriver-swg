"""Resolve a logical path key to the machine-specific path an estate binds to it.

A run reaches data that lives on one machine — a dataset held in a synced folder, a scratch
directory on a cluster — while the code that reaches it ships in a public repository. A
machine-specific absolute path written into that code publishes the private layout and bakes one
machine's arrangement into every machine's copy of the toolkit. So the path stays out of the code:
code names a path by a logical key, `domain.project.kind`, and a resolver returns the path from a
binding document the estate supplies.

The ruling this module implements, in its own two sentences, quoted verbatim so a later reader can
check the code against the ruling rather than against a paraphrase of it:

    "Code names a path by a logical key (domain, project, kind); a toolkit-side resolver returns the path from an estate-side binding, so no tracked toolkit file carries a machine-specific path."

    "An unbound logical key is a loud, actionable error — the resolver never silently defaults to a path — so a missing binding fails the run rather than resolving to a machine-specific location."

The refusals are the mechanism, not an error-reporting detail. The ruling's second sentence requires
a loud failure exactly where a silent default is available, so a refusal here owes its reader the
operational specifics needed to recover: which key, which document, and which keys the document does
bind. A refusal that named only the key would hand back the one fact the reader already had.

`PathResolutionError` is the root of every refusal in this module, so a caller treating any
resolution failure as one class catches the root instead of enumerating leaves. It derives from
`Exception` directly, and deliberately NOT from `KeyError`: `KeyError.__str__` renders its message
inside repr quotes, and a resolution refusal's message is the operator's whole recovery path, so
quoting it would deliver the actionable part as a quoted literal.

The two leaves so far are `UnboundPathKeyError`, for a well-formed key that the binding document does
not bind, and `EstateNotConfiguredError`, for a run whose estate root cannot be located at all.
Presence is a different question from shape — whether a key is spelled correctly is the key type's
own refusal — and this module consults no list of estate domains or projects, because embedding that
vocabulary in the public repository is the leak this module exists to prevent.

Not established: that any caller routes a path through this module. Outside `divergence_ledger.py`,
which reads a TOML file beside that module, this package contains no path-resolving call site, so
nothing here is exercised by the package's own code today.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePath
from types import MappingProxyType


class PathResolutionError(Exception):
    """The root of every refusal this module raises.

    A caller that wants any resolution failure as one class catches this rather than enumerating the
    leaves, which is why the leaves below derive from it rather than from separate stdlib errors.
    """


class UnboundPathKeyError(PathResolutionError):
    """A well-formed key that the binding document does not bind.

    The key being spelled correctly and the key being bound are separate questions, and only the
    second is asked here: a key can satisfy the key grammar and still name nothing the estate bound.
    Whoever raises this owes the reader the key, the document, and the keys that document does bind,
    because an operator can act only on the binding that is missing.
    """


class EstateNotConfiguredError(PathResolutionError):
    """No estate root could be located, so there is no binding document to read at all.

    Separated from `UnboundPathKeyError` because the reader's recovery differs: an unbound key is
    answered by adding one binding to a document that was found, while this is answered by pointing
    the run at the estate before any key can resolve.
    """


_SEGMENT_COUNT = 3

_SEGMENT_PATTERN = re.compile(r"[A-Za-z0-9_-]+")

_RESERVED_PROJECT_SEGMENT = "shared"


@dataclass(frozen=True)
class PathKey:
    """One logical path key: the `domain.project.kind` address a binding document answers.

    A key is exactly three segments, and this type refuses the SHAPE of a key at construction so
    that a malformed key cannot reach a lookup. Two segments is malformed and so is four; each
    segment must be a non-empty run of letters, digits, `_` and `-`; and the middle segment may not
    be the literal `shared`, which a domain reserves for the directories its projects share, so no
    estate project may shadow that name.

    A segment-count check alone would not be enough, and the charset is why: `a.b/c.d` splits into
    exactly three segments, and its middle one carries a separator, so only the charset refuses it.

    This type holds NO list of legal `domain` or `project` values, deliberately. Such a list would
    carry a private estate's vocabulary into the public repository — the leak this module exists to
    prevent — and would make every new estate domain a toolkit release. So `atlantis.nonexistent.
    raw_input` is a WELL-FORMED key that this type accepts; whether the estate binds it is a
    different question, asked at `resolve`, which raises `UnboundPathKeyError` naming the document
    and the keys that document does bind.

    Frozen, and therefore hashable, because `resolve` looks a key up in a mapping: a mutable key
    would let a caller change the key after the lookup target was chosen.

    Raises `PathResolutionError` rather than a dedicated leaf, because this module's exception
    hierarchy is fixed at the root plus the two leaves and a malformed key has no leaf among them;
    raising the root keeps a caller that catches any resolution failure catching this one too.
    """

    domain: str
    project: str
    kind: str

    def __post_init__(self) -> None:
        """Refuse a key whose shape no binding document could address."""
        for role, segment in (
            ("domain", self.domain),
            ("project", self.project),
            ("kind", self.kind),
        ):
            if not isinstance(segment, str) or not _SEGMENT_PATTERN.fullmatch(segment):
                raise PathResolutionError(
                    f"{role} segment {segment!r} is not a valid path-key segment: each of a key's "
                    "three segments must be a non-empty run of letters, digits, '_' and '-', so a "
                    "segment can carry no separator, no dot and no drive-letter colon, and cannot "
                    "be empty"
                )
        if self.project == _RESERVED_PROJECT_SEGMENT:
            raise PathResolutionError(
                f"project segment {self.project!r} is reserved: a domain reserves 'shared' for the "
                "directories its projects share, so no estate project may take that name; rename "
                "the project segment of this key"
            )

    @classmethod
    def parse(cls, key: str) -> PathKey:
        """Build a key from its dotted `domain.project.kind` spelling.

        This is the one place a dotted string becomes a `PathKey`, so the binding-document loader
        hands a document's key here rather than splitting it and re-deriving the grammar. A string
        that does not split into exactly three segments is refused here, before the per-segment
        refusals above, because "two segments" and "four segments" are one defect and the count is
        the fact the reader needs to recover.
        """
        segments = key.split(".")
        if len(segments) != _SEGMENT_COUNT:
            raise PathResolutionError(
                f"path key {key!r} splits into {len(segments)} dotted segments: a key is "
                "'domain.project.kind', exactly three segments, naming one domain, one project and "
                "one kind"
            )
        return cls(domain=segments[0], project=segments[1], kind=segments[2])

    def __str__(self) -> str:
        """Render the key in the dotted spelling `parse` accepts, so a key round-trips through text."""
        return f"{self.domain}.{self.project}.{self.kind}"


# The binding-document half starts here, after the key type because it cannot start before it: a
# document's key is validated AS a `PathKey`, so the type that refuses a malformed key must exist
# before the loader that parses one can be written.
BINDING_RULE = "path-bindings:1"

_BINDING_KEYS = ("rule", "paths")

# `$` and `{` between them refuse every placeholder spelling the document form names — `$`, `{` and
# `${` — so `${` needs no entry of its own, and a third entry would be a duplicate that could drift
# from the two that actually decide the refusal.
_UNRESOLVED_PLACEHOLDER_TOKENS = ("$", "{")


@dataclass(frozen=True)
class PathBindings:
    """One validated binding document: the absolute path this machine binds to each path key.

    A binding document is one TOML file of two top-level fields — `rule`, the version header, and
    `paths`, a flat table whose keys are `domain.project.kind` and whose values are absolute path
    strings. This type is that document AFTER validation, and `paths` here is keyed by `PathKey`
    rather than by the document's dotted spelling, so no lookup can be reached by a string that was
    never parsed as a key.

    THE DOCUMENT IS THE CONSTRUCTOR OPERAND, AND THAT IS DELIBERATE. Deliberately NOT a constructor
    taking `rule` and `paths` as separate fields: such a constructor cannot see a top-level key the
    form does not name, so the refusal for an unnamed top-level key would be the loader's alone and
    a hand-built instance could carry one unchecked. Taking the document makes every refusal but the
    TOML-parse refusal a construction check, which is exactly what `resolve` relies on when it trusts
    the bindings it is handed instead of re-reading the file.

    What it refuses, naming only the checks it owns. The refusal for a document that is not valid
    TOML belongs to `load_bindings`, because a document must parse before an object exists to carry
    the remaining checks:

    - a top-level key other than `rule` or `paths` — an unnamed key is a binding its writer believes
      is live while nothing reads it;
    - a `rule` that is absent or is not the literal `BINDING_RULE` — the version header is what makes
      a later grammar change loud rather than silently misread;
    - a `paths` that is absent or is not a table — without it there is no key space at all;
    - a `paths` key that is not a path key — the grammar is `PathKey`'s own, so the refusal is
      delegated to it rather than restated here;
    - a `paths` value that is a table — the unquoted-dotted-key failure, where
      `norfolk.old_swg_reproduction.raw_input` written inside `[paths]` builds a nested table and the
      flat key space silently becomes a tree; this refusal tells the writer to QUOTE the key;
    - a `paths` value that is not text — a binding is a path string;
    - a `paths` value that is not absolute, or that carries an unresolved placeholder token — a
      relative binding resolves against whatever working directory the caller happens to run in, the
      same defect the root lookup rules out, and a token that survives this check fails later at
      first open with a `FileNotFoundError` naming the literal token rather than failing here.

    `paths` is exposed read-only, so a caller cannot add or rewrite a binding after validation and
    reach a lookup with a path this type never checked.

    Not established: that a bound path EXISTS on this machine, that it holds what the key's domain
    and project mean, or that the document was authored on the machine the caller is running on. The
    document carries no machine field by decision, so its machine identity is its filename and
    nothing inside it, and the form of the document is the whole of what is checked here.
    """

    document: Mapping[str, object]

    rule: str = field(init=False)

    paths: Mapping[PathKey, str] = field(init=False)

    def __post_init__(self) -> None:
        """Refuse a document that does not carry a valid rule header and a flat table of absolute bindings."""
        for key in self.document:
            if key not in _BINDING_KEYS:
                raise PathResolutionError(
                    f"binding document carries the top-level key {key!r}: a binding document is "
                    f"exactly {', '.join(repr(name) for name in _BINDING_KEYS)}, so a key this form "
                    "does not name is a binding its writer believes is live while nothing reads it; "
                    "remove the key, or move its value under 'paths' as a binding"
                )

        rule = self.document.get("rule")
        if rule != BINDING_RULE:
            raise PathResolutionError(
                f"binding document's rule is {rule!r}: a binding document must declare rule = "
                f"{BINDING_RULE!r}, the version header that makes a later grammar change loud rather "
                "than silently misread by an older loader"
            )

        declared = self.document.get("paths")
        if not isinstance(declared, dict):
            raise PathResolutionError(
                f"binding document's 'paths' is {declared!r}: 'paths' must be a table of quoted "
                "dotted keys, each bound to an absolute path string, because without that table "
                "there is no key space for a lookup to read"
            )

        bindings: dict[PathKey, str] = {}
        for key, value in declared.items():
            bindings[_validated_binding(key, value)] = value

        object.__setattr__(self, "rule", rule)
        object.__setattr__(self, "paths", MappingProxyType(bindings))


def _validated_binding(key: object, value: object) -> PathKey:
    """The path key `key` spells, once `value` is refused if it cannot be a binding.

    THE ORDER OF THESE REFUSALS IS LOAD-BEARING. An UNQUOTED dotted key under `[paths]` nests, so
    `norfolk.old_swg_reproduction.raw_input = "..."` reaches here as a single-segment key carrying a
    TABLE value. Parsing the key first would report only that the key has the wrong segment count,
    and the operator would never be told that quoting it is what fixes the document — the one
    instruction this refusal exists to deliver. So the table-value refusal runs first and the key
    grammar is asked second, once the document's shape is known to be the flat one it is meant to be.
    """
    if isinstance(value, dict):
        # No single quoted spelling is suggested here, because the key this refusal sees is only the
        # FIRST segment of an unquoted dotted key: quoting that segment alone would bind the wrong
        # key. Walking the nested table to rebuild the full spelling is what this document's form
        # refuses to do, so the message asks for the whole dotted key instead of naming one.
        raise PathResolutionError(
            f"binding document's paths value for {key!r} is a table: a binding is one absolute path "
            "STRING and a table is never one. If the key was written UNQUOTED it nested, so the flat "
            "key space the resolver reads silently becomes a tree and every lookup would miss — "
            "QUOTE the whole dotted key, every segment of it, to bind it as one flat entry"
        )

    parsed = _parsed_binding_key(key)

    if not isinstance(value, str):
        raise PathResolutionError(
            f"binding document's paths value for {key!r} is {value!r}: a binding is the absolute "
            "path STRING this machine holds for this key, and nothing else"
        )

    for token in _UNRESOLVED_PLACEHOLDER_TOKENS:
        if token in value:
            raise PathResolutionError(
                f"binding document's paths value for {key!r} is {value!r}, which carries the "
                f"unresolved placeholder token {token!r}: the estate must bind a real path, because "
                "a value that begins with a separator and still carries a token passes an "
                "is-absolute test and then fails at first open with a FileNotFoundError naming the "
                "literal token instead of failing here"
            )

    if not PurePath(value).is_absolute():
        raise PathResolutionError(
            f"binding document's paths value for {key!r} is {value!r}, which is not absolute: a "
            "relative binding resolves against whatever working directory the caller happens to run "
            "in, so the same document would bind different paths in different runs"
        )

    return parsed


def _parsed_binding_key(key: object) -> PathKey:
    """The path key a document's `paths` key spells, or a refusal naming the spelling that does not parse."""
    if not isinstance(key, str):
        raise PathResolutionError(
            f"binding document's paths key {key!r} is not a dotted string: a paths key is the "
            "'domain.project.kind' spelling of a path key, and a key that is not text cannot be one"
        )
    try:
        return PathKey.parse(key)
    except PathResolutionError as error:
        raise PathResolutionError(f"binding document's paths key {key!r}: {error}") from error


def load_bindings(document_path: str | Path) -> PathBindings:
    """Read the binding document at `document_path` and return it validated.

    The argument is REQUIRED and there is no default. Deliberately NOT a default document location
    beside this module, which is the form the in-package `divergence_ledger` loader takes: a default
    here would be a toolkit-side well-known location, so an unconfigured estate would resolve against
    whatever ships in the wheel instead of failing loudly. The operator composes the two halves — the
    estate root from the root lookup, and the document named here — and the per-machine filename the
    estate uses is the operator's act on a path this function is handed, never something it derives.

    A document that is not valid TOML is refused here, because the object exists only once parsing
    has succeeded and there is nothing to bind until then. Every other refusal is `PathBindings`'
    own, and it is re-raised with this path attached so an operator holding one document per machine
    can tell which machine's document failed.

    Deliberately NOT wrapped: an unreadable path. `Path.read_text` raises the OS error, which already
    names the file an operator must fix, and the form's refusal set has no row for it — its row for
    an unreadable input is a document that is not valid TOML, which presupposes a document was read.
    Translating an `OSError` into this module's refusals would report a refusal this document's
    contract deliberately does not carry.

    Not established: that the path handed in names the machine this run is on, or that it lives at
    the estate's own location for that machine. Both are the caller's to get right.
    """
    path = Path(document_path)

    text = path.read_text(encoding="utf-8")

    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise PathResolutionError(
            f"binding document {path} is not valid TOML, so there is nothing to read: {error}"
        ) from error

    try:
        return PathBindings(document)
    except PathResolutionError as error:
        raise PathResolutionError(f"binding document {path}: {error}") from error


# The estate-root half and the lookup start here, after the document half because the two compose:
# the root locates where the estate lives, the loader above reads one document inside it, and the
# lookup below answers one key from that document. Nothing in this half can be written before the key
# and document types it consumes.
def estate_root(explicit: str | Path | None = None) -> Path:
    """Return this machine's estate root as the caller states it, or raise rather than invent one.

    The estate root is the directory the estate's own tree lives under. It enters this module exactly
    one way — as the caller's argument — because the binding document is a file INSIDE the root, named
    by the caller and read by `load_bindings`, and the root is the half the caller composes with it.

    THE CONTRACT IS ONE ROUTE. An explicit root is returned; a call carrying none raises
    `EstateNotConfiguredError` naming the explicit-argument route, because that is the only route
    there is. Deliberately NOT a fall-through in which an absent argument is answered from anywhere
    else: the run configuration is what names this machine's estate, and a root read from the
    surrounding process would bind a run to ambient state its own provenance record never captured.

    Deliberately NOT a `socket.gethostname()` lookup and deliberately NOT `os.getcwd()`, because both
    are the defect this module exists to remove: either would make a run's binding depend on which
    machine, or which directory, the process started in, and neither is recorded anywhere the run's
    provenance could replay it from — so the same code would resolve different paths on two runs and
    nothing in the output would say which. Deliberately NOT a default location either, because a
    default would make an unconfigured estate resolve against whatever ships in the wheel rather than
    failing loudly.

    Deliberately NOT naming the estate anywhere in this module. A toolkit-side identifier for the
    estate — a directory name, a path, a private token — is what the split-repository boundary exists
    to keep out of the public tree, and an identifier here would also have to change whenever the
    estate's own layout changes, making every estate rename a toolkit release.

    The parameter is annotated `str | Path | None` with `None` as its default ONLY so that a call
    carrying no root raises this module's own refusal instead of a `TypeError` from the interpreter: a
    caller who omits the root is exactly the caller the message is written for, and an interpreter
    error names neither the argument's purpose nor the recovery. `None` is REFUSED rather than
    resolved, so the default cannot become a fall-through by accident.

    A root that is not absolute is refused rather than returned, because a relative root resolves
    against the caller's working directory and so recreates the `os.getcwd()` dependence this function
    exists to rule out.

    Not established: that the returned root is a directory on this machine, that it holds a binding
    document, or that it is the estate the caller's document was authored for. Locating the root and
    reading a document inside it are separate acts, and this function performs only the first.
    """
    if explicit is None:
        raise EstateNotConfiguredError(
            "no estate root was given: pass this machine's estate directory explicitly — "
            "estate_root(explicit_root) — because this module consults no ambient configuration and "
            "holds no default location, and a root it invented would be machine-dependent in a way "
            "the run's provenance record could not reproduce"
        )

    return _absolute_root(explicit)


def _absolute_root(value: str | Path) -> Path:
    """The estate root `value` names, refused if it is not absolute.

    Absolute-ness is checked here rather than left to the caller because it is the same defect at a
    different layer: a relative root silently resolves against the working directory the process
    happens to start in, so the run would bind one machine's estate on one invocation and another's on
    the next, and the refusal is the only place that can say so while the operator is still holding
    the value that caused it. There is no route parameter because there is one route: the value is
    always the caller's argument, so a message distinguishing routes would distinguish nothing.

    The refusal is the module's root rather than a dedicated leaf, on the same ground `PathKey` gives
    for a malformed key: this module's exception hierarchy is fixed at the root plus the two leaves, an
    unusable root has no leaf among them, and raising the root keeps a caller that catches any
    resolution failure catching this one too.
    """
    root = Path(value)
    if not root.is_absolute():
        raise PathResolutionError(
            f"the estate root {value!r} is not absolute: a relative root resolves against whatever "
            "working directory the caller happens to run in, so the same run configuration would name "
            "different estates from different directories; give the absolute path of this machine's "
            "estate directory"
        )
    return root


def resolve(key: PathKey, bindings: PathBindings, document_path: str | Path) -> Path:
    """Return the absolute path `bindings` binds to `key`, or raise naming the document and its keys.

    This is the whole lookup, and the signature is where its safety lives. There is no default path,
    no optional `bindings`, and the return is `Path` — never `Path | None` — so a caller cannot reach a
    fallback by omitting an argument and cannot be handed `None` to substitute a path of its own for.
    Both operands arrive already validated: `key` refused its own shape at construction and `bindings`
    refused its own document at construction, so the only thing left for this function is the lookup
    or the refusal.

    `document_path` is a LABEL the refusal reports, and it is required. The refusal has to name the
    document a reader should edit, and a `PathBindings` deliberately cannot supply that: the document is
    the operand it was built from, and the document's machine identity is its FILENAME rather than any
    field inside it, so the path exists only in the caller's hands. Deliberately NOT derived from the
    estate root and deliberately NOT stored on `PathBindings`: deriving it would mean guessing which
    document was loaded, and guessing wrong names the wrong machine's file in the one message that
    exists to send the reader to the right one.

    A miss raises `UnboundPathKeyError` naming the key, the document it was looked up in, and EVERY key
    that document does bind. The last of those is the part a reader acts on: the recovery is to add one
    binding to a document that was found, and an operator told only the key has been told the one fact
    they already had.

    Deliberately NOT caching or pre-computing anything here: the bindings are the caller's validated
    value, and this function must return what they bind at the moment it is asked.

    Not established: that the returned path exists on this machine, that it holds what the key's
    domain and project mean, or that the document was authored on the machine this run is on.
    """
    document = Path(document_path)
    bound = bindings.paths

    if key in bound:
        return Path(bound[key])

    bound_keys = ", ".join(sorted(str(bound_key) for bound_key in bound)) or "none"
    raise UnboundPathKeyError(
        f"path key '{key}' is not bound by the binding document {document}: the keys that document "
        f"binds are {bound_keys}. Add this key to that document, as a QUOTED dotted key under "
        "[paths] bound to an absolute path, to make it resolvable"
    )
