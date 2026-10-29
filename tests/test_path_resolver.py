"""Check the path key's shape refusals: three dotted segments, each a charset run, and no project may be `shared`.

Each refusal is worth asserting only if it fires on the input a cheaper check would let through. The
cheaper check is a segment count, and it admits `a.b/c.d` — whose middle segment carries a separator
while the count is exactly three — so the charset refusal is asserted on that spelling rather than on
one a count check would also refuse.

The type also holds no list of legal domains, so a well-formed key naming nothing anyone has heard of
is ACCEPTED rather than refused. That arm is asserted too, because it is the one a toolkit-side
allowlist would break, and an allowlist would carry a private estate's vocabulary into the public
repository.

Every check names the plausible wrong implementation it is red under, because a check that cannot fail
for the reason it claims constrains nothing.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

from multidriver_swg.run_configuration.path_resolver import (
    BINDING_RULE,
    EstateNotConfiguredError,
    PathBindings,
    PathKey,
    PathResolutionError,
    UnboundPathKeyError,
    estate_root,
    load_bindings,
    resolve,
)


def test_two_segment_key_is_refused() -> None:
    """Property: a spelling that splits into two segments is not a key.

    Class: every string whose dotted split yields exactly two segments.
    Why it is required: a key addresses one domain, one project and one kind, so a two-segment
    spelling cannot name what a binding document would have to answer; accepting it would put a key
    into the loader that no lookup can reconstruct.
    What kills it: dropping the count check so `parse` unpacks its segments straight into the
    constructor, which turns a two-segment spelling into a TypeError at construction instead of a
    resolution refusal.
    Second correct implementation: one validating the whole dotted spelling against a single
    anchored pattern, rather than splitting and counting; it refuses `a.b` on the same grounds.
    """
    with pytest.raises(PathResolutionError):
        PathKey.parse("a.b")


def test_four_segment_key_is_refused() -> None:
    """Property: a spelling that splits into four segments is not a key.

    Class: every string whose dotted split yields more than three segments.
    Why it is required: `domain.project.kind` is the whole address; a fourth segment means the
    caller is spelling something else, and a permissive split would silently drop it.
    What kills it: the same unpack-without-counting implementation as the two-segment case, which
    raises TypeError rather than a resolution refusal on four segments.
    Second correct implementation: the single anchored-pattern spelling, which refuses `a.b.c.d`.
    """
    with pytest.raises(PathResolutionError):
        PathKey.parse("a.b.c.d")


def test_separator_inside_a_segment_is_refused_at_three_segments() -> None:
    """Property: a segment may not carry a separator, even when the segment count is exactly three.

    Class: every spelling whose three segment slots include one segment bearing a path separator.
    Why it is required: a segment-count check cannot see this case, because `a.b/c.d` splits into
    exactly three segments while its middle one carries a separator. If it were accepted, the key
    would carry a directory traversal into the path it resolves to.
    What kills it: replacing the per-segment charset check with a segment-count check alone — the
    named insufficient design — under which `a.b/c.d` is accepted and this assertion fails.
    Second correct implementation: the single anchored-pattern spelling, whose pattern admits no
    separator inside a segment.
    """
    with pytest.raises(PathResolutionError):
        PathKey.parse("a.b/c.d")


def test_empty_segment_is_refused() -> None:
    """Property: a segment may not be empty.

    Class: every spelling whose dotted split yields three slots with at least one empty.
    Why it is required: `a..b` addresses a project whose name is the empty string, which no estate
    project can have; accepting it makes the key a spelling of nothing while still looking like one.
    What kills it: a charset check written with `*` rather than `+`, or a count-only check, either of
    which admits the empty middle segment.
    Second correct implementation: the single anchored-pattern spelling, whose segment quantifier is
    one-or-more.
    """
    with pytest.raises(PathResolutionError):
        PathKey.parse("a..b")


def test_reserved_project_segment_is_refused() -> None:
    """Property: the project segment may not be `shared`.

    Class: every key whose middle segment is the reserved name.
    Why it is required: a domain reserves that name for the directories its projects share, so a
    project taking it would shadow the shared location rather than sit beside it.
    What kills it: deleting the reserved-name branch, under which `a.shared.c` constructs and this
    assertion fails.
    Second correct implementation: one applying the reservation inside the same per-segment loop as
    the charset check, rather than as a separate branch after it; it refuses the same key.
    """
    with pytest.raises(PathResolutionError):
        PathKey.parse("a.shared.c")


def test_reserved_name_is_allowed_outside_the_project_segment() -> None:
    """Property: `shared` in the domain or kind segment is accepted.

    Class: every key whose only occurrence of the reserved name is outside the project segment.
    Why it is required: the reservation is scoped to the project segment, because that is the
    position a project occupies. A check testing membership anywhere in the key would refuse names
    the ruling does not reserve, which is an over-refusal rather than a stricter guard.
    What kills it: widening the reserved check to test both other segments, under which
    `shared.a.b` and `a.b.shared` are refused.
    Second correct implementation: one comparing only the middle segment against the reserved name,
    however the comparison is spelled.
    """
    assert PathKey.parse("shared.a.b").domain == "shared"
    assert PathKey.parse("a.b.shared").kind == "shared"


def test_well_formed_key_naming_no_known_domain_is_accepted() -> None:
    """Property: a well-formed key is accepted even when its domain names nothing anyone recognises.

    Class: every spelling satisfying the grammar whose domain appears in no list this package holds.
    Why it is required: shape acceptance and estate binding are separate questions, and presence is
    asked at resolution against the binding document. A toolkit-side allowlist of domains would move
    that question into the public repository, which both publishes private vocabulary and makes every
    new estate domain a toolkit release.
    What kills it: refusing any domain outside a hardcoded set — the allowlist this type must not
    hold — under which this key is refused and the assertion fails.
    Second correct implementation: the single anchored-pattern spelling, which is indifferent to
    which domains exist because it checks shape only.
    """
    key = PathKey.parse("atlantis.nonexistent.raw_input")
    assert (key.domain, key.project, key.kind) == ("atlantis", "nonexistent", "raw_input")


# The binding-document half. Each check below is one row of the document's refusal set, and the set is
# CLOSED: a document that satisfies no row is a document this module accepts. The acceptance arm at the
# end is therefore load-bearing rather than decorative — every refusal check here is blind to an
# OVER-refusing implementation, and that is the only check that sees one.


def _written(tmp_path, body):
    """Write one binding document under its own name and return the path, so a check reads a real file."""
    path = tmp_path / "paths_test.toml"
    path.write_text(body, encoding="utf-8")
    return path


VALID_HEADER = 'rule = "path-bindings:1"\n'


def test_binding_document_that_is_not_toml_is_refused(tmp_path) -> None:
    """Property: a document that does not parse as TOML is refused, and the refusal names it.

    Class: every byte sequence that `tomllib` refuses, reached through the loader's own read.
    Why it is required: there is nothing to bind until a document has parsed, so every other row
    below presupposes this one; an unparsed document silently yielding no bindings would make an
    unbound key reachable through a document that looked acceptable.
    What kills it: letting the `tomllib.TOMLDecodeError` escape untranslated, or defaulting to an
    empty document on a parse failure. The check still reddens on the first and reddens on the
    second only because the assertion requires THIS module's error type.
    Second correct implementation: one that parses with a different TOML reader and translates that
    reader's parse error into the same refusal; it passes because the assertion turns on the error
    type and the named path, not on the parser's wording.
    """
    document = _written(tmp_path, 'rule = "path-bindings:1\n')
    with pytest.raises(PathResolutionError) as caught:
        load_bindings(document)
    assert str(document) in str(caught.value)


def test_unnamed_top_level_key_is_refused(tmp_path) -> None:
    """Property: a top-level key the form does not name is refused rather than ignored.

    Class: every document carrying a top-level key other than `rule` and `paths`, including the
    `machine` field this document deliberately does not carry.
    Why it is required: an ignored key is a binding its writer believes is live while nothing reads
    it, so silence here lets an operator maintain a value that has no effect.
    What kills it: iterating only the keys the form names, under which an extra key is invisible.
    Second correct implementation: one that refuses by comparing the document's key set against the
    two names in a single set operation; it passes because the assertion reads only the outcome.
    """
    document = _written(tmp_path, VALID_HEADER + 'paths = { "a.b.c" = "/abs/x" }\nmachine = "m"\n')
    with pytest.raises(PathResolutionError):
        load_bindings(document)


def test_rule_absent_or_wrong_version_is_refused(tmp_path) -> None:
    """Property: the version header must be present and must carry this form's version.

    Class: every document whose `rule` is absent, or is any string other than `path-bindings:1`.
    Why it is required: the header is what makes a later grammar change loud rather than silently
    misread by an older loader, so a document that omits it cannot be known to be this form.
    What kills it: reading `rule` only when present, or accepting any `rule` value.
    Second correct implementation: one comparing against a parsed version tuple rather than the
    literal string; it passes because both spellings refuse the same inputs.
    """
    with pytest.raises(PathResolutionError):
        load_bindings(_written(tmp_path, 'paths = { "a.b.c" = "/abs/x" }\n'))
    with pytest.raises(PathResolutionError):
        load_bindings(_written(tmp_path, 'rule = "path-bindings:2"\npaths = { "a.b.c" = "/abs/x" }\n'))


def test_paths_absent_or_not_a_table_is_refused(tmp_path) -> None:
    """Property: `paths` must be present and must be a table.

    Class: every document whose `paths` is absent, or is bound to a value that is not a table.
    Why it is required: without that table there is no key space at all, so a document missing it
    resolves nothing and every lookup would miss for a reason unrelated to the key asked for.
    What kills it: defaulting `paths` to an empty table, which turns a malformed document into a
    document that binds nothing and is refused only much later at the lookup.
    Second correct implementation: one that treats a missing `paths` as the same defect as a
    non-table `paths` and refuses both through a single branch; the assertions read only the outcome.
    """
    with pytest.raises(PathResolutionError):
        load_bindings(_written(tmp_path, VALID_HEADER))
    with pytest.raises(PathResolutionError):
        load_bindings(_written(tmp_path, VALID_HEADER + 'paths = "nope"\n'))


def test_paths_key_that_is_not_a_path_key_is_refused(tmp_path) -> None:
    """Property: every `paths` key must be a `PathKey`, so the document's keys are the key grammar's own.

    Class: every `paths` key that fails to parse as three dotted segments, every key a segment of
    which carries a character outside the segment charset, and every key whose project segment is
    the reserved `shared`.
    Why it is required: the key is `domain.project.kind`; a segment-count check alone admits a quoted
    key such as `"a.b/c.d"`, which splits into exactly three segments while its middle one carries a
    separator, so only the charset refuses it.
    What kills it: splitting the key and checking only the segment count — the insufficient design
    named above — under which `a.b/c.d` binds and this assertion fails.
    Second correct implementation: one validating the whole dotted spelling against a single anchored
    pattern with a back-reference for the reserved name; it refuses the same three spellings.
    """
    for written_key in ("a.b", "a.b/c.d", "a.shared.c"):
        with pytest.raises(PathResolutionError):
            load_bindings(_written(tmp_path, VALID_HEADER + 'paths = { "' + written_key + '" = "/abs/x" }\n'))


def test_paths_value_that_is_a_table_is_refused_and_asks_for_a_quoted_key(tmp_path) -> None:
    """Property: a table-valued binding is refused, and the refusal asks the writer to QUOTE the key.

    Class: every document binding a `paths` key to a table — in practice the unquoted dotted key,
    which TOML nests rather than flattening.
    Why it is required: the flat key space the resolver reads silently becomes a tree, so every
    lookup would miss for a reason the writer cannot see from the document. The message is asserted
    rather than only the refusal, because the refusal alone does not reach the writer's defect: with
    this row absent the same document is still refused, by the segment-count row, and the operator is
    told the key has one segment instead of being told to quote it.
    What kills it: removing the table check, under which the segment-count refusal fires first and
    the word "quote" is absent from the message.
    Second correct implementation: one that detects the nesting by comparing the document's key paths
    before flattening, and words the advice differently; it passes because the assertion looks for
    the word rather than for a sentence.
    """
    document = _written(tmp_path, VALID_HEADER + '[paths]\na.b.c = "/abs/x"\n')
    with pytest.raises(PathResolutionError) as caught:
        load_bindings(document)
    assert "quote" in str(caught.value).lower()


def test_paths_value_that_is_not_text_is_refused(tmp_path) -> None:
    """Property: a binding must be a string.

    Class: every `paths` value that is neither a string nor a table — an integer, a float, a boolean,
    a date and an array all reach this row.
    Why it is required: a binding is the absolute path string this machine holds for a key, and any
    other type would reach a lookup carrying a value the resolver never validated as a path.
    What kills it: checking only whether the value is a table, so an integer slips past into the map.
    Second correct implementation: one testing `isinstance(value, str)` positively and refusing on the
    complement; it refuses the same inputs.
    """
    with pytest.raises(PathResolutionError):
        load_bindings(_written(tmp_path, VALID_HEADER + 'paths = { "a.b.c" = 7 }\n'))
    with pytest.raises(PathResolutionError):
        load_bindings(_written(tmp_path, VALID_HEADER + 'paths = { "a.b.c" = ["/abs/x"] }\n'))


def test_paths_value_that_is_relative_or_carries_a_placeholder_is_refused(tmp_path) -> None:
    """Property: a binding must be absolute and must carry no unresolved placeholder token.

    Class: every relative value, and every value carrying `$`, `{` or `$`-before-`{` — including a
    value that BEGINS with a separator while still carrying a token, which passes an is-absolute test.
    Why it is required: a relative binding resolves against whatever working directory the caller runs
    in, so one document would bind different paths in different runs; and a token-bearing absolute
    value fails later at first open with an error naming the literal token instead of failing here.
    What kills it: deleting the token scan, under which all three token spellings are accepted; the
    absolute value `"/abs/${HOME}/x"` is the arm that proves the scan is not the absolute check.
    Second correct implementation: one testing absoluteness before the token scan and using a compiled
    pattern for the tokens; it refuses the same values.
    """
    for value in ("rel/x", "/abs/$(date)", "/abs/{name}", "/abs/${HOME}/x"):
        with pytest.raises(PathResolutionError):
            load_bindings(_written(tmp_path, VALID_HEADER + 'paths = { "a.b.c" = "' + value + '" }\n'))


def test_hand_built_bindings_are_checked_as_hard_as_a_loaded_document() -> None:
    """Property: every row but the TOML-parse row fires on a hand-built instance too.

    Class: every document object constructed without the loader — the back door around it.
    Why it is required: the loader's refusals buy nothing if a caller can build the type directly and
    reach a lookup with an unvalidated path, and the TOML-parse row is the one row a constructor
    cannot own, because a document must parse before an object exists to carry the rest.
    What kills it: validating inside `load_bindings` and constructing the type bare, under which the
    extra-key and table-value inputs below both construct and this assertion fails.
    Second correct implementation: one validating in the loader and delegating the same checks from a
    classmethod constructor; it passes because the type refuses the same instances.
    """
    PathBindings({"rule": BINDING_RULE, "paths": {"a.b.c": "/abs/x"}})
    for document in (
        {"rule": BINDING_RULE, "paths": {}, "machine": "m"},
        {"rule": BINDING_RULE, "paths": {"a.b.c": {"nested": 1}}},
        {"rule": BINDING_RULE, "paths": {"a.b.c": "rel"}},
    ):
        with pytest.raises(PathResolutionError):
            PathBindings(document)


def test_a_valid_document_is_accepted_and_its_binding_is_keyed_by_path_key(tmp_path) -> None:
    """Property: a document that satisfies every row is accepted, and its bindings are `PathKey`-keyed.

    Class: every document satisfying the closed set — the acceptance arm, which no refusal check above
    can supply, because each of them passes under an implementation that refuses everything.
    Why it is required: the reserved project name is scoped to the middle segment, so `shared` in the
    domain or in the kind is a legal binding; an implementation that reserved the name anywhere would
    refuse documents this form admits, and that over-refusal is invisible to every refusal check.
    What kills it: refusing any key that mentions `shared`, or storing the bindings under their dotted
    spelling so a key that was never parsed can reach a lookup.
    Second correct implementation: one that keeps the document's spelling and parses on lookup; it
    passes because indexing by `PathKey` and requesting `str(key)` address the same entry here.
    """
    document = _written(
        tmp_path,
        VALID_HEADER + '[paths]\n"shared.a-b.c_d" = "/abs/one"\n"n.p.shared" = "/abs/two"\n',
    )
    bindings = load_bindings(document)
    assert bindings.rule == BINDING_RULE
    assert bindings.paths[PathKey.parse("shared.a-b.c_d")] == "/abs/one"
    assert bindings.paths[PathKey.parse("n.p.shared")] == "/abs/two"


def test_loader_requires_its_document_argument() -> None:
    """Property: the loader derives no document of its own, so the argument is required.

    Class: the zero-argument call — the shape a default document location would make legal.
    Why it is required: a default would be a toolkit-side well-known location, so an unconfigured
    estate would resolve against whatever ships in the wheel instead of failing loudly, which is the
    one failure the ruling this module implements exists to remove.
    What kills it: giving the parameter a default, under which the call succeeds and this assertion
    fails on a missing exception rather than on a wrong value.
    Second correct implementation: one requiring the document as a keyword-only argument; it passes
    because the zero-argument call is refused identically.
    """
    with pytest.raises(TypeError):
        load_bindings()


# The root-location and lookup half. Every check below is HELD rather than red: each asserts a
# property that holds on the current tree, so its value is that a later change which breaks the
# property reddens it. A held check that no named change could redden is not a guard, so each names
# the mutation it is red under.


def test_estate_root_returns_the_explicit_root_it_is_given() -> None:
    """Property: an explicit root is returned unchanged, and no state survives from one call to the next.

    Class: every absolute root a caller passes, taken one at a time so a second call cannot inherit the
    first.
    Why it is required: the explicit argument is now the ONLY route, and the failure this guards is an
    implementation that answers from somewhere other than what the caller named — a default location,
    or a value cached from an earlier call. Either one satisfies a single call whenever the argument
    happens to agree with it, which is why two DIFFERENT roots are asserted rather than one.
    What kills it: answering from a module-level default instead of the argument, or memoising the
    first call's result; the second assertion then returns the first root and fails.
    Second correct implementation: one returning a `Path` for a `Path` argument and wrapping a `str`
    argument before returning it; it passes because the same two values come back for these two inputs.
    """
    assert estate_root("/estate/explicit") == Path("/estate/explicit")
    assert estate_root(Path("/estate/second")) == Path("/estate/second")


def test_estate_root_refuses_a_call_that_carries_no_root() -> None:
    """Property: a call carrying no root raises, and the refusal names the explicit-argument route.

    Class: every call whose root argument is absent — with one route, that is the only way to supply a
    root at all.
    Why it is required: the run configuration is what names this machine's estate, so a call that
    returned anything here would be the silent default the module exists to remove; and a reader told
    only that nothing was found has no act available to them, which is why the message has to name the
    one route there is.
    What kills it: deleting the raise and returning a location, or giving the argument a real default;
    the assertion then fails on a missing exception rather than on a wrong value.
    Second correct implementation: one holding the message in a module constant and rendering it from
    there; it passes because the assertion reads the same two substrings of the text a reader sees.
    """
    with pytest.raises(EstateNotConfiguredError) as caught:
        estate_root()
    message = str(caught.value)
    assert "explicit" in message
    assert "estate_root(" in message


def test_an_unbound_key_is_refused_naming_the_key_the_document_and_every_bound_key(tmp_path) -> None:
    """Property: a miss names the key, the document, and EVERY key the document binds.

    Class: every pair where the key satisfies the grammar and the document does not bind it.
    Why it is required: the recovery is to add one binding to a document that was found, so the
    enumeration is the part a reader acts on. A refusal naming only the key hands back the one fact
    the reader already had, and a message listing some of the bound keys reads as the whole set.
    What kills it: reporting only the missing key, or enumerating the bindings from a subset; the
    assertion requires both bound spellings and the document path, so either narrowing reddens it.
    Second correct implementation: one carrying the sorted bindings on the exception as an attribute
    and rendering the message from that attribute; it passes because the same spellings appear.
    """
    document = tmp_path / "paths_test.toml"
    document.write_text(
        VALID_HEADER + '[paths]\n"norf.demo.raw_input" = "/estate/raw"\n"norf.demo.outputs" = "/estate/out"\n',
        encoding="utf-8",
    )
    bindings = load_bindings(document)
    with pytest.raises(UnboundPathKeyError) as caught:
        resolve(PathKey.parse("atlantis.absent.raw_input"), bindings, document)
    message = str(caught.value)
    assert "atlantis.absent.raw_input" in message
    assert str(document) in message
    assert "norf.demo.raw_input" in message
    assert "norf.demo.outputs" in message


def test_a_bound_key_resolves_to_the_path_the_document_binds(tmp_path) -> None:
    """Property: a bound key returns its binding, through the loader a caller actually uses.

    Class: every pair where the document binds the key to an absolute path string.
    Why it is required: this is the acceptance arm, and no refusal check can supply it — every
    assertion above passes under an implementation that raises on every lookup. It also exercises
    the composition a caller takes, a document on disk read by the loader and then resolved, rather
    than a hand-built value, so the loader and the lookup are shown to agree on the key space.
    What kills it: returning a path derived from the key rather than the binding, or raising on
    every key; the assertion compares the returned path to the binding the document states.
    Second correct implementation: one storing bindings as `Path` objects rather than strings; it
    returns an equal path on the same input.
    """
    document = tmp_path / "paths_test.toml"
    document.write_text(VALID_HEADER + '[paths]\n"norf.demo.raw_input" = "/estate/raw"\n', encoding="utf-8")
    bindings = load_bindings(document)
    assert resolve(PathKey.parse("norf.demo.raw_input"), bindings, document) == Path("/estate/raw")


# The package surface. Every check above imports the resolver module directly, so all of them stay
# green under a package that re-exports nothing; this closing section is the only check that reads
# the package a sibling actually imports from.
_PACKAGE_REEXPORTS = {
    "PathKey": "path_resolver",
    "PathBindings": "path_resolver",
    "load_bindings": "path_resolver",
    "estate_root": "path_resolver",
    "resolve": "path_resolver",
    "PathResolutionError": "path_resolver",
    "UnboundPathKeyError": "path_resolver",
    "EstateNotConfiguredError": "path_resolver",
    "ProvenanceError": "provenance",
    "ImplementationIdentity": "provenance",
    "IdentityDeclaration": "provenance",
    "ProvenanceRecord": "provenance",
    "capture": "provenance",
    "require_complete": "provenance",
    "SeededCallable": "seed_contract",
    "SeedGenerator": "seed_contract",
    "require_seed": "seed_contract",
}


def test_the_package_reexports_the_resolution_surface_and_keeps_the_two_earlier_ones() -> None:
    """Property: every name the package promises is importable from it, listed in `__all__`, and its module's own object.

    Class: every public name in the package's re-export set — the eight path-resolution names, the
    provenance refusal, and the eight names the package re-exported before the resolution surface
    existed. It is one-way: a later chunk that ADDS a name is outside it, so only a removal or a
    shadowing reddens it.
    Why it is required: the package docstring tells a sibling to import these from the package, so an
    absent name fails at that sibling's import time and nowhere in the module-level checks above,
    which all import `path_resolver` directly. A `__all__` entry with no matching attribute is the
    same failure one step later, at `from ... import *`.
    What kills it: dropping one name from the package's import block or from `__all__`, or defining a
    duplicate object locally in the package; the absent, unlisted and shadowed assertions each
    redden on the name they lose, and the shadowed one compares identity rather than the name.
    Second correct implementation: a package that re-exports lazily through a module-level
    `__getattr__`; it passes because the attribute still resolves to the object its own module
    defines.
    """
    package = importlib.import_module("multidriver_swg.run_configuration")
    absent = sorted(name for name in _PACKAGE_REEXPORTS if not hasattr(package, name))
    unlisted = sorted(name for name in _PACKAGE_REEXPORTS if name not in package.__all__)
    shadowed = sorted(
        name
        for name, module_name in _PACKAGE_REEXPORTS.items()
        if getattr(package, name, None)
        is not getattr(importlib.import_module(f"multidriver_swg.run_configuration.{module_name}"), name)
    )
    assert absent == []
    assert unlisted == []
    assert shadowed == []


# The repository scans. These checks assert properties of the TREE rather than of the resolver, so they
# read files instead of calling into the module; they live in this file because no other test module
# owns the published trees. They follow the package-surface check rather than interrupting it, and that
# check remains the only one in this file that reads the package a sibling imports from.
#
# Each arm is spelled ONCE below, and every scan, every control and every exclusion is built from those
# same constants, so a control cannot silently exercise an arm a scan no longer carries. THE
# ALTERNATION PIPES ARE BARE, and that is load-bearing rather than cosmetic: in `grep -E`, and in
# Python's `re`, a backslash before a pipe makes the pipe LITERAL, so an escaped alternation matches one
# string containing pipe characters and can never fire on any path. An escaped or misspelled arm leaves
# a pattern that still matches nothing, so a control exercising ONE arm cannot detect the other arms
# going dead — which is why every (scan, arm) membership below carries its own must-fire control.
_DRIVE_LETTER_ARM = r'"[A-Za-z]:[\\/]'
_QUOTED_HOME_ARM = r'"/home/'
_QUOTED_USERS_ARM = r'"/Users/'
_QUOTED_MNT_ARM = r'"/mnt/'
_HOME_ARM = r"/home/"
_USERS_ARM = r"/Users/"
_MNT_ARM = r"/mnt/"
_SYNCED_FOLDER_TOKEN = r"Dropbox"

# The package arm set: a QUOTED absolute path literal, which is how one would be written into Python
# source. Kept as the tuple beside the compiled scan because every control and every exclusion below is
# built from it, so a control cannot drift from the arm it guards.
_PACKAGE_ARMS = (_DRIVE_LETTER_ARM, _QUOTED_HOME_ARM, _QUOTED_USERS_ARM, _QUOTED_MNT_ARM)
_PACKAGE_SCAN = re.compile("|".join(_PACKAGE_ARMS))

# The docs arm set: the same drive-letter arm, then the path prefixes WITHOUT a leading quote, because a
# page carries one inside a YAML value or in running prose as often as inside a quoted string, plus the
# synced-folder name, which is a private token rather than a path.
_DOCS_ARMS = (_DRIVE_LETTER_ARM, _HOME_ARM, _USERS_ARM, _MNT_ARM, _SYNCED_FOLDER_TOKEN)
_DOCS_SCAN = re.compile("|".join(_DOCS_ARMS))

_REPO_ROOT = Path(__file__).resolve().parents[1]

# The one tracked file in this repository that carries a real data root — the frozen legacy corpus's
# hardcoded one — and so the only drive-letter input this corpus offers as a control.
_LEGACY_DATA_ROOT = Path("_old_code_to_refactor") / "_inputs.py"

# The file whose own `_src_path` value carries a home-directory path, and which carries the synced-folder
# token nowhere — the property that makes it an arm-attributable control.
_COPIER_ANSWERS = Path(".copier-answers.yml")


def _scanned_and_matched(root: Path, pattern: re.Pattern[str]) -> tuple[list[str], list[tuple[str, int, str]]]:
    """Every UTF-8-decodable file under `root`, and every line among them that `pattern` matches.

    The population is returned alongside the matches because a zero-match check over an ABSENT tree and
    one over a clean tree return the same empty list, and only the population tells the two apart — so
    each zero arm asserts its population is non-empty before it asserts the absence.

    A file that does not decode as UTF-8 is skipped, and the narrowing has a stated ground: a compiled
    `.pyc` is a derived copy of a source file this scan already reads, so a literal it carried would be
    carried by that source too, while decoding binary with replacement characters would let a byte
    sequence edit itself into a match no author wrote.

    `root` may lie OUTSIDE the repository — a temporary directory holding a written control input — and
    then a path has no repo-relative spelling, so its absolute one is reported as the label instead.
    """
    scanned: list[str] = []
    matched: list[tuple[str, int, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        try:
            relative = path.relative_to(_REPO_ROOT).as_posix()
        except ValueError:
            relative = path.as_posix()
        scanned.append(relative)
        for number, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                matched.append((relative, number, line.strip()))
    return scanned, matched


def test_the_package_carries_no_absolute_path_literal() -> None:
    """Property: no file under `src/` carries an absolute path literal.

    Class: every UTF-8-decodable file under `src/` and every line of each — stated as a tree rather than
    as one file, so a literal added to a module this change never touched is caught by the same check.
    Why it is required: a machine-specific absolute path in the published package publishes a private
    layout and bakes one machine's arrangement into every machine's copy. A resolver removes only the
    paths that route through it, so a literal outside that routing is the one failure a resolver that
    works can still leave behind.
    What kills it: writing an absolute path literal into any module under `src/`. The assertion reports
    each offending file, line number and line, so a failure names the literal instead of only a count.
    Second correct implementation: shelling out to `grep -rnE` over the same tree and reading its exit
    status; it reports the same absence over the same population.
    Not established: that a path assembled at run time from separately written fragments is absent.
    This scan reads literals, and a path concatenated out of parts carries no literal to find.
    """
    scanned, matched = _scanned_and_matched(_REPO_ROOT / "src", _PACKAGE_SCAN)
    assert scanned, "the scan population is empty: an absent tree returns no matches and is not a clean one"
    assert matched == []


def test_the_docs_tree_carries_no_absolute_path_or_private_token() -> None:
    """Property: no file under `docs/` carries an absolute path prefix or a private sync-folder name.

    Class: every UTF-8-decodable file under `docs/` and every line of each, which includes any page later
    added to that tree.
    Why it is required: the docs tree is published with the package, so a page that illustrated a
    binding with a real one would publish the layout and the account name the private token names.
    Describing a value's form needs no example value, so no illustrative absolute path is owed — and
    this check is what keeps one out once pages about machine-specific bindings are added.
    What kills it: adding an illustrative absolute path — a drive letter, a home directory, a
    synced-folder name — to any page under `docs/`. The assertion reports the file, the line number and
    the line.
    Second correct implementation: shelling out to `grep -rnE` over the same tree and reading its exit
    status; it reports the same absence over the same population.
    """
    scanned, matched = _scanned_and_matched(_REPO_ROOT / "docs", _DOCS_SCAN)
    assert scanned, "the scan population is empty: an absent tree returns no matches and is not a clean one"
    assert matched == []


def test_the_drive_letter_arm_fires_on_the_legacy_corpus_data_root() -> None:
    """Property: the package scan's drive-letter arm fires on a line that carries a drive-letter path.

    Class: the arm text `_PACKAGE_SCAN` itself splices in, exercised against the frozen legacy corpus's
    hardcoded data root, which is the one piece of real data-root text this repository still carries.
    Why it is required: the package scan is a ZERO-MATCH check, and a pattern that cannot fire and a
    tree that carries nothing return the same zero, so without this control the absence above certifies
    nothing. It is needed twice over for THIS arm, because the obvious control for a drive-letter path
    also carries a sync-folder alternative — `Dropbox` — and that fires on this same line, so the paired
    command run verbatim would show the instrument alive while saying nothing about the drive-letter
    arm. This check therefore exercises the composed pattern, which carries no `Dropbox`, and then the
    arm alone.
    What kills it: joining the arms with an ESCAPED pipe, which turns the alternation into one literal
    string and reddens both assertions; or narrowing the arm's separator class, under which a
    slash-separated drive path no longer matches.
    Second correct implementation: `grep -nE` over the same file with the same arm text, reading its
    matched line; it reports the same single line.
    """
    text = (_REPO_ROOT / _LEGACY_DATA_ROOT).read_text(encoding="utf-8")
    composed = [line for line in text.splitlines() if _PACKAGE_SCAN.search(line)]
    matched = [line for line in text.splitlines() if re.search(_DRIVE_LETTER_ARM, line)]
    assert composed != []
    assert len(matched) == 1
    assert "D:/" in matched[0]


def test_the_home_directory_arm_fires_where_the_synced_folder_token_cannot() -> None:
    """Property: the docs scan's home-directory arm fires on a line no other arm of that scan reaches.

    Class: the arm text `_DOCS_SCAN` itself splices in, exercised against the one tracked file in this
    repository whose value carries a home-directory path.
    Why it is required: the docs scan is a zero-match check, so it needs a control, and this input was
    chosen for a property the drive-letter control cannot claim. Pairing the arm with a sync-folder
    alternative makes the match ambiguous by construction — whereas this file carries that token
    nowhere, which makes its firing attributable to the home arm and to no other alternative in the
    pattern. A control must vary the pattern arm it guards rather than re-use a branch another arm
    already covers.
    What kills it: deleting the home arm from `_DOCS_ARMS`, under which nothing here fires and both
    assertions red; or adding the synced-folder name to this file, under which the attribution
    assertion reds even though the scan still fires.
    Second correct implementation: `grep -nE '/home/'` over the same file, reading its matched line; it
    reports the same single line.
    """
    text = (_REPO_ROOT / _COPIER_ANSWERS).read_text(encoding="utf-8")
    composed = [line for line in text.splitlines() if _DOCS_SCAN.search(line)]
    matched = [line for line in text.splitlines() if re.search(_HOME_ARM, line)]
    assert composed != []
    assert len(matched) == 1
    assert [line for line in text.splitlines() if re.search(_SYNCED_FOLDER_TOKEN, line)] == []


# The memberships no tracked file in this repository can exercise on its own, each given an input WRITTEN
# for it rather than found. The samples are synthetic on purpose: a control's job is to show that its arm
# fires, and writing a value is cheaper and more portable than hunting the corpus one arm at a time.
# `docs-scan-drive-letter` is here even though the drive-letter ARM has a corpus control, because that
# control fires under the package scan: an arm being live says nothing about a second scan that splices
# it, and that second membership is what a control at this granularity has to reach.
_WRITTEN_MEMBERSHIPS = (
    (_PACKAGE_ARMS, _QUOTED_HOME_ARM, 'DATA_ROOT = "/home/alice/estate"'),
    (_PACKAGE_ARMS, _QUOTED_USERS_ARM, 'DATA_ROOT = "/Users/alice/estate"'),
    (_PACKAGE_ARMS, _QUOTED_MNT_ARM, 'DATA_ROOT = "/mnt/scratch/estate"'),
    (_DOCS_ARMS, _DRIVE_LETTER_ARM, 'DATA_ROOT = "D:/corpus/estate"'),
    (_DOCS_ARMS, _USERS_ARM, "The estate lives at /Users/alice/estate on a laptop."),
    (_DOCS_ARMS, _MNT_ARM, "Mounted at /mnt/scratch/estate for the night."),
    (_DOCS_ARMS, _SYNCED_FOLDER_TOKEN, "Synced through Dropbox every night."),
)

_WRITTEN_MEMBERSHIP_IDS = (
    "package-scan-quoted-home",
    "package-scan-quoted-users",
    "package-scan-quoted-mnt",
    "docs-scan-drive-letter",
    "docs-scan-users",
    "docs-scan-mnt",
    "docs-scan-synced-folder-token",
)


@pytest.mark.parametrize(("arms", "arm", "sample"), _WRITTEN_MEMBERSHIPS, ids=_WRITTEN_MEMBERSHIP_IDS)
def test_every_remaining_membership_fires_on_a_written_known_bearing_input(tmp_path, arms, arm, sample) -> None:
    """Property: each remaining (scan, arm) membership fires on an input written to carry that arm.

    Class: the seven memberships no tracked file in this repository can exercise on its own — the three
    package arms other than the drive letter, and the docs scan's drive-letter, users, mnt and token
    arms — each driven by a line written into a temporary file, so the input is constructed rather than
    found.
    Why it is required: every arm here belongs to a zero-match instrument, and a dead arm and a clean
    tree are the same zero. A single control over one arm cannot vouch for an alternation: an arm whose
    text is misspelled, or whose pipe is escaped, leaves a pattern that matches nothing while the other
    arms' controls stay green. So each membership is exercised on its own, and the exclusion asserts
    that removing THAT arm stops the match — which is what makes the membership load-bearing rather
    than merely present.
    What kills it: deleting the arm from its scan's arm tuple, or respelling it so it no longer matches
    — a narrowed separator class, a missing quote, a different token. The arm-alone and composed
    assertions red on the respelling, and the exclusion reds if the sample stops isolating its arm.
    Second correct implementation: shelling out to one `grep -E` per arm over the written file and
    reading each exit status; the same three verdicts come back.
    """
    document = tmp_path / "written_sample.txt"
    document.write_text(sample + "\n", encoding="utf-8")

    _, arm_alone = _scanned_and_matched(tmp_path, re.compile(arm))
    _, composed = _scanned_and_matched(tmp_path, re.compile("|".join(arms)))
    remaining = "|".join(other for other in arms if other != arm)
    _, without_this_arm = _scanned_and_matched(tmp_path, re.compile(remaining))

    assert len(arm_alone) == 1
    assert len(composed) == 1
    assert without_this_arm == []


# The arm-by-arm control, closed without a corpus input. A zero-match scan needs a must-fire control
# for EVERY arm it splices in, because a control that exercises one arm is silent about the rest —
# and an arm that has gone dead keeps returning the same zero a clean tree returns. The comment above
# records that only two arms have a real bearing file in this tree; that is a limit of the CORPUS and
# not of the instrument, because a control needs a KNOWN-BEARING INPUT and one can be written as
# readily as found. Each arm below is therefore exercised both ways: a bearing probe must fire it, and
# a benign probe that differs in the property the arm encodes must not — a liveness assertion alone is
# satisfied by a pattern that fires on everything.
_ARM_PROBES: tuple[tuple[str, str, str], ...] = (
    ("_DRIVE_LETTER_ARM", '"D:/corpus"', '"D:corpus"'),
    ("_QUOTED_HOME_ARM", '"/home/an_operator/x"', "/home/an_operator/x"),
    ("_QUOTED_USERS_ARM", '"/Users/an_operator/x"', "/Users/an_operator/x"),
    ("_QUOTED_MNT_ARM", '"/mnt/an_operator/x"', "/mnt/an_operator/x"),
    ("_HOME_ARM", "/home/an_operator/x", "/estate/an_operator/x"),
    ("_USERS_ARM", "/Users/an_operator/x", "/estate/an_operator/x"),
    ("_MNT_ARM", "/mnt/an_operator/x", "/estate/an_operator/x"),
    ("_SYNCED_FOLDER_TOKEN", "sync/Dropbox/x", "sync/other/x"),
)

# Which arm each composed pattern splices in. Spelled out rather than inferred, because "spliced from
# the same constants" is only a property of an arm if that arm is genuinely one of the spliced set.
_COMPOSED_MEMBERS: tuple[tuple[re.Pattern[str], tuple[str, ...]], ...] = (
    (_PACKAGE_SCAN, ("_DRIVE_LETTER_ARM", "_QUOTED_HOME_ARM", "_QUOTED_USERS_ARM", "_QUOTED_MNT_ARM")),
    (_DOCS_SCAN, ("_DRIVE_LETTER_ARM", "_HOME_ARM", "_USERS_ARM", "_MNT_ARM", "_SYNCED_FOLDER_TOKEN")),
)


def test_every_spliced_arm_fires_on_a_known_bearing_probe() -> None:
    """Property: every arm constant, and every composed pattern that splices it, fires on a bearing probe.

    Class: each of the eight arm constants, and each of the nine (pattern, arm) memberships the two
    composed patterns declare.
    Why it is required: each zero-match scan needs a must-fire control for every arm it splices in. A
    control exercising one arm is silent about the others, and an arm that has gone dead returns the
    same zero a clean tree returns, so the absence of a control for an arm is invisible until that arm
    stops working. The corpus supplies a real bearing file for only two arms, which leaves the other
    six able to go dead unnoticed.
    What kills it: a typo'd or narrowed arm regex, under which that arm's bearing assertion reddens;
    and joining either composed pattern's arms with an ESCAPED pipe, which turns the alternation into
    one literal string and reddens the composed assertion for every member while the per-arm
    assertions stay green.
    Second correct implementation: reading a real bearing file per arm; it passes wherever this corpus
    carries one, and this check deliberately does not depend on that.
    """
    for name, bearing, benign in _ARM_PROBES:
        arm = globals()[name]
        assert re.search(arm, bearing), f"{name} is dead: it does not fire on {bearing!r}"
        assert not re.search(arm, benign), f"{name} over-fires: it matches {benign!r}"

    probes = {name: bearing for name, bearing, _ in _ARM_PROBES}
    for composed, members in _COMPOSED_MEMBERS:
        for name in members:
            assert composed.search(probes[name]), (
                f"{name}'s bearing probe does not fire under the composed pattern, so that arm is not live in it"
            )
