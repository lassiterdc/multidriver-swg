# Estate Bindings

Data lives on a particular machine — a dataset in a synced folder, a scratch directory on a
cluster — while the code that reaches it ships in a public repository. The toolkit resolves the
difference by naming rather than holding: code names a path by a logical key, and the path itself
comes from a binding document that the estate supplies. The estate is a separate, private
repository, so the document lives there and this repository carries none of its values.

A key the document does not bind is refused rather than defaulted, so a missing binding fails the
run instead of silently resolving to a machine-specific location.

This page is the contract that document must satisfy, and the operator step that creates it.

## Where the document lives

One binding document per machine, at `config/paths_{machine}.toml` beneath the estate root — the
directory the estate's own tree lives under.

`{machine}` is the name this machine is known by in the estate, and the filename is the only place
that name appears. The document carries no field naming its machine, and that is a decision rather
than an omission: a field would be a second statement of the same fact, and no surface could
compare the two without the toolkit adopting the estate's filename convention, so the second
statement could only ever agree or disagree silently. The consequence is worth stating plainly — a
document separated from its path, pasted into an issue or quoted in a log, loses its machine
association, and nothing recovers it.

## The document's two fields

A binding document is one TOML file. Its top level holds exactly two fields:

| Field | Form | What it carries |
|---|---|---|
| `rule` | the literal string `path-bindings:1` | The version header, which is what makes a later grammar change loud rather than silently misread by an older loader. |
| `paths` | A flat table of quoted dotted keys, each bound to an absolute path string | One binding per path key, for this machine. |

The shape of the file, with every value a placeholder rather than a binding:

```toml
rule = "path-bindings:1"

[paths]
"domain.project.kind" = "{the absolute path this machine holds for that key}"
```

Two things about that skeleton are load-bearing rather than conventional. Every key is written
quoted in full, and every value is an absolute path — both are discussed below.

## The key grammar

A `paths` key is a logical path key spelled `domain.project.kind`: exactly three dotted segments,
naming one domain, one project and one kind.

Each segment is a non-empty run of letters, digits, `_` and `-`. That charset is what stops a
segment carrying a separator, a dot (other than the two that separate segments) or a drive-letter
colon, and it is why an empty segment is refused. A count of segments alone would not be enough: a
key whose middle segment carries a separator still splits into three.

The middle — project — segment may not be the literal `shared`. A domain reserves `shared` for the
directories its projects share, so no estate project may take that name.

**The grammar checks shape, not vocabulary.** It consults no list of the estate's domains or
projects, so a well-formed key naming a domain or project this estate has never heard of satisfies
the grammar and is refused later, at lookup, as unbound. A misspelled domain segment is therefore
caught by the resolver rather than by the document, which is why an unbound-key refusal names both
the document and every key it does bind.

**Quote the whole dotted key, every segment of it.** Written unquoted inside the `paths` table, the
dots are TOML's own table separators, the entry nests into a tree rather than binding as one flat
entry, and every lookup would miss. The document refuses that nesting precisely so the miss reads
as a malformed document rather than as an absent binding.

## What the `paths` table is expected to hold

The table carries the machine-scope **roots** that a project's own configuration then builds on —
the couple of estate directories a run starts from.

It is not a catalogue of every path a run touches. Most file paths are defined in per-project
configuration files, and those files are where they belong; this table exists to name the
machine-scope roots they build on, and turning it into a second path catalogue is the misuse this
paragraph exists to rule out.

## What the document refuses

Every row below is a refusal, and every one of them stops the document from being read at all. A
partially-read binding map would leave a key reachable through a document that looked acceptable,
so the only safe report is a halt.

| Refused when | Why |
|---|---|
| The file is not valid TOML | There is nothing to read. |
| A top-level key other than `rule` or `paths` is present | An unnamed key is a binding its writer believes is live while nothing reads it. |
| `rule` is absent, or is not `path-bindings:1` | The version header is what makes a later grammar change loud. |
| `paths` is absent, or is not a table | Without it there is no key space for a lookup to read. |
| A `paths` key does not parse as exactly three dotted segments, or a segment does not satisfy the charset above | The key is `domain.project.kind`, and a segment-count check alone would admit a quoted key whose middle segment carries a separator. |
| A `paths` value is a table | This is the unquoted-dotted-key failure: the entry nested instead of binding, so the flat key space silently became a tree. Quote the whole dotted key. |
| A `paths` value is not text | A binding is the absolute path string this machine holds for this key, and nothing else. |
| A `paths` value is not absolute, or carries an unresolved placeholder token (`$`, `${`, `{`) | A relative binding resolves against whatever working directory a run happens to start in, so the same document would bind different paths in different runs. And a value that begins at a filesystem root while still carrying a token passes an is-absolute test and then fails at first open with an error naming the literal token, instead of failing here with the one message that names the fix. |

The placeholder refusal exists because a token that survives validation does not fail in the
document; it fails much later, at the first attempt to open the path, where the message names a
literal token rather than the document that carries it.

## The operator step

Nothing in the toolkit creates this document. The estate owns its values, and the toolkit refuses
rather than inventing one, so the document is created on the machine, by an operator, once.

1. **Locate the estate root** — the directory the estate's own tree lives under.

2. **Create the document** at `config/paths_{machine}.toml` beneath that root, with `{machine}`
   replaced by this machine's name in the estate. Write the two fields above: the `rule` header,
   and a `[paths]` table whose keys are quoted `domain.project.kind` spellings bound to absolute
   paths.

3. **Declare the new file in the estate repository, in the same commit.** The estate's own rule
   binds every change that adds a tracked path, and its tree test cannot see a file that is
   undeclared inside a directory that already has a layout row. So the commit that adds
   `config/paths_{machine}.toml` also adds a `present` row for it to that repository's README
   layout table.

Step 3 is not paperwork attached to step 2; without it the file is invisible to the estate's own
test, and the omission is silent. Creating the document and declaring it are one act, recorded in
one commit.
