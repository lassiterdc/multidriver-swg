"""The stub body: a rule that computes nothing, writes its declared outputs, and persists its identity.

The graph's twelve rules exist so the run's shape is real before any system computes: the engine orders
them by their declared edges, each job produces its declared artifacts, and every job records which system
produced them. A stub is therefore not an empty body — it is the body that makes those three facts true
without computing anything.

The identity's `system` comes from the rule's `params`, never from the loop variable the workflow file is
rendering with. A rendered body runs when its job runs, which is after the rendering loop has finished, so
a body that read the loop's row would read the LAST row every time and every rule would claim the last
system's identity. The workflow file binds the row's fields into `params` at rendering time; this module
takes them as arguments and knows nothing about loops.

`implementation` is the literal `stub`, so a run of the graph says of every artifact that no compute system
produced it. `code_version` is the running package's own commit, read through the revision gate, so a
reader of any artifact can tell which checkout's stubs wrote it.

The declared output's CONTENT is deliberately empty: the stub computes nothing, and the artifact's
existence is what the graph orders on and what a later system's plan replaces. Writing a plausible payload
here would be an invented result that a reader could mistake for one.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from multidriver_swg.run_configuration.provenance import ImplementationIdentity, persist_identity
from multidriver_swg.run_configuration.revision_gate import running_revision
from multidriver_swg.workflow.graph import IDENTITY_FILE

# The implementation name every stub records. A literal rather than a parameter: the stub is one
# implementation for every rule, and a per-rule spelling would let two stubs claim different code.
STUB_IMPLEMENTATION = "stub"


def write_stub_outputs(rule_name: str, system: str, declared_outputs: Iterable[str]) -> None:
    """Create each declared output empty and persist `system`'s stub identity beside them.

    `declared_outputs` are the paths the engine handed the rule — the row's `outputs` under the rule's own
    directory — and they are written exactly as given, because the engine already decided where they live.
    Their parent directory is created here for the identity file's sake as well: the identity is not a
    declared output, so no engine step guarantees its directory.

    The identity is written through the provenance module's persisted form, so a reader that knows the
    form reads stubs' records with the same function as a compute system's.

    Not established: that `rule_name` is a row of the graph or that `system` owns it. The workflow file
    binds both from the row it renders, and a hand-called stub is the caller's to get right.
    """
    for output in declared_outputs:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")

    identity_path = Path(rule_name) / IDENTITY_FILE
    identity_path.parent.mkdir(parents=True, exist_ok=True)
    persist_identity(
        identity_path,
        ImplementationIdentity(
            system=system,
            implementation=STUB_IMPLEMENTATION,
            code_version=running_revision(),
        ),
    )
