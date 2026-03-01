# Usage

## Getting started

Import the package and call the sample function:

```python
import multidriver_swg

result = multidriver_swg.hello("world")
print(result)  # Hello, world!
```

## Development workflow

This project follows a plan-then-implement workflow:

```mermaid
sequenceDiagram
    participant D as Developer
    participant C as Claude Code
    participant P as Planning Doc

    D->>C: Describe task
    C->>P: Write implementation plan
    P-->>D: Review and approve
    D->>C: @.prompts/proceed_with_implementation.md
    C->>C: Preflight check
    D->>C: Approve
    C->>C: Implement
    C->>C: @.prompts/qaqc_and_commit.md
    C-->>D: QA report + commit
```
