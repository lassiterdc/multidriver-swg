"""Check that Claude agent files are consistent with source files.

This script is intended to be run as a pre-commit hook. Populate
AGENT_MAPPING and CLAUDE_MD_TRIGGERS with project-specific mappings
before enabling.

Pre-commit hook configuration (in .pre-commit-config.yaml):
    - repo: local
      hooks:
        - id: check-claude-docs
          name: Check Claude doc freshness
          entry: python scripts/check_doc_freshness.py
          language: python
          pass_filenames: false
          always_run: true
"""

# TODO: populate with project-specific mappings
# Maps agent files to the source files they describe.
# If any source file is newer than the agent file, the hook fails.
AGENT_MAPPING: dict[str, list[str]] = {
    # "path/to/agent.md": ["src/module.py", "src/other.py"],
}

# TODO: populate with project-specific mappings
# Maps CLAUDE.md sections to the source files that trigger a freshness warning.
CLAUDE_MD_TRIGGERS: dict[str, list[str]] = {
    # "Section heading": ["src/module.py"],
}


def main() -> None:
    """Run freshness checks."""
    # TODO: implement checks using AGENT_MAPPING and CLAUDE_MD_TRIGGERS
    pass


if __name__ == "__main__":
    main()
