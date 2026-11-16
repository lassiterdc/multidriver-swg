# Changelog

## [Unreleased]

### Added

- A release now runs the whole test suite at the tagged commit and refuses the tag unless that run was whole, complete and passing on `main`, with both source files recording the tagged version; nothing is built or uploaded until that gate passes.
