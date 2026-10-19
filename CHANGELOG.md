# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-19

### Added

- Templates built from a seed command (`template init --seed`) or copied from a live database (`template init --from-url`).
- Template refresh that builds a new copy on the side and swaps it in (`template refresh`).
- Clones made with APFS `clonefile`, each running its own Postgres on a free local port.
- TTL expiry and parent-exit reaping: clones are removed when their time is up, and clones made through MCP are removed when the agent session exits.
- Crash reconcile: after a daemon crash, running clones are found again and stale ones are cleaned up.
- `mitosis gc` to remove leftovers.
- Daemon that starts itself on first use.
- MCP server (`mitosis mcp`) with the tools `clone_create`, `clone_list` and `clone_destroy`.
- Configurable limits through `MITOSIS_MAX_CLONES` and `MITOSIS_MIN_FREE_DISK`.
- Benchmark script, `scripts/bench.sh`.

[Unreleased]: https://github.com/irfanfaraaz/mitosis/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/irfanfaraaz/mitosis/releases/tag/v0.1.0
