# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - Unreleased

### Added

- Template generations: `template history`, `template rollback [--to N]`, and `--keep` to prune old ones.
- Template settings (`template set`, also on `template init`): `--every`, `--mode`, `--migrate`, `--auto-migrate`, `--on-stale`.
- Layered refresh (`template refresh --layered`): copy the current generation and run only the migrate command.
- A scheduler in the daemon for scheduled refreshes, auto-migrate and stale-while-refresh.
- `mitosis promote <clone> [--template <name>]`: make a clone's data the newest generation.
- `mitosis template upgrade <name> --to <major> [--as <name>]` with `pg_upgrade --clone` and a `--copy` fallback. Tested on Postgres 18.
- Named clones with stable URLs: reserved port and password per name, `--fresh`, `--protect`, `destroy --force`, `destroy --forget`.
- `.env` injection: `--env-file` and `--env-var` on `clone`, `mitosis env`, exact restore on destroy, a warning when the file is not git-ignored.
- RAM and disk budgets, a FIFO queue with `--wait`, the `budget_full` error code, and `mitosis top`.
- `<home>/config.toml`, `mitosis config` and `mitosis config set`, with new env vars `MITOSIS_RAM_BUDGET`, `MITOSIS_DISK_BUDGET`, `MITOSIS_QUEUE_WAIT` and `MITOSIS_CLONE_DISK_WARN`.
- Login service: `mitosis service install`, `uninstall` and `status` (launchd user agent).
- Reboot and crash recovery for named, protected and parentless clones, and suspect-clone detection.
- `mitosis doctor [--fix] [--json]`.
- MCP `clone_create` gains `name`, `fresh`, `env_file`, `env_var` and `wait_secs`; its reply gains `generation`, `existing`, `stale`, `refreshing`, `suspect`, `duration_ms` and `warnings`.

### Changed

- `template init` on an existing name adds a new generation instead of replacing the template. The database name belongs to the template; a different `--database` is refused.
- Asking again for a running named clone returns the existing clone (`existing: true`).
- Daemon protocol is v2. The first 0.2 command replaces a running 0.1 daemon.
- The state store migrates from 0.1 by itself.
- 0.2 migrates the store, and 0.1 cannot read it afterwards. There is no downgrade.
- A bad config value or env var is an `invalid_request` naming the key, before any daemon starts.

### Fixed

- An unmarked, non-empty `MITOSIS_HOME` is adopted only when it holds a `state.db`.
- The port-in-use check reads only the current start try's log lines.
- `pg_config` errors name the binary and say so when there is no output.
- A Homebrew Postgres install is no longer listed twice.
- The daemon logs connection read errors, no longer floods the log on accept errors, and reads `status` off the runtime thread.
- `scripts/bench.sh` always stops the daemon and keeps the temp home while a clone's Postgres still runs.
- Added tests for the port-in-use retry, oversized requests, daemon unavailable and the shutdown wait.

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

[Unreleased]: https://github.com/irfanfaraaz/mitosis/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/irfanfaraaz/mitosis/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/irfanfaraaz/mitosis/releases/tag/v0.1.0
