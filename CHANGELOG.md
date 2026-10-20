# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - Unreleased

### Changed

- The CLI, `--json` output, MCP tools, config keys, error codes and daemon protocol are now frozen until 2.0; see [COMPATIBILITY.md](COMPATIBILITY.md). Contract tests pin them.
- Renames made before the freeze (no aliases, since 1.0 is the first stable release):
  - `template init` / `template set`: `--mode` is now `--every-mode`, matching the `every_mode` field.
  - `template init`: `--pg-version` is now `--pg-major`. It always took a major; `pg install --version` takes an exact minor.
  - `clone --json`, `list --json` and the MCP `clone_create` / `clone_list` replies: `generation` is now `gen`.
  - `diff --json` and the MCP `clone_diff` reply: `generation` is now `gen`.
  - `template list --json`: `generations` (a count) is now `gen_count`, so `generations` only ever names a list (as in `template history --json`).
  - The `clone.created` event's `detail.generation` is now `detail.gen`.
  - MCP `clone_destroy`: the input `id` is now `id_or_name`, as in `clone_diff`.
  - MCP `clone_create`: the input `wait_secs` (a number) is now `wait`, a duration like `30s` (or `0`), as for `ttl` and the CLI's `--wait`.
- MCP `clone_destroy` replies with JSON, `{"destroyed": "<id>"}`, like the other tools. It used to reply `destroyed <id>`.
- `config --json`: `source` is always a string, `default`, `file` or `env`; an `env` entry also has `env_var`. An environment override used to be the object `{"env": "MITOSIS_X"}`.
- `mitosis config --json set ...` is refused: `config`'s own flags do not go with its subcommand.
- `mitosis events` without `--json` says on stderr when it showed only the newest 1000 events (or `--limit`).
- `mitosis clone --fresh` without `--name` (MCP `fresh` without `name`) now fails with `invalid_request`. It used to be silently ignored.
- `mitosis mcp` no longer starts the daemon when it launches. Pending `.env` restores are finished by the first tool call, or at once if a daemon already runs.
- The `cap_reached` message changed: `clone limit reached (max_clones = N): a, b, c and K more`. It shows clone names (ids for unnamed clones), at most three.
- `budget_full` messages leave out `largest:` when no clone uses that resource.
- Destroying a clone during its wake, sleep or the end of its create now says `clone X is busy (starting, waking or going to sleep); try again in a moment`.
- `mitosis env` now refuses an invalid `--var`, and a clone that is starting or being destroyed.
- `mitosis top` exits on SIGTERM, SIGHUP and SIGINT with the terminal restored. Exit status is 128 plus the signal number.
- Port warnings: a reserved port outside `port_range` now reads `port N is outside port_range; …`. A brand-new name whose port moved during start-up no longer gets a warning.
- `mitosis list` warns when a Ready clone's server is not running.
- The router answers `57P03 clone X is changing state; retry` when a clone flips state three times while one connection opens.
- A failed per-item pending `.env` restore warning now says `… for an earlier clone: …`.
- More leftovers are cleaned up: the daemon removes diff leftovers (`templates/.diff-*`, `sockets/d<hex>`, `logs/diff-*.log`) at start; gc removes `logs/upgrade-*` folders when no build runs, and a row-less migrated plain template folder.
- A create waiting in the queue while the daemon shuts down now fails with `daemon_unavailable` instead of being admitted.
- A missing install or extension, or a parent process that is gone, now fails before the create queues. It used to come after the wait, or as `cap_reached`.
- Protocol-mismatch hint for a newer daemon: run the newer binary. The old text was a circular `mitosis daemon stop`.
- `mitosis template set <name>` with no setting and no `--resume` now fails with `invalid_request`. It used to succeed silently.
- `template set --migrate` / `template init --migrate` fail when the recorded working folder does not exist.
- `template list` and the MCP template info mask `NAME_PASSWORD=value` assignments in migrate and seed commands.
- `template set --resume` also lets the next automatic build retry at once (it clears the retry timer).
- `mitosis config` shows durations in a single unit (`90s`, not `1m30s`) so the value can be pasted into `config set`.
- `mitosis config set` prints a note on stderr when an environment variable overrides the saved value.
- `mitosis promote` on a clone that is not ready says what state it is in and what to do.
- Promote commits the new generation before the clone restarts (the clone is down slightly longer on big databases).
- `mitosis doctor --fix` no longer runs gc when gc would destroy clones; it lists them and tells you to run `mitosis gc`. `doctor` itself lists them under `gc_dry_run`.
- `mitosis doctor` removes throwaway `logs/doctor-*` folders left by an interrupted run.
- `mitosis doctor` reports a read error of `state.db` instead of treating the table as empty.
- `mitosis service install` waits for launchd to unload the old job before loading the new one.
- Upgrade logs and error tails hide SCRAM password hashes.

## [0.2.0] - 2026-10-20

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
