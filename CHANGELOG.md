# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - Unreleased

### Added

- A router: every clone is reached through one fixed port (`router_port`, default 54300), picked by database name, at `postgres://postgres:<password>@127.0.0.1:54300/<clone name or id>`. Passwords stay end to end, query cancels are routed, and containers use `host.docker.internal`. New config keys `router`, `router_port` and `router_listen`.
- `clone`, `list` and the MCP replies carry `direct_url` (the clone's own port) next to `url`. `mitosis env <clone> --direct` prints the direct URL, waking a sleeping clone first.
- Sleep and wake: idle clones stop and wake on the first router connection (`idle_sleep`), the status `sleeping`, `mitosis wake <clone>`, and eviction of idle clones to fit the RAM budget before a request queues. Clones sleep only while the router is listening; with the router off or its port taken they keep running, and the daemon starts clones that were asleep. `idle_reap` destroys idle unnamed, unprotected clones.
- `mitosis diff <clone> [--table <t> [--limit N]] [--json]` and the MCP tool `clone_diff`: schema changes, row counts and changed rows against the template generation the clone came from.
- Masking and subsets: `template init|set --mask <file>` with the strategies `email`, `name`, `hash`, `null`, `redact`, `const:<value>` and `keep`, and `[subset]` filters or percents that keep foreign keys valid. A masked template never holds the raw values.
- Per-branch templates: `mitosis clone --branch [<branch>]`, MCP `clone_create` `branch`, `template set --base-branch`, and gc of unused branch templates.
- `template init --basebackup <url>`: a physical copy of a running server with `pg_basebackup`.
- `mitosis pg install <major> [--version <x.y>]`, `pg list [--json]` and `pg remove`: Postgres builds downloaded from the `theseus-rs/postgresql-binaries` releases with a SHA-256 check. These are the only commands that download.
- `mitosis events [--follow] [--clone] [--template] [--kind] [--since] [--limit N] [--json]` and the event log in the store (30 days or 50,000 events).
- `COMPATIBILITY.md` and contract tests (`tests/contract.rs`) that pin the CLI, `--json` output, MCP tools, error codes and config keys.
- `mitosis doctor` checks the router, `router_listen`, managed Postgres builds, mask files, branch templates and the event log.
- gc records a `template.pruned` event (`detail.dropped: true`, `detail.cause` `branch deleted` or `unused`) for each branch template it drops, and removes its build log.
- `scripts/bench.sh` also prints the connect time through the router against the direct port, and the wake time.

### Changed

- `url` in `clone`, `list`, the MCP replies and `.env` injection is now the router URL while the router is on (the default). It used to be the direct URL, which is now `direct_url`.
- Idle clones sleep after 10 minutes by default (`idle_sleep`), and a sleeping clone's direct URL refuses connections until it wakes.
- After a reboot or a dead server, recovered named, protected and parentless clones come back `sleeping` while the router is listening, instead of being started at once.
- Daemon protocol is v3. The first 1.0 command replaces a running 0.2 daemon.
- `router_listen` accepts only `127.0.0.1`, `::1`, `0.0.0.0` or `::`, and `router_port` must be outside `port_range` (54320-54999).
- Clone names that look like a clone id (`c` plus 6 hex digits, such as `cafe123`) are refused, because the router would take them for that id.
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
- The masking step also turns off sampled statement and transaction logging (`log_transaction_sample_rate`, `log_min_duration_sample`), so the salt cannot reach the build log through them.

### Fixed

- `mitosis doctor --fix` reports the true number of finished `.env` restores. It used to count warnings.
- A restart of a clone whose `.env` rewrite failed keeps the URL the file holds in its record, as `--fresh` does.
- `.env` records are written while the create is still marked in flight, and `.env` restores no longer block the daemon's runtime.
- A reused process id of a dead MCP session is no longer taken for the same session (the name guard compares the parent's start time too).
- A failed create gives back the port and password reservation it made.
- gc no longer removes a generation folder while a clone is being copied from it, and no longer holds its read guard while it deletes.
- gc finishes a prune that crashed half-way instead of calling the folder foreign.
- A put to sleep checks direct connections again before it stops the server, and a failed revert is logged.
- A waking clone's memory counts toward the RAM budget while it starts.
- A promote commits the new generation before it restarts the clone, so a kill in between cannot lose the promoted data. Its build log is named safely for branch templates, and a failed resume is logged.
- Deleting a template removes it and its generations in one transaction.
- `ensure_daemon` starts our daemon again when its child lost the lock race and no socket is left, instead of waiting 30 seconds.
- A kickstarted login service that dies no longer waits the full 30 seconds, and the hint shows `service.log`.
- `config.toml` writes use a unique temporary file, and the login service compares homes through symlinks.

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

[Unreleased]: https://github.com/irfanfaraaz/mitosis/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/irfanfaraaz/mitosis/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/irfanfaraaz/mitosis/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/irfanfaraaz/mitosis/releases/tag/v0.1.0
