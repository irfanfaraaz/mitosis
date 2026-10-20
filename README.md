# Mitosis for Postgres

[![CI](https://github.com/irfanfaraaz/mitosis/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/irfanfaraaz/mitosis/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mitosis-db.svg)](https://crates.io/crates/mitosis-db)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Give each AI agent task its own private Postgres database, cloned from a template.

Mitosis is a CLI and a small background daemon. It copies a template Postgres data directory with APFS `clonefile` (copy-on-write), starts a real Postgres process on the copy, and deletes it when the task ends or its TTL passes. Cloning a database takes about a tenth of a second and costs almost no extra disk, even for a 10 GB template (see [Benchmarks](#benchmarks)).

## Install

```sh
cargo install mitosis-db
```

Homebrew will come later.

You also need a Postgres server install on the machine, for example:

```sh
brew install postgresql@18
```

## Quick start

Build a template once. `--seed` runs in the folder where you ran the command, with your `PATH` and with `DATABASE_URL` set to the new database. Your other environment variables are not passed on, so set any the seed needs inside the command:

```sh
mitosis template init --seed 'psql "$DATABASE_URL" -f schema.sql'
# or copy a live database (pg_dump of the source, restored into the template):
mitosis template init --from-url postgres://user@host/db
```

You can leave the password out of `--from-url`, so it stays out of your shell history. `pg_dump` then reads it from `~/.pgpass`, or from `PGPASSWORD` in the environment the daemon was started in. A password in the URL also works; it is masked in `template list` and in logs.

`template init` also takes:

| Flag | Meaning |
|---|---|
| `[NAME]` | Template name (default `main`) |
| `--database <DATABASE>` | Database name for a `--seed` template (default `app`); `--from-url` uses the name in the URL |
| `--migrations <MIGRATIONS>` | Migrations folder; the template is flagged stale when it changes |
| `--max-age <MAX_AGE>` | Age after which clones warn, like `7d` |
| `--pg-version <PG_VERSION>` | Postgres major version to build with |

List what you have:

```sh
mitosis template list [--json]
```

Clone it. Only the URL is printed, so this works in a shell:

```sh
export DATABASE_URL=$(mitosis clone)
```

Look around and clean up:

```sh
mitosis list
mitosis destroy <id-or-name>
mitosis gc
```

Useful flags: `mitosis clone --template main --name my-test --ttl 30m --durable --json`. See [Named clones](#named-clones), [`.env` injection](#env-injection) and [Budgets](#budgets-queue-and-top) for the rest.

Templates older than 7 days produce a warning on `clone`; run `mitosis template refresh` to rebuild one. The daemon starts by itself on first use. `mitosis daemon status` and `mitosis daemon stop` manage it. To keep it running from login, see [Login service](#login-service).

## MCP setup

Mitosis ships an MCP server with three tools: `clone_create`, `clone_list` and `clone_destroy`. Clones created through it are removed when the agent session ends. `clone_list` and `clone_destroy` only see the clones of their own session, so one agent cannot remove another agent's database.

`clone_create` takes these optional parameters:

| Parameter | Meaning |
|---|---|
| `template` | Template to clone (default `main`) |
| `name` | Stable name. Asking again for a running clone of that name returns it (`existing: true`) |
| `ttl` | Lifetime, like `30m` or `2h` (default `2h`) |
| `durable` | Keep crash-safe writes (slower) |
| `fresh` | With `name`: destroy the running clone and make a new one from the current template |
| `env_file` | A `.env` file that gets the clone's URL (relative paths resolve against the MCP server's folder) |
| `env_var` | Variable to set in `env_file` (default `DATABASE_URL`) |
| `wait_secs` | Seconds to wait for room under the limits; `0` fails at once |

The reply also carries `generation`, `existing`, `stale`, `refreshing`, `suspect`, `duration_ms` and `warnings`. The tool does not expose `protect`, promote, upgrade or config: those stay with a human.

For Claude Code, add this to your MCP config:

```json
{"mcpServers": {"mitosis": {"command": "mitosis", "args": ["mcp"]}}}
```

## Generations

Every build of a template is a numbered generation. `template init` on a name that exists adds a new generation. Clones come from the current one.

```sh
mitosis template history main      # list generations; * marks the current one
mitosis template rollback main     # go back one generation
mitosis template rollback main --to 2
```

Rollback does not delete newer generations. It pauses the template's automatic builds, so they do not undo it a minute later. They start again after the next manual `template refresh` or `template set main --resume`.

`--keep <n>` sets how many generations stay on disk (default 3, at least 1). The current generation is never pruned.

## Template settings

`template init` and `template set` take the same flags:

| Flag | Meaning |
|---|---|
| `--every <dur\|off>` | Rebuild on a schedule, like `24h` (at least `1m`) |
| `--mode full\|layered` | How scheduled rebuilds run (default `full`) |
| `--migrate "<cmd>"` | Migration command for layered refreshes; runs in your folder with `DATABASE_URL` set. `off` clears it |
| `--auto-migrate on\|off` | Run a layered refresh when the `--migrations` folder changes. Needs `--migrations` and `--migrate` |
| `--on-stale warn\|refresh` | When a template is older than `--max-age`: warn only, or also refresh in the background. The clone is served at once from the current generation |
| `--keep <n>`, `--max-age <dur>` | Generations to keep; age after which clones warn |

A layered refresh copies the current generation and runs only your migrate command on it. Mitosis does not parse migrations. Your tool decides what is new. Use `mitosis template refresh <name> --layered` to run one by hand.

## Promote

Turn a clone's data into the newest generation of a template:

```sh
mitosis promote my-test                 # into the clone's own template
mitosis promote my-test --template qa   # into another template; created if missing
```

The clone keeps running on the same port. The target must use the same Postgres major and database name. A template made only by promote can take layered refreshes and further promotes, but not a full refresh. Promote is CLI only.

## Upgrade a template to a newer Postgres

```sh
# The template runs on Postgres 17; Postgres 18 is installed too.
mitosis template upgrade main --to 18
mitosis template upgrade main --to 18 --as main18   # keep the original as it is
```

You need the target major installed next to the current one. Mitosis runs `pg_upgrade --clone` and falls back to `--copy` if clone mode is refused. Without `--as`, the result is a new generation, so `template rollback` returns to the old major. Later builds use the current generation's major.

Tested on Postgres 18 so far. Upgrading across majors has not been verified yet.

## Named clones

A name gives a clone a stable URL:

```sh
mitosis clone --name my-test
mitosis clone --name my-test --fresh     # reset: destroy it and make a new one, same URL
mitosis clone --name keepme --protect    # no TTL; destroy needs --force
mitosis destroy my-test --forget         # also release the reserved port and password
```

Mitosis reserves a port and a password per name. A named clone gets the same URL back when its port is free. If the port was taken, it gets a new port and the reply warns that the URL changed. Reservations unused for 30 days are dropped by `gc`. `destroy` keeps the reservation, `destroy --forget` drops it.

Asking again for a running clone of the same name and template returns it (`existing: true`). A different template is an error. A longer `--ttl` extends the deadline. A different `--durable` or `--protect` is refused: use `--fresh`.

## `.env` injection

```sh
mitosis clone --name my-test --env-file .env.local [--env-var DATABASE_URL]
mitosis env my-test [--var NAME]     # prints NAME=<url>, for eval or direnv
```

Only files whose name starts with `.env` are accepted. Mitosis replaces the first `NAME=` line or appends one, and keeps every other byte. It creates a missing file with mode 0600.

On destroy, by any path, it restores the exact original line text, but only if the file still holds this clone's URL. If the restore is refused, it is recorded as pending. The next `clone`, `destroy`, `list` or `doctor --fix` finishes it.

If the file is in a git work tree and not git-ignored, Mitosis warns, because the URL holds a password.

## Budgets, queue and `top`

Set a RAM budget, a disk budget, or both. When the clone count or a budget is full, a new request waits in a first-in, first-out queue instead of failing. A destroy (yours, TTL, or session exit) lets the next one in. The queue never destroys a clone to make room.

```sh
mitosis clone --wait 2m     # wait up to 2 minutes; --wait 0 fails at once
mitosis top                 # live view of clones, RAM, disk, queue (q to quit)
```

After the wait, the request fails with `budget_full`. The count cap still fails with `cap_reached`.

## Config

Settings live in `<home>/config.toml`. Environment variables override the file.

```sh
mitosis config                          # each value and where it came from
mitosis config set ram_budget 4GB
```

| Key | Env var | Default |
|---|---|---|
| `max_clones` | `MITOSIS_MAX_CLONES` | 8 |
| `min_free_disk` | `MITOSIS_MIN_FREE_DISK` | 2 GB |
| `ram_budget` | `MITOSIS_RAM_BUDGET` | off |
| `disk_budget` | `MITOSIS_DISK_BUDGET` | off |
| `queue_wait` | `MITOSIS_QUEUE_WAIT` | 60 s |
| `clone_disk_warn` | `MITOSIS_CLONE_DISK_WARN` | 5 GB |

Sizes take bytes or units like `500MB` or `4GB`. A bad value is an `invalid_request` naming the key, and the CLI refuses it before it starts a daemon. `config set` writes the file and tells a running daemon to reload.

The daemon reads its own environment, and the CLI reads its own. Under the login service, the daemon sees the `PATH` and `MITOSIS_HOME` from install time. So `mitosis config` can show different values than the daemon uses if an env var is set only in your shell. Put settings in `config.toml` to keep them the same.

## Login service

```sh
mitosis service install     # launchd user agent, starts at login
mitosis service status
mitosis service uninstall
```

The plist is `~/Library/LaunchAgents/io.github.irfanfaraaz.mitosis.plist`. It restarts the daemon after a crash (`KeepAlive` with `SuccessfulExit=false`, `ThrottleInterval` 30 s). `mitosis daemon stop` stays stopped. With the service installed, the CLI asks launchd to start the daemon instead of starting its own.

### macOS privacy and protected folders

A daemon started by launchd does not get your terminal's access to `~/Documents`, `~/Desktop` or iCloud Drive. If a template's seed, migrations folder or `.env` file is there, background work (scheduled refreshes, migrations-hash checks, `.env` restores) can fail with a permission error. Either move the project out of those folders, or grant the `mitosis` binary access under System Settings > Privacy & Security (Full Disk Access). `mitosis doctor` warns about this case. The CLI writes `.env` files itself at clone time, with your terminal's access.

## Reboot recovery and suspect clones

After a reboot, Mitosis starts again named clones, protected clones and parentless CLI clones that are still inside their TTL, on the same folder, port and password. Clones whose session is gone are destroyed.

Before restarting, it reads the cluster's control state. A clone that did not shut down cleanly and ran with `fsync=off` is *suspect*. An unnamed suspect clone is destroyed. A named one is started and marked `suspect` in `list --json`, in the clone reply and in `doctor`. Reset it with `mitosis clone --name <x> --fresh`. Durable clones use normal Postgres crash recovery and are not suspect.

## Doctor

```sh
mitosis doctor [--json] [--fix]
```

It works even when the daemon cannot start. Each check is `ok`, `warn` or `fail`, with a one-line fix. It checks:

- the home: APFS, same volume, marker, permissions (home 0700, `state.db` and socket 0600), socket path length
- the config file and environment variables
- the daemon: reachable, same version as the binary
- the login service: installed, loaded, binary exists
- Postgres installs, and a match for every template's major version and extensions
- a throwaway start and stop of a cluster for each installed major
- free disk against `min_free_disk`
- templates: stale, migrations changed, last refresh error, paused
- suspect clones, orphan folders and stale rows (a read-only look, nothing is deleted)
- `.env` files that still point at a destroyed clone, and pending restores
- the macOS privacy case above

The exit code is 1 when any check fails. `--fix` does two safe things only: it finishes pending `.env` restores and runs `gc`. Everything else is a printed hint.

## Upgrading from 0.1

Nothing to do by hand.

- The store migrates by itself on first use, in one transaction. Existing templates become generation 1.
- The first 0.2 command replaces a running 0.1 daemon (the protocol is v2). If the old daemon cannot be stopped, the error tells you how.
- Your 0.1 templates and clones keep working.

## How it works

1. **Template.** `mitosis template init` runs a real Postgres, loads your seed or the copied database, and keeps the resulting data directory as the template.
2. **Clonefile.** `mitosis clone` copies the template with APFS `clonefile`. Blocks are shared until a clone writes to them.
3. **Postgres.** The daemon starts a Postgres on a free local port and returns the URL.
4. **Reap.** The daemon stops and deletes a clone when you destroy it, or when its TTL passes. Clones made through MCP are also removed when the agent session exits.

Clone Postgres processes run in their own process group, so they survive the daemon's death.

## Defaults

| Setting | Value |
|---|---|
| Template name | `main` |
| Clone TTL | 2 h |
| Max clones | 8; set `max_clones` or `MITOSIS_MAX_CLONES` (a whole number, at least 1) |
| Min free disk | 2 GB; set `min_free_disk` or `MITOSIS_MIN_FREE_DISK` |
| Keep generations | 3 |
| Port range | 54320-54999 |
| Template max age | 7 days |
| Seed database name | `app` |
| Postgres settings | `listen_addresses=127.0.0.1`, `shared_buffers=32MB`, `max_connections=20` |
| Non-durable extras | `fsync=off`, `synchronous_commit=off`, `full_page_writes=off` |

Clone URL format: `postgres://postgres:<password>@127.0.0.1:<port>/<database>`.

See [Config](#config) for all settings. `mitosis config set` reloads a running daemon. Environment variables are read by the daemon when it starts, so set them where the first `mitosis` command runs, or run `mitosis daemon stop` after changing them.

Everything lives in `$MITOSIS_HOME`, which defaults to `~/Library/Application Support/mitosis/`. Point it at a new or empty folder: Mitosis marks the folders it creates and refuses to use one that already holds other files.

## Benchmarks

Run `scripts/bench.sh` (needs `pgbench`, `jq` and a release build). It prints the table below. Each cell is a median of 5 runs.

| Template | Clone time | Extra disk per clone | Idle RAM per clone |
|---|---|---|---|
| ~1 GB (2.1 GB on disk) | 115 ms | 0.6 MB | 51.3 MB |
| ~10 GB (12 GB on disk) | 114 ms | 0.7 MB | 51.4 MB |

Measured on an Apple M4 with 24 GB RAM, APFS, Postgres 18.6. Clone time does not grow with database size, because `clonefile` copies no data.

## Platform

macOS on APFS only. On any other file system Mitosis fails with the error code `not_apfs`.

## Contributing

Bug reports, fixes and ideas are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test and send a pull request. Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please report vulnerabilities privately, not in a public issue. See [SECURITY.md](SECURITY.md).

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
