# Mitosis for Postgres

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

Useful flags: `mitosis clone --template main --name my-test --ttl 30m --durable --json`.

Templates older than 7 days produce a warning on `clone`; run `mitosis template refresh` to rebuild one. The daemon starts by itself on first use. `mitosis daemon status` and `mitosis daemon stop` manage it.

## MCP setup

Mitosis ships an MCP server with three tools: `clone_create`, `clone_list` and `clone_destroy`. Clones created through it are removed when the agent session ends. `clone_list` and `clone_destroy` only see the clones of their own session, so one agent cannot remove another agent's database.

For Claude Code, add this to your MCP config:

```json
{"mcpServers": {"mitosis": {"command": "mitosis", "args": ["mcp"]}}}
```

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
| Max clones | 8; set `MITOSIS_MAX_CLONES` (a whole number, at least 1) |
| Min free disk | 2 GB; set `MITOSIS_MIN_FREE_DISK` (bytes, e.g. `500000000`) |
| Port range | 54320-54999 |
| Template max age | 7 days |
| Seed database name | `app` |
| Postgres settings | `listen_addresses=127.0.0.1`, `shared_buffers=32MB`, `max_connections=20` |
| Non-durable extras | `fsync=off`, `synchronous_commit=off`, `full_page_writes=off` |

Clone URL format: `postgres://postgres:<password>@127.0.0.1:<port>/<database>`.

`MITOSIS_MAX_CLONES` and `MITOSIS_MIN_FREE_DISK` are read when the daemon starts, so set them where the first `mitosis` command runs (it starts the daemon), or run `mitosis daemon stop` after changing them.

Everything lives in `$MITOSIS_HOME`, which defaults to `~/Library/Application Support/mitosis/`. Point it at a new or empty folder: Mitosis marks the folders it creates and refuses to use one that already holds other files.

## Benchmarks

Run `scripts/bench.sh` (needs `pgbench`, `jq` and a release build). It prints the table below. Each cell is a median of 5 runs.

| Template | Clone time | Extra disk per clone | Idle RAM per clone |
|---|---|---|---|
| ~1 GB (2.1 GB on disk) | 115 ms | 0.6 MB | 51.3 MB |
| ~10 GB (12 GB on disk) | 114 ms | 0.7 MB | 51.4 MB |

Measured on an Apple M4 with 24 GB RAM, APFS, Postgres 18.6. Clone time does not grow with database size, because `clonefile` copies no data.

## Platform

macOS on APFS only in v0.1. On any other file system Mitosis fails with the error code `not_apfs`.

## License

MIT OR Apache-2.0
