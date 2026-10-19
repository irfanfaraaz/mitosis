# Mitosis for Postgres

Give each AI agent task its own private Postgres database, cloned from a template.

Mitosis is a CLI and a small background daemon. It copies a template Postgres data directory with APFS `clonefile` (copy-on-write), starts a real Postgres process on the copy, and deletes it when the task ends or its TTL passes. Cloning a database should take about a second and cost almost no extra disk. That is the target, and `scripts/bench.sh` measures it.

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

Build a template once. The seed command runs with `DATABASE_URL` set to the new database:

```sh
mitosis template init --seed 'psql "$DATABASE_URL" -f schema.sql'
# or restore a dump:
mitosis template init --from-url postgres://user:pass@host/db
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

Mitosis ships an MCP server with three tools: `clone_create`, `clone_list` and `clone_destroy`. Clones created through it are removed when the agent session ends.

For Claude Code, add this to your MCP config:

```json
{"mcpServers": {"mitosis": {"command": "mitosis", "args": ["mcp"]}}}
```

## How it works

1. **Template.** `mitosis template init` runs a real Postgres, loads your seed or dump, and keeps the resulting data directory as the template.
2. **Clonefile.** `mitosis clone` copies the template with APFS `clonefile`. Blocks are shared until a clone writes to them.
3. **Postgres.** The daemon starts a Postgres on a free local port and returns the URL.
4. **Reap.** The daemon stops and deletes a clone when you destroy it, or when its TTL passes. Clones made through MCP are also removed when the agent session exits.

Clone Postgres processes run in their own process group, so they survive the daemon's death.

## Defaults

| Setting | Value |
|---|---|
| Template name | `main` |
| Clone TTL | 2 h |
| Max clones | 8 |
| Min free disk | 2 GB |
| Port range | 54320-54999 |
| Template max age | 7 days |
| Seed database name | `app` |
| Postgres settings | `listen_addresses=127.0.0.1`, `shared_buffers=32MB`, `max_connections=20` |
| Non-durable extras | `fsync=off`, `synchronous_commit=off`, `full_page_writes=off` |

Clone URL format: `postgres://postgres:<password>@127.0.0.1:<port>/<database>`.

Everything lives in `$MITOSIS_HOME`, which defaults to `~/Library/Application Support/mitosis/`.

## Benchmarks

Run `scripts/bench.sh` (needs `pgbench`, `jq` and a release build). It prints the table below. Each cell is a median of 5 runs.

| Template | Clone time | Extra disk per clone | Idle RAM per clone |
|---|---|---|---|
| ~1 GB | fill from scripts/bench.sh | fill from scripts/bench.sh | fill from scripts/bench.sh |
| ~10 GB | fill from scripts/bench.sh | fill from scripts/bench.sh | fill from scripts/bench.sh |

## Platform

macOS on APFS only in v0.1. On any other file system Mitosis fails with the error code `not_apfs`.

## License

MIT OR Apache-2.0
