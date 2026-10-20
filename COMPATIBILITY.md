# Compatibility promise

Mitosis follows [Semantic Versioning](https://semver.org/). From 1.0.0 until 2.0.0, the things
below stay compatible: a 1.x release can add to them, but it never removes or renames anything
and never changes what something means. Breaking changes wait for 2.0.

The contract tests in `tests/contract.rs` pin this surface. Their snapshots live in
`tests/contract/*.json`. A test fails whenever the surface changes, so every change is
deliberate: the snapshot and `CHANGELOG.md` are updated in the same commit.

## What is stable until v2

### CLI commands and flags

- Every command, subcommand, flag and positional argument that `mitosis --help` shows keeps its
  name and meaning.

- New commands and new flags may appear.

- A changed default value is called out in the CHANGELOG.

### `--json` output

These commands print JSON with `--json`: `clone`, `list`, `template list`, `template history`,
`events`, `diff`, `doctor`, `pg list`, `config` and `service status`.

- No field is removed or renamed, and no field changes its type or its meaning.

- New fields may appear in any object. Scripts must ignore fields they do not know.

- A field marked optional may be left out. A missing optional field means "none", as `null`
  does.

- String fields with a fixed set of values (a clone's `status`, an event's `kind`, a doctor
  check's `level`, a generation's `kind` and the like) keep the meaning of every value. New
  values may appear, so scripts must handle values they do not know.

- `events --json` prints one JSON array. With `--follow` it prints one JSON object per line
  instead, because the stream does not end.

- An event's `detail` follows the same rules as any other object: keys are added, never
  removed or renamed.

### MCP tools

- The tool names `clone_create`, `clone_list`, `clone_destroy` and `clone_diff` stay.

- Input schemas are additive only. A new input is always optional, and leaving it out keeps the
  old behaviour.

- Output schemas are additive only, under the same rules as `--json` output.

- New tools may appear.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Failure. Also `doctor` when a check fails, and `daemon status` when no daemon runs |
| `2` | The command line could not be parsed |
| `3` | `mitosis daemon` found another daemon already serving this home |
| `128 + N` | `top` ended by signal `N` (for example 143 for SIGTERM) |

### Error codes

The error codes (`invalid_request`, `clone_not_found`, `budget_full` and the rest) keep their
names and meaning. New codes may appear. Error messages and hints are written for people and may
change in any release.

### Config keys and environment variables

- Every key that `mitosis config` lists keeps its name, its accepted values and its meaning.
  The matching `MITOSIS_*` environment variable stays too.

- `MITOSIS_HOME` and `MITOSIS_PG_RELEASES_URL` stay.

- `router_listen` accepts `127.0.0.1`, `::1`, `0.0.0.0` and `::`. `router_port` accepts a port
  from 1 to 65535 outside `port_range` (54320-54999).

- New keys may appear. A new key's default keeps the old behaviour.

### Names

- Template and clone names match `^[a-z0-9][a-z0-9_-]{0,62}$`. Branch templates (`<base>~<branch>`)
  are the only names with `~`.

- A clone name may not look like a clone id (`c` plus 6 lowercase hex digits, such as
  `cafe123`), because the router takes the database name for a name first and an id second.

### Daemon protocol

- The protocol version check stays. A client never talks to a daemon of another protocol
  version: the newer binary replaces an older daemon, and an older client tells you to run the
  newer binary.

- Within protocol version 3, changes are new requests and new fields only. A new field always
  has a default, so a message without it still parses.

### State store

- `state.db` is always migrated forward. A 1.x release opens and upgrades the store of any
  earlier release (0.1, 0.2 and every 1.x) in one transaction, so a crash leaves either the old
  store or the new one.

- A store is never left in a state its own release cannot read.

- There is no downgrade. After a newer release opens the store, an older binary refuses it with
  a clear error instead of damaging it.

### Router URL

The router URL keeps this format:

```
postgres://postgres:<password>@127.0.0.1:<router_port>/<clone name or id>
```

`url` always holds this form while `router = on`, even when `router_listen` is `::1`,
`0.0.0.0` or `::`. `direct_url` keeps the v0.2 form (the clone's own port) and is also stable.

## What is not covered

- Human-readable output: tables, notes, warnings, progress lines and their wording. Use `--json`
  in scripts.

- Error messages and hints (the error code is stable, the text is not).

- Log files under the Mitosis home, and the layout of the home folder.

- The Rust library API of the `mitosis-db` crate. The binary is the product; the library exists
  for the binary and its tests.

- Environment variables not listed above (for example `MITOSIS_SERVICE`, which the login service
  sets, and test-only switches).

- Performance figures such as clone time, wake time and router connect time.

## Renames and removals after 1.0

A command, subcommand or flag that is renamed or removed after 1.0.0 stays as a **hidden alias**
until 2.0.0:

- The old name still works exactly as before.

- It no longer shows in `--help`.

- Each use prints one line on stderr, for example
  `warning: --old-flag is deprecated and will be removed in 2.0; use --new-flag`.

The same rule holds for the rest of the surface:

- A renamed config key or environment variable is still read under its old name. `mitosis
  config` and `mitosis doctor` warn about the old name.

- A renamed MCP input is still accepted under its old name.

- A renamed `--json` or MCP output field is written under both names until 2.0.

The alias and its removal are both listed in the CHANGELOG.
