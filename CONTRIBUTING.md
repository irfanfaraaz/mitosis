# Contributing to Mitosis

Thanks for wanting to help. Bug reports, fixes and ideas are all welcome.

## Prerequisites

- macOS on APFS (Mitosis does not run anywhere else).
- Rust stable, 1.89 or newer.
- A Postgres server install, for example `brew install postgresql@18`.

## Build

```sh
cargo build
```

## Checks

Run these before you open a pull request. CI runs the same three:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Database tests skip themselves when no Postgres server is found, and a skipped test still shows as passed. If you changed anything that touches Postgres, check that those tests really ran: install Postgres first, and read the test output for skip messages.

## Benchmark

`scripts/bench.sh` measures clone time, extra disk and idle RAM. It needs a release build (`cargo build --release`), `jq` and `pgbench`. The 10 GB case needs about 25 GB of free disk. Run it if you change anything on the clone path, and put the numbers in your pull request.

## Commits

Use a short conventional prefix, as in `git log`:

- `feat:` for new behavior
- `fix:` for bug fixes
- `docs:` for documentation only
- `style:` for formatting only

Keep each commit about one thing, and write the subject in the imperative, for example `fix: stop the daemon when the lock is stale`.

## Pull requests

1. Fork the repo and create a branch from `main`.
2. Make your change, with tests for new behavior.
3. Run the checks above.
4. Open a pull request and fill in the template. Say what changed and why.

Small, focused pull requests are easiest to review. For a big change, open an issue first so we can agree on the direction.

## Reporting problems

Use the issue templates. For security problems, do not open a public issue: see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion in this project is dual licensed under MIT OR Apache-2.0, as in the Rust project, without any additional terms or conditions.
