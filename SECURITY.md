# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | Yes |

Older versions get no fixes. Please update to the latest 0.1.x first.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately with GitHub's private vulnerability reporting: https://github.com/irfanfaraaz/mitosis/security/advisories/new

Please include:

- What the problem is and what an attacker can do with it.
- Steps to reproduce it, or a small proof of concept.
- Your Mitosis version (`mitosis --version`), macOS version and Postgres version.
- Any logs that help. Remove passwords first.

## What to expect

This is a one-person project, so replies are best effort and there is no SLA. I will try to acknowledge a report within a few days, and to tell you when a fix is out. Credit is given in the advisory if you want it.

## Threat model

Mitosis is a local developer tool, not a service for untrusted networks. What it does today:

- Each clone's Postgres listens on `127.0.0.1` only, never on another interface.
- Each clone gets its own random password.
- The home folder (`~/Library/Application Support/mitosis/` or `$MITOSIS_HOME`) is mode 0700.
- `state.db`, which holds clone passwords, is mode 0600.
- The daemon socket is mode 0600. Authentication is local: any process running as the same user can talk to the daemon, and can read the clone passwords. Other users on the machine cannot.
- Passwords in `--from-url` are masked in `template list`, errors and logs.

Anything that lets another user, or a remote machine, reach a clone or the daemon is in scope for a report.
