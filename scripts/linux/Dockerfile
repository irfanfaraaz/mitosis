# Linux test image for Mitosis. Deps layer first so later builds hit the cache.
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg build-essential pkg-config git \
        btrfs-progs xfsprogs util-linux mount \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-18 \
    && trash /var/lib/apt/lists/*
# Postgres refuses to run as root: the entry script sets up mounts as root, then runs cargo as this user.
RUN useradd -m -s /bin/bash tester
USER tester
ENV CARGO_HOME=/home/tester/.cargo RUSTUP_HOME=/home/tester/.rustup PATH=/home/tester/.cargo/bin:/usr/lib/postgresql/18/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable -c rustfmt -c clippy
USER root
WORKDIR /work
