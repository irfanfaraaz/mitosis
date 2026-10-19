#!/usr/bin/env bash
# Mitosis benchmark: clone time, extra disk and idle RAM for ~1 GB and ~10 GB templates.
#
# Binary: $MITOSIS_BIN, else ./target/release/mitosis.
# Needs: jq, perl, and pgbench (on PATH, or in a Homebrew Postgres/libpq bin dir).
# Uses a temporary MITOSIS_HOME on the same volume as $TMPDIR; it is deleted on exit.
# Note: pgbench scale-to-size is approximate (scale 70 is about 1 GB, scale 700 about 10 GB).
# The 10 GB template needs roughly 25 GB of free disk while it is being seeded.
set -euo pipefail

MITOSIS="${MITOSIS_BIN:-./target/release/mitosis}"
RUNS=5
SIZES=("1gb:70" "10gb:700")

[ -x "$MITOSIS" ] || { echo "error: mitosis binary not found at $MITOSIS (set MITOSIS_BIN)" >&2; exit 1; }
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

if ! command -v pgbench >/dev/null; then
  for d in /opt/homebrew/opt/postgresql@*/bin /opt/homebrew/opt/libpq/bin /usr/local/opt/postgresql@*/bin /usr/local/opt/libpq/bin; do
    if [ -x "$d/pgbench" ]; then PATH="$d:$PATH"; break; fi
  done
fi
command -v pgbench >/dev/null || { echo "error: pgbench not found; put it on PATH (brew install postgresql@18)" >&2; exit 1; }

# /tmp, not $TMPDIR: macOS $TMPDIR is long enough to break the 103-byte socket path limit.
MITOSIS_HOME="$(mktemp -d /tmp/mitosis-bench.XXXXXX)"
BENCH_HOME="$MITOSIS_HOME"
export MITOSIS_HOME

cleanup() {
  # errexit/pipefail stay active in an EXIT trap; every step here must run regardless.
  set +e +o pipefail
  # Clones run in their own process group, so destroy them before stopping the daemon.
  # Skip the destroys when no daemon socket exists, so cleanup never auto-starts one.
  if [ -S "$BENCH_HOME/daemon.sock" ]; then
    ids=$("$MITOSIS" list --json 2>/dev/null | jq -r '.[].id' 2>/dev/null)
    for id in $ids; do
      "$MITOSIS" destroy "$id" >/dev/null 2>&1 || true
    done
  fi
  # Always try: a daemon can be running with its socket file gone. Errors are ignored.
  "$MITOSIS" daemon stop >/dev/null 2>&1 || true
  # Never delete the folder under a running clone Postgres: keep it and say where it is.
  running=0
  for pidfile in "$BENCH_HOME"/clones/*/postmaster.pid; do
    [ -f "$pidfile" ] || continue
    pid=$(head -n 1 "$pidfile" 2>/dev/null)
    if [[ $pid =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then running=1; break; fi
  done
  if [ "$running" = 1 ]; then
    echo "warning: a clone Postgres is still running; left $BENCH_HOME in place (delete it by hand)" >&2
  # Delete for real (not trash): trash would keep the ~10 GB template on disk in ~/.Trash.
  elif [ -n "$BENCH_HOME" ] && [ "$MITOSIS_HOME" = "$BENCH_HOME" ]; then
    trash -- "$BENCH_HOME" || true
  fi
}
trap cleanup EXIT

now() { perl -MTime::HiRes=time -e 'printf "%.6f\n", time'; }
# Called inside $(...): a non-zero return trips errexit in the caller.
free_bytes() {
  local kb
  kb=$(df -k "$MITOSIS_HOME" | awk 'NR==2{print $4}')
  [[ $kb =~ ^[0-9]+$ ]] || { echo "error: could not read free space for $MITOSIS_HOME (got '$kb')" >&2; return 1; }
  echo $(( kb * 1024 ))
}
median() { sort -g | awk '{a[NR]=$1} END{print (NR%2 ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2)}'; }

echo "| Template | Median clone time | Median extra disk | Median idle RAM |"
echo "|---|---|---|---|"

for entry in "${SIZES[@]}"; do
  label="${entry%%:*}"; scale="${entry##*:}"
  name="bench-$label"
  "$MITOSIS" template init "$name" --seed "pgbench -i -s $scale \"\$DATABASE_URL\"" >&2

  times=(); disks=(); rams=()
  for _ in $(seq "$RUNS"); do
    free_before=$(free_bytes)
    t0=$(now)
    info=$("$MITOSIS" clone --template "$name" --json)
    t1=$(now)
    free_after=$(free_bytes)
    id=$(jq -r '.id // empty' <<<"$info")
    [ -n "$id" ] || { echo "error: clone --json returned no id: $info" >&2; exit 1; }
    sleep 5
    rss=$("$MITOSIS" list --json | jq -r --arg id "$id" '.[] | select(.id==$id) | .rss_bytes')
    "$MITOSIS" destroy "$id" >/dev/null
    disk=$(( free_before - free_after ))
    [[ $rss =~ ^[0-9]+$ ]] || { echo "error: clone $id: rss_bytes is '$rss', not a non-negative integer" >&2; exit 1; }
    if [ "$disk" -lt 0 ]; then
      echo "warning: free space grew during clone $id; counted as 0" >&2
      disk=0
    fi

    times+=("$(awk -v a="$t0" -v b="$t1" 'BEGIN{print (b-a)*1000}')")
    disks+=("$disk")
    rams+=("$rss")
  done

  t=$(printf '%s\n' "${times[@]}" | median)
  d=$(printf '%s\n' "${disks[@]}" | median)
  r=$(printf '%s\n' "${rams[@]}" | median)
  awk -v l="$label" -v t="$t" -v d="$d" -v r="$r" \
    'BEGIN{printf "| %s | %.0f ms | %.1f MB | %.1f MB |\n", l, t, d/1e6, r/1e6}'
done
