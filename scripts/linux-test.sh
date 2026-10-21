#!/usr/bin/env bash
# Runs the Mitosis test suite on Linux in a privileged Docker container.
# Usage: scripts/linux-test.sh <btrfs|xfs|copy> [cargo test args...]
# Exit code is cargo's.
set -euo pipefail

fs="${1:-}"
case "$fs" in
btrfs | xfs | copy) shift ;;
*) echo "usage: $0 <btrfs|xfs|copy> [cargo test args...]" >&2; exit 2 ;;
esac

repo="$(cd "$(dirname "$0")/.." && pwd)"
image=mitosis-linux-test
docker build -q -t "$image" "$repo/scripts/linux" >/dev/null

backend=copy
[ "$fs" = copy ] || backend=reflink

# Quote each cargo arg so it survives the `bash -c` hop.
args=""
for a in "$@"; do args+=" $(printf '%q' "$a")"; done

# Named volumes keep the cargo registry and target dir between runs.
docker run --rm --privileged \
    -v "$repo":/work \
    -v mitosis-cargo-registry:/home/tester/.cargo/registry \
    -v mitosis-cargo-target:/work/target \
    -e FS="$fs" -e MITOSIS_BACKEND="$backend" -e MITOSIS_TEST_ROOT=/mnt/mitosis \
    "$image" bash -c '
set -euo pipefail
chown tester:tester /home/tester/.cargo/registry /work/target
mkdir -p /mnt/mitosis
if [ "$FS" != copy ]; then
    truncate -s 4G /var/mitosis.img
    dev=$(losetup -f --show /var/mitosis.img)
    if [ "$FS" = btrfs ]; then mkfs.btrfs -q -f "$dev"; else mkfs.xfs -q -f -m reflink=1 "$dev"; fi
    mount "$dev" /mnt/mitosis
fi
chown tester:tester /mnt/mitosis
cd /work
exec runuser -u tester -- env MITOSIS_BACKEND="$MITOSIS_BACKEND" MITOSIS_TEST_ROOT=/mnt/mitosis cargo test'"$args"'
'
