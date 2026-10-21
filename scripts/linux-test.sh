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
# --init reaps orphaned Postgres processes, as init does on a real system.
docker run --rm --init --privileged \
    -v "$repo":/work \
    -v mitosis-cargo-registry:/home/tester/.cargo/registry \
    -v mitosis-cargo-target:/work/target \
    -e FS="$fs" -e MITOSIS_BACKEND="$backend" -e MITOSIS_TEST_ROOT=/mnt/mitosis \
    "$image" bash -c '
set -euo pipefail
chown tester:tester /home/tester/.cargo/registry /work/target
# mkfs_mount <btrfs|xfs> <image> <mountpoint>: a 4 GB loop file system.
mkfs_mount() {
    truncate -s 4G "$2"
    # Docker shares the VM loop devices; make the next free node if the container lacks it.
    free=$(losetup -f)
    [ -e "$free" ] || mknod "$free" b 7 "${free#/dev/loop}"
    dev=$(losetup --show "$free" "$2")
    devs+=("$dev")
    if [ "$1" = btrfs ]; then mkfs.btrfs -q -f "$dev"; else mkfs.xfs -q -f -m reflink=1 "$dev"; fi
    mkdir -p "$3"
    mount "$dev" "$3"
}
devs=()
# Unmount and free the loop devices on exit, or later runs run out of them.
cleanup() {
    umount /mnt/mitosis /mnt/tmp 2>/dev/null || true
    for d in "${devs[@]}"; do losetup -d "$d" || true; done
}
trap cleanup EXIT
# The tests use fixed ports 55000-60249; keep outgoing ports below them.
sysctl -q -w net.ipv4.ip_local_port_range="32768 54999"
mkdir -p /mnt/mitosis
# Unit tests make homes in TMPDIR with the auto back end, so TMPDIR must clone.
# The copy suite keeps MITOSIS_TEST_ROOT on the plain overlay file system.
if [ "$FS" = copy ]; then
    mkfs_mount btrfs /var/tmp.img /mnt/tmp
    tmp=/mnt/tmp
else
    mkfs_mount "$FS" /var/mitosis.img /mnt/mitosis
    tmp=/mnt/mitosis
fi
chown tester:tester /mnt/mitosis "$tmp"
cd /work
runuser -u tester -- env MITOSIS_BACKEND="$MITOSIS_BACKEND" MITOSIS_TEST_ROOT=/mnt/mitosis TMPDIR="$tmp" cargo test'"$args"'
'
