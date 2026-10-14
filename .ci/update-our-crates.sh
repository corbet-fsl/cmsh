#!/usr/bin/env bash
# Build against the newest main of our own crates (cmtymeet policy: Cargo.lock keeps third-party
# pins; internal crates are advanced here, in CI and in the integration workspace, never by lock sweeps).
# The list of our crates is .ci/our-crates.txt, a generated copy of cmeet's ci/our-crates.txt.
# Generated copy of cmeet ci/update-our-crates.sh; change it there and run `cargo xtask ci-sync`.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
[ -f Cargo.lock ] || exit 0
[ -f .ci/our-crates.txt ] || { echo 'update-our-crates: .ci/our-crates.txt missing' >&2; exit 1; }
args=()
# only packages the lock takes from our forge and that the shared list names
while IFS= read -r name; do
    args+=(-p "$name")
done < <(awk '
    FNR == NR { if ($0 !~ /^#/ && $0 != "") ours[$0] = 1; next }
    /^name = / { gsub(/"/, "", $3); cur = $3 }
    /^source = "git\+https:\/\/git\.corbet\.ch\// { if (cur in ours) print cur }
' .ci/our-crates.txt Cargo.lock | sort -u)
if [ "${#args[@]}" -eq 0 ]; then
    echo 'update-our-crates: no internal crates in Cargo.lock'
    exit 0
fi
echo "update-our-crates: advancing $(( ${#args[@]} / 2 )) internal packages to the newest main"
cargo update "${args[@]}"
