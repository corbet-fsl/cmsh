#!/usr/bin/env bash
# Build against the newest main of our own crates (cmtymeet policy: Cargo.lock keeps third-party
# pins; internal crates are advanced here, in CI and in the integration workspace, never by lock sweeps).
# The list of our crates is .ci/our-crates.txt, a generated copy of cmeet's ci/our-crates.txt.
# Generated copy of cmeet ci/update-our-crates.sh; change it there and run `cargo xtask ci-sync`.
# Usage: update-our-crates.sh [REPO]   REPO defaults to the repository this copy lives in. The fixture builders of
# cmeet (ci/test-env, xtask) pass the checkout they build, so they run cmeet's current script, never the possibly
# older copy inside that checkout (2026-11-07: cmsg's copy still passed bare names; `-p cmbl` failed "ambiguous").
set -euo pipefail
cd "${1:-$(dirname "${BASH_SOURCE[0]}")/..}"
[ -f Cargo.lock ] || exit 0
[ -f .ci/our-crates.txt ] || { echo 'update-our-crates: .ci/our-crates.txt missing' >&2; exit 1; }
args=()
# Only packages the lock takes from our forge and that the shared list names. Each is passed as the full
# package spec `<source>#<name>@<version>` (the lock's source without its `#<commit>`), never as a bare
# name: a repo's lock can hold the same name twice, e.g. cwlt's own path package `cwlt-engine` and the
# git copy of it that ckmg depends on, and `cargo update -p cwlt-engine` then fails "ambiguous".
while IFS= read -r spec; do
    args+=(-p "$spec")
done < <(awk '
    FNR == NR { if ($0 !~ /^#/ && $0 != "") ours[$0] = 1; next }
    /^\[\[package\]\]/ { cur = ""; ver = "" }
    /^name = / { gsub(/"/, "", $3); cur = $3 }
    /^version = / { gsub(/"/, "", $3); ver = $3 }
    /^source = "git\+https:\/\/git\.corbet\.ch\// {
        if (cur in ours) { src = $3; gsub(/"/, "", src); sub(/#.*$/, "", src); print src "#" cur "@" ver }
    }
' .ci/our-crates.txt Cargo.lock | sort -u)
if [ "${#args[@]}" -eq 0 ]; then
    echo 'update-our-crates: no internal crates in Cargo.lock'
    exit 0
fi
echo "update-our-crates: advancing $(( ${#args[@]} / 2 )) internal packages to the newest main"
cargo update "${args[@]}"
