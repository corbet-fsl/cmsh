#!/usr/bin/env bash
# Execute the manifest's selected checks with content-keyed result reuse.
set -euo pipefail
: "${CCID_BIN:?Missing verified ccid job runtime}"
: "${CI_COMMIT_SHA:?Missing verified source revision}"
: "${CI_REPOSITORY_URL:?Missing canonical repository identity}"
: "${CHECKS:?Missing declared check selection}"
if [ "$CHECKS" != "verify" ]; then
  echo "CHECKS must be exactly 'verify', got: $CHECKS" >&2
  exit 2
fi
cache_root=${CI_CACHE_ROOT:-${CARGO_HOME:-}}
[[ $cache_root = /* ]] || { echo 'An absolute persistent CI_CACHE_ROOT or CARGO_HOME is required' >&2; exit 2; }
namespace=$(printf '%s' "$CI_REPOSITORY_URL" | sha256sum | cut -c1-24)
state="$cache_root/ccid-cached/$namespace"
toolchains=${CI_TOOLCHAIN_ROOT:-$cache_root/ccid-cached/toolchains}
[[ $toolchains = /* ]] || { echo 'CI_TOOLCHAIN_ROOT must be absolute' >&2; exit 2; }
mkdir -p "$state" "$toolchains"
exec 9>"$state/lock"
flock -w "${CI_TIMEOUT:-2700}" 9
mkdir -p "$state/moon-cache/hashes" "$state/moon-cache/outputs" "$state/moon-home"
mkdir -p .moon/cache
ln -s "$state/moon-cache/hashes" .moon/cache/hashes
ln -s "$state/moon-cache/outputs" .moon/cache/outputs
# Tool installation is an explicit part of this job, in persistent CI storage.
tool_source=github:NixOS/nixpkgs/b7c2ada94fe99c15b0dbcf4d11fd7850b957a436?narHash=sha256-IItrdb7Puk05RqOBWZYFC5X6Wl1sJmCfh5MWVHw5iMM=
tool_paths=$(nix build --no-link --print-out-paths "$tool_source#moon" "$tool_source#proto" "$tool_source#nodejs_22" "$tool_source#python3" "$tool_source#cargo-llvm-cov" "$tool_source#jq" "$tool_source#curl" "$tool_source#git" "$tool_source#gcc" "$tool_source#gnumake" "$tool_source#pkg-config" "$tool_source#libevent" "$tool_source#openssl" "$tool_source#zlib" "$tool_source#cargo-deny" "$tool_source#llvmPackages.clang-unwrapped")
for path in $tool_paths; do PATH="$path/bin:$PATH"; done
# Tor fixture build inputs: tor-tools.sh compiles tor 0.4.9.12 from the pinned
# tarball on cache miss, needing -dev headers. Export store path lists only;
# verify.sh scopes LD_LIBRARY_PATH/PKG_CONFIG_PATH/CC to tor-records so
# stable gates keep the proto toolchain environment.
export CI_TOR_LIB_OUT="$(nix build --no-link --print-out-paths "$tool_source#libevent^out" "$tool_source#openssl^out" "$tool_source#zlib^out")"
export CI_TOR_LIB_DEV="$(nix build --no-link --print-out-paths "$tool_source#libevent^dev" "$tool_source#openssl^dev" "$tool_source#zlib^dev")"
export PROTO_HOME="$toolchains/proto" RUSTUP_HOME="$toolchains/rustup"
shared_cargo_home=${CARGO_HOME:-}
export CARGO_HOME="$toolchains/cargo" MOON_HOME="$state/moon-home"
export PROTO_AUTO_CLEAN=false PROTO_TELEMETRY=false PROTO_YES=true
export RUSTUP_INIT_SKIP_PATH_CHECK=yes RUSTUP_TOOLCHAIN=stable
export PATH="$(dirname "$CCID_BIN"):$CARGO_HOME/bin:$PROTO_HOME/shims:$PROTO_HOME/bin:$PATH"
export CI_JOBS="${CI_JOBS:-2}" CARGO_BUILD_JOBS="${CI_JOBS:-2}"
export RUST_TEST_THREADS="${CI_TEST_THREADS:-2}"
export CARGO_NET_GIT_FETCH_WITH_CLI=true
# Append to any git config the verified runtime already supplies through the environment.
git_config_base=${GIT_CONFIG_COUNT:-0}
export GIT_CONFIG_COUNT=$((git_config_base + 2))
export "GIT_CONFIG_KEY_$((git_config_base + 0))=url.https://git.corbet.ch/corbet-libs/.insteadOf"
export "GIT_CONFIG_VALUE_$((git_config_base + 0))=https://github.com/corbet-libs/"
export "GIT_CONFIG_KEY_$((git_config_base + 1))=url.https://git.corbet.ch/corbet-foss/.insteadOf"
export "GIT_CONFIG_VALUE_$((git_config_base + 1))=https://github.com/corbet-foss/"
# Mirror the CI actions/cache entry for the fixture-owned Tor tool cache in
# persistent job state (best effort; a miss only rebuilds ephemerally).
if [ -n "${HOME:-}" ]; then
  mkdir -p "$state/ctrn-tools" "$HOME/.cache"
  if [ ! -e "$HOME/.cache/ctrn-tools" ]; then ln -s "$state/ctrn-tools" "$HOME/.cache/ctrn-tools"; fi
fi
# Shared-first toolchain lock: jobs share a provisioned toolchain and take
# exclusive only to provision missing pieces. Same lock file, so old
# whole-job-exclusive runners interoperate (they just serialize).
TOOLCHAIN_LOCK_FILE="$toolchains/install.lock"
TOOLCHAIN_LOCK_TIMEOUT="${CI_TIMEOUT:-2700}"
TOOLCHAIN_WANT_NIGHTLY=1
TOOLCHAIN_WASM_TARGETS="stable:wasm32-unknown-unknown"
# shellcheck source=toolchain-lock.sh
source "$(dirname "${BASH_SOURCE[0]}")/toolchain-lock.sh"
toolchain_ensure
cargo_bin=$("$CARGO_HOME/bin/rustup" which --toolchain stable cargo)
test -x "$cargo_bin"
export PATH="$(dirname "$cargo_bin"):$PATH"
export CCID_SOURCE_REVISION="$CI_COMMIT_SHA"
"$CCID_BIN" render --repo "$PWD" --check
proto --version
moon --version
rustc -vV
cargo --version
node --version
python3 --version
jq --version
curl --version
git --version
gcc --version
make --version
pkg-config --version
rustup run nightly rustc -vV
rustup run nightly cargo llvm-cov --version
# rusqlite links the system SQLite (non-bundled via tor-dirmgr); execute the
# check inside the pinned shell (same cmty pattern, sqlite instead of openssl).
nix-shell .ci/check-shell.nix --run 'export PATH="$(dirname "$CCID_BIN"):$PATH"; set -e; "$CCID_BIN" cached --repo "$PWD" --check verify; test -s ".ccid/results/verify.jsonl"'
