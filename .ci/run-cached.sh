#!/usr/bin/env bash
# Execute the manifest's selected checks with content-keyed result reuse.
set -euo pipefail
: "${CCID_BIN:?Missing verified ccid job runtime}"
: "${CI_COMMIT_SHA:?Missing verified source revision}"
: "${CI_REPOSITORY_URL:?Missing canonical repository identity}"
: "${CHECKS:?Missing declared check selection}"
# CHECKS is the manifest job selection, validated by ccid against the
# manifest; fresh milestone jobs route through `ccid check` below.
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
# Tool installation is an explicit part of this job, in persistent CI storage.
tool_source=github:NixOS/nixpkgs/b7c2ada94fe99c15b0dbcf4d11fd7850b957a436?narHash=sha256-IItrdb7Puk05RqOBWZYFC5X6Wl1sJmCfh5MWVHw5iMM=
tool_paths=$(nix build --no-link --print-out-paths "$tool_source#moon" "$tool_source#proto" "$tool_source#nodejs_22" "$tool_source#python3" "$tool_source#cargo-llvm-cov" "$tool_source#jq" "$tool_source#curl" "$tool_source#git" "$tool_source#gcc" "$tool_source#gnumake" "$tool_source#pkg-config" "$tool_source#libevent" "$tool_source#openssl" "$tool_source#zlib")
for path in $tool_paths; do PATH="$path/bin:$PATH"; done
# Tor fixture build inputs: tor-tools.sh compiles tor 0.4.9.12 from the pinned
# tarball on cache miss, needing -dev headers. Export store path lists only;
# verify.sh scopes LD_LIBRARY_PATH/PKG_CONFIG_PATH/CC to tor-records so
# stable gates keep the proto toolchain environment.
export CI_TOR_LIB_OUT="$(nix build --no-link --print-out-paths "$tool_source#libevent^out" "$tool_source#openssl^out" "$tool_source#zlib^out")"
export CI_TOR_LIB_DEV="$(nix build --no-link --print-out-paths "$tool_source#libevent^dev" "$tool_source#openssl^dev" "$tool_source#zlib^dev")"
export PROTO_HOME="$toolchains/proto"
export MOON_HOME="$state/moon-home"
export PROTO_AUTO_CLEAN=false PROTO_TELEMETRY=false PROTO_YES=true
export RUSTUP_INIT_SKIP_PATH_CHECK=yes RUSTUP_TOOLCHAIN=stable
export PATH="$(dirname "$CCID_BIN"):$PROTO_HOME/shims:$PROTO_HOME/bin:$PATH"
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
  # A persisted venv bakes absolute paths at creation: pip's shebang points at
  # the creating job's python, which vanishes with old job HOMEs (bad
  # interpreter). python3 itself is usually a symlink to a live base python,
  # so its executability proves nothing — read pip's shebang instead. Operate
  # only on the owned state dir (the HOME link must canonicalize to it; a
  # venv that is itself a symlink is refused), under the held state lock
  # above. No HOME/credential moves, no other jobs touched.
  ctrn_cache="$HOME/.cache/ctrn-tools"
  if [ -L "$ctrn_cache" ] && [ "$(readlink -m "$ctrn_cache")" = "$(readlink -m "$state/ctrn-tools")" ] \
    && [ -f "$state/ctrn-tools/venv/bin/pip" ] && [ ! -L "$state/ctrn-tools/venv" ]; then
    pip_stale=0
    pip_shebang=$(head -n 1 "$state/ctrn-tools/venv/bin/pip")
    case "$pip_shebang" in "#!"*) pip_interp=${pip_shebang#"#!"}; pip_interp=${pip_interp%%[[:space:]]*}; [ -n "$pip_interp" ] && [ -x "$pip_interp" ] || pip_stale=1;; *) pip_stale=1;; esac
    [ -x "$state/ctrn-tools/venv/bin/python3" ] || pip_stale=1
    if [ "$pip_stale" = 1 ]; then
      rm -rf "$state/ctrn-tools/venv"
    fi
  fi
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
cargo_bin=$(rustup which --toolchain stable cargo)
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
# nix-shell resolves its --run shell via <nixpkgs>.bashInteractive from ambient
# NIX_PATH, which deterministic workers lack; pin it explicitly from the same
# tool closure so the shell is deterministic (check-shell inputs, tool pin,
# workflows and gates unchanged).
NIX_BUILD_SHELL=""
for out in $(nix build --no-link --print-out-paths "$tool_source#bashInteractive"); do
  if [ -x "$out/bin/bash" ]; then NIX_BUILD_SHELL="$out/bin/bash"; fi
done
test -n "$NIX_BUILD_SHELL" || { echo 'pinned bashInteractive provides no bin/bash' >&2; exit 2; }
export NIX_BUILD_SHELL
if [ "$CHECKS" = "verify" ]; then
  nix-shell .ci/check-shell.nix --run 'export PATH="$(dirname "$CCID_BIN"):$PATH"; set -e; "$CCID_BIN" cached --repo "$PWD" --check verify; test -s ".ccid/results/verify.jsonl"'
else
  # The milestone Records job executes fresh on every dispatch: live-network
  # evidence must never be restored from a result cache. Warm compile caches
  # still apply through the shared target lock; the exit status is the result.
  nix-shell .ci/check-shell.nix --run 'export PATH="$(dirname "$CCID_BIN"):$PATH"; set -e; "$CCID_BIN" check --repo "$PWD" --check "$CHECKS"'
fi
