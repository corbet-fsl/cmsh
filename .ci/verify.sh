#!/usr/bin/env bash
set -euo pipefail
exec 2>&1
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"
export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
export RUNNER_TEMP="${RUNNER_TEMP:-$(mktemp -d)}"
export GITHUB_PATH="${GITHUB_PATH:-$RUNNER_TEMP/github_path.txt}"
export GITHUB_ENV="${GITHUB_ENV:-$RUNNER_TEMP/github_env.txt}"
touch "$GITHUB_PATH" "$GITHUB_ENV"

rustc --version
cargo metadata --locked --format-version 1 > dependency-metadata.json
python3 .github/check-first-party.py
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
cargo check --locked --target wasm32-unknown-unknown --lib

cargo metadata --locked --format-version 1 > dependency-metadata.json
version=$(jq -r '.packages[] | select(.name == "wasm-bindgen") | .version' dependency-metadata.json)
archive="wasm-bindgen-${version}-x86_64-unknown-linux-musl.tar.gz"
base="${WASM_BINDGEN_DOWNLOAD_BASE:-https://github.com/wasm-bindgen/wasm-bindgen/releases/download}/${version}"
mkdir -p "$RUNNER_TEMP/wasm-runner"
(
  cd "$RUNNER_TEMP/wasm-runner"
  curl -A 'transport-ci/1.0' --fail --location --retry 3 --remote-name "$base/$archive"
  curl -A 'transport-ci/1.0' --fail --location --retry 3 --remote-name "$base/$archive.sha256sum"
  checksum=$(cut -d ' ' -f1 "$archive.sha256sum")
  printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check
  tar -xzf "$archive" --strip-components=1
  echo "$PWD" >> "$GITHUB_PATH"
)
export PATH="$RUNNER_TEMP/wasm-runner:$PATH"

CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner cargo test --locked --all-features --target wasm32-unknown-unknown --test it

python3 .github/check-first-party.py

# --- tor-records (Linux integration gate) ---
# Job-local Ubuntu-image adaptation: tor-tools.sh (ctrn @9894d84, exact
# fixture resolved from Cargo.lock) installs libevent-dev/libssl-dev/
# zlib1g-dev via `sudo apt-get`, then builds tor 0.4.9.12 from the pinned
# dist.torproject.org tarball (sha256-checked), clones chutney at 6cc1588,
# and creates a venv. The job supplies the identical libraries from pinned
# nix (libevent 2.1.13, openssl 3.6.3, zlib 1.3.2) plus gcc 15.3.0, gnumake,
# pkg-config and git, so only the image-specific installer is neutralized:
# `sudo` passes commands through, `apt-get` is a documented no-op.
: "${CI_TOR_LIB_OUT:?Missing nix Tor library paths from run-cached.sh}"
: "${CI_TOR_LIB_DEV:?Missing nix Tor header paths from run-cached.sh}"
mkdir -p "$RUNNER_TEMP/job-bin"
printf '#!/usr/bin/env bash\nexec "$@"\n' > "$RUNNER_TEMP/job-bin/sudo"
printf '#!/usr/bin/env bash\nexit 0\n' > "$RUNNER_TEMP/job-bin/apt-get"
chmod +x "$RUNNER_TEMP/job-bin/sudo" "$RUNNER_TEMP/job-bin/apt-get"
export PATH="$RUNNER_TEMP/job-bin:$PATH"
# verify.sh runs inside the pinned nix-shell (sqlite headers/libs); save the
# shell-supplied compiler/library env before scoping Tor-only paths, then
# restore (never unset) so later gates keep the shell environment.
_orig_cc="${CC:-}"
_orig_ld="${LD_LIBRARY_PATH:-}"
_orig_pc="${PKG_CONFIG_PATH:-}"
export CC=gcc
tor_ld=""
for p in $CI_TOR_LIB_OUT; do tor_ld="${tor_ld:+$tor_ld:}$p/lib"; done
export LD_LIBRARY_PATH="$tor_ld${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
tor_pc=""
for p in $CI_TOR_LIB_DEV; do tor_pc="${tor_pc:+$tor_pc:}$p/lib/pkgconfig:$p/share/pkgconfig"; done
export PKG_CONFIG_PATH="$tor_pc${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
bash .ci/records.sh
if [ -n "$_orig_cc" ]; then export CC="$_orig_cc"; else unset CC; fi
if [ -n "$_orig_ld" ]; then export LD_LIBRARY_PATH="$_orig_ld"; else unset LD_LIBRARY_PATH; fi
if [ -n "$_orig_pc" ]; then export PKG_CONFIG_PATH="$_orig_pc"; else unset PKG_CONFIG_PATH; fi
export PATH="$(printf '%s' "$PATH" | tr ':' '\n' | { grep -v "$RUNNER_TEMP/job-bin" || true; } | paste -sd: -)"

rustup run nightly rustc --version
NIGHTLY_RUSTC="$(rustup which --toolchain nightly rustc)"
NIGHTLY_CARGO="$(rustup which --toolchain nightly cargo)"
NIGHTLY_BIN="$(dirname "$NIGHTLY_RUSTC")"
export RUSTC="$NIGHTLY_RUSTC"
export CARGO="$NIGHTLY_CARGO"
export PATH="$NIGHTLY_BIN:$PATH"
export RUSTUP_TOOLCHAIN=nightly
"$NIGHTLY_CARGO" llvm-cov --version
# Bug 2 isolation: coverage data must not reuse the shared cargo target
# across jobs (stale checkout paths fail the next gate); scope the
# coverage target to this job only. Native gates keep the shared warm
# target; outer traps and services are untouched.
(
coverage_target="$(mktemp -d)"
case "$coverage_target" in ""|"$PWD"/*) echo 'unsafe coverage target' >&2; exit 2;; esac
trap 'rm -rf -- "$coverage_target"' EXIT
export CARGO_TARGET_DIR="$coverage_target"
"$NIGHTLY_CARGO" llvm-cov --locked --workspace --all-features --branch --no-cfg-coverage --no-cfg-coverage-nightly --ignore-filename-regex '/tests/|/examples/' --json --output-path coverage.json
"$NIGHTLY_CARGO" llvm-cov report --branch --ignore-filename-regex '/tests/|/examples/' --lcov --output-path coverage.lcov
"$NIGHTLY_CARGO" llvm-cov report --branch --ignore-filename-regex '/tests/|/examples/' --text --output-path coverage.txt
)
python3 .github/test-source-coverage.py && python3 .github/check-source-coverage.py coverage.lcov coverage.json coverage.txt
