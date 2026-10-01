#!/usr/bin/env bash
set -euo pipefail
: "${RUNNER_TEMP:?CI worker required}"
cargo metadata --locked --format-version 1 > records-metadata.json
transport_source=$(python3 - <<'PY'
import json
from pathlib import Path
packages=json.load(open('records-metadata.json'))['packages']
transport=[p for p in packages if p['name']=='ctrn']
assert len(transport)==1
assert transport[0]['source'].startswith('git+https://github.com/corbet-foss/ctrn?branch=main#')
print(Path(transport[0]['manifest_path']).parent)
PY
)
# Execute the exact resolved transport owner's maintained disposable fixture.
# No copied directory authority, endpoint encoding or Tor discovery implementation.
bash "$transport_source/.ci/tor-tools.sh"
cargo build --locked --example tor_records --features tor
record_target=$(python3 -c 'import json; print(json.load(open("records-metadata.json"))["target_directory"])')
export CHUTNEY_SOURCE="$HOME/.cache/ctrn-tools/chutney"
export TOR_BIN="$HOME/.cache/ctrn-tools/bin/tor"
export TOR_GENCERT_BIN="$HOME/.cache/ctrn-tools/bin/tor-gencert"
export TOR_PROBE_BIN="$record_target/debug/examples/tor_records"
export TOR_ARTIFACT="$RUNNER_TEMP/records-tor-evidence"
"$HOME/.cache/ctrn-tools/venv/bin/python" "$transport_source/.ci/private-network.py"
