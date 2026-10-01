# Private Tor Records experiment

The native `tor_records` example executes the real Mesh message facade, Tor/Arti
and Ferry against cdht's original signed Veilid RPC bytes. Eight independent
clients publish their own onion services. Five have independent device record
stores; the other three exercise the original owner, persisted offline queue and
fresh-device read paths. No operator DHT store is constructed.

The example checks create/get/set, an actual WatchValue request and signed reverse
ValueChanged, stopped-transport queue persistence, independent-device flush, and
fresh empty-cache decrypt. It then evicts four copies through actual capacity-one
stores and verifies owner open inspects a surviving holder and republishes the
same original ciphertext/nonce/signatures to five stores without a signer.
Capacity one is a bounded eviction fixture, not a production default or retention
measurement. Expensive population/churn work is a separate gate.

The explicit fixture roster supplies known node keys and observed onion endpoints.
It does **not** implement production signed NodeInfo registration, authenticated
bootstrap or iterative discovery. Keys/read capabilities use public test seeds;
node, anonymous watcher and record-owner keys are distinct. Watch requests use
the separate watcher capability and gain no member quota. This is not a passkey
or membership recovery ceremony. No production DHT capability
is advertised. Public Tor, browser DHT and retention need independent evidence.

The disposable signed Tor network and tools run directly from the exact resolved
ctrn source. Existing native/Wasm Mesh tests, production coverage and Tor's separate
private-network/HTTPS browser contracts remain required. The example's wrapper and
its `tests/support` implementation are test harness, excluded from the production
coverage denominator; no production source is excluded.
