# Browser port contract: `cmsh` over Tor

Scope: embedding the current `cmsh` message facade in the real browser
`cmsg` runtime over Tor. Tor only; no new API, no new adapter, no
alternative network.

## What `cmsh` already provides (exact surface)

Construction and execution:

- `Mesh::builder(spawn)` takes `Arc<dyn Spawn>` and returns `MeshBuilder`
  ([src/mesh.rs](../src/mesh.rs)). `MeshBuilder::backend`,
  `::order`, `::minimum`, `::policy`, `::build` register backends and
  delegate all network selection to Fallback (`cfbk`).
- `Spawn::spawn` runs background listener pumps on the embedding runtime.
  The documented browser form is `wasm_bindgen_futures::spawn_local`
  ([src/lib.rs](../src/lib.rs)).
- `BoxFuture` is `Send` on native targets and local on `wasm32`;
  `MaybeSend` requires `Send` and `MaybeSync` requires `Sync` natively,
  and both are blanket on `wasm32` ([src/lib.rs](../src/lib.rs)). The
  `Backend`, `Listener` and `ReplyPort` traits use
  `async_trait(?Send)` on `wasm32` ([src/port.rs](../src/port.rs)).

Message ports (network-independent, unchanged in the browser):

- `Mesh::app_message`, `Mesh::app_call`, `Mesh::listen`
  ([src/mesh.rs](../src/mesh.rs)); events `Incoming::Message`,
  `Incoming::Call` with one-use `Reply`, `Incoming::ListenerClosed`.
- `Reply::new` adapts a backend-owned one-use reply with its payload
  bound ([src/port.rs](../src/port.rs)).
- Addresses are opaque `Scheme`/bytes pairs; only the serving backend
  interprets them. `Debug` never prints bytes
  ([src/address.rs](../src/address.rs)). Errors carry a coarse
  `ErrorKind` plus a static string, never payloads or peer data
  ([src/error.rs](../src/error.rs)).

Tor input (owner-supplied):

- `Tor(pub ctrn::messages::Messages)`, behind the `tor` feature
  (`tor = ["dep:ctrn"]`, `ctrn` with `default-features = false`;
  see [Cargo.toml](../Cargo.toml)), implements `Backend` over the
  `ctrn` complete-message port ([src/tor.rs](../src/tor.rs)).
- No production source in `src/` constructs the `ctrn` stack: nothing
  there calls `Messages::new` or builds a `ctrn` node/network; the
  facade takes an owner-built `Messages` as given. Construction appears
  in two fixture locations: the `adapter` helper in
  [tests/tor_port.rs](../tests/tor_port.rs), which pairs a fixture
  `ctrn::Network` over in-memory byte streams with `Node::new` and
  `Messages::new`, and the `participant` helper in
  [tests/support/tor_records/main.rs](../tests/support/tor_records/main.rs)
  (reached via [examples/tor_records.rs](../examples/tor_records.rs)),
  which builds a real `ctrn::arti::ArtiNetwork`, `Node::new` and
  `Messages::new` per onion participant on native targets (the
  example's `main` is empty on `wasm32`). Both are fixtures, not
  production composition.
- `Tor` reports `Capabilities { anonymous: true, datagrams: false,
  offline_delivery: false, dht: false, latency: Interactive }`.
  `MeshBuilder::build` refuses unqualified `datagrams`/`dht`/
  `offline_delivery` claims, always inserts `Property::Anonymous`, and
  rejects bounds outside `1..=1 MiB` ([src/mesh.rs](../src/mesh.rs),
  [src/tor.rs](../src/tor.rs)).

Readiness and uncertain sends:

- `active`, `status`, `offered`, `report`, `take_switches` expose
  Fallback selection and coarse per-backend health; `offered`
  describes the selected backend only, never a union across networks
  ([src/mesh.rs](../src/mesh.rs), [docs/CONTRACT.md](CONTRACT.md)).
- `app_message`/`app_call` select exactly one backend, check its bound
  before I/O, and on failure return without replaying on another
  network, even when the failure changes subsequent selection
  ([src/mesh.rs](../src/mesh.rs), [docs/CONTRACT.md](CONTRACT.md)).

## Two adapters, not one

- `cmsh::Tor` is a Rust adapter: it maps `Backend`/`Listener`/
  `ReplyPort` onto `ctrn::messages::Messages`,
  `ctrn::messages::MessageListener`, `ctrn::Received` and `ctrn::Reply`
  ([src/tor.rs](../src/tor.rs)). Its source is written for both
  targets (local futures and `?Send` traits on `wasm32`; see above)
  and carries no JavaScript, WebSocket, WebTransport or bridge logic;
  none exists anywhere in `src/` or `tests/`. The `wasm32` check gate
  is `cargo check --locked --target wasm32-unknown-unknown --lib`;
  this document executes nothing and claims no build result.
- How Tor bytes traverse the browser (bridge/relay path, JavaScript
  glue) is the separate `ctrn` JavaScript/Wasm browser adapter plus
  the browser-network spike outcome. It is owned outside this package;
  real private-Tor and HTTPS browser relay-gateway evidence is gated
  in `ctrn` CI ([docs/CONTRACT.md](CONTRACT.md)).

## Missing composition (owner prerequisite, not invented here)

To run `cmsh` in the browser over Tor, the embedding owner (`cmsg`
with `ctrn`) must supply, from browser-capable pieces:

1. A `ctrn` network input that works in the browser and a
   `ctrn::messages::Messages` built on it (the analogue of the
   `Node::new` + `Messages::new` compositions used by the
   [tests/tor_port.rs](../tests/tor_port.rs) fixture and the
   [tests/support/tor_records/main.rs](../tests/support/tor_records/main.rs)
   private-Tor fixture).
2. That value wrapped as `cmsh::Tor`, registered via
   `Mesh::builder(spawn).backend(...)` with a `spawn_local`
   `Spawn`.

No browser constructor exists in `cmsh`. If the `ctrn` browser input
is unavailable, the browser port is blocked at prerequisite 1;
whether the facade requires changes for that composition is unproven
until the composition is built and exercised. First milestone
selectable set is Tor only.

## Evidence map (existing gates, by kind)

No execution is claimed by this document. All behavior below is
exercised by existing suites; see [docs/CI.md](CI.md) and
[.github/workflows/ci.yml](../.github/workflows/ci.yml).

| Kind | Native | Wasm (`wasm32-unknown-unknown`) |
|---|---|---|
| Build and execution | `cargo test --locked --workspace --all-features` builds and runs the suite natively (check job) | `cargo check --locked --target wasm32-unknown-unknown --lib` checks the library for wasm without execution (check job) |
| Controlled-byte roundtrip | `cargo test` runs [tests/contracts.rs](../tests/contracts.rs) (real Ferry frames over in-memory byte I/O) and [tests/tor_port.rs](../tests/tor_port.rs) (real `ctrn` Node/Ferry over fixture byte streams) | Same two files executed under `wasm-bindgen-test-runner`: `cargo test --locked --all-features --target wasm32-unknown-unknown --test contracts --test tor_port` |
| Real Tor | `bash .ci/records.sh` (tor-records job): native private-Tor record messages through the real facade ([docs/RECORDS-EXPERIMENT.md](RECORDS-EXPERIMENT.md), [examples/tor_records.rs](../examples/tor_records.rs)) | Real Tor/browser probes are gated in `ctrn`, not here ([docs/CONTRACT.md](CONTRACT.md)) |

Quality gates preserved: `cargo fmt --check`, Clippy with `-D warnings`,
100% line and branch coverage with no production-code exclusions
(coverage job). Unit paths for address, error and property live under
[tests/support](../tests/support) via `#[path]` includes in
[src/address.rs](../src/address.rs), [src/error.rs](../src/error.rs)
and [src/property.rs](../src/property.rs).

## Selector and CI notes for owners

- The Wasm executed-vectors command names `--test contracts --test
  tor_port` explicitly. A future browser-port integration file would
  not be picked up by that command until its `--test` name is added,
  which belongs to CI owners, not this package. Native `cargo test
  --workspace --all-features` selects integration files automatically.
