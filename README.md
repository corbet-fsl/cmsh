# cmsh

Mesh is the member device's transport-neutral facade for bounded messages,
request/reply calls, observable network health and qualified backend selection.
It uses [Fallback (`cfbk`)](https://github.com/corbet-foss/cfbk) for selection;
anonymity remains mandatory. There is no direct-network fallback or second
policy machine.

Mesh owns its message ports. The optional `tor` adapter depends downward on
[ctrn](https://github.com/corbet-foss/ctrn), whose
[Ferry (`cfry`)](https://github.com/corbet-foss/cfry) sublibrary owns framing,
partial reads, backpressure and stream lifecycle. Veilid's `app_message` and
`app_call` model the public shape; no byte-stream assumption reaches consumers.
The old `cmsh-api` workspace package and secret-bearing DHT port are removed.

API, bounds, failure semantics and verification: [contract](docs/CONTRACT.md).

## Scope

### Purpose

cmsh is how bytes travel between members: a network-independent bounded-message interface over two full backends, with network choice executed by cfbk.

### Owns

The message ports (send one bounded message, one bounded call with a one-use reply, listen); backend registration for ctrn (Tor) and cvln (Veilid) with coarse health and capability reporting; passing minimum and consumer requirements to cfbk while always requiring anonymity; a payload-agnostic shape that keeps later datagrams and voice possible.

### Never

Assume anything Tor-specific in its interface; expose raw streams, framing, or signing keys; fall back to direct transport, silently or forcibly downgrade, run one connection over two networks, or claim Tor and Veilid have equal anonymity; keep a second fallback policy engine, application retries, discovery, keys, or message storage; replay an uncertain send on another backend.

### States

Starting, Available for the selected backend, Unavailable, and Stopped, derived from backend health; Tor is the only selectable backend at the first milestone.

### Test obligations

Real Tor round trips through the message interface; selection and error tests proving required properties are never relaxed and that a disabled or unready backend cannot be selected; backend loss reported exactly once with no false acknowledgement; oversized payloads refused before I/O; listener cleanup on failure. Shared obligations: current stable Rust with native and Wasm builds using identical vectors; explicit state machine with injected clock, randomness, storage, and network; thin facade with no duplicate state, crypto, retry, or roster logic; full line and branch coverage with real round trips and injected delay, loss, duplication, cancellation, corruption, and conflicts; bounded bytes, queues, and work with no secrets or identifiers in errors; isolation of keys, identities, sessions, and stores per community; reuse of maintained third-party code with no own crypto.

## Reuse and validation

- `cfbk` performs ordered selection under minimum guarantees and health.
- `futures` supplies bounded receive channels and cancellation.
- `ctrn` supplies real Tor onion I/O; Ferry uses maintained tokio-util and yamux.
- Native and executed Wasm contracts use actual Ferry frames over backpressured
  byte I/O. Actual private Tor/browser probes remain separately gated in ctrn.

First-party dependencies follow `main`; CI resolves one shared lock snapshot and
checks one revision per crate. Dependabot updates Cargo and Actions. Strict line
and branch coverage must both pass before review; no compile-only acceptance.

FSL-1.1-ALv2; see [LICENSE.md](LICENSE.md).
