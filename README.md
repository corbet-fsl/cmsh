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

**Purpose.** A network-independent, bounded-message facade shaped like Veilid's
`app_message` and `app_call`, with Tor at launch and Veilid separately qualified.

**Owns.** Backend registration, coarse health/capability reports, complete messages,
one-use call replies and listeners. Pass minimum guarantees and consumer needs to
Fallback (`cfbk`), always requiring anonymity.

**Never.** Expose byte streams, framing, raw signing keys or Tor-specific member
API; select direct transport; add a second fallback policy engine; discover peers;
store messages; replay an uncertain send or split one connection across networks.

**States and ports.** Readiness derives from backend health. Upward: `app_message`,
`app_call`, `listen`, `status`, `take_switches`; listener events are Message, Call
with Reply, and ListenerClosed. Downward: `ctrn`, qualified `cvln`, and `cfbk`.
Payloads are opaque, preserving a path for future voice support.

**Invariants and tests.** No silent downgrade, direct fallback or false delivery ACK.
Bounds are checked before I/O and replies inherit the advertised limit. Real
Ferry/native/Wasm vectors cover selection, refusal, cancellation and listener
cleanup; real Tor/browser integration is independently gated in `ctrn`. Disabled
or unready backends are not selectable. Source line and branch gates target 100%.

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
