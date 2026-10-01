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
