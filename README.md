# cmsh

Mesh is the member device's transport-neutral facade. It wires the existing
LGPL `cmsh-api` backend port, `cfbk` selection, yamux sessions and the bounded
`streams` module. Configure only `ctrn` for the launch; future Veilid support
plugs into the same interface. There is no direct-network fallback.

API, states, format, bounds and verification: [docs/CONTRACT.md](docs/CONTRACT.md).

## Reuse and maintained candidates

- [futures-io](https://docs.rs/futures-io): frozen ordered byte-stream port.
- [tokio-util LengthDelimitedCodec](https://docs.rs/tokio-util/latest/tokio_util/codec/length_delimited/): existing framing, retained with explicit EOF and cancellation lifecycle.
- [yamux](https://docs.rs/yamux): existing bounded multiplexing, retained.
- [cfbk](https://github.com/corbet-foss/cfbk): existing ordered selection, pinned by revision. No second policy engine.
- cmsg framing and browser stream adapters: reference for fail-closed ownership
  during partial I/O. Domain messages and ACKs remain outside Mesh.

## CI

GitHub Actions runs stable Rust fmt, Clippy, native tests, a wasm32 check and
identical wasm stream vectors in the wasm-bindgen runner. Dependency identities
are retained in Cargo.lock and checked for unique immutable first-party pins.
The real private Tor network integration runs in ctrn; no accounts are needed.
No package is published and no infrastructure is deployed.

## License

The facade and streams module are FSL-1.1-ALv2; see [LICENSE.md](LICENSE.md).
The pre-existing `cmsh-api/` backend port remains LGPL-3.0-only WITH
LGPL-3.0-linking-exception. The unnamed streams implementation stays a module
here until the owner selects its eventual leaf name.
