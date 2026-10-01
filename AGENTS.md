# Agent instructions

Write comments and documentation in English.

## Product boundary

- Thin transport facade over cmsh-api, cfbk, yamux and the streams module.
- No Tor-specific type in Mesh; only the backend interprets opaque addresses.
- Tor is the launch backend; future Veilid is a separate implementation.
- No application messages, delivery ACKs, credentials, storage or retry policy.
- Reuse maintained codecs and primitives. Record candidates in README.
- Root facade/modules are FSL; existing cmsh-api is LGPL with linking exception.
- Streams has no final leaf name: keep it as a module here.

## Quality boundary

- Current stable Rust, fmt, Clippy with warnings denied, real native tests and
  wasm32 build plus executed identical framing vectors on GitHub Actions.
- Do not run Cargo on the workstation. Use standalone rustfmt if needed.
- Pin first-party git dependencies by full revision; one revision per crate.
- Commit explicit paths in small steps; plain English imperative, no AI credit.
- Pull with rebase before every push to main; never force push.
- Never deploy, publish to registries or cause payments.
