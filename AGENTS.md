# Agent instructions

Write comments and documentation in English.

## Product boundary

- Thin bounded-message facade over cfbk and owner network adapters.
- No Tor-specific type in Mesh; only the backend interprets opaque addresses.
- Tor is the launch backend; future Veilid is a separate implementation.
- Carry opaque messages and calls; no domain messages, delivery ACKs, credentials, storage or retry policy.
- Reuse maintained codecs and primitives. Record candidates in README.
- Facade is FSL. No cmsh-api protocol drawer or leaf dependency upward.
- Framing, partial reads, backpressure and stream lifecycle belong in cfry below ctrn.

## Quality boundary

- Current stable Rust, fmt, Clippy with warnings denied, real native tests and
  wasm32 build plus executed identical complete-message vectors on GitHub Actions.
- Do not run Cargo on the workstation. Use standalone rustfmt if needed.
- First-party git dependencies follow main; lock exactly one revision per crate.
- Commit explicit paths in small steps; plain English imperative, no AI credit.
- Pull with rebase before every push to main; never force push.
- Never deploy, publish to registries or cause payments.
