# cmsh agent notes

Work in a lane (`~/agents/scratch/lane-new <lane>`); never edit `~/agents/scratch/ws`.
Run from the lane root (`<lane>/ws`); `cbuild <lane> ...` builds there.

Build and test (quote the verdict line, never infer from a pipe):
- `cbuild <lane> fmt -p cmsh -- --check` — must be clean
- `cbuild <lane> test -p cmsh --all-features` — lib `12`, `contracts` `8`,
  `tor_port` `1`, doc `0` passed, exit 0; long, background it
- `cbuild <lane> clippy -p cmsh --locked --all-targets --all-features -- -D warnings` — exit 0
- `cbuild <lane> check -p cmsh --locked --target wasm32-unknown-unknown --lib` — exit 0
- Full gate (first-party refresh, wasm vectors, real Tor/browser probes in
  ctrn, coverage) runs in CI; see `.ci/verify.sh`, `docs/CI.md`, `docs/CONTRACT.md`.

Land: `cargo fmt` in touched repos, rebase on main (never force-push),
`cbuild <lane> xtask land --dry`, then `xtask land`.

Invariants (rule -> enforcing test):
- Fallback is explicit and never replays a failed call onto another
  network; no silent downgrade, direct fallback or false delivery ACK
  -> `tests/contracts.rs::fallback_is_explicit_and_never_replays_a_failed_call`
- Mandatory anonymity and declared capabilities fail closed
  -> `::mandatory_anonymity_and_declared_capabilities_fail_closed`
- Payload bounds checked before I/O and on received messages, calls and
  replies; inbound traffic plus its reply cannot exceed the bound
  -> `::actual_inbound_messages_and_calls_cannot_exceed_mesh_bound`,
  `::original_call_and_reply_bounds_are_enforced_independently`
- Close withdraws listeners; health events are bounded, no change adds none
  -> `::close_withdraws_listener_status_before_executor_polls_abort`,
  `::health_events_are_bounded_and_no_change_adds_no_event`
- Errors and Debug carry no payloads, addresses or dependency diagnostics
  -> `error::tests::adapter_errors_keep_only_coarse_static_context`
- Tor adapter moves bounded messages with one-use replies over real
  Node/Ferry byte I/O (needs `--features tor`)
  -> `tests/tor_port.rs::mesh_tor_port_uses_bounded_messages_and_original_one_use_reply`

Pointers: `src/mesh.rs`, `src/port.rs`, `src/address.rs`, `docs/CONTRACT.md`.
Commits: explicit paths, imperative, author per `~/agents/AGENT.md`; no AI trailers.
`publish=false`; FSL-1.1-ALv2; no leaf dependency upward, no `cmsh-api` drawer.
