# Mesh message contract

Mesh owns complete-message ports and delegates selection to `cfbk`. Backend
addresses are opaque scheme/bytes pairs; only their owner interprets them.
The launch configuration registers Tor. A future native Veilid adapter maps its
message/call operations directly; it is not required to emulate Tor streams.

`Mesh::app_message(peer_addresses, payload)` submits one bounded message.
`Mesh::app_call(peer_addresses, payload)` returns one bounded opaque reply.
`listen()` yields `Incoming::Message`, `Incoming::Call` with a one-use `Reply`,
or one `ListenerClosed` event per lost backend listener. Neither a completed
send nor a terminal event acknowledges application processing or persistence.
The caller authenticates and interprets opaque bytes. No signing secrets enter
Mesh; record RPC bytes and signatures belong to Records (`cdht`).

The builder passes minimum and consumer requirements to Fallback and always
requires anonymity. `status`, `active`, `offered`, `report` and `take_switches`
expose selection and coarse health. `offered` describes the selected backend;
it never unions guarantees across networks. Unqualified DHT/datagram/offline
claims are refused. Composition reports network health changes explicitly.
A failed send is returned without retrying another network, even when that
failure changes subsequent selection. Application retries belong to owners.

Payload bounds are positive and at most 1 MiB, checked before sending and on
received messages/replies. Limits can be smaller for a particular backend.
Listeners use bounded channels and cancellation; a failed setup or dropped
handle cancels its already-started receive pumps. `close` withdraws addresses.
Errors and Debug carry no payloads, peer addresses or dependency diagnostics.

The public API has no `dial`, stream, framing or raw signing-key interface.
The optional `tor` adapter depends downward on ctrn's complete-message port.
ctrn owns Tor byte I/O; its Ferry sublibrary owns maintained yamux/length framing,
partial reads, backpressure and stream lifetimes. No leaf imports Mesh.

Native and actual Wasm tests exercise this facade, real Fallback selection and
Ferry's actual framing over bounded byte I/O. Fixture capability flags are test
inputs, not anonymity evidence. ctrn separately tests actual private Tor and
HTTPS browser relay-gateway refusal. Public transport, DHT restoration and
assembled credential/MLS acceptance require their own evidence.
