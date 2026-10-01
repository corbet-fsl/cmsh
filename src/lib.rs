//! P2P transport facade.
//!
//! `cmsh` offers one transport interface to the member side (`cmsg`, the vault's
//! "send to my device" port, `cdht` through the DHT capability) over network
//! leaves that implement [`cmsh_api::Backend`] (`cvln` for Veilid, `ctrn` for
//! Tor):
//!
//! - listen and dial by abstract [`Address`] (scheme + bytes);
//! - reliable streams, multiplexed once here with `yamux` ([`Connection`],
//!   [`Substream`]) and framed with `tokio-util`'s length-delimited codec
//!   ([`frame`]);
//! - declared [`Capabilities`], surfaced per backend ([`Mesh::status`]) and in
//!   combination ([`Mesh::offered`]);
//! - a connection [`Policy`] (default: anonymous only) plus the operator's
//!   minimum standard, executed by `cfbk`: nothing below either is ever used,
//!   and every switch is recorded ([`Mesh::take_switches`]);
//! - optional datagrams ([`Mesh::datagrams`]) and the DHT capability hook
//!   ([`Mesh::dht`]) where a backend offers them.
//!
//! The facade is runtime-agnostic: background futures (session drivers,
//! listener pumps) go to the embedding runtime through [`Spawn`].
#![forbid(unsafe_code)]

pub mod frame;
mod mesh;
#[cfg(any(test, feature = "mock"))]
pub mod mock;
mod property;
mod session;
pub mod streams;

#[cfg(target_arch = "wasm32")]
pub mod browser;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub use cfbk::{Health, Switch, Unavailable};
pub use cmsh_api::{
    Address, Backend, BoxStream, ByteStream, Capabilities, Datagrams, Dht, DhtKeyPair, DhtSchema,
    DhtSchemaMember, DhtValue, Error, ErrorKind, LatencyClass, Listener, MaybeSend, MaybeSync,
    RecordKey, Scheme,
};
pub use mesh::{
    BackendStatus, Connection, DatagramPort, DhtPort, Incoming, Listening, Mesh, MeshBuilder,
    MeshError,
};
pub use property::{Policy, Property, properties};
pub use session::{MAX_SUBSTREAMS, Role, Session, SessionEvent, Substream};

use std::future::Future;
use std::pin::Pin;

/// A boxed future; `Send` on native targets.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// A boxed future; `Send` on native targets.
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Runs the facade's background futures on the embedding runtime, for example
/// `|f| { tokio::spawn(f); }` natively or `wasm_bindgen_futures::spawn_local`
/// in the browser.
pub trait Spawn: MaybeSend + MaybeSync {
    /// Run `future` to completion in the background.
    fn spawn(&self, future: BoxFuture<'static, ()>);
}

impl<F> Spawn for F
where
    F: Fn(BoxFuture<'static, ()>) + MaybeSend + MaybeSync,
{
    fn spawn(&self, future: BoxFuture<'static, ()>) {
        self(future)
    }
}
