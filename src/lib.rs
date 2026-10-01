//! Mesh selects anonymous network adapters and carries complete bounded messages.
#![forbid(unsafe_code)]
mod address;
mod capabilities;
mod error;
mod mesh;
mod port;
mod property;
#[cfg(feature = "tor")]
mod tor;
pub use address::{Address, MAX_ADDRESS_BYTES, Scheme};
pub use async_trait::async_trait;
pub use capabilities::{Capabilities, LatencyClass};
pub use cfbk::{Health, Switch, Unavailable};
pub use error::{Error, ErrorKind};
pub use mesh::{BackendStatus, Incoming, Listening, Mesh, MeshBuilder, MeshError};
pub use port::{Backend, Event, Listener, Reply, ReplyPort};
pub use property::{Policy, Property, properties};
#[cfg(feature = "tor")]
pub use tor::Tor;
/// Send on native runtimes, local on Wasm.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}
/// Send on native runtimes, local on Wasm.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}
/// Sync on native runtimes, local on Wasm.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}
/// Sync on native runtimes, local on Wasm.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}
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
