//! Mesh-owned message ports. Leaves are adapted here, never depend upward.
use crate::{Address, Capabilities, Error, MaybeSend, MaybeSync, Scheme};
use async_trait::async_trait;

/// One anonymous network's complete-message capability.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
pub trait Backend: MaybeSend + MaybeSync {
    /// Network discriminator, interpreted by its adapter alone.
    fn scheme(&self) -> Scheme;
    /// Implemented guarantees; never inferred merely from a network name.
    fn capabilities(&self) -> Capabilities;
    /// Largest complete message or reply, before any I/O or allocation.
    fn max_payload(&self) -> usize;
    /// Publish current reachability and accept complete messages.
    async fn listen(&self) -> Result<Box<dyn Listener>, Error>;
    /// Submit once; local completion is not a peer receipt.
    async fn app_message(&self, to: &Address, payload: &[u8]) -> Result<(), Error>;
    /// Submit once and return the same call's opaque reply.
    async fn app_call(&self, to: &Address, payload: &[u8]) -> Result<Vec<u8>, Error>;
}
/// Backend-owned receive loop. Dropping it withdraws its reachability.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
pub trait Listener: MaybeSend {
    /// Opaque address for this listener.
    fn address(&self) -> &Address;
    /// Receive the next complete payload, or report lost reachability.
    async fn next(&mut self) -> Result<Event, Error>;
}
/// A complete inbound payload; the application authenticates it.
pub enum Event {
    /// One-way message without an implicit receipt.
    Message(Vec<u8>),
    /// Request and a one-use, network-bound reply capability.
    Call {
        /// Opaque request bytes.
        payload: Vec<u8>,
        /// Send once on this call's original network.
        reply: Reply,
    },
}
/// Backend implementation of a one-use reply; never a member-facing byte stream.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
pub trait ReplyPort: MaybeSend {
    /// Send this call's response once.
    async fn send(self: Box<Self>, payload: &[u8]) -> Result<(), Error>;
}
/// Opaque reply bound to its original call. It cannot be serialized or cloned.
pub struct Reply {
    port: Box<dyn ReplyPort>,
    maximum: usize,
}
impl Reply {
    /// Adapt a backend-owned one-use reply and its payload bound.
    pub fn new(port: Box<dyn ReplyPort>, maximum: usize) -> Self {
        Self { port, maximum }
    }
    pub(crate) fn limit_to(mut self, maximum: usize) -> Self {
        self.maximum = self.maximum.min(maximum);
        self
    }
    /// Reply once, with the original network's limit and failure semantics.
    pub async fn send(self, payload: &[u8]) -> Result<(), Error> {
        if payload.len() > self.maximum {
            return Err(Error::new(
                crate::ErrorKind::Limit,
                "reply size out of bounds",
            ));
        }
        self.port.send(payload).await
    }
}
