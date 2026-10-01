//! Downward adapter from Mesh messages to the Tor backend's owner port.
use crate::{
    Address, Backend, Capabilities, Error, ErrorKind, Event, LatencyClass, Listener, Reply,
    ReplyPort, Scheme,
};
use async_trait::async_trait;

/// Mesh adapter over one community-local Tor message transport.
pub struct Tor(pub ctrn::messages::Messages);
fn failure(error: ctrn::Error) -> Error {
    let kind = match error.kind() {
        ctrn::ErrorKind::InvalidAddress => ErrorKind::InvalidAddress,
        ctrn::ErrorKind::Unsupported => ErrorKind::Unsupported,
        ctrn::ErrorKind::Network => ErrorKind::Network,
        ctrn::ErrorKind::Unreachable => ErrorKind::Unreachable,
        ctrn::ErrorKind::Timeout => ErrorKind::Timeout,
        ctrn::ErrorKind::Closed => ErrorKind::Closed,
        ctrn::ErrorKind::Limit => ErrorKind::Limit,
        ctrn::ErrorKind::Protocol => ErrorKind::Protocol,
    };
    Error::new(kind, "Tor message transport failed")
}
fn endpoint(address: &Address) -> Result<ctrn::Address, Error> {
    let bytes = address.expect_scheme(&scheme())?;
    ctrn::Address::new(ctrn::scheme(), bytes.to_vec()).map_err(failure)
}
fn scheme() -> Scheme {
    Scheme::new("tor").expect("constant network")
}
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32",async_trait(?Send))]
impl Backend for Tor {
    fn scheme(&self) -> Scheme {
        scheme()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            anonymous: true,
            datagrams: false,
            offline_delivery: false,
            dht: false,
            latency: LatencyClass::Interactive,
        }
    }
    fn max_payload(&self) -> usize {
        self.0.max_payload()
    }
    async fn listen(&self) -> Result<Box<dyn Listener>, Error> {
        let inner = self.0.listen().await.map_err(failure)?;
        let address = Address::new(scheme(), inner.address().bytes().to_vec())?;
        Ok(Box::new(TorListener {
            inner,
            address,
            maximum: self.max_payload(),
        }))
    }
    async fn app_message(&self, to: &Address, payload: &[u8]) -> Result<(), Error> {
        self.0
            .app_message(&endpoint(to)?, payload)
            .await
            .map_err(failure)
    }
    async fn app_call(&self, to: &Address, payload: &[u8]) -> Result<Vec<u8>, Error> {
        self.0
            .app_call(&endpoint(to)?, payload)
            .await
            .map_err(failure)
    }
}
struct TorListener {
    inner: ctrn::messages::MessageListener,
    address: Address,
    maximum: usize,
}
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32",async_trait(?Send))]
impl Listener for TorListener {
    fn address(&self) -> &Address {
        &self.address
    }
    async fn next(&mut self) -> Result<Event, Error> {
        match self.inner.next().await.map_err(failure)? {
            ctrn::Received::Message(payload) => Ok(Event::Message(payload)),
            ctrn::Received::Call { payload, reply } => Ok(Event::Call {
                payload,
                reply: Reply::new(Box::new(TorReply(reply)), self.maximum),
            }),
        }
    }
}
struct TorReply(ctrn::Reply);
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32",async_trait(?Send))]
impl ReplyPort for TorReply {
    async fn send(self: Box<Self>, payload: &[u8]) -> Result<(), Error> {
        self.0.send(payload).await.map_err(failure)
    }
}
