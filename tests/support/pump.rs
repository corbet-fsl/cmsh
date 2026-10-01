//! Real completed message followed by the receiving application's channel shutdown.
use super::*;
use crate::Listener;
use tokio_util::compat::TokioAsyncReadCompatExt;
struct Time;
impl cfry::streams::Clock for Time {
    fn now(&self) -> u64 {
        0
    }
    fn sleep_until(&self, _: u64) -> cfry::BoxFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
}
struct Wire {
    peer: cfry::Peer,
    address: Address,
}
#[crate::async_trait]
impl Listener for Wire {
    fn address(&self) -> &Address {
        &self.address
    }
    async fn next(&mut self) -> Result<Event, Error> {
        match self.peer.next().await.unwrap() {
            cfry::Received::Message(payload) => Ok(Event::Message(payload)),
            _ => panic!("fixture sends one-way messages"),
        }
    }
}
#[tokio::test]
async fn consumer_shutdown_terminates_a_real_listener_pump() {
    let (a, b) = tokio::io::duplex(64);
    let limits = cfry::MessageLimits {
        max_payload: 1024,
        timeout_ms: 1000,
    };
    let (a, ad) = cfry::Peer::new(
        Box::new(a.compat()),
        cfry::Role::Dialer,
        Arc::new(Time),
        limits,
    )
    .unwrap();
    let (b, bd) = cfry::Peer::new(
        Box::new(b.compat()),
        cfry::Role::Listener,
        Arc::new(Time),
        limits,
    )
    .unwrap();
    tokio::spawn(ad);
    tokio::spawn(bd);
    let scheme = Scheme::new("fixture").unwrap();
    let listener = Box::new(Wire {
        peer: b,
        address: Address::new(scheme.clone(), vec![1]).unwrap(),
    });
    let (sender, mut receiver) = mpsc::channel(16);
    // The external receiver may close while the owned task is delivering its
    // final frame. Exercise that real channel boundary before abort is observed.
    receiver.close();
    let (send, ()) = futures::join!(
        a.app_message(b"already received"),
        forward(listener, scheme, 1024, sender)
    );
    send.unwrap();
    assert!(receiver.next().await.is_none());
}
