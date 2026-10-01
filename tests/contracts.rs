//! Mesh message facade over real Ferry/yamux with only external byte I/O in memory.
//! Fixture capability flags exercise selection; they make no anonymity claim.
use cmsh::{
    Address, Backend, BoxFuture, Capabilities, Error, ErrorKind, Event, Health, Incoming,
    LatencyClass, Listener, Mesh, MeshError, Policy, Property, Reply, ReplyPort, Scheme,
};
use std::sync::Arc;
use tokio_util::compat::TokioAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
struct Time;
impl cfry::streams::Clock for Time {
    fn now(&self) -> u64 {
        0
    }
    fn sleep_until(&self, _: u64) -> cfry::BoxFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
}
fn spawn(f: BoxFuture<'static, ()>) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::spawn(f);
    }
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(f);
}
fn failure(e: cfry::Error) -> Error {
    Error::new(
        match e.kind() {
            cfry::ErrorKind::Closed => ErrorKind::Closed,
            cfry::ErrorKind::Limit => ErrorKind::Limit,
            cfry::ErrorKind::Protocol => ErrorKind::Protocol,
            cfry::ErrorKind::Timeout => ErrorKind::Timeout,
        },
        "Ferry fixture I/O failed",
    )
}
struct Wire {
    peer: cfry::Peer,
    here: Address,
    there: Address,
    caps: Capabilities,
    maximum: usize,
}
fn pair(name: &str, anonymous: bool) -> (Arc<Wire>, Arc<Wire>) {
    let (a, b) = tokio::io::duplex(64);
    let lim = cfry::MessageLimits {
        max_payload: 1024,
        timeout_ms: 1000,
    };
    let (a, ad) = cfry::Peer::new(
        Box::new(a.compat()),
        cfry::Role::Dialer,
        Arc::new(Time),
        lim,
    )
    .unwrap();
    let (b, bd) = cfry::Peer::new(
        Box::new(b.compat()),
        cfry::Role::Listener,
        Arc::new(Time),
        lim,
    )
    .unwrap();
    spawn(ad);
    spawn(bd);
    let scheme = Scheme::new(name).unwrap();
    let left = Address::new(scheme.clone(), vec![1]).unwrap();
    let right = Address::new(scheme, vec![2]).unwrap();
    let caps = Capabilities {
        anonymous,
        datagrams: false,
        dht: false,
        offline_delivery: false,
        latency: LatencyClass::Interactive,
    };
    (
        Arc::new(Wire {
            peer: a,
            here: left.clone(),
            there: right.clone(),
            caps,
            maximum: 1024,
        }),
        Arc::new(Wire {
            peer: b,
            here: right,
            there: left,
            caps,
            maximum: 1024,
        }),
    )
}
#[cfg_attr(not(target_arch = "wasm32"), cmsh::async_trait)]
#[cfg_attr(target_arch="wasm32", cmsh::async_trait(?Send))]
impl Backend for Wire {
    fn scheme(&self) -> Scheme {
        self.here.scheme().clone()
    }
    fn capabilities(&self) -> Capabilities {
        self.caps
    }
    fn max_payload(&self) -> usize {
        self.maximum
    }
    async fn listen(&self) -> Result<Box<dyn Listener>, Error> {
        Ok(Box::new(Receiver(self.peer.clone(), self.here.clone())))
    }
    async fn app_message(&self, to: &Address, payload: &[u8]) -> Result<(), Error> {
        if to != &self.there {
            return Err(Error::new(
                ErrorKind::Unreachable,
                "unknown fixture endpoint",
            ));
        }
        self.peer.app_message(payload).await.map_err(failure)
    }
    async fn app_call(&self, to: &Address, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if to != &self.there {
            return Err(Error::new(
                ErrorKind::Unreachable,
                "unknown fixture endpoint",
            ));
        }
        self.peer.app_call(payload).await.map_err(failure)
    }
}
struct Receiver(cfry::Peer, Address);
#[cfg_attr(not(target_arch = "wasm32"), cmsh::async_trait)]
#[cfg_attr(target_arch="wasm32", cmsh::async_trait(?Send))]
impl Listener for Receiver {
    fn address(&self) -> &Address {
        &self.1
    }
    async fn next(&mut self) -> Result<Event, Error> {
        Ok(match self.0.next().await.map_err(failure)? {
            cfry::Received::Message(p) => Event::Message(p),
            cfry::Received::Call { payload, reply } => Event::Call {
                payload,
                reply: Reply::new(Box::new(Response(reply)), 1024),
            },
        })
    }
}
struct Response(cfry::Reply);
#[cfg_attr(not(target_arch = "wasm32"), cmsh::async_trait)]
#[cfg_attr(target_arch="wasm32", cmsh::async_trait(?Send))]
impl ReplyPort for Response {
    async fn send(self: Box<Self>, payload: &[u8]) -> Result<(), Error> {
        self.0.send(payload).await.map_err(failure)
    }
}
fn mesh(wires: Vec<Arc<Wire>>) -> Mesh {
    let mut builder = Mesh::builder(Arc::new(spawn));
    for wire in wires {
        builder = builder.backend(wire);
    }
    builder.build().unwrap()
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn actual_frames_remain_below_the_public_message_api() {
    let (a, b) = pair("fixture", true);
    let ma = mesh(vec![a.clone()]);
    let mb = mesh(vec![b.clone()]);
    let mut listening = mb.listen().await.unwrap();
    let receive = async {
        let Some(Incoming::Message { backend, payload }) = listening.next().await else {
            panic!("message")
        };
        assert_eq!(backend, b.scheme());
        assert_eq!(payload, b"hello");
        let Some(Incoming::Call { payload, reply, .. }) = listening.next().await else {
            panic!("call")
        };
        assert_eq!(payload, vec![17; 1024]);
        reply.send(&payload).await.unwrap();
    };
    let send = async {
        ma.app_message(listening.addresses(), b"hello")
            .await
            .unwrap();
        assert_eq!(
            ma.app_call(listening.addresses(), &[17; 1024])
                .await
                .unwrap(),
            vec![17; 1024]
        );
        assert!(
            ma.app_message(listening.addresses(), &[0; 1025])
                .await
                .is_err()
        );
    };
    futures::join!(send, receive);
    a.peer.close().await.unwrap();
    assert!(matches!(
        listening.next().await,
        Some(Incoming::ListenerClosed(_, ErrorKind::Closed))
    ));
    assert!(listening.next().await.is_none());
    listening.close();
    assert!(listening.addresses().is_empty());
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn fallback_is_explicit_and_never_replays_a_failed_call() {
    let (a1, b1) = pair("first", true);
    let (a2, b2) = pair("second", true);
    let a = mesh(vec![a1.clone(), a2.clone()]);
    let b = mesh(vec![b1.clone(), b2.clone()]);
    let listening = b.listen().await.unwrap();
    assert_eq!(a.active().unwrap(), a1.scheme());
    assert!(a.offered().unwrap().anonymous);
    // A terminal I/O failure completes this attempt; another backend remains untouched.
    a1.peer.close().await.unwrap();
    assert!(
        a.app_call(listening.addresses(), b"uncertain")
            .await
            .is_err()
    );
    a.report(&a1.scheme(), Health::Failed).unwrap();
    assert_eq!(a.active().unwrap(), a2.scheme());
    assert_eq!(a.take_switches().len(), 1);
    assert!(a.take_switches().is_empty());
    let receive = async {
        loop {
            match listening.next().await.unwrap() {
                Incoming::ListenerClosed(..) => {}
                Incoming::Call {
                    backend,
                    payload,
                    reply,
                } => {
                    assert_eq!(backend, a2.scheme());
                    assert_eq!(payload, b"new-call");
                    reply.send(b"reply").await.unwrap();
                    break;
                }
                _ => panic!("no replay"),
            }
        }
    };
    let send = async {
        assert_eq!(
            a.app_call(listening.addresses(), b"new-call")
                .await
                .unwrap(),
            b"reply"
        );
    };
    futures::join!(send, receive);
    assert!(
        a.report(&Scheme::new("missing").unwrap(), Health::Healthy)
            .is_err()
    );
    a.report(&a1.scheme(), Health::Healthy).unwrap();
    assert_eq!(a.active().unwrap(), a1.scheme());
    assert_eq!(a.status().len(), 2);
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn mandatory_anonymity_and_declared_capabilities_fail_closed() {
    let (direct, _) = pair("clear", false);
    let m = Mesh::builder(Arc::new(spawn))
        .backend(direct)
        .policy(Policy::unrestricted())
        .build()
        .unwrap();
    assert!(m.active().is_err());
    assert!(m.offered().is_none());
    assert!(!m.status()[0].acceptable);
    assert!(m.status()[0].missing.contains(&Property::Anonymous));
    assert!(m.listen().await.is_err());
    assert!(matches!(
        m.app_message(&[], b"x").await,
        Err(MeshError::Unavailable(_))
    ));
    let (a, _) = pair("fixture", true);
    assert!(
        Mesh::builder(Arc::new(spawn))
            .backend(a.clone())
            .backend(a.clone())
            .build()
            .is_err()
    );
    assert!(
        Mesh::builder(Arc::new(spawn))
            .backend(a.clone())
            .order([Scheme::new("missing").unwrap()])
            .build()
            .is_err()
    );
    let m = mesh(vec![a.clone()]);
    assert!(matches!(
        m.app_call(&[], b"x").await,
        Err(MeshError::NoCommonNetwork)
    ));
    let wrong = Address::new(a.scheme(), vec![3]).unwrap();
    assert!(m.app_message(&[wrong], b"x").await.is_err());
    let limited = Mesh::builder(Arc::new(spawn))
        .backend(a.clone())
        .minimum([Property::OfflineDelivery])
        .build()
        .unwrap();
    assert!(limited.active().is_err());
    drop(m);
    drop(limited);
    let mut a = Arc::try_unwrap(a).ok().unwrap();
    a.maximum = 0;
    assert!(
        Mesh::builder(Arc::new(spawn))
            .backend(Arc::new(a))
            .build()
            .is_err()
    );
}
