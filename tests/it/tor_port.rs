//! Mesh's downward Tor adapter over real Node/Ferry with external byte I/O.
//! This is adapter evidence; actual onion/private-Tor evidence belongs to ctrn CI.
#![cfg(feature = "tor")]
use cmsh::{Backend, Incoming, Mesh};
use futures::{SinkExt, StreamExt, channel::mpsc, lock::Mutex};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::compat::TokioAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;

struct Time;
impl ctrn::Clock for Time {
    fn now(&self) -> u64 {
        0
    }
    fn sleep_until(&self, _: u64) -> ctrn::BoxFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
}
fn spawn(f: cmsh::BoxFuture<'static, ()>) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::spawn(f);
    }
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(f);
}
struct ByteNetwork {
    online: AtomicBool,
    dial_failure: Mutex<Option<ctrn::ErrorKind>>,
    here: ctrn::Address,
    there: ctrn::Address,
    incoming: Arc<Mutex<mpsc::Receiver<ctrn::BoxStream>>>,
    outgoing: mpsc::Sender<ctrn::BoxStream>,
}
struct ByteListener {
    address: ctrn::Address,
    incoming: Arc<Mutex<mpsc::Receiver<ctrn::BoxStream>>>,
}
#[cfg_attr(not(target_arch = "wasm32"), ctrn::async_trait)]
#[cfg_attr(target_arch = "wasm32", ctrn::async_trait(?Send))]
impl ctrn::Listener for ByteListener {
    fn address(&self) -> &ctrn::Address {
        &self.address
    }
    async fn accept(&mut self) -> Result<ctrn::BoxStream, ctrn::Error> {
        self.incoming
            .lock()
            .await
            .next()
            .await
            .ok_or_else(|| ctrn::Error::new(ctrn::ErrorKind::Closed, "byte fixture closed"))
    }
}
#[cfg_attr(not(target_arch = "wasm32"), ctrn::async_trait)]
#[cfg_attr(target_arch = "wasm32", ctrn::async_trait(?Send))]
impl ctrn::Network for ByteNetwork {
    async fn start(&self, _: &ctrn::Scope) -> Result<(), ctrn::Error> {
        self.online.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn ready(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }
    fn stop(&self) {
        self.online.store(false, Ordering::SeqCst);
    }
    async fn listen(&self) -> Result<Box<dyn ctrn::Listener>, ctrn::Error> {
        Ok(Box::new(ByteListener {
            address: self.here.clone(),
            incoming: self.incoming.clone(),
        }))
    }
    async fn dial(&self, to: &ctrn::OnionEndpoint) -> Result<ctrn::BoxStream, ctrn::Error> {
        if let Some(kind) = *self.dial_failure.lock().await {
            return Err(ctrn::Error::new(kind, "external transport refusal"));
        }
        if to.address() != self.there {
            return Err(ctrn::Error::new(
                ctrn::ErrorKind::Unreachable,
                "unknown byte peer",
            ));
        }
        let (a, b) = tokio::io::duplex(64);
        self.outgoing
            .clone()
            .send(Box::new(b.compat()))
            .await
            .map_err(|_| ctrn::Error::new(ctrn::ErrorKind::Closed, "byte peer closed"))?;
        Ok(Box::new(a.compat()))
    }
}
async fn adapter(network: Arc<ByteNetwork>, scope: u8) -> (Arc<ctrn::Node>, Arc<cmsh::Tor>) {
    let clock: Arc<dyn ctrn::Clock> = Arc::new(Time);
    let node = Arc::new(
        ctrn::Node::new(
            network,
            clock.clone(),
            ctrn::Scope([scope; 32]),
            ctrn::Limits {
                streams: 4,
                bootstrap_ms: 1000,
                operation_ms: 1000,
            },
        )
        .unwrap(),
    );
    node.start().await.unwrap();
    let transport = Arc::new(cmsh::Tor(
        ctrn::messages::Messages::new(
            node.clone(),
            Arc::new(spawn),
            clock,
            ctrn::MessageLimits {
                max_payload: 1024,
                timeout_ms: 1000,
            },
        )
        .unwrap(),
    ));
    (node, transport)
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn mesh_tor_port_uses_bounded_messages_and_original_one_use_reply() {
    let a = ctrn::OnionEndpoint::parse(
        "a4dqobyha4dqobyha4dqobyha4dqobyha4dqobyha4dqobyha4dwc6ad.onion",
        443,
    )
    .unwrap()
    .address();
    let b = ctrn::OnionEndpoint::parse(
        "baeaqcaibaeaqcaibaeaqcaibaeaqcaibaeaqcaibaeaqcaibaecymad.onion",
        443,
    )
    .unwrap()
    .address();
    let (to_a, from_b) = mpsc::channel(2);
    let (to_b, from_a) = mpsc::channel(2);
    let a_address = a.clone();
    let a_network = Arc::new(ByteNetwork {
        online: AtomicBool::new(false),
        dial_failure: Mutex::new(None),
        here: a.clone(),
        there: b.clone(),
        incoming: Arc::new(Mutex::new(from_b)),
        outgoing: to_b,
    });
    let (a_node, a) = adapter(a_network.clone(), 1).await;
    let (b_node, b) = adapter(
        Arc::new(ByteNetwork {
            online: AtomicBool::new(false),
            dial_failure: Mutex::new(None),
            here: b,
            there: a_address,
            incoming: Arc::new(Mutex::new(from_a)),
            outgoing: to_a,
        }),
        2,
    )
    .await;
    assert_eq!(a.max_payload(), 1024);
    assert!(!a.capabilities().dht);
    let ma = Mesh::builder(Arc::new(spawn))
        .backend(a.clone())
        .build()
        .unwrap();
    let mb = Mesh::builder(Arc::new(spawn)).backend(b).build().unwrap();
    let listening = mb.listen().await.unwrap();
    let receive = async {
        let Incoming::Message { payload, .. } = listening.next().await.unwrap() else {
            panic!("message")
        };
        assert_eq!(payload, b"one-way");
        let Incoming::Call { payload, reply, .. } = listening.next().await.unwrap() else {
            panic!("call")
        };
        assert_eq!(payload, vec![9; 1024]);
        reply.send(b"reply").await.unwrap();
    };
    let send = async {
        ma.app_message(listening.addresses(), b"one-way")
            .await
            .unwrap();
        assert_eq!(
            ma.app_call(listening.addresses(), &[9; 1024])
                .await
                .unwrap(),
            b"reply"
        );
    };
    futures::join!(send, receive);
    let other = cmsh::Address::new(cmsh::Scheme::new("other").unwrap(), vec![1]).unwrap();
    assert_eq!(
        a.app_call(&other, b"x").await.unwrap_err().kind(),
        cmsh::ErrorKind::Unsupported
    );
    let malformed = cmsh::Address::new(a.scheme(), vec![1]).unwrap();
    assert_eq!(
        a.app_message(&malformed, b"x").await.unwrap_err().kind(),
        cmsh::ErrorKind::InvalidAddress
    );
    let send = async {
        assert!(
            ma.app_call(listening.addresses(), b"lost reply")
                .await
                .is_err()
        );
    };
    let receive = async {
        let Incoming::Call { reply, .. } = listening.next().await.unwrap() else {
            panic!("call")
        };
        b_node.stop();
        assert_eq!(
            reply.send(b"response").await.unwrap_err().kind(),
            cmsh::ErrorKind::Closed
        );
    };
    futures::join!(send, receive);
    for (external, expected) in [
        (ctrn::ErrorKind::Unsupported, cmsh::ErrorKind::Unsupported),
        (ctrn::ErrorKind::Network, cmsh::ErrorKind::Network),
        (ctrn::ErrorKind::Unreachable, cmsh::ErrorKind::Unreachable),
        (ctrn::ErrorKind::Timeout, cmsh::ErrorKind::Timeout),
        (ctrn::ErrorKind::Closed, cmsh::ErrorKind::Closed),
        (ctrn::ErrorKind::Limit, cmsh::ErrorKind::Limit),
        (ctrn::ErrorKind::Protocol, cmsh::ErrorKind::Protocol),
    ] {
        *a_network.dial_failure.lock().await = Some(external);
        let error = a
            .app_message(&listening.addresses()[0], b"secret payload")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.context(), "Tor message transport failed");
        assert!(!error.to_string().contains("secret payload"));
        assert!(!error.to_string().contains("external transport refusal"));
    }
    *a_network.dial_failure.lock().await = Some(ctrn::ErrorKind::Network);
    assert!(matches!(ma.app_call(listening.addresses(), b"once").await,
        Err(cmsh::MeshError::Backend(error)) if error.kind() == cmsh::ErrorKind::Network));
    assert_eq!(ma.status()[0].health, cmsh::Health::Failed);
    assert!(ma.active().is_err());
    a_node.stop();
}
