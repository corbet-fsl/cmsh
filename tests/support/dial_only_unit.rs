//! A dial-only records client (`TorDiscovery::dial_only`) over a client-only Tor node:
//! it reaches a serving node and gets answers, but never listens, never sends a sender
//! PeerInfo, is never routed by the node it asks, and holds no watch.
#[path = "tor_discovery/crypto.rs"]
mod crypto;
use super::*;
use crate::tor_records::{TorRecordLimits, TorRecords};
use cdht::Signer;
use cdht::rpc::{
    Answer, Query, Statement,
    peers::{Advertisement, DIRECT, ExtensionDialInfo},
};
use crypto::{NodeCipher, TestVerifier, key};
use futures::{SinkExt, channel::mpsc, lock::Mutex as AsyncMutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::compat::TokioAsyncReadCompatExt;

struct Monotonic;
impl ctrn::Clock for Monotonic {
    fn now(&self) -> u64 {
        0
    }
    fn sleep_until(&self, _: u64) -> ctrn::BoxFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
}
struct Wall;
impl TorEnvelopeTime for Wall {
    fn now_us(&self) -> Result<u64, Error> {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_micros()
            .try_into()
            .map_err(|_| Error::Unavailable)
    }
}
fn spawn(f: crate::BoxFuture<'static, ()>) {
    tokio::spawn(f);
}

/// One side of an in-memory byte network. `listened` counts every listen the driver sees.
struct Bytes {
    here: ctrn::Address,
    there: ctrn::Address,
    incoming: Arc<AsyncMutex<mpsc::Receiver<ctrn::BoxStream>>>,
    outgoing: mpsc::Sender<ctrn::BoxStream>,
    listened: AtomicUsize,
    online: std::sync::atomic::AtomicBool,
}
struct BytesListener {
    address: ctrn::Address,
    incoming: Arc<AsyncMutex<mpsc::Receiver<ctrn::BoxStream>>>,
}
#[ctrn::async_trait]
impl ctrn::Listener for BytesListener {
    fn address(&self) -> &ctrn::Address {
        &self.address
    }
    async fn accept(&mut self) -> Result<ctrn::BoxStream, ctrn::Error> {
        use futures::StreamExt;
        self.incoming
            .lock()
            .await
            .next()
            .await
            .ok_or_else(|| ctrn::Error::new(ctrn::ErrorKind::Closed, "closed"))
    }
}
#[ctrn::async_trait]
impl ctrn::Network for Bytes {
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
        self.listened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(BytesListener {
            address: self.here.clone(),
            incoming: self.incoming.clone(),
        }))
    }
    async fn dial(&self, to: &ctrn::OnionEndpoint) -> Result<ctrn::BoxStream, ctrn::Error> {
        if to.address() != self.there {
            return Err(ctrn::Error::new(ctrn::ErrorKind::Unreachable, "unknown"));
        }
        let (a, b) = tokio::io::duplex(4096);
        self.outgoing
            .clone()
            .send(Box::new(b.compat()))
            .await
            .map_err(|_| ctrn::Error::new(ctrn::ErrorKind::Closed, "closed"))?;
        Ok(Box::new(a.compat()))
    }
}

struct NodeKey(u8);
impl TorNodeIdentity for NodeKey {
    fn node_id(&self) -> NodeId {
        key(self.0).public_key()
    }
    fn question<'a>(
        &'a self,
        question: &'a Question,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { question.sign(destination, &key(self.0)) })
    }
    fn answer<'a>(
        &'a self,
        answer: &'a FindAnswer,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { answer.sign(destination, &key(self.0)) })
    }
    fn record_answer<'a>(
        &'a self,
        answer: &'a Answer,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { answer.sign(destination, &key(self.0)) })
    }
    fn statement<'a>(
        &'a self,
        statement: &'a Statement,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { statement.sign(destination, &key(self.0)) })
    }
    fn seal<'a>(
        &'a self,
        operation: &'a SignedOperation,
        destination: &'a NodeId,
        timestamp_us: u64,
    ) -> crate::BoxFuture<'a, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            Envelope::seal(
                operation.as_bytes(),
                destination,
                timestamp_us,
                &key(self.0),
                &NodeCipher(self.0),
            )
        })
    }
    fn open<'a>(
        &'a self,
        envelope: &'a Envelope,
    ) -> crate::BoxFuture<'a, Result<AuthenticatedOperation, Error>> {
        Box::pin(async move { envelope.open_operation(&NodeCipher(self.0), &TestVerifier) })
    }
}
impl TorWatcher for NodeKey {
    fn public_key(&self, _: &cdht::RecordKey) -> Result<NodeId, Error> {
        Ok(key(self.0 + 100).public_key())
    }
    fn question<'a>(
        &'a self,
        _: &'a cdht::RecordKey,
        question: &'a Question,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { question.sign(destination, &key(self.0 + 100)) })
    }
}

fn messages(node: Arc<ctrn::Node>) -> Arc<ctrn::messages::Messages> {
    Arc::new(
        ctrn::messages::Messages::new(
            node,
            Arc::new(spawn),
            Arc::new(Monotonic),
            ctrn::MessageLimits {
                max_payload: 32 * 1024,
                timeout_ms: 5000,
            },
        )
        .unwrap(),
    )
}
async fn node(network: Arc<dyn ctrn::Network>, scope: u8) -> Arc<ctrn::Node> {
    let node = Arc::new(
        ctrn::Node::new(
            network,
            Arc::new(Monotonic),
            ctrn::Scope([scope; 32]),
            ctrn::Limits {
                streams: 4,
                bootstrap_ms: 1000,
                operation_ms: 5000,
            },
        )
        .unwrap(),
    );
    node.start().await.unwrap();
    node
}
const LIMITS: TorDiscoveryLimits = TorDiscoveryLimits {
    peers: NonZeroUsize::new(8).unwrap(),
    lanes: NonZeroUsize::new(2).unwrap(),
    timeout_ms: 5000,
};
const WINDOW: TimeWindow = TimeWindow {
    behind_us: 60_000_000,
    ahead_us: 60_000_000,
};
fn record_limits() -> TorRecordLimits {
    TorRecordLimits {
        consensus_width: NonZeroUsize::new(10).unwrap(),
        watches: NonZeroUsize::new(4).unwrap(),
    }
}

#[tokio::test]
async fn a_dial_only_client_is_answered_but_never_listens_or_gets_routed() {
    let serving_address = ctrn::OnionEndpoint::parse(
        "baeaqcaibaeaqcaibaeaqcaibaeaqcaibaeaqcaibaeaqcaibaecymad.onion",
        443,
    )
    .unwrap()
    .address();
    let unused = ctrn::OnionEndpoint::parse(
        "a4dqobyha4dqobyha4dqobyha4dqobyha4dqobyha4dqobyha4dwc6ad.onion",
        443,
    )
    .unwrap()
    .address();
    let (to_server, server_incoming) = mpsc::channel(4);
    let (to_client, client_incoming) = mpsc::channel(4);
    // Serving node 2: listens and advertises DHTV with a store.
    let server_network = Arc::new(Bytes {
        here: serving_address.clone(),
        there: unused.clone(),
        incoming: Arc::new(AsyncMutex::new(server_incoming)),
        outgoing: to_client,
        listened: AtomicUsize::new(0),
        online: Default::default(),
    });
    let server_messages = messages(node(server_network.clone(), 2).await);
    let listener = server_messages.listen().await.unwrap();
    let endpoint = ctrn::OnionEndpoint::from_address(listener.address()).unwrap();
    let advertisement = UnverifiedPeerInfoBytes::sign_extension(
        Advertisement {
            timestamp_us: Wall.now_us().unwrap(),
            capabilities: vec![DHTV],
            outbound_protocols: vec![ctrn::TOR3],
            address_types: vec![ctrn::TOR3],
            dial_info: vec![ExtensionDialInfo {
                class: DIRECT,
                protocol: ctrn::TOR3,
                detail: endpoint.address().bytes().to_vec(),
            }],
        },
        &key(2),
    )
    .unwrap();
    let server = Arc::new(
        TorDiscovery::new(
            TorDiscoveryConfig {
                messages: server_messages,
                identity: NodeKey(2),
                verifier: TestVerifier,
                own: UnverifiedPeerInfoBytes::from_bytes(advertisement.as_bytes().to_vec())
                    .unwrap(),
                bootstrap: vec![],
                clock: Arc::new(Monotonic),
                limits: LIMITS,
                envelope_time: Arc::new(Wall),
                envelope_window: WINDOW,
            },
            &listener,
        )
        .unwrap(),
    );
    let server_records = Arc::new(
        TorRecords::new(
            server.clone(),
            NodeKey(2),
            Some((
                cdht::Capacity::NATIVE,
                cdht::watches::WatchLimits::native(5_000_000, NonZeroUsize::new(8).unwrap()),
            )),
            record_limits(),
        )
        .unwrap(),
    );
    let serving = server_records.clone();
    tokio::spawn(async move {
        let _ = serving
            .run(listener, NonZeroUsize::new(2).unwrap(), |_| {})
            .await;
    });

    // Client node 7: a client-only Tor node (ctrn::ClientOnly) and a dial-only discovery.
    let client_driver = Arc::new(Bytes {
        here: unused,
        there: serving_address,
        incoming: Arc::new(AsyncMutex::new(client_incoming)),
        outgoing: to_server,
        listened: AtomicUsize::new(0),
        online: Default::default(),
    });
    let client_node = node(Arc::new(ctrn::ClientOnly::new(client_driver.clone())), 7).await;
    let client_messages = messages(client_node);
    assert!(
        client_messages.listen().await.is_err(),
        "a client-only node never listens"
    );
    let client = Arc::new(
        TorDiscovery::dial_only(TorDialConfig {
            messages: client_messages,
            identity: NodeKey(7),
            verifier: TestVerifier,
            bootstrap: vec![advertisement],
            clock: Arc::new(Monotonic),
            limits: LIMITS,
            envelope_time: Arc::new(Wall),
            envelope_window: WINDOW,
        })
        .unwrap(),
    );
    assert!(client.is_dial_only() && !client.stores_records());
    let client_records =
        Arc::new(TorRecords::new(client.clone(), NodeKey(7), None, record_limits()).unwrap());

    // The client's questions carry no sender PeerInfo and are still answered.
    let report = client.discover(key(9).public_key()).await.unwrap();
    assert_eq!(
        report
            .peers
            .iter()
            .map(|p| *p.node_id())
            .collect::<Vec<_>>(),
        vec![key(2).public_key()]
    );
    let record = cdht::RecordKey([5; 32]);
    let found = cdht::Network::descriptor(&*client_records, &key(2).public_key(), &record)
        .await
        .unwrap();
    assert!(found.is_none(), "an unknown record is answered as absent");

    // The serving node answered, but never routed the client: nobody learns of it.
    assert!(server.route(&key(7).public_key()).is_err());
    assert!(server.closer(&key(7).public_key()).unwrap().is_empty());
    // A watch could never be delivered: the client refuses it, and so does the node.
    assert_eq!(
        cdht::Network::watch(&*client_records, &key(2).public_key(), &record).await,
        Err(Error::WatchRefused)
    );
    let question = client
        .with_sender(
            Question::new(&Query::Watch {
                key: record,
                subkeys: std::iter::once(0u32).collect(),
                duration_us: 0,
                count: 1,
                watch_id: 1,
            })
            .unwrap(),
        )
        .unwrap();
    assert!(
        client
            .call_watch(&key(2).public_key(), &record, &question, &NodeKey(7))
            .await
            .is_err()
    );
    // It serves nothing and never listened.
    assert_eq!(client.endpoint().err(), Some(Error::Unavailable));
    assert_eq!(
        client.answer_find(vec![0; 8]).await.err(),
        Some(Error::Unavailable)
    );
    assert_eq!(client_driver.listened.load(Ordering::SeqCst), 0);
    assert_eq!(server_network.listened.load(Ordering::SeqCst), 1);
}
