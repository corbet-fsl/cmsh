//! Nine independent Arti nodes, original node envelopes and signed dynamic hints.
mod crypto;
use cdht::{
    Error, Signer,
    rpc::{
        Question, SignedOperation, UnverifiedPeerInfoBytes,
        envelope::{AuthenticatedOperation, Envelope, TimeWindow},
        peers::{Advertisement, DHTV, DIRECT, ExtensionDialInfo, FindAnswer},
    },
};
use cmsh::tor_discovery::{
    TorDiscovery, TorDiscoveryConfig, TorDiscoveryLimits, TorEnvelopeTime, TorNodeIdentity,
};
use crypto::{NodeCipher, TestVerifier, key};
use ctrn::{
    Node, Scope,
    arti::{ArtiNetwork, Directories, Identity, TokioClock},
};
use std::{
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tor_hscrypto::pk::HsIdKeypair;
use tor_llcrypto::pk::ed25519;
use tor_rtcompat::tokio::TokioRustlsRuntime;

struct Time;
impl TorEnvelopeTime for Time {
    fn now_us(&self) -> Result<u64, Error> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_micros()
            .try_into()
            .map_err(|_| Error::Unavailable)
    }
}
struct NodeKey(u8);
impl Identity for NodeKey {
    fn onion_key(&self, scope: &Scope) -> Result<HsIdKeypair, ctrn::Error> {
        assert_eq!(scope.0, [93; 32]);
        Ok(HsIdKeypair::from(ed25519::ExpandedKeypair::from(
            &ed25519::Keypair::from_bytes(&[self.0; 32]),
        )))
    }
}
impl TorNodeIdentity for NodeKey {
    fn node_id(&self) -> [u8; 32] {
        key(self.0).public_key()
    }
    fn question<'a>(
        &'a self,
        question: &'a Question,
        destination: &'a [u8; 32],
    ) -> cmsh::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { question.sign(destination, &key(self.0)) })
    }
    fn answer<'a>(
        &'a self,
        answer: &'a FindAnswer,
        destination: &'a [u8; 32],
    ) -> cmsh::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { answer.sign(destination, &key(self.0)) })
    }
    fn record_answer<'a>(
        &'a self,
        answer: &'a cdht::rpc::Answer,
        destination: &'a [u8; 32],
    ) -> cmsh::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { answer.sign(destination, &key(self.0)) })
    }
    fn statement<'a>(
        &'a self,
        statement: &'a cdht::rpc::Statement,
        destination: &'a [u8; 32],
    ) -> cmsh::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { statement.sign(destination, &key(self.0)) })
    }
    fn seal<'a>(
        &'a self,
        operation: &'a SignedOperation,
        destination: &'a [u8; 32],
        timestamp_us: u64,
    ) -> cmsh::BoxFuture<'a, Result<Vec<u8>, Error>> {
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
    ) -> cmsh::BoxFuture<'a, Result<AuthenticatedOperation, Error>> {
        Box::pin(async move { envelope.open_operation(&NodeCipher(self.0), &TestVerifier) })
    }
}
impl cmsh::tor_discovery::TorWatcher for NodeKey {
    fn public_key(&self, _record: &cdht::RecordKey) -> Result<[u8; 32], Error> {
        Ok(key(self.0 + 100).public_key())
    }
    fn question<'a>(
        &'a self,
        _record: &'a cdht::RecordKey,
        question: &'a Question,
        destination: &'a [u8; 32],
    ) -> cmsh::BoxFuture<'a, Result<SignedOperation, Error>> {
        Box::pin(async move { question.sign(destination, &key(self.0 + 100)) })
    }
}
type Records = cmsh::tor_records::TorRecords<NodeKey, TestVerifier, NodeKey>;
fn record_runtime(route: Arc<TorDiscovery<NodeKey, TestVerifier>>, index: u8) -> Arc<Records> {
    let storage = (3..=7).contains(&index).then(|| {
        (
            cdht::Capacity::NATIVE,
            cdht::watches::WatchLimits::native(5_000_000, NonZeroUsize::new(128).unwrap()),
        )
    });
    Arc::new(
        Records::new(
            route,
            NodeKey(index),
            storage,
            cmsh::tor_records::TorRecordLimits {
                consensus_width: NonZeroUsize::new(10).unwrap(),
                watches: NonZeroUsize::new(32).unwrap(),
            },
        )
        .unwrap(),
    )
}
struct Participant {
    node: Arc<Node>,
    messages: Arc<ctrn::messages::Messages>,
    listener: Option<ctrn::messages::MessageListener>,
    advertisement: UnverifiedPeerInfoBytes,
    clock: Arc<TokioClock>,
    index: u8,
}
async fn participant(base: &Path, index: u8, config: &serde_json::Value) -> Participant {
    let clock = Arc::new(TokioClock::default());
    let network = ArtiNetwork::new(
        TokioRustlsRuntime::current().unwrap(),
        serde_json::from_value(config.clone()).unwrap(),
        Directories {
            state: base.join(format!("state-{index}")),
            cache: base.join(format!("cache-{index}")),
        },
        Arc::new(NodeKey(index)),
    )
    .unwrap();
    let node = Arc::new(
        Node::new(
            Arc::new(network),
            clock.clone(),
            Scope([93; 32]),
            ctrn::Limits {
                streams: 32,
                bootstrap_ms: 240_000,
                operation_ms: 60_000,
            },
        )
        .unwrap(),
    );
    let started = node.start().await;
    if let Err(error) = &started {
        eprintln!("private Tor participant {index} startup failed: {error}");
    }
    started.unwrap();
    let spawn = Arc::new(|future: ctrn::BoxFuture<'static, ()>| {
        tokio::spawn(future);
    });
    let messages = Arc::new(
        ctrn::messages::Messages::new(
            node.clone(),
            spawn,
            clock.clone(),
            ctrn::MessageLimits {
                max_payload: cdht::rpc::envelope::MAX_ENVELOPE_BYTES,
                timeout_ms: 60_000,
            },
        )
        .unwrap(),
    );
    let listener = messages.listen().await.unwrap();
    let advertisement = UnverifiedPeerInfoBytes::sign_extension(
        Advertisement {
            timestamp_us: Time.now_us().unwrap(),
            capabilities: if (3..=7).contains(&index) {
                vec![DHTV]
            } else {
                vec![]
            },
            outbound_protocols: vec![ctrn::TOR3],
            address_types: vec![ctrn::TOR3],
            dial_info: vec![ExtensionDialInfo {
                class: DIRECT,
                protocol: ctrn::TOR3,
                detail: listener.address().bytes().to_vec(),
            }],
        },
        &key(index),
    )
    .unwrap();
    Participant {
        node,
        messages,
        listener: Some(listener),
        advertisement,
        clock,
        index,
    }
}
fn original(info: &UnverifiedPeerInfoBytes) -> UnverifiedPeerInfoBytes {
    UnverifiedPeerInfoBytes::from_bytes(info.as_bytes().to_vec()).unwrap()
}
fn routing(
    p: &Participant,
    bootstrap: Vec<UnverifiedPeerInfoBytes>,
) -> Arc<TorDiscovery<NodeKey, TestVerifier>> {
    Arc::new(
        TorDiscovery::new(
            TorDiscoveryConfig {
                messages: p.messages.clone(),
                identity: NodeKey(p.index),
                verifier: TestVerifier,
                own: original(&p.advertisement),
                bootstrap,
                clock: p.clock.clone(),
                limits: TorDiscoveryLimits {
                    peers: NonZeroUsize::new(32).unwrap(),
                    lanes: NonZeroUsize::new(3).unwrap(),
                    timeout_ms: 120_000,
                },
                envelope_time: Arc::new(Time),
                envelope_window: TimeWindow {
                    behind_us: 120_000_000,
                    ahead_us: 10_000_000,
                },
            },
            p.listener.as_ref().unwrap(),
        )
        .unwrap(),
    )
}
fn serve(
    listener: ctrn::messages::MessageListener,
    records: Arc<Records>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = records
            .run(listener, NonZeroUsize::new(8).unwrap(), |_| {})
            .await;
    })
}
async fn contract() {
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::var("TOR_FIXTURE_JSON").unwrap()).unwrap())
            .unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut peers =
        futures::future::join_all((1..=7).map(|i| participant(state.path(), i, &config))).await;
    // Only the two original bootstrap advertisements enter construction. No
    // participant is handed the five-member population or another member's address.
    let bootstrap: Vec<_> = peers[..2]
        .iter()
        .map(|p| original(&p.advertisement))
        .collect();
    let mut routes = Vec::new();
    let mut records = Vec::new();
    let mut tasks = Vec::new();
    for p in &mut peers {
        let route = routing(
            p,
            bootstrap
                .iter()
                .filter(|info| {
                    info.verify_extension(&TestVerifier).unwrap().node_id()
                        != &key(p.index).public_key()
                })
                .map(original)
                .collect(),
        );
        let store = record_runtime(route.clone(), p.index);
        tasks.push(serve(p.listener.take().unwrap(), store.clone()));
        records.push(store);
        routes.push(route);
    }
    for route in &routes[2..] {
        route.discover([42; 32]).await.unwrap();
    }
    let mut fresh = participant(state.path(), 8, &config).await;
    let fresh_route = routing(&fresh, bootstrap.iter().map(original).collect());
    let fresh_records = record_runtime(fresh_route.clone(), fresh.index);
    tasks.push(serve(fresh.listener.take().unwrap(), fresh_records.clone()));
    let expected: std::collections::BTreeSet<_> = (3..=7).map(|i| key(i).public_key()).collect();
    let report = fresh_route.discover([42; 32]).await.unwrap();
    assert!(report.exhausted && !report.limited);
    assert_eq!(
        report
            .peers
            .iter()
            .map(|p| *p.node_id())
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    assert!(
        report
            .peers
            .iter()
            .all(|p| p.advertisement().capabilities.contains(&DHTV))
    );
    // A distinct owner device knows only the same two original bootstrap hints.
    // The original replication engine discovers and writes independent real peers.
    let mut owner = participant(state.path(), 9, &config).await;
    let owner_route = routing(&owner, bootstrap.iter().map(original).collect());
    let owner_records = record_runtime(owner_route, owner.index);
    tasks.push(serve(owner.listener.take().unwrap(), owner_records.clone()));
    let writer = key(40);
    let encryption = cdht::EncryptionKey::from_bytes([37; 32]);
    let mut writer_dht = cdht::Dht::new(
        cdht::NetworkBackend::new(owner_records.network(), TestVerifier),
        TestVerifier,
    );
    let record_key = writer_dht
        .create(&cdht::Schema::dflt(1).unwrap(), &writer)
        .await
        .unwrap();
    assert!(matches!(
        writer_dht
            .set(
                &record_key,
                0,
                b"original encrypted record",
                &writer,
                Some(&encryption)
            )
            .await
            .unwrap(),
        cdht::WriteOutcome::Written { seq: 0 }
    ));
    let mut reader_dht = cdht::Dht::new(
        cdht::NetworkBackend::new(fresh_records.network(), TestVerifier),
        TestVerifier,
    );
    reader_dht.open(&record_key).await.unwrap();
    assert_eq!(
        &*reader_dht
            .get(&record_key, 0, Some(&encryption))
            .await
            .unwrap()
            .unwrap(),
        b"original encrypted record"
    );
    let watched = reader_dht.watch(&record_key).await.unwrap();
    let inspected = fresh_records
        .inspect(
            report.peers[0].node_id(),
            &cdht::rpc::inspect::Inspection {
                key: record_key,
                subkeys: cdht::rpc::SubkeyRanges::new(),
                want_descriptor: true,
            },
            None,
        )
        .await
        .unwrap();
    assert!(inspected.accepted);
    assert_eq!(inspected.seqs, vec![0]);
    assert_eq!(inspected.descriptor.unwrap().key(), record_key);
    assert!(matches!(
        writer_dht
            .set(
                &record_key,
                0,
                b"changed encrypted record",
                &writer,
                Some(&encryption)
            )
            .await
            .unwrap(),
        cdht::WriteOutcome::Written { seq: 1 }
    ));
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let hints = reader_dht.changes(watched).await.unwrap();
            if !hints.is_empty() {
                assert!(
                    hints
                        .iter()
                        .all(|hint| hint.key == record_key && hint.subkey == 0 && hint.seq == 1)
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        &*reader_dht
            .get(&record_key, 0, Some(&encryption))
            .await
            .unwrap()
            .unwrap(),
        b"changed encrypted record"
    );
    reader_dht.cancel_watch(watched).await.unwrap();
    assert_eq!(records[0].storage_usage(), None);
    assert_eq!(records[1].storage_usage(), None);
    assert!(
        records[2..]
            .iter()
            .all(|storage| storage.storage_usage().unwrap().0 == 1)
    );
    peers[0].node.stop();
    tasks[0].abort();
    let report = fresh_route.discover([43; 32]).await.unwrap();
    assert_eq!(report.peers.len(), 5);
    peers[2].node.stop();
    tasks[2].abort();
    let report = fresh_route.discover([44; 32]).await.unwrap();
    assert!(
        !report
            .peers
            .iter()
            .any(|p| p.node_id() == &key(3).public_key())
    );
    assert_eq!(report.peers.len(), 4);
    let interrupted = reader_dht.watch(&record_key).await.unwrap();
    // Cancel the actual production service future and wait for its listener
    // lease to drop. No synthesized network-loss event is injected.
    tasks[7].abort();
    assert!((&mut tasks[7]).await.unwrap_err().is_cancelled());
    assert_eq!(
        reader_dht.changes(interrupted).await.unwrap_err(),
        Error::UnknownWatch
    );
    reader_dht.cancel_watch(interrupted).await.unwrap();
    for handler in tasks {
        handler.abort();
    }
    for p in peers {
        p.node.stop();
    }
    fresh.node.stop();
    owner.node.stop();
    println!(
        "original ENV0/PeerInfo/FindNode and encrypted Get/Set/Watch over nine independent onion services passed; bootstrap storage absent; dynamic routing loss verified"
    );
}
#[tokio::main]
pub async fn main() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!(
            "private discovery assertion at fixture line {}",
            info.location().map_or(0, |l| l.line())
        )
    }));
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("TLS provider");
    if tokio::time::timeout(Duration::from_secs(900), contract())
        .await
        .is_err()
    {
        eprintln!("private discovery deadline exceeded");
        std::process::exit(1);
    }
}
