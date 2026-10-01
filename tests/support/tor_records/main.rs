//! Bounded real private-Tor record experiment. Explicit fixture roster only;
//! this is not production discovery, churn, passkey recovery or public evidence.
mod crypto;
mod network;
use cdht::{
    Backend, Capacity, Descriptor, Dht, EncryptionKey, LocalBackend, NetworkBackend, Schema,
    Signer, WriteOutcome,
};
use cmsh::{Listening, Mesh};
use crypto::{TestKey, TestVerifier, key};
use ctrn::{
    Node, Scope,
    arti::{ArtiNetwork, Directories, Identity, TokioClock},
};
use network::{Roster, Wire};
use std::{path::Path, sync::Arc, time::Duration};
use tor_hscrypto::pk::HsIdKeypair;
use tor_llcrypto::pk::ed25519;
use tor_rtcompat::tokio::TokioRustlsRuntime;

struct OnionKey(u8);
impl Identity for OnionKey {
    fn onion_key(&self, scope: &Scope) -> Result<HsIdKeypair, ctrn::Error> {
        assert_eq!(scope.0, [91; 32]);
        Ok(HsIdKeypair::from(ed25519::ExpandedKeypair::from(
            &ed25519::Keypair::from_bytes(&[self.0; 32]),
        )))
    }
}
struct Participant {
    node: Arc<Node>,
    mesh: Arc<Mesh>,
    signer: Arc<TestKey>,
    watcher: Arc<TestKey>,
    listener: Option<Listening>,
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
        Arc::new(OnionKey(index)),
    )
    .unwrap();
    let node = Arc::new(
        Node::new(
            Arc::new(network),
            clock.clone(),
            Scope([91; 32]),
            ctrn::Limits {
                streams: 32,
                bootstrap_ms: 240_000,
                operation_ms: 60_000,
            },
        )
        .unwrap(),
    );
    node.start().await.unwrap();
    let spawn = Arc::new(|f: cmsh::BoxFuture<'static, ()>| {
        tokio::spawn(f);
    });
    let messages = ctrn::messages::Messages::new(
        node.clone(),
        spawn.clone(),
        clock,
        ctrn::MessageLimits {
            max_payload: cdht::rpc::MAX_RPC_BYTES,
            timeout_ms: 60_000,
        },
    )
    .unwrap();
    let mesh = Arc::new(
        Mesh::builder(spawn)
            .backend(Arc::new(cmsh::Tor(messages)))
            .build()
            .unwrap(),
    );
    let listener = mesh.listen().await.unwrap();
    Participant {
        node,
        mesh,
        signer: Arc::new(key(index)),
        watcher: Arc::new(key(index + 30)),
        listener: Some(listener),
    }
}
fn client(
    p: &Participant,
    roster: Arc<Roster>,
    holders: Vec<[u8; 32]>,
) -> Dht<NetworkBackend<Wire, TestVerifier>, TestVerifier> {
    Dht::new(
        NetworkBackend::new(
            Wire::new(
                p.mesh.clone(),
                p.signer.clone(),
                p.watcher.clone(),
                roster,
                holders,
            ),
            TestVerifier,
        ),
        TestVerifier,
    )
}
async fn contract() {
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::var("TOR_FIXTURE_JSON").unwrap()).unwrap())
            .unwrap();
    let state = tempfile::tempdir().unwrap();
    // Five storing devices, one original owner, one queued-state returning
    // device, and one genuinely fresh device with no cached record state.
    let mut participants =
        futures::future::join_all((1..=8).map(|index| participant(state.path(), index, &config)))
            .await;
    let roster = Arc::new(Roster {
        nodes: participants
            .iter()
            .map(|p| {
                (
                    p.signer.public_key(),
                    p.listener.as_ref().unwrap().addresses().to_vec(),
                )
            })
            .collect(),
        watchers: participants
            .iter()
            .map(|p| {
                (
                    p.watcher.public_key(),
                    p.listener.as_ref().unwrap().addresses().to_vec(),
                )
            })
            .collect(),
    });
    assert_eq!(roster.nodes.len(), 8);
    assert_eq!(roster.watchers.len(), 8);
    let holders: Vec<_> = participants[..5]
        .iter()
        .map(|p| p.signer.public_key())
        .collect();
    let mut stores = Vec::new();
    let mut tasks = Vec::new();
    for p in &mut participants[..5] {
        let store = LocalBackend::with_capacity(
            TestVerifier,
            Capacity {
                records: 1,
                bytes: 1_048_576,
            },
        );
        tasks.push(network::serve(
            p.mesh.clone(),
            p.listener.take().unwrap(),
            p.signer.clone(),
            roster.clone(),
            store.clone(),
        ));
        stores.push(store);
    }
    println!("eight independent onion participants and five device stores ready");
    let owner_key = key(101); // Test-only scoped owner capability, never a wire field.
    let read = EncryptionKey::from_bytes([102; 32]);
    let mut owner = client(&participants[5], roster.clone(), holders.clone());
    tasks.push(
        owner
            .backend()
            .network()
            .receive(participants[5].listener.take().unwrap()),
    );
    let locator = owner
        .create(&Schema::dflt(1).unwrap(), &owner_key)
        .await
        .unwrap();
    assert!(stores.iter().all(|s| s.usage().0 == 0)); // Empty creation is local upstream state.
    assert_eq!(
        owner
            .set(
                &locator,
                0,
                b"first encrypted record",
                &owner_key,
                Some(&read)
            )
            .await
            .unwrap(),
        WriteOutcome::Written { seq: 0 }
    );
    let first = owner.get_signed(&locator, 0).await.unwrap().unwrap();
    for store in &stores {
        let held = store.get(&locator, 0).await.unwrap().unwrap();
        assert_eq!(held.raw_blob().unwrap(), first.raw_blob().unwrap());
        assert_eq!(held.signature, first.signature);
        assert_ne!(held.data, b"first encrypted record");
        assert_eq!(store.usage().0, 1);
    }
    println!("original encrypted bytes and signatures retained by five independent stores");
    let watch = owner.watch(&locator).await.unwrap();
    assert_eq!(
        owner
            .set(
                &locator,
                0,
                b"changed encrypted record",
                &owner_key,
                Some(&read)
            )
            .await
            .unwrap(),
        WriteOutcome::Written { seq: 1 }
    );
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let changes = owner.changes(watch).await.unwrap();
            if !changes.is_empty() {
                assert_eq!(changes[0].seq, 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        owner
            .get(&locator, 0, Some(&read))
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"changed encrypted record"
    );
    owner.cancel_watch(watch).await.unwrap();
    println!("real WatchValue and signed reverse ValueChanged passed");
    // Actually stop the transport; no fake network success or direct fallback.
    participants[5].node.stop();
    let seq = owner
        .queue_set(
            &locator,
            0,
            b"queued encrypted record",
            &owner_key,
            Some(&read),
        )
        .unwrap();
    assert_eq!(seq, 2);
    let queued = serde_json::to_vec(owner.state()).unwrap();
    assert!(
        !queued
            .windows(b"queued encrypted record".len())
            .any(|w| w == b"queued encrypted record")
    );
    let report = owner.flush().await.unwrap();
    assert_eq!(report.written, 0);
    assert_eq!(owner.state().queue().len(), 1);
    // A new transport/device instance uses only the persisted signed queue.
    let staged: cdht::ClientState = serde_json::from_slice(&queued).unwrap();
    let mut returning = Dht::with_state(
        NetworkBackend::new(
            Wire::new(
                participants[6].mesh.clone(),
                participants[6].signer.clone(),
                participants[6].watcher.clone(),
                roster.clone(),
                holders.clone(),
            ),
            TestVerifier,
        ),
        TestVerifier,
        staged,
    );
    tasks.push(
        returning
            .backend()
            .network()
            .receive(participants[6].listener.take().unwrap()),
    );
    let report = returning.flush().await.unwrap();
    assert_eq!(report.written, 1);
    assert!(returning.state().queue().is_empty());
    let current = returning.get_signed(&locator, 0).await.unwrap().unwrap();
    assert_eq!(current.seq, 2);
    println!("actual stopped transport kept signed queue; independent device flushed it");
    // Fresh empty device cache, known public locator and test read capability.
    // No old local snapshot or plaintext/secret is supplied by a storage node.
    let mut fresh = client(&participants[7], roster.clone(), holders.clone());
    fresh.open(&locator).await.unwrap();
    assert_eq!(
        fresh
            .get(&locator, 0, Some(&read))
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"queued encrypted record"
    );
    assert_eq!(
        fresh
            .get_signed(&locator, 0)
            .await
            .unwrap()
            .unwrap()
            .raw_blob()
            .unwrap(),
        current.raw_blob().unwrap()
    );
    println!("fresh empty device restored ciphertext and opened it through read capability");
    // Bounded real eviction, not a retention/population measurement: each test
    // store was configured with capacity one. No store mutation bypasses its API.
    let pressure_owner = key(103);
    let unrelated = Descriptor::create(&Schema::dflt(1).unwrap(), &pressure_owner).unwrap();
    let pressure_cipher = EncryptionKey::from_bytes([104; 32]);
    let nonce = [105; 24]; // Single use under an independent public fixture key.
    let pressure = cdht::SignedValue::sign_with_nonce(
        &unrelated.owner,
        0,
        0,
        pressure_cipher
            .crypt(&nonce, b"bounded eviction record")
            .unwrap()
            .to_vec(),
        Some(nonce),
        &pressure_owner,
    )
    .unwrap();
    for (store, node) in stores[..4].iter().zip(&holders[..4]) {
        assert_eq!(
            cdht::Network::set(
                returning.backend().network(),
                node,
                &unrelated,
                0,
                &pressure
            )
            .await
            .unwrap(),
            cdht::SetOutcome::Accepted
        );
        assert!(store.descriptor(&locator).await.unwrap().is_none());
        assert_eq!(store.usage().2, 1);
    }
    assert!(
        stores[4]
            .get(&locator, 0)
            .await
            .unwrap()
            .unwrap()
            .same_value(&current)
    );
    let cached = serde_json::to_vec(fresh.state()).unwrap();
    drop(fresh);
    let mut rehydrating = Dht::with_state(
        NetworkBackend::new(
            Wire::new(
                participants[7].mesh.clone(),
                participants[7].signer.clone(),
                participants[7].watcher.clone(),
                roster.clone(),
                holders.clone(),
            ),
            TestVerifier,
        ),
        TestVerifier,
        serde_json::from_slice(&cached).unwrap(),
    );
    rehydrating.open(&locator).await.unwrap();
    for store in &stores {
        let restored = store.get(&locator, 0).await.unwrap().unwrap();
        assert_eq!(restored.raw_blob().unwrap(), current.raw_blob().unwrap());
        assert_eq!(restored.signature, current.signature);
    }
    assert!(rehydrating.state().queue().is_empty());
    println!(
        "owner open inspected one surviving holder and rehydrated five original copies without signing"
    );
    for task in tasks {
        task.abort();
    }
    for p in participants {
        p.node.stop();
    }
    println!(
        "private Tor Records message experiment passed; production discovery, public Tor and churn unproven"
    );
}
#[tokio::main]
pub async fn main() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!(
            "private Records assertion at fixture line {}",
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
        eprintln!("private Records experiment deadline exceeded");
        std::process::exit(1);
    }
}
