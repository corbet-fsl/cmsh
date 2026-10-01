//! Fixture-only Records port over actual Mesh messages. The explicit roster is
//! test setup, never production discovery or an authenticated NodeInfo format.
use super::crypto::{TestKey, TestVerifier};
use cdht::{
    Backend, Change, Descriptor, Error, LocalBackend, Network, NodeId, RecordKey, SetOutcome,
    SignedValue, Signer, WatchId,
    rpc::{
        self, Answer, ChangeHint, Query, Question, Response, SignedOperation, Statement,
        SubkeyRanges,
    },
};
use cmsh::{Address, Incoming, Listening, Mesh};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

type ChangeQueue = BTreeMap<(NodeId, WatchId), Vec<Change>>;

pub struct Roster {
    pub nodes: BTreeMap<NodeId, Vec<Address>>,
    pub watchers: BTreeMap<NodeId, Vec<Address>>,
}
pub struct Wire {
    pub mesh: Arc<Mesh>,
    pub signer: Arc<TestKey>,
    pub watcher: Arc<TestKey>,
    pub roster: Arc<Roster>,
    pub stores: Vec<NodeId>,
    pub received: Arc<Mutex<ChangeQueue>>,
    watches: Arc<Mutex<BTreeMap<(NodeId, WatchId), RecordKey>>>,
    started: Instant,
}
impl Wire {
    pub fn new(
        mesh: Arc<Mesh>,
        signer: Arc<TestKey>,
        watcher: Arc<TestKey>,
        roster: Arc<Roster>,
        stores: Vec<NodeId>,
    ) -> Self {
        Self {
            mesh,
            signer,
            watcher,
            roster,
            stores,
            received: Arc::new(Mutex::new(BTreeMap::new())),
            watches: Arc::new(Mutex::new(BTreeMap::new())),
            started: Instant::now(),
        }
    }
    async fn ask(
        &self,
        node: &NodeId,
        query: Query,
        descriptor: Option<&Descriptor>,
    ) -> Result<Response, Error> {
        let signer = if matches!(&query, Query::Watch { .. }) {
            self.watcher.as_ref()
        } else {
            self.signer.as_ref()
        };
        let question = Question::new(&query)?;
        let signed = question.sign(node, signer)?;
        let address = self.roster.nodes.get(node).ok_or(Error::Unavailable)?;
        let reply = self
            .mesh
            .app_call(address, signed.as_bytes())
            .await
            .map_err(|_| Error::Unavailable)?;
        let reply = SignedOperation::verify(reply, &signer.public_key(), node, &TestVerifier)?;
        question.answer(&reply, descriptor, &TestVerifier)
    }
    pub fn receive(&self, listener: Listening) -> tokio::task::JoinHandle<()> {
        let roster = self.roster.clone();
        let public = self.watcher.public_key();
        let watches = self.watches.clone();
        let received = self.received.clone();
        tokio::spawn(async move {
            while let Some(event) = listener.next().await {
                let Incoming::Message { payload, .. } = event else {
                    continue;
                };
                let operation = authenticate(payload, &public, roster.nodes.keys())
                    .expect("authenticated fixture storage node");
                let (_, hint) = rpc::value_changed(&operation).unwrap();
                let id = (*operation.signer(), WatchId(hint.watch_id));
                assert_eq!(watches.lock().unwrap().get(&id), Some(&hint.key));
                assert_eq!(hint.subkeys, SubkeyRanges::from_iter([0]));
                assert_eq!(hint.count, 0); // Fixture leases request one notification.
                let value = hint.value.unwrap();
                // Notification is only a hint. the consumer subsequently reads and
                // validates the actual current signed record through Mesh.
                received
                    .lock()
                    .unwrap()
                    .entry(id)
                    .or_default()
                    .push(Change {
                        key: hint.key,
                        subkey: 0,
                        seq: value.seq,
                    });
            }
        })
    }
}
impl Network for Wire {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
    async fn closest(&self, _: &RecordKey) -> Result<Vec<NodeId>, Error> {
        Ok(self.stores.clone()) // Explicit fixture roster; no discovery claim.
    }
    async fn descriptor(
        &self,
        node: &NodeId,
        key: &RecordKey,
    ) -> Result<Option<Descriptor>, Error> {
        match self
            .ask(
                node,
                Query::Get {
                    key: *key,
                    subkey: 0,
                    want_descriptor: true,
                },
                None,
            )
            .await?
        {
            Response::Get { descriptor, .. } => Ok(descriptor),
            _ => Err(Error::Encoding),
        }
    }
    async fn get(
        &self,
        node: &NodeId,
        key: &RecordKey,
        subkey: u32,
    ) -> Result<Option<SignedValue>, Error> {
        match self
            .ask(
                node,
                Query::Get {
                    key: *key,
                    subkey,
                    want_descriptor: true,
                },
                None,
            )
            .await?
        {
            Response::Get { value, .. } => Ok(value.map(|v| *v)),
            _ => Err(Error::Encoding),
        }
    }
    async fn set(
        &self,
        node: &NodeId,
        descriptor: &Descriptor,
        subkey: u32,
        value: &SignedValue,
    ) -> Result<SetOutcome, Error> {
        match self
            .ask(
                node,
                Query::Set {
                    key: descriptor.key(),
                    subkey,
                    value: Box::new(value.clone()),
                    descriptor: Some(descriptor.clone()),
                },
                Some(descriptor),
            )
            .await?
        {
            Response::Set {
                accepted: true,
                need_descriptor: false,
                value,
                ..
            } => Ok(value.map_or(SetOutcome::Accepted, |v| SetOutcome::Newer(*v))),
            _ => Err(Error::Unavailable),
        }
    }
    async fn watch(&self, node: &NodeId, key: &RecordKey) -> Result<WatchId, Error> {
        match self
            .ask(
                node,
                Query::Watch {
                    key: *key,
                    subkeys: SubkeyRanges::new(),
                    duration_us: 600_000_000,
                    count: 1,
                    watch_id: 0,
                },
                None,
            )
            .await?
        {
            Response::Watch {
                accepted: true,
                duration_us,
                watch_id,
                ..
            } if duration_us != 0 && watch_id != 0 => {
                let id = WatchId(watch_id);
                self.watches.lock().unwrap().insert((*node, id), *key);
                Ok(id)
            }
            _ => Err(Error::Unavailable),
        }
    }
    async fn cancel_watch(&self, node: &NodeId, id: WatchId) -> Result<(), Error> {
        let key = *self
            .watches
            .lock()
            .unwrap()
            .get(&(*node, id))
            .ok_or(Error::UnknownWatch)?;
        let response = self
            .ask(
                node,
                Query::Watch {
                    key,
                    subkeys: SubkeyRanges::new(),
                    duration_us: 0,
                    count: 0,
                    watch_id: id.0,
                },
                None,
            )
            .await?;
        if !matches!(response, Response::Watch { accepted: true, duration_us: 0, watch_id, .. } if watch_id == id.0)
        {
            return Err(Error::UnknownWatch);
        }
        self.watches.lock().unwrap().remove(&(*node, id));
        Ok(())
    }
    async fn changes(&self, node: &NodeId, id: WatchId) -> Result<Vec<Change>, Error> {
        if !self.watches.lock().unwrap().contains_key(&(*node, id)) {
            return Err(Error::UnknownWatch);
        }
        Ok(self
            .received
            .lock()
            .unwrap()
            .remove(&(*node, id))
            .unwrap_or_default())
    }
}
fn authenticate<'a>(
    bytes: Vec<u8>,
    destination: &NodeId,
    mut keys: impl Iterator<Item = &'a NodeId>,
) -> Result<SignedOperation, Error> {
    // Identities/endpoints are independently supplied by fixture setup. Trying
    // this bounded roster is test glue, not a production peer directory.
    keys.find_map(|key| {
        SignedOperation::verify(bytes.clone(), destination, key, &TestVerifier).ok()
    })
    .ok_or(Error::BadSignature)
}
pub fn serve(
    mesh: Arc<Mesh>,
    listener: Listening,
    signer: Arc<TestKey>,
    roster: Arc<Roster>,
    store: LocalBackend<TestVerifier>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut watches: BTreeMap<WatchId, (RecordKey, NodeId)> = BTreeMap::new();
        let started = Instant::now();
        while let Some(event) = listener.next().await {
            let Incoming::Call { payload, reply, .. } = event else {
                continue;
            };
            store.advance_to(started.elapsed().as_millis() as u64);
            let request = authenticate(
                payload,
                &signer.public_key(),
                roster.nodes.keys().chain(roster.watchers.keys()),
            )
            .unwrap();
            let (_, query) = rpc::query(&request).unwrap();
            // Distinct anonymous watcher capability; it never becomes a node or
            // gains an authenticated member watch quota in this fixture.
            if matches!(&query, Query::Watch { .. }) {
                assert!(roster.watchers.contains_key(request.signer()));
                assert!(!roster.nodes.contains_key(request.signer()));
            } else {
                assert!(roster.nodes.contains_key(request.signer()));
            }
            let mut changed_key = None;
            let response = match query {
                Query::Get {
                    key,
                    subkey,
                    want_descriptor,
                } => {
                    let descriptor = store.descriptor(&key).await.unwrap();
                    let value = if descriptor.is_some() {
                        store.get(&key, subkey).await.unwrap().map(Box::new)
                    } else {
                        None
                    };
                    Response::Get {
                        accepted: true,
                        value,
                        descriptor: if want_descriptor { descriptor } else { None },
                        peers: vec![],
                    }
                }
                Query::Set {
                    key,
                    subkey,
                    value,
                    descriptor,
                } => {
                    assert_eq!(subkey, 0); // This bounded fixture uses one-subkey records.
                    store.create(&descriptor.unwrap()).await.unwrap();
                    let outcome = store.set(&key, subkey, &value).await.unwrap();
                    changed_key = Some(key);
                    Response::Set {
                        accepted: true,
                        need_descriptor: false,
                        value: match outcome {
                            SetOutcome::Accepted => None,
                            SetOutcome::Newer(v) => Some(Box::new(v)),
                        },
                        peers: vec![],
                    }
                }
                Query::Watch {
                    key,
                    subkeys,
                    duration_us,
                    count,
                    watch_id,
                } => {
                    assert!(subkeys.is_empty());
                    if count == 0 {
                        assert_eq!(
                            watches.remove(&WatchId(watch_id)),
                            Some((key, *request.signer()))
                        );
                        store.cancel_watch(WatchId(watch_id)).await.unwrap();
                        Response::Watch {
                            accepted: true,
                            duration_us: 0,
                            watch_id,
                            peers: vec![],
                        }
                    } else {
                        assert_eq!((count, watch_id), (1, 0));
                        let watch = store.watch_for(&key, duration_us / 1000, count).unwrap();
                        watches.insert(watch, (key, *request.signer()));
                        Response::Watch {
                            accepted: true,
                            duration_us,
                            watch_id: watch.0,
                            peers: vec![],
                        }
                    }
                }
            };
            let answer = Answer::new(&request, &response)
                .unwrap()
                .sign(request.signer(), signer.as_ref())
                .unwrap();
            reply.send(answer.as_bytes()).await.unwrap();
            if let Some(key) = changed_key {
                for (watch, (_, watcher)) in
                    watches.iter().filter(|(_, (record, _))| *record == key)
                {
                    let changes = match store.changes(*watch).await {
                        Ok(c) => c,
                        Err(Error::UnknownWatch) => continue,
                        Err(e) => panic!("store watch failure: {e}"),
                    };
                    if changes.is_empty() {
                        continue;
                    }
                    assert_eq!(changes.len(), 1);
                    let value = store.get(&key, 0).await.unwrap().unwrap();
                    let hint = ChangeHint {
                        key,
                        subkeys: SubkeyRanges::from_iter([0]),
                        count: 0,
                        watch_id: watch.0,
                        value: Some(Box::new(value)),
                    };
                    let statement = Statement::value_changed(&hint)
                        .unwrap()
                        .sign(watcher, signer.as_ref())
                        .unwrap();
                    mesh.app_message(roster.watchers.get(watcher).unwrap(), statement.as_bytes())
                        .await
                        .unwrap();
                }
            }
        }
    })
}
