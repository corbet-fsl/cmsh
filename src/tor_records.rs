//! Original record RPCs over Tor's authenticated dynamic routing owner. Bootstrap
//! construction has no store; member storage keeps original signed values intact.
use crate::tor_applications::TorApplications;
use crate::tor_discovery::{CallKind, TorDiscovery, TorNodeIdentity, TorWatcher};
use cdht::rpc::envelope::AuthenticatedOperation;
use cdht::{
    Backend, Capacity, Change, Descriptor, Error, LocalBackend, Network, NodeId, RecordKey,
    SetOutcome, SignedValue, Verifier, WatchId,
    rpc::inspect::{Inspection, InspectionResponse, inspection},
    rpc::{Answer, Query, Question, Response, Statement, SubkeyRanges, query, value_changed},
    watches::WatchLimits,
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

/// Explicit original responsibility width and bounded client-watch resources.
#[derive(Clone, Copy)]
pub struct TorRecordLimits {
    /// Original network.dht.consensus_width (maintained native default is ten).
    pub consensus_width: NonZeroUsize,
    /// Maximum retained per-node client leases, including lost leases to reconcile.
    pub watches: NonZeroUsize,
}
#[derive(Clone)]
struct ClientWatch {
    descriptor: Descriptor,
    subkeys: SubkeyRanges,
    changed: SubkeyRanges,
    expires_ms: u64,
    count: u32,
    lost: bool,
}
struct ListenerLease<'a>(&'a Mutex<BTreeMap<(NodeId, u64), ClientWatch>>);
impl Drop for ListenerLease<'_> {
    fn drop(&mut self) {
        if let Ok(mut watches) = self.0.lock() {
            for watch in watches.values_mut() {
                watch.lost = true;
            }
        }
    }
}
enum Finished {
    Request(Result<(), Error>),
    Maintenance(Result<(), Error>),
}
enum Signal {
    Incoming(Result<ctrn::Received, ctrn::Error>),
    Tick,
    Finished(Finished),
}
/// One actual node's record transport, member storage and original watch leases.
/// The runtime must drive `handle_call`, `handle_statement`, and `maintain` from
/// its retained actual listener/clock, and call `transport_lost` on listener loss.
pub struct TorRecords<I, V: Verifier, W> {
    discovery: Arc<TorDiscovery<I, V>>,
    store: Option<LocalBackend<V>>,
    watcher: W,
    limits: TorRecordLimits,
    watches: Mutex<BTreeMap<(NodeId, u64), ClientWatch>>,
}
impl<I: TorNodeIdentity, V: Verifier, W: TorWatcher> TorRecords<I, V, W> {
    /// Construct without operator storage when `storage` is None. An advertised
    /// DHTV capability must agree with an actual member store, never a ready flag.
    pub fn new(
        discovery: Arc<TorDiscovery<I, V>>,
        watcher: W,
        storage: Option<(Capacity, WatchLimits)>,
        limits: TorRecordLimits,
    ) -> Result<Self, Error>
    where
        V: Clone,
    {
        if limits.consensus_width.get() > 20 || discovery.stores_records() != storage.is_some() {
            return Err(Error::Encoding);
        }
        let store = storage
            .map(|(capacity, watches)| {
                LocalBackend::with_authenticated_watches(
                    discovery.verifier().clone(),
                    capacity,
                    watches,
                )
            })
            .transpose()?;
        Ok(Self {
            discovery,
            store,
            watcher,
            limits,
            watches: Mutex::new(BTreeMap::new()),
        })
    }
    /// Original routing owner; discovery does not imply record availability.
    pub fn discovery(&self) -> &Arc<TorDiscovery<I, V>> {
        &self.discovery
    }
    /// Actual storage inventory. A bootstrap has no member record store at all.
    pub fn storage_usage(&self) -> Option<(usize, usize, usize)> {
        self.store.as_ref().map(LocalBackend::usage)
    }

    /// Drive a dedicated actual record-service listener and its one-second timer.
    /// Requests have bounded concurrency; one maintenance operation can progress
    /// beside them, so renewal I/O never blocks inbound replies. Invalid requests
    /// are refused individually and reported only as coarse errors. The callback
    /// must return promptly and receives no keys, peer identities or payloads.
    ///
    /// The caller retains this future for the service lifetime. Listener failure,
    /// return or cancellation marks every client watch lost. This listener is for
    /// original record RPCs; a generic application listener is a distinct port.
    pub async fn run(
        &self,
        listener: ctrn::messages::MessageListener,
        concurrency: NonZeroUsize,
        on_error: impl FnMut(Error),
    ) -> Result<(), Error>
    where
        V: crate::MaybeSend + crate::MaybeSync + 'static,
        I: 'static,
    {
        self.run_inner(listener, concurrency, None, on_error).await
    }

    /// Share this exact listener with bounded original AppCall/AppMessage
    /// delivery. Application consumers never hold a Records request lane while
    /// awaiting member authority; a full application queue refuses immediately.
    pub async fn run_shared(
        &self,
        listener: ctrn::messages::MessageListener,
        concurrency: NonZeroUsize,
        application: &TorApplications<I, V>,
        on_error: impl FnMut(Error),
    ) -> Result<(), Error>
    where
        V: crate::MaybeSend + crate::MaybeSync + 'static,
        I: 'static,
    {
        if !Arc::ptr_eq(&self.discovery, application.discovery()) {
            return Err(Error::Encoding);
        }
        let _application = application.lease();
        self.run_inner(listener, concurrency, Some(application), on_error)
            .await
    }

    async fn run_inner(
        &self,
        listener: ctrn::messages::MessageListener,
        concurrency: NonZeroUsize,
        application: Option<&TorApplications<I, V>>,
        mut on_error: impl FnMut(Error),
    ) -> Result<(), Error>
    where
        V: crate::MaybeSend + crate::MaybeSync + 'static,
        I: 'static,
    {
        // A dial-only client serves nothing; it never runs a listener.
        if self.discovery.is_dial_only() {
            return Err(Error::Unavailable);
        }
        let _lease = ListenerLease(&self.watches);
        // MessageListener::next may already own an accepted partial connection.
        // Unfold retains that in-flight future across timer/completion selection;
        // dropping a temporary StreamExt::next cannot discard that connection.
        let arrivals = futures::stream::unfold(listener, |mut listener| async move {
            let result = listener.next().await;
            Some((result, listener))
        });
        futures::pin_mut!(arrivals);
        let mut operations: FuturesUnordered<crate::BoxFuture<'_, Finished>> =
            FuturesUnordered::new();
        let mut requests = 0;
        let mut maintaining = false;
        let mut next_tick = self.discovery.now_ms();
        loop {
            let accept = requests < concurrency.get();
            let signal = {
                let incoming = async {
                    if accept {
                        arrivals
                            .next()
                            .await
                            .expect("listener unfold always yields")
                    } else {
                        futures::future::pending().await
                    }
                }
                .fuse();
                let completed = async {
                    match operations.next().await {
                        Some(result) => result,
                        None => futures::future::pending().await,
                    }
                }
                .fuse();
                let tick = self.discovery.sleep_until(next_tick).fuse();
                futures::pin_mut!(incoming, completed, tick);
                futures::select_biased! {
                    () = tick => Signal::Tick,
                    result = completed => Signal::Finished(result),
                    result = incoming => Signal::Incoming(result),
                }
            };
            match signal {
                Signal::Tick => {
                    next_tick = self
                        .discovery
                        .now_ms()
                        .checked_add(1_000)
                        .ok_or(Error::Unavailable)?;
                    if !maintaining {
                        maintaining = true;
                        operations.push(Box::pin(async {
                            Finished::Maintenance(self.maintain().await)
                        }));
                    }
                }
                Signal::Incoming(Err(_)) => return Err(Error::Unavailable),
                Signal::Incoming(Ok(message)) => {
                    requests += 1;
                    operations.push(Box::pin(async move {
                        if let Some(application) = application {
                            return Finished::Request(application.receive(self, message).await);
                        }
                        Finished::Request(match message {
                            ctrn::Received::Call { payload, reply } => {
                                match self.handle_call(payload).await {
                                    Ok(answer) => {
                                        reply.send(&answer).await.map_err(|_| Error::Unavailable)
                                    }
                                    Err(error) => Err(error),
                                }
                            }
                            ctrn::Received::Message(payload) => {
                                self.handle_statement(payload).await
                            }
                        })
                    }));
                }
                Signal::Finished(result) => {
                    let result = match result {
                        Finished::Request(result) => {
                            requests -= 1;
                            result
                        }
                        Finished::Maintenance(result) => {
                            maintaining = false;
                            result
                        }
                    };
                    if let Err(error) = result {
                        on_error(error);
                    }
                }
            }
        }
    }

    async fn request(
        &self,
        node: &NodeId,
        request: Query,
        descriptor: Option<&Descriptor>,
    ) -> Result<Response, Error> {
        let question = self.discovery.with_sender(Question::new(&request)?)?;
        let answer = self.discovery.call_record(node, &question).await?;
        let response =
            question.answer(answer.operation(), descriptor, self.discovery.verifier())?;
        self.observe_response(&response)?;
        Ok(response)
    }
    fn observe_response(&self, response: &Response) -> Result<(), Error> {
        let peers = match response {
            Response::Get { peers, .. }
            | Response::Set { peers, .. }
            | Response::Watch { peers, .. } => peers,
        };
        self.discovery.observe_hints(peers)
    }
    /// Inspect original remote sequence hints without downloading value contents.
    /// A sequence hint is never proof of a record value, membership or freshness.
    pub async fn inspect(
        &self,
        node: &NodeId,
        request: &Inspection,
        descriptor: Option<&Descriptor>,
    ) -> Result<InspectionResponse, Error> {
        let question = self.discovery.with_sender(request.question()?)?;
        let answer = self.discovery.call_record(node, &question).await?;
        let response = question.inspection_answer(
            answer.operation(),
            descriptor,
            self.discovery.verifier(),
        )?;
        self.discovery.observe_hints(&response.peers)?;
        Ok(response)
    }
    /// Handle an original call and return bytes for the actual one-use reply port.
    /// Caller errors must close/refuse that call; they never become successful ACKs.
    pub async fn handle_call(&self, bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
        let received = self.discovery.authenticate(bytes).await?;
        self.handle_authenticated_call(received).await
    }
    pub(crate) async fn handle_authenticated_call(
        &self,
        received: AuthenticatedOperation,
    ) -> Result<Vec<u8>, Error> {
        // Each branch admits its caller once: a dial-only caller only for a lookup,
        // read or write, within its call budget (TorDiscovery::admit_caller).
        if cdht::rpc::peers::find_query(received.operation()).is_ok() {
            return self.discovery.answer_find_authenticated(&received).await;
        }
        if let Ok((_, request)) = inspection(received.operation()) {
            self.discovery.admit_caller(&received, CallKind::Inspect)?;
            if received.operation().signer() != received.sender() {
                return Err(Error::BadSignature);
            }
            let peers = self.discovery.closer(&request.key.0)?;
            let accepted = self.store.is_some() && peers.len() < self.limits.consensus_width.get();
            let found = self
                .store
                .as_ref()
                .map(|store| store.inspect(&request.key, &request.subkeys))
                .transpose()?
                .flatten();
            let (descriptor, seqs) = match found {
                Some((descriptor, seqs)) => (request.want_descriptor.then_some(descriptor), seqs),
                None => (None, Vec::new()),
            };
            let response = InspectionResponse {
                accepted,
                seqs,
                peers,
                descriptor,
            };
            return self
                .discovery
                .reply_record(
                    &Answer::inspection(received.operation(), &response)?,
                    received.sender(),
                )
                .await;
        }
        let (_, request) = query(received.operation())?;
        // A dial-only caller's watch is refused here: it has no listener to deliver to.
        self.discovery.admit_caller(
            &received,
            match &request {
                Query::Get { .. } => CallKind::Get,
                Query::Set { .. } => CallKind::Set,
                Query::Watch { .. } => CallKind::Watch,
            },
        )?;
        if !matches!(request, Query::Watch { .. })
            && received.operation().signer() != received.sender()
        {
            return Err(Error::BadSignature);
        }
        let key = match &request {
            Query::Get { key, .. } | Query::Set { key, .. } | Query::Watch { key, .. } => key,
        };
        let peers = self.discovery.closer(&key.0)?;
        let accepted = self.store.is_some() && peers.len() < self.limits.consensus_width.get();
        let response = match request {
            Query::Get {
                key,
                subkey,
                want_descriptor,
            } => {
                let (descriptor, value) = if let Some(store) = &self.store {
                    let descriptor = store.descriptor(&key).await?;
                    let value = if descriptor.is_some() {
                        store.get(&key, subkey).await?
                    } else {
                        None
                    };
                    (descriptor.filter(|_| want_descriptor), value.map(Box::new))
                } else {
                    (None, None)
                };
                Response::Get {
                    accepted,
                    descriptor,
                    value,
                    peers,
                }
            }
            Query::Set { .. } => {
                let mut need_descriptor = false;
                let mut value = None;
                if accepted {
                    match self
                        .store
                        .as_ref()
                        .ok_or(Error::Unavailable)?
                        .accept_value(&received)
                    {
                        Ok(SetOutcome::Accepted) => {}
                        Ok(SetOutcome::Newer(current)) => value = Some(Box::new(current)),
                        Err(Error::UnknownRecord) => need_descriptor = true,
                        Err(error) => return Err(error),
                    }
                }
                Response::Set {
                    accepted,
                    need_descriptor,
                    value,
                    peers,
                }
            }
            Query::Watch { watch_id, .. } => {
                let mut id = watch_id;
                let mut duration_us = 0;
                if accepted {
                    match self
                        .store
                        .as_ref()
                        .ok_or(Error::Unavailable)?
                        .accept_watch(&received, self.discovery.now_us()?)
                    {
                        Ok(lease) => {
                            id = lease.id;
                            duration_us = lease.duration_us;
                        }
                        Err(Error::UnknownRecord) => {}
                        Err(error) => return Err(error),
                    }
                }
                Response::Watch {
                    accepted,
                    duration_us,
                    watch_id: id,
                    peers,
                }
            }
        };
        self.discovery
            .reply_record(
                &Answer::new(received.operation(), &response)?,
                received.sender(),
            )
            .await
    }
    /// Authenticate a real node notification against its exact active lease.
    /// Included values are verified, but readers still inspect the affected subkeys.
    pub async fn handle_statement(&self, bytes: Vec<u8>) -> Result<(), Error> {
        let received = self.discovery.authenticate(bytes).await?;
        self.handle_authenticated_statement(received)
    }
    pub(crate) fn handle_authenticated_statement(
        &self,
        received: AuthenticatedOperation,
    ) -> Result<(), Error> {
        if received.operation().signer() != received.sender() {
            return Err(Error::BadSignature);
        }
        let (_, hint) = value_changed(received.operation())?;
        let mut watches = self.watches.lock().map_err(|_| Error::Unavailable)?;
        let watch = watches
            .get_mut(&(*received.sender(), hint.watch_id))
            .ok_or(Error::UnknownWatch)?;
        if watch.lost
            || self.discovery.now_ms() >= watch.expires_ms
            || hint.key != watch.descriptor.key()
        {
            watch.lost = true;
            return Err(Error::UnknownWatch);
        }
        let schema = watch
            .descriptor
            .validate_for(&hint.key, self.discovery.verifier())?;
        if hint
            .subkeys
            .last()
            .is_some_and(|subkey| subkey as usize >= schema.subkey_count())
            || hint.count > watch.count
        {
            watch.lost = true;
            return Err(Error::Encoding);
        }
        if let Some(value) = &hint.value {
            let subkey = hint.subkeys.first().ok_or(Error::Encoding)?;
            value.validate(
                &hint.key,
                &watch.descriptor.owner,
                &schema,
                subkey,
                self.discovery.verifier(),
            )?;
        }
        if hint.count == watch.count {
            return Ok(());
        } // Original duplicate-count suppression.
        watch.count = hint.count;
        watch.changed |= &hint.subkeys;
        watch.lost |= hint.count == 0 || hint.subkeys.is_empty();
        Ok(())
    }
    /// Actual listener/lifecycle loss invalidates every retained client lease.
    pub fn transport_lost(&self) {
        if let Ok(mut watches) = self.watches.lock() {
            for watch in watches.values_mut() {
                watch.lost = true;
            }
        }
    }
    async fn watch_request(
        &self,
        node: &NodeId,
        watch: &ClientWatch,
        id: u64,
        count: u32,
    ) -> Result<(u64, u64), Error> {
        let key = watch.descriptor.key();
        let question = self.discovery.with_sender(Question::new(&Query::Watch {
            key,
            subkeys: watch.subkeys.clone(),
            duration_us: 0,
            count,
            watch_id: id,
        })?)?;
        let start = self.discovery.now_ms();
        let answer = self
            .discovery
            .call_watch(node, &key, &question, &self.watcher)
            .await?;
        let response = question.answer(
            answer.operation(),
            Some(&watch.descriptor),
            self.discovery.verifier(),
        )?;
        self.observe_response(&response)?;
        let (duration_us, watch_id) = match response {
            Response::Watch {
                accepted: true,
                duration_us,
                watch_id,
                ..
            } => (duration_us, watch_id),
            Response::Watch {
                accepted: false, ..
            } => return Err(Error::WatchRefused),
            _ => return Err(Error::Encoding),
        };
        let now = self.discovery.now_ms();
        if now < start {
            return Err(Error::Unavailable);
        }
        if count == 0 {
            return if duration_us == 0 {
                Ok((watch_id, now))
            } else {
                Err(Error::Encoding)
            };
        }
        let expires = start
            .checked_add((now - start) / 2)
            .and_then(|mid| mid.checked_add(duration_us / 1000))
            .ok_or(Error::Encoding)?;
        if watch_id == 0 || expires <= now {
            return Err(Error::UnknownWatch);
        }
        Ok((watch_id, expires))
    }
    /// Drive original pending notifications and the upstream 30-second renewal
    /// window from the actual runtime timer. Failures remain observable lease loss.
    pub async fn maintain(&self) -> Result<(), Error> {
        let mut failure = None;
        if let Some(store) = &self.store {
            let notifications = store.notifications(self.discovery.now_us()?)?;
            for notification in notifications {
                let result = match Statement::value_changed(&notification.hint) {
                    Ok(statement) => {
                        self.discovery
                            .send_statement(&statement, &notification.target)
                            .await
                    }
                    Err(error) => Err(error),
                };
                // Taking a batch consumes its count budget. A failed destination
                // must not silently discard notifications for other destinations
                // or prevent this node's own expiring watches from being renewed.
                if let Err(error) = result {
                    failure.get_or_insert(error);
                }
            }
        }
        let now = self.discovery.now_ms();
        let renew: Vec<_> = self
            .watches
            .lock()
            .map_err(|_| Error::Unavailable)?
            .iter()
            .filter(|(_, w)| !w.lost && now.saturating_add(30_000) >= w.expires_ms)
            .map(|(key, w)| (*key, w.clone()))
            .collect();
        for ((node, id), previous) in renew {
            let result = if now >= previous.expires_ms {
                Err(Error::UnknownWatch)
            } else {
                self.watch_request(&node, &previous, id, previous.count)
                    .await
            };
            if let Some(watch) = self
                .watches
                .lock()
                .map_err(|_| Error::Unavailable)?
                .get_mut(&(node, id))
            {
                match result {
                    Ok((same, expires)) if same == id => watch.expires_ms = expires,
                    _ => {
                        watch.lost = true;
                        failure.get_or_insert(Error::UnknownWatch);
                    }
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
impl<I: TorNodeIdentity, V: Verifier, W: TorWatcher> Network for TorRecords<I, V, W> {
    fn now_ms(&self) -> u64 {
        self.discovery.now_ms()
    }
    fn hints(&self, key: &RecordKey) -> Result<Vec<NodeId>, Error> {
        self.discovery.storage_hints(&key.0)
    }
    async fn closest(&self, key: &RecordKey) -> Result<Vec<NodeId>, Error> {
        let report = self.discovery.discover(key.0).await?;
        if report.peers.is_empty() {
            return Err(Error::Unavailable);
        }
        Ok(report.peers.iter().map(|p| *p.node_id()).collect())
    }
    async fn descriptor(
        &self,
        node: &NodeId,
        key: &RecordKey,
    ) -> Result<Option<Descriptor>, Error> {
        match self
            .request(
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
            .request(
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
            .request(
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
            Response::Set { .. } => Err(Error::Unavailable),
            _ => Err(Error::Encoding),
        }
    }
    async fn watch(&self, node: &NodeId, key: &RecordKey) -> Result<WatchId, Error> {
        // No listener: a dial-only client could never receive the ValueChanged.
        if self.discovery.is_dial_only() {
            return Err(Error::WatchRefused);
        }
        {
            let watches = self.watches.lock().map_err(|_| Error::Unavailable)?;
            if watches.len() >= self.limits.watches.get() {
                return Err(Error::Unavailable);
            }
            if watches
                .iter()
                .any(|((n, _), w)| n == node && w.descriptor.key() == *key)
            {
                return Err(Error::Conflict);
            }
        }
        let descriptor = Network::descriptor(self, node, key)
            .await?
            .ok_or(Error::UnknownRecord)?;
        let schema = descriptor.validate_for(key, self.discovery.verifier())?;
        let watch = ClientWatch {
            descriptor,
            subkeys: SubkeyRanges::from_iter(0..schema.subkey_count() as u32),
            changed: SubkeyRanges::new(),
            expires_ms: 0,
            count: u32::MAX,
            lost: false,
        };
        let (id, expires_ms) = self.watch_request(node, &watch, 0, watch.count).await?;
        let mut accepted = watch.clone();
        accepted.expires_ms = expires_ms;
        let retained = {
            let mut watches = self.watches.lock().map_err(|_| Error::Unavailable)?;
            if watches.len() >= self.limits.watches.get() || watches.contains_key(&(*node, id)) {
                false
            } else {
                watches.insert((*node, id), accepted);
                true
            }
        };
        if !retained {
            let _ = self.watch_request(node, &watch, id, 0).await;
            return Err(Error::Unavailable);
        }
        Ok(WatchId(id))
    }
    async fn cancel_watch(&self, node: &NodeId, id: WatchId) -> Result<(), Error> {
        let previous = self
            .watches
            .lock()
            .map_err(|_| Error::Unavailable)?
            .remove(&(*node, id.0));
        if let Some(previous) = previous {
            self.watch_request(node, &previous, id.0, 0).await?;
        }
        Ok(())
    }
    async fn changes(&self, node: &NodeId, id: WatchId) -> Result<Vec<Change>, Error> {
        let previous = self
            .watches
            .lock()
            .map_err(|_| Error::Unavailable)?
            .get(&(*node, id.0))
            .cloned()
            .ok_or(Error::UnknownWatch)?;
        if previous.lost || self.discovery.now_ms() >= previous.expires_ms {
            return Err(Error::UnknownWatch);
        }
        let key = previous.descriptor.key();
        let mut changes = Vec::new();
        for subkey in &previous.changed {
            let value = Network::get(self, node, &key, subkey)
                .await?
                .ok_or(Error::Unavailable)?;
            changes.push(Change {
                key,
                subkey,
                seq: value.seq,
            });
        }
        if let Some(watch) = self
            .watches
            .lock()
            .map_err(|_| Error::Unavailable)?
            .get_mut(&(*node, id.0))
            && watch.count == previous.count
        {
            watch.changed = &watch.changed - &previous.changed;
        }
        Ok(changes)
    }
}

/// Shared network port for Records' original replication engine. Cloning shares
/// this node's transport owner, never another node's storage or watcher identity.
pub struct TorRecordNetwork<I, V: Verifier, W>(Arc<TorRecords<I, V, W>>);
impl<I, V: Verifier, W> Clone for TorRecordNetwork<I, V, W> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<I: TorNodeIdentity, V: Verifier, W: TorWatcher> TorRecords<I, V, W> {
    /// Borrow this actual runtime through the original transport-independent port.
    pub fn network(self: &Arc<Self>) -> TorRecordNetwork<I, V, W> {
        TorRecordNetwork(self.clone())
    }
}
impl<I: TorNodeIdentity, V: Verifier, W: TorWatcher> Network for TorRecordNetwork<I, V, W> {
    fn now_ms(&self) -> u64 {
        Network::now_ms(&*self.0)
    }
    fn hints(&self, key: &RecordKey) -> Result<Vec<NodeId>, Error> {
        self.0.hints(key)
    }
    async fn closest(&self, key: &RecordKey) -> Result<Vec<NodeId>, Error> {
        self.0.closest(key).await
    }
    async fn descriptor(
        &self,
        node: &NodeId,
        key: &RecordKey,
    ) -> Result<Option<Descriptor>, Error> {
        Network::descriptor(&*self.0, node, key).await
    }
    async fn get(
        &self,
        node: &NodeId,
        key: &RecordKey,
        subkey: u32,
    ) -> Result<Option<SignedValue>, Error> {
        self.0.get(node, key, subkey).await
    }
    async fn set(
        &self,
        node: &NodeId,
        descriptor: &Descriptor,
        subkey: u32,
        value: &SignedValue,
    ) -> Result<SetOutcome, Error> {
        self.0.set(node, descriptor, subkey, value).await
    }
    async fn watch(&self, node: &NodeId, key: &RecordKey) -> Result<WatchId, Error> {
        self.0.watch(node, key).await
    }
    async fn cancel_watch(&self, node: &NodeId, id: WatchId) -> Result<(), Error> {
        self.0.cancel_watch(node, id).await
    }
    async fn changes(&self, node: &NodeId, id: WatchId) -> Result<Vec<Change>, Error> {
        self.0.changes(node, id).await
    }
}
