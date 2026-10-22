//! Original Veilid discovery over actual Tor complete-message calls. No fixed
//! population, unsigned key/onion map, operator record store or direct sockets.
use cdht::{
    Error, NodeId, Verifier,
    discovery::Discovery,
    rpc::{
        Answer, Question, SignedOperation, Statement, UnverifiedPeerInfoBytes,
        envelope::{AuthenticatedOperation, Envelope, TimeWindow},
        peers::{DHTV, FindAnswer, VerifiedExtensionPeer, find_node, find_query, find_response},
    },
};
use futures::{
    StreamExt,
    future::{Either, select},
    stream::FuturesUnordered,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

/// Keys supplies a node-purpose signing owner. Implementations authorize, then
/// use their synchronous borrowed signer callback; no borrowed key crosses I/O.
pub trait TorNodeIdentity: crate::MaybeSend + crate::MaybeSync {
    /// This community-local node's public VLD0 coordinate.
    fn node_id(&self) -> NodeId;
    /// Sign precisely this original question for one authenticated destination.
    fn question<'a>(
        &'a self,
        question: &'a Question,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>>;
    /// Sign this original FindNode answer for the authenticated requester.
    fn answer<'a>(
        &'a self,
        answer: &'a FindAnswer,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>>;
    /// Sign an original record answer for the authenticated requester node.
    fn record_answer<'a>(
        &'a self,
        answer: &'a Answer,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>>;
    /// Sign an original ValueChanged statement for the authenticated watch target.
    fn statement<'a>(
        &'a self,
        statement: &'a Statement,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>>;
    /// Seal an already signed original operation with this node's original ENV0
    /// signature and DH, independently of the inner watcher/record signer.
    fn seal<'a>(
        &'a self,
        operation: &'a SignedOperation,
        destination: &'a NodeId,
        timestamp_us: u64,
    ) -> crate::BoxFuture<'a, Result<Vec<u8>, Error>>;
    /// Decrypt an authenticated envelope through the retained scoped node owner.
    fn open<'a>(
        &'a self,
        envelope: &'a Envelope,
    ) -> crate::BoxFuture<'a, Result<AuthenticatedOperation, Error>>;
}
/// Original anonymous or schema-member watch signer, independent of node identity.
/// Keys authorizes the exact record question and lends its signer synchronously.
pub trait TorWatcher: crate::MaybeSend + crate::MaybeSync {
    /// Public original watcher identity for this record capability.
    fn public_key(&self, key: &cdht::RecordKey) -> Result<NodeId, Error>;
    /// Sign only this record's original WatchValueQ for one authenticated node.
    fn question<'a>(
        &'a self,
        key: &'a cdht::RecordKey,
        question: &'a Question,
        destination: &'a NodeId,
    ) -> crate::BoxFuture<'a, Result<SignedOperation, Error>>;
}
/// Actual runtime wall clock for original ENV0 timestamp authentication. The Tor
/// monotonic Clock remains the separate source for operation deadlines.
pub trait TorEnvelopeTime: crate::MaybeSend + crate::MaybeSync {
    /// Current Unix time in microseconds. Failure refuses envelope acceptance.
    fn now_us(&self) -> Result<u64, Error>;
}
/// Resource limits for discovery only; no record expiry or replication policy.
#[derive(Clone, Copy)]
pub struct TorDiscoveryLimits {
    /// Maximum retained authenticated routing identities.
    pub peers: NonZeroUsize,
    /// Concurrent discovery RPCs. Native core's configured fanout is the reference.
    pub lanes: NonZeroUsize,
    /// Overall lookup deadline, in monotonic milliseconds.
    pub timeout_ms: u64,
}
/// Explicit construction inputs for one bounded routing owner.
pub struct TorDiscoveryConfig<I, V> {
    /// Complete-message port on the actual running Tor node.
    pub messages: Arc<ctrn::messages::Messages>,
    /// Scoped node signing owner.
    pub identity: I,
    /// Maintained strict VLD0 verifier.
    pub verifier: V,
    /// Original signed advertisement for the actual listener.
    pub own: UnverifiedPeerInfoBytes,
    /// Operator bootstrap hints, independently signed by each advertised node.
    pub bootstrap: Vec<UnverifiedPeerInfoBytes>,
    /// The same node's monotonic runtime clock.
    pub clock: Arc<dyn ctrn::Clock>,
    /// Caller-selected work bounds.
    pub limits: TorDiscoveryLimits,
    /// Actual runtime wall clock; request payloads never set acceptance time.
    pub envelope_time: Arc<dyn TorEnvelopeTime>,
    /// Explicit original envelope freshness limits.
    pub envelope_window: TimeWindow,
}
/// Construction inputs of a dial-only client: no listener and no own advertisement.
/// Its questions carry no sender PeerInfo, so no peer ever routes or advertises it.
pub struct TorDialConfig<I, V> {
    /// Complete-message port on the actual running (client-only) Tor node.
    pub messages: Arc<ctrn::messages::Messages>,
    /// Scoped node signing owner (for example an ephemeral bootstrap key).
    pub identity: I,
    /// Maintained strict VLD0 verifier.
    pub verifier: V,
    /// Operator bootstrap hints, independently signed by each advertised node.
    pub bootstrap: Vec<UnverifiedPeerInfoBytes>,
    /// The same node's monotonic runtime clock.
    pub clock: Arc<dyn ctrn::Clock>,
    /// Caller-selected work bounds.
    pub limits: TorDiscoveryLimits,
    /// Actual runtime wall clock; request payloads never set acceptance time.
    pub envelope_time: Arc<dyn TorEnvelopeTime>,
    /// Explicit original envelope freshness limits.
    pub envelope_window: TimeWindow,
}
#[derive(Clone)]
struct Peer {
    info: Arc<VerifiedExtensionPeer>,
    endpoint: ctrn::Address,
    responsive: bool,
}
fn peer(info: VerifiedExtensionPeer) -> Result<Peer, Error> {
    let ad = info.advertisement();
    let mut endpoint = None;
    for dial in &ad.dial_info {
        let checked = ctrn::OnionEndpoint::from_node_info(
            dial.protocol,
            &ad.outbound_protocols,
            &ad.address_types,
            &dial.detail,
        )
        .map_err(|_| Error::Encoding)?;
        if endpoint.is_none() {
            endpoint = Some(checked.address());
        }
    }
    Ok(Peer {
        endpoint: endpoint.ok_or(Error::Encoding)?,
        info: Arc::new(info),
        responsive: false,
    })
}
/// Results expose partial knowledge; no discovery result establishes DHT absence.
pub struct TorDiscoveryReport {
    /// Responsive, independently authenticated storage peers, closest first.
    pub peers: Vec<Arc<VerifiedExtensionPeer>>,
    /// Every discovered query completed before the deadline and resource bound.
    pub exhausted: bool,
    /// The lookup hit its explicit resource limit.
    pub limited: bool,
}
/// Calls one dial-only (sender-less) caller may make to this node per
/// [`DIAL_ONLY_WINDOW_MS`]. A dial-only caller sends no PeerInfo, and a Tor stream
/// carries one call and hides its circuit, so the caller's authenticated node key (one
/// per registration scope) is the only connection identity a node has for it. A
/// registration needs a few lookups, reads and writes per node; this leaves room for
/// retries while keeping a node from being a free lookup service. A code constant on
/// purpose: every node enforces the same budget, no deployment can loosen it.
pub const DIAL_ONLY_CALLS_PER_WINDOW: u32 = 64;
/// The window [`DIAL_ONLY_CALLS_PER_WINDOW`] counts over, in runtime milliseconds.
pub const DIAL_ONLY_WINDOW_MS: u64 = 60_000;
/// Dial-only callers a node tracks at once. Their keys are free to mint, so this bounds
/// both the tracking memory and the node's total sender-less load: while every slot is
/// inside its window, a new dial-only caller is refused.
pub const DIAL_ONLY_CALLERS: usize = 256;

/// The call a node is asked to answer, as far as admission is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallKind {
    /// A FindNode lookup.
    Lookup,
    /// A record read (descriptor or subkey value).
    Get,
    /// A record write; the value stays signed by its record's own writer keys.
    Set,
    /// A sequence-hint inspection.
    Inspect,
    /// A watch lease.
    Watch,
    /// An application call.
    Application,
}

/// Fixed-window call counts per dial-only caller, bounded by [`DIAL_ONLY_CALLERS`].
#[derive(Default)]
pub(crate) struct DialOnlyCallers {
    /// Caller -> (window start in runtime ms, calls admitted in that window).
    windows: BTreeMap<NodeId, (u64, u32)>,
}
impl DialOnlyCallers {
    pub(crate) fn admit(&mut self, caller: NodeId, now_ms: u64) -> Result<(), Error> {
        let open = |start: u64| now_ms.saturating_sub(start) < DIAL_ONLY_WINDOW_MS;
        if let Some((start, calls)) = self.windows.get_mut(&caller) {
            if !open(*start) {
                *start = now_ms;
                *calls = 0;
            }
            if *calls >= DIAL_ONLY_CALLS_PER_WINDOW {
                return Err(Error::RateLimited);
            }
            *calls += 1;
            return Ok(());
        }
        if self.windows.len() >= DIAL_ONLY_CALLERS {
            self.windows.retain(|_, (start, _)| open(*start));
            if self.windows.len() >= DIAL_ONLY_CALLERS {
                return Err(Error::RateLimited);
            }
        }
        self.windows.insert(caller, (now_ms, 1));
        Ok(())
    }
}

/// Bounded signed routing state for one community's actual Tor runtime. A
/// bootstrap can use this alone: the type has no DHT record storage capability.
pub struct TorDiscovery<I, V> {
    messages: Arc<ctrn::messages::Messages>,
    identity: I,
    verifier: V,
    /// None for a dial-only client: it has no listener and advertises nothing.
    own: Option<Peer>,
    clock: Arc<dyn ctrn::Clock>,
    limits: TorDiscoveryLimits,
    routes: Mutex<BTreeMap<NodeId, Peer>>,
    dial_only_callers: Mutex<DialOnlyCallers>,
    envelope_time: Arc<dyn TorEnvelopeTime>,
    envelope_window: TimeWindow,
}
impl<I: TorNodeIdentity, V: Verifier> TorDiscovery<I, V> {
    /// Bind signed self-advertisement to an ACTUAL live listener and matching
    /// scoped node identity. Bootstrap entries are original independently signed
    /// PeerInfo, not caller-supplied coordinates or a complete device roster.
    pub fn new(
        config: TorDiscoveryConfig<I, V>,
        listener: &ctrn::messages::MessageListener,
    ) -> Result<Self, Error> {
        let TorDiscoveryConfig {
            messages,
            identity,
            verifier,
            own,
            bootstrap,
            clock,
            limits,
            envelope_time,
            envelope_window,
        } = config;
        let own = peer(own.verify_extension(&verifier)?)?;
        if own.info.node_id() != &identity.node_id()
            || own.endpoint.bytes() != listener.address().bytes()
        {
            return Err(Error::WrongRecord);
        }
        Self::build(
            TorDialConfig {
                messages,
                identity,
                verifier,
                bootstrap,
                clock,
                limits,
                envelope_time,
                envelope_window,
            },
            Some(own),
        )
    }
    /// A dial-only client: it needs no listener, publishes no advertisement and sends
    /// no sender PeerInfo, so it can look up, read and write records but is never a
    /// routing peer, never answers, and never holds a watch (nothing could deliver it).
    pub fn dial_only(config: TorDialConfig<I, V>) -> Result<Self, Error> {
        Self::build(config, None)
    }
    fn build(config: TorDialConfig<I, V>, own: Option<Peer>) -> Result<Self, Error> {
        let TorDialConfig {
            messages,
            identity,
            verifier,
            bootstrap,
            clock,
            limits,
            envelope_time,
            envelope_window,
        } = config;
        if limits.timeout_ms == 0
            || limits.lanes.get() > limits.peers.get()
            || bootstrap.len() > limits.peers.get()
        {
            return Err(Error::Encoding);
        }
        let this = Self {
            messages,
            identity,
            verifier,
            own,
            clock,
            limits,
            routes: Mutex::new(BTreeMap::new()),
            dial_only_callers: Mutex::new(DialOnlyCallers::default()),
            envelope_time,
            envelope_window,
        };
        for info in bootstrap {
            this.observe(peer(info.verify_extension(&this.verifier)?)?)?;
        }
        Ok(this)
    }
    /// True for a [`Self::dial_only`] client.
    pub fn is_dial_only(&self) -> bool {
        self.own.is_none()
    }
    fn serving(&self) -> Result<&Peer, Error> {
        self.own.as_ref().ok_or(Error::Unavailable)
    }
    fn observe(&self, peer: Peer) -> Result<(), Error> {
        let id = *peer.info.node_id();
        if id == self.identity.node_id() {
            return Ok(());
        }
        let mut routes = self.routes.lock().map_err(|_| Error::Unavailable)?;
        if let Some(previous) = routes.get(&id) {
            let old = previous.info.advertisement().timestamp_us;
            let new = peer.info.advertisement().timestamp_us;
            if new < old {
                return Ok(());
            }
            if new == old {
                return if previous.info.advertisement() == peer.info.advertisement() {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
        } else if routes.len() == self.limits.peers.get() {
            return Err(Error::Unavailable);
        }
        routes.insert(id, peer);
        Ok(())
    }
    async fn call_find(
        &self,
        to: Arc<VerifiedExtensionPeer>,
        target: NodeId,
    ) -> Result<Vec<Peer>, Error> {
        let question = self.with_sender(find_node(&target, &[DHTV])?)?;
        let signed = self.identity.question(&question, to.node_id()).await?;
        if signed.operation() != question.as_bytes() || signed.signer() != &self.identity.node_id()
        {
            return Err(Error::Encoding);
        }
        let received = self.exchange(&to, &question, &signed).await?;
        find_response(&signed, received.operation(), &self.verifier)?
            .into_iter()
            .map(peer)
            .collect()
    }
    async fn exchange(
        &self,
        to: &VerifiedExtensionPeer,
        question: &Question,
        signed: &SignedOperation,
    ) -> Result<AuthenticatedOperation, Error> {
        let checked = peer(to.original().verify_extension(&self.verifier)?)?;
        if signed.operation() != question.as_bytes() {
            return Err(Error::Encoding);
        }
        // Verify the signed result, including exact destination, before transmission.
        let signed = SignedOperation::verify(
            signed.as_bytes().to_vec(),
            to.node_id(),
            signed.signer(),
            &self.verifier,
        )?;
        let now = self.envelope_time.now_us()?;
        let outgoing = self.identity.seal(&signed, to.node_id(), now).await?;
        let outgoing = Envelope::verify(
            outgoing,
            to.node_id(),
            now,
            self.envelope_window,
            &self.verifier,
        )?;
        if outgoing.sender() != &self.identity.node_id() {
            return Err(Error::BadSignature);
        }
        let answer = self
            .messages
            .app_call(&checked.endpoint, outgoing.as_bytes())
            .await
            .map_err(|_| Error::Unavailable)?;
        let answer = Envelope::verify(
            answer,
            &self.identity.node_id(),
            self.envelope_time.now_us()?,
            self.envelope_window,
            &self.verifier,
        )?;
        if answer.sender() != to.node_id() {
            return Err(Error::BadSignature);
        }
        let received = self.identity.open(&answer).await?;
        if received.sender() != to.node_id() || received.operation().signer() != to.node_id() {
            return Err(Error::BadSignature);
        }
        Ok(received)
    }
    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now()
    }
    pub(crate) fn now_us(&self) -> Result<u64, Error> {
        self.envelope_time.now_us()
    }
    pub(crate) fn verifier(&self) -> &V {
        &self.verifier
    }
    pub(crate) fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }
    pub(crate) fn sleep_until(&self, deadline_ms: u64) -> crate::BoxFuture<'_, ()> {
        self.clock.sleep_until(deadline_ms)
    }
    pub(crate) fn endpoint(&self) -> Result<&ctrn::Address, Error> {
        Ok(&self.serving()?.endpoint)
    }
    pub(crate) fn node_for_endpoint(&self, endpoint: &[u8]) -> Result<NodeId, Error> {
        let routes = self.routes.lock().map_err(|_| Error::Unavailable)?;
        let mut matches = routes
            .values()
            .filter(|peer| peer.endpoint.bytes() == endpoint);
        let node = *matches.next().ok_or(Error::Unavailable)?.info.node_id();
        if matches.any(|peer| peer.info.node_id() != &node) {
            return Err(Error::Conflict);
        }
        Ok(node)
    }
    pub(crate) fn stores_records(&self) -> bool {
        self.own
            .as_ref()
            .is_some_and(|own| own.info.advertisement().capabilities.contains(&DHTV))
    }
    pub(crate) fn mark_responsive(&self, node: &NodeId, responsive: bool) -> Result<(), Error> {
        if let Some(peer) = self
            .routes
            .lock()
            .map_err(|_| Error::Unavailable)?
            .get_mut(node)
        {
            peer.responsive = responsive;
        }
        Ok(())
    }
    pub(crate) fn route(&self, node: &NodeId) -> Result<Arc<VerifiedExtensionPeer>, Error> {
        self.routes
            .lock()
            .map_err(|_| Error::Unavailable)?
            .get(node)
            .map(|p| p.info.clone())
            .ok_or(Error::Unavailable)
    }
    pub(crate) fn observe_hints(&self, hints: &[UnverifiedPeerInfoBytes]) -> Result<(), Error> {
        for hint in hints {
            self.observe(peer(hint.verify_extension(&self.verifier)?)?)?;
        }
        Ok(())
    }
    pub(crate) fn storage_hints(&self, target: &NodeId) -> Result<Vec<NodeId>, Error> {
        let mut nodes: Vec<_> = self
            .routes
            .lock()
            .map_err(|_| Error::Unavailable)?
            .values()
            .filter(|peer| peer.info.advertisement().capabilities.contains(&DHTV))
            .map(|peer| *peer.info.node_id())
            .collect();
        nodes.sort_unstable_by_key(|node| std::array::from_fn::<_, 32, _>(|i| node[i] ^ target[i]));
        nodes.truncate(cdht::MAX_FANOUT_NODES);
        Ok(nodes)
    }
    pub(crate) fn closer(&self, target: &NodeId) -> Result<Vec<UnverifiedPeerInfoBytes>, Error> {
        let own_distance = std::array::from_fn::<_, 32, _>(|i| self.node_id()[i] ^ target[i]);
        let mut peers: Vec<_> = self
            .routes
            .lock()
            .map_err(|_| Error::Unavailable)?
            .values()
            .filter(|p| p.responsive && p.info.advertisement().capabilities.contains(&DHTV))
            .map(|p| p.info.clone())
            .collect();
        peers.sort_by_key(|p| std::array::from_fn::<_, 32, _>(|i| p.node_id()[i] ^ target[i]));
        peers
            .into_iter()
            .filter(|p| {
                std::array::from_fn::<_, 32, _>(|i| p.node_id()[i] ^ target[i]) < own_distance
            })
            .take(20)
            .map(|p| UnverifiedPeerInfoBytes::from_bytes(p.original().as_bytes().to_vec()))
            .collect()
    }
    pub(crate) async fn call_record(
        &self,
        to: &NodeId,
        question: &Question,
    ) -> Result<AuthenticatedOperation, Error> {
        let route = self.route(to)?;
        let signed = self.identity.question(question, to).await?;
        if signed.signer() != &self.node_id() {
            return Err(Error::BadSignature);
        }
        let result = self.exchange(&route, question, &signed).await;
        self.mark_responsive(to, result.is_ok())?;
        result
    }
    pub(crate) fn with_sender(&self, question: Question) -> Result<Question, Error> {
        match &self.own {
            Some(own) => question.with_sender(own.info.original()),
            // A dial-only client sends no sender PeerInfo: nothing to route or advertise.
            None => Ok(question),
        }
    }
    pub(crate) async fn authenticate(
        &self,
        bytes: Vec<u8>,
    ) -> Result<AuthenticatedOperation, Error> {
        self.serving()?;
        let envelope = Envelope::verify(
            bytes,
            &self.node_id(),
            self.now_us()?,
            self.envelope_window,
            &self.verifier,
        )?;
        let received = self.identity.open(&envelope).await?;
        if received.sender() != envelope.sender() {
            return Err(Error::BadSignature);
        }
        Ok(received)
    }
    /// Route an authenticated sender by its own PeerInfo. A sender without PeerInfo is a
    /// dial-only client: it is answered on its call but never routed, so it is never
    /// handed to other peers. Returns whether the sender is routable.
    pub(crate) fn observe_sender(&self, received: &AuthenticatedOperation) -> Result<bool, Error> {
        let Some(sender) = received.sender_peer_optional(&self.verifier)? else {
            return Ok(false);
        };
        self.observe(peer(sender)?)?;
        self.mark_responsive(received.sender(), true)?;
        Ok(true)
    }
    /// Admit one authenticated call: a sender with PeerInfo is routed as before; a
    /// dial-only (sender-less) caller may only look up, read and write records, within
    /// its [`DIAL_ONLY_CALLS_PER_WINDOW`] budget. Returns whether the sender is routable.
    pub(crate) fn admit_caller(
        &self,
        received: &AuthenticatedOperation,
        kind: CallKind,
    ) -> Result<bool, Error> {
        if self.observe_sender(received)? {
            return Ok(true);
        }
        match kind {
            CallKind::Lookup | CallKind::Get | CallKind::Set => {}
            // No listener: a ValueChanged for it could never arrive.
            CallKind::Watch => return Err(Error::WatchRefused),
            CallKind::Inspect | CallKind::Application => return Err(Error::Refused),
        }
        self.dial_only_callers
            .lock()
            .map_err(|_| Error::Unavailable)?
            .admit(*received.sender(), self.now_ms())?;
        Ok(false)
    }
    pub(crate) async fn call_watch<W: TorWatcher>(
        &self,
        to: &NodeId,
        key: &cdht::RecordKey,
        question: &Question,
        watcher: &W,
    ) -> Result<AuthenticatedOperation, Error> {
        let route = self.route(to)?;
        let signed = watcher.question(key, question, to).await?;
        if signed.signer() != &watcher.public_key(key)? {
            return Err(Error::BadSignature);
        }
        let result = self.exchange(&route, question, &signed).await;
        self.mark_responsive(to, result.is_ok())?;
        result
    }
    pub(crate) async fn reply_record(
        &self,
        answer: &Answer,
        target: &NodeId,
    ) -> Result<Vec<u8>, Error> {
        let signed = self.identity.record_answer(answer, target).await?;
        if signed.operation() != answer.as_bytes() {
            return Err(Error::Encoding);
        }
        let checked = SignedOperation::verify(
            signed.as_bytes().to_vec(),
            target,
            &self.node_id(),
            &self.verifier,
        )?;
        self.identity.seal(&checked, target, self.now_us()?).await
    }
    pub(crate) async fn send_statement(
        &self,
        statement: &Statement,
        target: &NodeId,
    ) -> Result<(), Error> {
        let route = self.route(target)?;
        let route = peer(route.original().verify_extension(&self.verifier)?)?;
        let signed = self.identity.statement(statement, target).await?;
        if signed.operation() != statement.as_bytes() {
            return Err(Error::Encoding);
        }
        let checked = SignedOperation::verify(
            signed.as_bytes().to_vec(),
            target,
            &self.node_id(),
            &self.verifier,
        )?;
        let bytes = self.identity.seal(&checked, target, self.now_us()?).await?;
        self.messages
            .app_message(&route.endpoint, &bytes)
            .await
            .map_err(|_| Error::Unavailable)
    }
    /// Query dynamic original peer hints over Tor, unique and XOR-ordered, with
    /// bounded concurrent lanes and an actual runtime deadline. A timed-out lookup
    /// returns only its authenticated partial results, never a false empty vault.
    pub async fn discover(&self, target: NodeId) -> Result<TorDiscoveryReport, Error> {
        let start = self.clock.now();
        let deadline = start
            .checked_add(self.limits.timeout_ms)
            .ok_or(Error::Unavailable)?;
        let mut search = Discovery::new(target, self.identity.node_id(), self.limits.peers);
        for entry in self.routes.lock().map_err(|_| Error::Unavailable)?.values() {
            search.insert(entry.info.clone())?;
        }
        let work = async {
            let mut pending = FuturesUnordered::new();
            loop {
                while pending.len() < self.limits.lanes.get() {
                    let Some(peer) = search.next_peer() else {
                        break;
                    };
                    pending.push(async move {
                        let id = *peer.node_id();
                        (id, self.call_find(peer, target).await)
                    });
                }
                let Some((id, result)) = pending.next().await else {
                    break;
                };
                match result {
                    Ok(peers) => {
                        search.finish(&id, true)?;
                        self.mark_responsive(&id, true)?;
                        for next in peers {
                            let info = next.info.clone();
                            self.observe(next)?;
                            if search.insert(info).is_err() {
                                return Ok::<(), Error>(());
                            }
                        }
                    }
                    Err(_) => {
                        search.finish(&id, false)?;
                        self.mark_responsive(&id, false)?;
                    }
                }
                if self.clock.now() < start {
                    return Err(Error::Unavailable);
                }
            }
            Ok(())
        };
        let expired = match select(Box::pin(work), self.clock.sleep_until(deadline)).await {
            Either::Left((result, _)) => {
                result?;
                false
            }
            Either::Right((_, work)) => {
                drop(work);
                true
            }
        };
        Ok(TorDiscoveryReport {
            peers: search.storage_peers(),
            exhausted: !expired && search.exhausted(),
            limited: search.limited(),
        })
    }
    /// Handle a bootstrap/routing FindNode request. Only the actual request signer
    /// may introduce its self-signed endpoint. Returned peers remain original bytes.
    /// The caller sends this result through the received one-use Tor reply port.
    pub async fn answer_find(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.serving()?;
        let envelope = Envelope::verify(
            request,
            &self.identity.node_id(),
            self.envelope_time.now_us()?,
            self.envelope_window,
            &self.verifier,
        )?;
        let received = self.identity.open(&envelope).await?;
        if received.sender() != envelope.sender()
            || received.operation().signer() != received.sender()
        {
            return Err(Error::BadSignature);
        }
        self.answer_find_authenticated(&received).await
    }
    pub(crate) async fn answer_find_authenticated(
        &self,
        received: &AuthenticatedOperation,
    ) -> Result<Vec<u8>, Error> {
        if received.operation().signer() != received.sender() {
            return Err(Error::BadSignature);
        }
        let own = self.serving()?.info.clone();
        let request = received.operation();
        let (_, target, caps) = find_query(request)?;
        self.admit_caller(received, CallKind::Lookup)?;
        let mut peers: Vec<_> = self
            .routes
            .lock()
            .map_err(|_| Error::Unavailable)?
            .values()
            .map(|p| p.info.clone())
            .chain(std::iter::once(own))
            .filter(|p| {
                caps.iter()
                    .all(|cap| p.advertisement().capabilities.contains(cap))
            })
            .collect();
        peers.sort_unstable_by_key(|p| {
            std::array::from_fn::<_, 32, _>(|i| p.node_id()[i] ^ target[i])
        });
        peers.truncate(20);
        let original = peers
            .iter()
            .map(|p| UnverifiedPeerInfoBytes::from_bytes(p.original().as_bytes().to_vec()))
            .collect::<Result<Vec<_>, _>>()?;
        let answer = FindAnswer::new(request, &original)?;
        let signed = self.identity.answer(&answer, request.signer()).await?;
        if signed.operation() != answer.as_bytes() {
            return Err(Error::Encoding);
        }
        let checked = SignedOperation::verify(
            signed.as_bytes().to_vec(),
            request.signer(),
            &self.identity.node_id(),
            &self.verifier,
        )?;
        self.identity
            .seal(&checked, received.sender(), self.envelope_time.now_us()?)
            .await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "../tests/support/dial_only_unit.rs"]
mod dial_only_unit;
