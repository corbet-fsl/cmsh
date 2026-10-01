//! The facade: backend registration, policy, selection via `cfbk`, dialing,
//! listening, datagrams and the DHT capability.
use crate::Spawn;
use crate::property::{Policy, Property, properties};
use crate::session::{Role, Session, Substream};
use cfbk::{Candidate, Config, Fallback, Health, Switch, Unavailable};
use cmsh_api::{Address, Backend, BoxStream, Capabilities, Dht, Error, ErrorKind, Scheme};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::future::{AbortHandle, Abortable};
use futures::lock::Mutex as AsyncMutex;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Switches kept for [`Mesh::take_switches`]; older ones are dropped.
const SWITCH_JOURNAL: usize = 64;
/// Accepted inbound connections queued before [`Listening::accept`].
const ACCEPT_QUEUE: usize = 16;

/// Why the facade could not do what was asked. Never carries addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshError {
    /// Invalid registration or configuration.
    Config(&'static str),
    /// No backend meeting the policy and minimum standard is usable.
    Unavailable(Unavailable),
    /// The peer publishes no address on any usable backend.
    NoCommonNetwork,
    /// Every usable backend with an address for the peer failed, in order.
    Failed(Vec<(Scheme, ErrorKind)>),
    /// A backend or session error.
    Backend(Error),
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(reason) => write!(f, "configuration: {reason}"),
            Self::Unavailable(reason) => write!(f, "unavailable: {reason}"),
            Self::NoCommonNetwork => f.write_str("peer has no address on a usable network"),
            Self::Failed(attempts) => write!(f, "all {} attempts failed", attempts.len()),
            Self::Backend(error) => write!(f, "backend: {error}"),
        }
    }
}

impl std::error::Error for MeshError {}

impl From<Error> for MeshError {
    fn from(error: Error) -> Self {
        Self::Backend(error)
    }
}

/// Builds a [`Mesh`].
pub struct MeshBuilder {
    backends: Vec<Arc<dyn Backend>>,
    order: Option<Vec<Scheme>>,
    minimum: BTreeSet<Property>,
    policy: Policy,
    spawn: Arc<dyn Spawn>,
}

impl MeshBuilder {
    /// Register a backend. One backend per scheme.
    pub fn backend(mut self, backend: Arc<dyn Backend>) -> Self {
        self.backends.push(backend);
        self
    }

    /// Preference order by scheme (default: registration order). Backends
    /// missing from the order are never used.
    pub fn order(mut self, order: impl IntoIterator<Item = Scheme>) -> Self {
        self.order = Some(order.into_iter().collect());
        self
    }

    /// The operator's minimum standard ("set by us").
    pub fn minimum(mut self, minimum: impl IntoIterator<Item = Property>) -> Self {
        self.minimum = minimum.into_iter().collect();
        self
    }

    /// The consumer's connection policy (default: anonymous only).
    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    /// Validate and build.
    pub fn build(self) -> Result<Mesh, MeshError> {
        let mut candidates = Vec::with_capacity(self.backends.len());
        for backend in &self.backends {
            let capabilities = backend.capabilities();
            if capabilities.datagrams != backend.datagrams().is_some() {
                return Err(MeshError::Config(
                    "datagram capability disagrees with the datagram hook",
                ));
            }
            if capabilities.dht != backend.dht().is_some() {
                return Err(MeshError::Config(
                    "DHT capability disagrees with the DHT hook",
                ));
            }
            candidates.push(Candidate::new(backend.scheme(), properties(&capabilities)));
        }
        let order = self
            .order
            .unwrap_or_else(|| self.backends.iter().map(|b| b.scheme()).collect());
        let mut minimum = self.minimum;
        minimum.extend(self.policy.requirements().iter().copied());
        let fallback = Fallback::new(candidates, Config { order, minimum }).map_err(|error| {
            MeshError::Config(match error {
                cfbk::ConfigError::DuplicateCandidate(_) => "two backends serve one scheme",
                cfbk::ConfigError::DuplicateInOrder(_) => "scheme listed twice in the order",
                cfbk::ConfigError::UnknownInOrder(_) => "order names an unregistered scheme",
            })
        })?;
        Ok(Mesh {
            backends: self.backends,
            fallback: Mutex::new(fallback),
            switches: Mutex::new(Vec::new()),
            spawn: self.spawn,
        })
    }
}

/// Status of one registered backend, for surfacing in the product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendStatus {
    /// The backend's scheme.
    pub scheme: Scheme,
    /// Declared capabilities.
    pub capabilities: Capabilities,
    /// Reported health.
    pub health: Health,
    /// Whether it is in the order and meets policy and minimum standard.
    pub acceptable: bool,
    /// Required properties it lacks (empty when acceptable or unordered).
    pub missing: BTreeSet<Property>,
}

/// The P2P transport facade.
///
/// Selection rules: only backends that meet the consumer's [`Policy`] and the
/// operator's minimum standard are ever used; the configured order decides
/// among them (executed by `cfbk`); a connection uses exactly one network;
/// listening on every acceptable network at once is fine. A network-level
/// failure switches to the next acceptable backend and is recorded as a
/// [`Switch`] ([`Mesh::take_switches`]), so no fallback is silent.
pub struct Mesh {
    backends: Vec<Arc<dyn Backend>>,
    fallback: Mutex<Fallback<Scheme, Property>>,
    switches: Mutex<Vec<Switch<Scheme>>>,
    spawn: Arc<dyn Spawn>,
}

impl Mesh {
    /// Start building a mesh. `spawn` runs the facade's background futures
    /// (session drivers, listener pumps) on the embedding runtime.
    pub fn builder(spawn: Arc<dyn Spawn>) -> MeshBuilder {
        MeshBuilder {
            backends: Vec::new(),
            order: None,
            minimum: BTreeSet::new(),
            policy: Policy::default(),
            spawn,
        }
    }

    /// The backend a new connection would use first, if any.
    pub fn active(&self) -> Result<Scheme, Unavailable> {
        self.lock().select().cloned()
    }

    /// Per-backend status in registration order.
    pub fn status(&self) -> Vec<BackendStatus> {
        let fallback = self.lock();
        let excluded: Vec<(Scheme, BTreeSet<Property>)> = fallback
            .excluded()
            .map(|(scheme, missing)| (scheme.clone(), missing))
            .collect();
        self.backends
            .iter()
            .map(|backend| {
                let scheme = backend.scheme();
                BackendStatus {
                    capabilities: backend.capabilities(),
                    health: fallback.health(&scheme).unwrap_or(Health::Failed),
                    acceptable: fallback.is_acceptable(&scheme),
                    missing: excluded
                        .iter()
                        .find(|(excluded, _)| *excluded == scheme)
                        .map(|(_, missing)| missing.clone())
                        .unwrap_or_default(),
                    scheme,
                }
            })
            .collect()
    }

    /// What the mesh can offer right now: the combined capabilities of all
    /// usable backends (each feature uses the best backend offering it), or
    /// `None` when no backend is usable. This is what the product surfaces
    /// (for example "offline delivery available").
    pub fn offered(&self) -> Option<Capabilities> {
        let usable: Vec<Capabilities> = {
            let fallback = self.lock();
            fallback
                .usable()
                .filter_map(|scheme| self.backend(scheme))
                .map(|backend| backend.capabilities())
                .collect()
        };
        usable.into_iter().reduce(|a, b| Capabilities {
            anonymous: a.anonymous && b.anonymous,
            datagrams: a.datagrams || b.datagrams,
            offline_delivery: a.offline_delivery || b.offline_delivery,
            dht: a.dht || b.dht,
            latency: a.latency.min(b.latency),
        })
    }

    /// Record a health observation from outside (for example a stalled
    /// connection, or a successful probe after a failure).
    pub fn report(&self, scheme: &Scheme, health: Health) -> Result<(), MeshError> {
        let switch = self
            .lock()
            .report(scheme, health)
            .map_err(|_| MeshError::Config("unknown scheme"))?;
        self.journal(switch);
        Ok(())
    }

    /// Switches of the active backend since the last call, oldest first.
    pub fn take_switches(&self) -> Vec<Switch<Scheme>> {
        std::mem::take(&mut *self.switches.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Dial a peer given its published addresses (at most one per network).
    ///
    /// Tries the usable backends in order, one at a time, using the peer's
    /// address for that network. A network failure marks the backend failed
    /// and moves on; an unreachable peer moves on without touching health.
    pub async fn dial(&self, peer: &[Address]) -> Result<Connection, MeshError> {
        let candidates = self.usable_candidates(&BTreeSet::new())?;
        let mut attempts = Vec::new();
        for scheme in candidates {
            let Some(address) = peer.iter().find(|a| *a.scheme() == scheme) else {
                continue;
            };
            let Some(backend) = self.backend(&scheme) else {
                continue;
            };
            // Re-check: another task may have reported this backend failed.
            if !self.lock().usable().any(|usable| *usable == scheme) {
                continue;
            }
            match backend.dial(address).await {
                Ok(stream) => {
                    return Ok(self.connect(scheme, backend.capabilities(), stream, Role::Dialer));
                }
                Err(error) => {
                    if error.kind() == ErrorKind::Network {
                        self.report(&scheme, Health::Failed)?;
                    }
                    attempts.push((scheme, error.kind()));
                }
            }
        }
        if attempts.is_empty() {
            Err(MeshError::NoCommonNetwork)
        } else {
            Err(MeshError::Failed(attempts))
        }
    }

    /// Listen on every acceptable backend. Returns the addresses to publish
    /// and a handle that yields inbound connections from all of them.
    /// Backends whose listen fails with a network error are reported failed.
    pub async fn listen(&self) -> Result<Listening, MeshError> {
        let schemes: Vec<Scheme> = {
            let fallback = self.lock();
            fallback
                .order()
                .filter(|scheme| fallback.usable().any(|usable| usable == *scheme))
                .cloned()
                .collect()
        };
        let (sender, receiver) = mpsc::channel(ACCEPT_QUEUE);
        let mut addresses = Vec::new();
        let mut failures = Vec::new();
        let mut pumps = Vec::new();
        for scheme in schemes {
            let Some(backend) = self.backend(&scheme) else {
                continue;
            };
            match backend.listen().await {
                Ok(mut listener) => {
                    addresses.push(listener.address().clone());
                    let mut sender = sender.clone();
                    let capabilities = backend.capabilities();
                    let pump_scheme = scheme.clone();
                    let (abort, registration) = AbortHandle::new_pair();
                    pumps.push(abort);
                    self.spawn.spawn(Box::pin(async move {
                        let _ = Abortable::new(
                            async move {
                                loop {
                                    let event = match listener.accept().await {
                                        Ok(stream) => Accepted::Stream(
                                            pump_scheme.clone(),
                                            capabilities,
                                            stream,
                                        ),
                                        Err(error) => {
                                            Accepted::Closed(pump_scheme.clone(), error.kind())
                                        }
                                    };
                                    let closed = matches!(event, Accepted::Closed(..));
                                    if futures::SinkExt::send(&mut sender, event).await.is_err()
                                        || closed
                                    {
                                        return;
                                    }
                                }
                            },
                            registration,
                        )
                        .await;
                    }));
                }
                Err(error) => {
                    if error.kind() == ErrorKind::Network {
                        self.report(&scheme, Health::Failed)?;
                    }
                    failures.push((scheme, error.kind()));
                }
            }
        }
        if addresses.is_empty() {
            return Err(if failures.is_empty() {
                MeshError::Unavailable(
                    self.active()
                        .err()
                        .unwrap_or(Unavailable::NoCandidateMeetsStandard),
                )
            } else {
                MeshError::Failed(failures)
            });
        }
        Ok(Listening {
            addresses,
            receiver: AsyncMutex::new(receiver),
            spawn: self.spawn.clone(),
            pumps,
        })
    }

    /// Datagrams over the first usable backend that offers them.
    pub fn datagrams(&self) -> Result<DatagramPort, MeshError> {
        let backend = self.select_with(Property::Datagrams)?;
        Ok(DatagramPort { backend })
    }

    /// The DHT capability of the first usable backend that offers it. This is
    /// how `cdht` reaches a network DHT without skipping the facade.
    pub fn dht(&self) -> Result<DhtPort, MeshError> {
        let backend = self.select_with(Property::Dht)?;
        Ok(DhtPort { backend })
    }

    fn select_with(&self, property: Property) -> Result<Arc<dyn Backend>, MeshError> {
        let scheme = self
            .lock()
            .select_meeting(&BTreeSet::from([property]))
            .cloned()
            .map_err(MeshError::Unavailable)?;
        self.backend(&scheme)
            .ok_or(MeshError::Config("selected backend missing"))
    }

    fn usable_candidates(&self, extra: &BTreeSet<Property>) -> Result<Vec<Scheme>, MeshError> {
        let fallback = self.lock();
        let usable: Vec<Scheme> = fallback.usable_meeting(extra).cloned().collect();
        if usable.is_empty() {
            let reason = fallback
                .select_meeting(extra)
                .err()
                .unwrap_or(Unavailable::AllAcceptableFailed);
            return Err(MeshError::Unavailable(reason));
        }
        Ok(usable)
    }

    fn connect(
        &self,
        scheme: Scheme,
        capabilities: Capabilities,
        stream: BoxStream,
        role: Role,
    ) -> Connection {
        connect(&*self.spawn, scheme, capabilities, stream, role)
    }

    fn backend(&self, scheme: &Scheme) -> Option<Arc<dyn Backend>> {
        self.backends
            .iter()
            .find(|backend| backend.scheme() == *scheme)
            .cloned()
    }

    fn journal(&self, switch: Option<Switch<Scheme>>) {
        if let Some(switch) = switch {
            let mut switches = self.switches.lock().unwrap_or_else(|e| e.into_inner());
            if switches.len() == SWITCH_JOURNAL {
                switches.remove(0);
            }
            switches.push(switch);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Fallback<Scheme, Property>> {
        self.fallback.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn connect(
    spawn: &dyn Spawn,
    scheme: Scheme,
    capabilities: Capabilities,
    stream: BoxStream,
    role: Role,
) -> Connection {
    let (session, driver) = Session::new(stream, role);
    spawn.spawn(driver);
    Connection {
        scheme,
        capabilities,
        session,
    }
}

enum Accepted {
    Stream(Scheme, Capabilities, BoxStream),
    Closed(Scheme, ErrorKind),
}

/// A multiplexed connection to one peer over exactly one network.
#[derive(Clone)]
pub struct Connection {
    scheme: Scheme,
    capabilities: Capabilities,
    session: Session,
}

impl Connection {
    /// The network this connection uses.
    pub fn backend(&self) -> &Scheme {
        &self.scheme
    }

    /// That network's capabilities.
    pub fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    /// Open a substream.
    pub async fn open(&self) -> Result<Substream, Error> {
        self.session.open().await
    }

    /// Accept a substream opened by the peer.
    pub async fn accept(&self) -> Result<Substream, Error> {
        self.session.accept().await
    }

    /// Terminal event once across cloned connections. Never a delivery receipt.
    pub async fn next_event(&self) -> Option<crate::SessionEvent> {
        self.session.next_event().await
    }

    /// Close the connection (all substreams).
    pub async fn close(&self) -> Result<(), Error> {
        self.session.close().await
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connection({})", self.scheme)
    }
}

/// What a listening mesh yields.
#[derive(Debug)]
pub enum Incoming {
    /// A new inbound connection.
    Connection(Connection),
    /// A backend's listener is gone (for example its route died); its address
    /// is no longer valid and must be replaced by listening again.
    ListenerClosed(Scheme, ErrorKind),
}

/// Inbound connections from every backend the mesh listens on.
pub struct Listening {
    addresses: Vec<Address>,
    receiver: AsyncMutex<mpsc::Receiver<Accepted>>,
    spawn: Arc<dyn Spawn>,
    pumps: Vec<AbortHandle>,
}

impl Drop for Listening {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
    }
}

impl Listening {
    /// Stop every owned listener pump and withdraw its reachability.
    pub fn close(&mut self) {
        for pump in self.pumps.drain(..) {
            pump.abort();
        }
        self.addresses.clear();
        self.receiver.get_mut().close();
    }
    /// The addresses to publish (one per network, in preference order).
    pub fn addresses(&self) -> &[Address] {
        &self.addresses
    }

    /// Wait for the next inbound connection or listener event. Returns
    /// `None` when every listener has closed.
    pub async fn accept(&self) -> Option<Incoming> {
        let event = self.receiver.lock().await.next().await?;
        Some(match event {
            Accepted::Stream(scheme, capabilities, stream) => Incoming::Connection(connect(
                &*self.spawn,
                scheme,
                capabilities,
                stream,
                Role::Listener,
            )),
            Accepted::Closed(scheme, kind) => Incoming::ListenerClosed(scheme, kind),
        })
    }
}

/// Datagrams through one selected backend.
pub struct DatagramPort {
    backend: Arc<dyn Backend>,
}

impl DatagramPort {
    /// The network in use.
    pub fn backend(&self) -> Scheme {
        self.backend.scheme()
    }

    /// Largest payload.
    pub fn max_payload(&self) -> usize {
        self.hook().map(|hook| hook.max_payload()).unwrap_or(0)
    }

    /// Send to the peer's address on this network.
    pub async fn send(&self, peer: &[Address], payload: &[u8]) -> Result<(), MeshError> {
        let scheme = self.backend.scheme();
        let address = peer
            .iter()
            .find(|a| *a.scheme() == scheme)
            .ok_or(MeshError::NoCommonNetwork)?;
        Ok(self.hook()?.send(address, payload).await?)
    }

    /// Receive the next datagram (the sender is anonymous).
    pub async fn receive(&self) -> Result<Vec<u8>, MeshError> {
        Ok(self.hook()?.receive().await?)
    }

    fn hook(&self) -> Result<&dyn cmsh_api::Datagrams, MeshError> {
        self.backend
            .datagrams()
            .ok_or(MeshError::Config("datagram hook missing"))
    }
}

/// The DHT capability of one selected backend.
pub struct DhtPort {
    backend: Arc<dyn Backend>,
}

impl DhtPort {
    /// The network providing the DHT.
    pub fn backend(&self) -> Scheme {
        self.backend.scheme()
    }

    /// The DHT operations.
    pub fn dht(&self) -> &dyn Dht {
        // Registration verified that the hook exists whenever `dht` is declared.
        self.backend
            .dht()
            .expect("DHT hook verified at registration")
    }
}
