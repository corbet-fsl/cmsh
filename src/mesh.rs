use crate::{
    Address, Backend, Capabilities, Error, ErrorKind, Event, Policy, Property, Reply, Scheme,
    Spawn, properties,
};
use cfbk::{Candidate, Config, Fallback, Health, Switch, Unavailable};
use futures::{
    SinkExt, StreamExt,
    channel::mpsc,
    future::{AbortHandle, Abortable},
    lock::Mutex as AsyncMutex,
};
use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex},
};

/// Coarse facade failures without payloads or peer identifiers.
#[derive(Debug)]
pub enum MeshError {
    /// Invalid static configuration.
    Config(&'static str),
    /// No backend satisfies current policy and health requirements.
    Unavailable(Unavailable),
    /// The peer offers no address on an acceptable network.
    NoCommonNetwork,
    /// Selected transport failed; the payload is never retried on another network.
    Backend(Error),
}
impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mesh: {self:?}")
    }
}
impl std::error::Error for MeshError {}
impl From<Error> for MeshError {
    fn from(error: Error) -> Self {
        Self::Backend(error)
    }
}
/// Explicit backend and guarantee configuration.
pub struct MeshBuilder {
    backends: Vec<Arc<dyn Backend>>,
    order: Option<Vec<Scheme>>,
    minimum: BTreeSet<Property>,
    policy: Policy,
    spawn: Arc<dyn Spawn>,
}
impl MeshBuilder {
    /// Register a backend once for its network.
    pub fn backend(mut self, backend: Arc<dyn Backend>) -> Self {
        self.backends.push(backend);
        self
    }
    /// Set explicit network preference. Unlisted backends are not used.
    pub fn order(mut self, order: impl IntoIterator<Item = Scheme>) -> Self {
        self.order = Some(order.into_iter().collect());
        self
    }
    /// Set minimum required guarantees.
    pub fn minimum(mut self, minimum: impl IntoIterator<Item = Property>) -> Self {
        self.minimum = minimum.into_iter().collect();
        self
    }
    /// Add consumer requirements; anonymity always remains mandatory.
    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }
    /// Validate ports and delegate all network selection to Fallback.
    pub fn build(self) -> Result<Mesh, MeshError> {
        let mut candidates = Vec::new();
        for backend in &self.backends {
            if !(1..=1024 * 1024).contains(&backend.max_payload()) {
                return Err(MeshError::Config("invalid message bound"));
            }
            let caps = backend.capabilities();
            // These separate ports must be integrated and tested before advertisement.
            if caps.datagrams || caps.dht || caps.offline_delivery {
                return Err(MeshError::Config("unqualified optional capability"));
            }
            candidates.push(Candidate::new(backend.scheme(), properties(&caps)));
        }
        let order = self
            .order
            .unwrap_or_else(|| self.backends.iter().map(|b| b.scheme()).collect());
        let mut minimum = self.minimum;
        minimum.extend(self.policy.requirements());
        minimum.insert(Property::Anonymous);
        let fallback = Fallback::new(candidates, Config { order, minimum })
            .map_err(|_| MeshError::Config("invalid backend order"))?;
        Ok(Mesh {
            backends: self.backends,
            fallback: Mutex::new(fallback),
            switches: Mutex::new(Vec::new()),
            spawn: self.spawn,
        })
    }
}
/// One adapter's measured capability and current selection eligibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendStatus {
    /// Opaque network discriminator.
    pub scheme: Scheme,
    /// Implemented adapter guarantees.
    pub capabilities: Capabilities,
    /// Reported coarse network health.
    pub health: Health,
    /// Meets all configured requirements.
    pub acceptable: bool,
    /// Required guarantees absent from the backend.
    pub missing: BTreeSet<Property>,
}
/// Thin anonymous-message facade; no framing, storage, keys or retry machine.
pub struct Mesh {
    backends: Vec<Arc<dyn Backend>>,
    fallback: Mutex<Fallback<Scheme, Property>>,
    switches: Mutex<Vec<Switch<Scheme>>>,
    spawn: Arc<dyn Spawn>,
}
impl Mesh {
    /// Supply the executor for bounded listener pumps.
    pub fn builder(spawn: Arc<dyn Spawn>) -> MeshBuilder {
        MeshBuilder {
            backends: Vec::new(),
            order: None,
            minimum: BTreeSet::new(),
            policy: Policy::default(),
            spawn,
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, Fallback<Scheme, Property>> {
        self.fallback.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn backend(&self, scheme: &Scheme) -> Option<Arc<dyn Backend>> {
        self.backends
            .iter()
            .find(|b| b.scheme() == *scheme)
            .cloned()
    }
    /// First qualified healthy network, as selected by Fallback.
    pub fn active(&self) -> Result<Scheme, Unavailable> {
        self.lock().select().cloned()
    }
    /// Per-backend status; no union of incompatible network guarantees.
    pub fn status(&self) -> Vec<BackendStatus> {
        let fallback = self.lock();
        self.backends
            .iter()
            .map(|b| {
                let scheme = b.scheme();
                let missing = fallback
                    .excluded()
                    .find(|(s, _)| **s == scheme)
                    .map(|(_, m)| m.clone())
                    .unwrap_or_default();
                BackendStatus {
                    health: fallback.health(&scheme).unwrap_or(Health::Failed),
                    acceptable: fallback.is_acceptable(&scheme),
                    scheme,
                    capabilities: b.capabilities(),
                    missing,
                }
            })
            .collect()
    }
    /// Guarantees of the currently selected backend only.
    pub fn offered(&self) -> Option<Capabilities> {
        self.active()
            .ok()
            .and_then(|s| self.backend(&s))
            .map(|b| b.capabilities())
    }
    /// Report health; switches are surfaced explicitly to the caller.
    pub fn report(&self, scheme: &Scheme, health: Health) -> Result<(), MeshError> {
        let switch = self
            .lock()
            .report(scheme, health)
            .map_err(|_| MeshError::Config("unknown network"))?;
        if let Some(switch) = switch {
            let mut log = self.switches.lock().unwrap_or_else(|e| e.into_inner());
            if log.len() == 64 {
                log.remove(0);
            }
            log.push(switch);
        }
        Ok(())
    }
    /// Drain observable network switches in order.
    pub fn take_switches(&self) -> Vec<Switch<Scheme>> {
        std::mem::take(&mut *self.switches.lock().unwrap_or_else(|e| e.into_inner()))
    }
    fn select(
        &self,
        peers: &[Address],
        size: usize,
    ) -> Result<(Arc<dyn Backend>, Address), MeshError> {
        let candidates: Vec<Scheme> = self.lock().usable().cloned().collect();
        if candidates.is_empty() {
            return Err(MeshError::Unavailable(
                self.active()
                    .err()
                    .unwrap_or(Unavailable::AllAcceptableFailed),
            ));
        }
        for scheme in candidates {
            if let Some(address) = peers.iter().find(|a| *a.scheme() == scheme) {
                let backend = self
                    .backend(&scheme)
                    .ok_or(MeshError::Config("missing backend"))?;
                if size > backend.max_payload() {
                    return Err(Error::new(ErrorKind::Limit, "message size out of bounds").into());
                }
                return Ok((backend, address.clone()));
            }
        }
        Err(MeshError::NoCommonNetwork)
    }
    fn outcome<T>(&self, scheme: &Scheme, result: Result<T, Error>) -> Result<T, MeshError> {
        if result
            .as_ref()
            .is_err_and(|e| e.kind() == ErrorKind::Network)
        {
            self.report(scheme, Health::Failed)?;
        }
        result.map_err(Into::into)
    }
    /// Submit one bounded message; never replay an uncertain send across networks.
    pub async fn app_message(&self, peer: &[Address], payload: &[u8]) -> Result<(), MeshError> {
        let (backend, address) = self.select(peer, payload.len())?;
        self.outcome(
            &backend.scheme(),
            backend.app_message(&address, payload).await,
        )
    }
    /// Submit one bounded call and check the original backend's reply bound.
    pub async fn app_call(&self, peer: &[Address], payload: &[u8]) -> Result<Vec<u8>, MeshError> {
        let (backend, address) = self.select(peer, payload.len())?;
        let reply = self.outcome(&backend.scheme(), backend.app_call(&address, payload).await)?;
        if reply.len() > backend.max_payload() {
            return Err(Error::new(ErrorKind::Protocol, "oversized reply").into());
        }
        Ok(reply)
    }
    /// Publish acceptable networks and yield only bounded complete payloads.
    pub async fn listen(&self) -> Result<Listening, MeshError> {
        let schemes: Vec<Scheme> = self.lock().usable().cloned().collect();
        let (sender, receiver) = mpsc::channel(16);
        let mut listening = Listening {
            addresses: Vec::new(),
            receiver: AsyncMutex::new(receiver),
            pumps: Vec::new(),
        };
        for scheme in schemes {
            let backend = self
                .backend(&scheme)
                .ok_or(MeshError::Config("missing backend"))?;
            let listener = self.outcome(&scheme, backend.listen().await)?;
            if listener.address().scheme() != &scheme {
                return Err(MeshError::Config("listener network mismatch"));
            }
            listening.addresses.push(listener.address().clone());
            let maximum = backend.max_payload();
            let sender = sender.clone();
            let (abort, registration) = AbortHandle::new_pair();
            listening.pumps.push(abort);
            self.spawn.spawn(Box::pin(async move {
                let _ =
                    Abortable::new(forward(listener, scheme, maximum, sender), registration).await;
            }));
        }
        if listening.addresses.is_empty() {
            return Err(MeshError::Unavailable(
                self.active()
                    .err()
                    .unwrap_or(Unavailable::AllAcceptableFailed),
            ));
        }
        Ok(listening)
    }
}
async fn forward(
    mut listener: Box<dyn crate::Listener>,
    scheme: Scheme,
    maximum: usize,
    mut sender: mpsc::Sender<Incoming>,
) {
    loop {
        let event = match listener.next().await {
            Ok(Event::Message(payload)) if payload.len() <= maximum => Incoming::Message {
                backend: scheme.clone(),
                payload,
            },
            Ok(Event::Call { payload, reply }) if payload.len() <= maximum => Incoming::Call {
                backend: scheme.clone(),
                payload,
                reply: reply.limit_to(maximum),
            },
            Ok(_) => Incoming::ListenerClosed(scheme.clone(), ErrorKind::Protocol),
            Err(error) => Incoming::ListenerClosed(scheme.clone(), error.kind()),
        };
        let closed = matches!(event, Incoming::ListenerClosed(..));
        if sender.send(event).await.is_err() || closed {
            break;
        }
    }
}

/// Message events reveal no caller identity; owners authenticate opaque payloads.
pub enum Incoming {
    /// Complete one-way message.
    Message {
        /// Selected network.
        backend: Scheme,
        /// Opaque bytes.
        payload: Vec<u8>,
    },
    /// Complete call and its one-use reply.
    Call {
        /// Selected network.
        backend: Scheme,
        /// Opaque bytes.
        payload: Vec<u8>,
        /// Network-bound reply.
        reply: Reply,
    },
    /// Reachability was lost; this address must be withdrawn.
    ListenerClosed(Scheme, ErrorKind),
}
/// Bounded listener handle; dropping it stops all owned receive pumps.
pub struct Listening {
    addresses: Vec<Address>,
    receiver: AsyncMutex<mpsc::Receiver<Incoming>>,
    pumps: Vec<AbortHandle>,
}
impl Listening {
    /// Initially published addresses. Withdraw a network's address when its
    /// [`Incoming::ListenerClosed`] event arrives; `close` clears the whole list.
    pub fn addresses(&self) -> &[Address] {
        &self.addresses
    }
    /// Next complete payload or one terminal event for a failed listener.
    pub async fn next(&self) -> Option<Incoming> {
        self.receiver.lock().await.next().await
    }
    /// Stop pumps and withdraw reachability immediately.
    pub fn close(&mut self) {
        for p in self.pumps.drain(..) {
            p.abort();
        }
        self.addresses.clear();
        self.receiver.get_mut().close();
    }
}
impl Drop for Listening {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "../tests/support/pump.rs"]
mod pump_tests;
