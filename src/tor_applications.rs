//! Mesh application delivery on the same original authenticated Tor RPC listener.
use crate::tor_discovery::{CallKind, TorDiscovery, TorNodeIdentity, TorWatcher};
use crate::tor_records::TorRecords;
use crate::{
    Address, Backend, Capabilities, ErrorKind, Event, LatencyClass, Listener, Reply, ReplyPort,
    Scheme,
};
use cdht::rpc::envelope::AuthenticatedOperation;
use cdht::{
    Error, Verifier,
    rpc::{Answer, Application, OperationKind, Question, Statement, application, operation_kind},
};
use futures::{StreamExt, channel::mpsc};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

fn refused() -> crate::Error {
    crate::Error::new(
        ErrorKind::Closed,
        "authenticated Tor application unavailable",
    )
}
fn scheme() -> Scheme {
    Scheme::new("tor").expect("constant scheme")
}

/// Application adapter over the same discovery and onion listener as Records.
/// The bounded queue carries opaque application bytes; node authentication is
/// never member authority. Consumers must verify the original application data.
pub struct TorApplications<I, V> {
    discovery: Arc<TorDiscovery<I, V>>,
    address: Address,
    maximum: usize,
    sender: Mutex<Option<mpsc::Sender<Event>>>,
    receiver: Mutex<Option<mpsc::Receiver<Event>>>,
    closed: Arc<AtomicBool>,
}
impl<I, V> Drop for TorApplications<I, V> {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl<I: TorNodeIdentity + 'static, V: Verifier + crate::MaybeSend + crate::MaybeSync + 'static>
    TorApplications<I, V>
{
    /// Explicit queue/payload budgets. The conservative 32 KiB payload ceiling
    /// leaves space for original RPC framing and the signed sender advertisement.
    /// Construction does not publish a second onion service or grant readiness.
    pub fn new(
        discovery: Arc<TorDiscovery<I, V>>,
        capacity: NonZeroUsize,
        maximum: NonZeroUsize,
    ) -> Result<Self, Error> {
        if maximum.get() > 32 * 1024 || capacity.get() > 1024 {
            return Err(Error::ValueTooLarge);
        }
        let address = Address::new(scheme(), discovery.endpoint()?.bytes().to_vec())
            .map_err(|_| Error::Encoding)?;
        // futures mpsc gives this single retained sender one additional slot.
        // Never clone it: capacity is the complete queue bound, including that slot.
        let (sender, receiver) = mpsc::channel(capacity.get() - 1);
        Ok(Self {
            discovery,
            address,
            maximum: maximum.get(),
            sender: Mutex::new(Some(sender)),
            receiver: Mutex::new(Some(receiver)),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }
    pub(crate) fn discovery(&self) -> &Arc<TorDiscovery<I, V>> {
        &self.discovery
    }
    pub(crate) fn lease(&self) -> ApplicationLease<'_, I, V> {
        ApplicationLease(self)
    }
    /// Stop delivery and invalidate every outstanding application reply.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        if let Ok(mut receiver) = self.receiver.lock() {
            receiver.take();
        }
    }
    fn live(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::Unavailable)
        } else {
            Ok(())
        }
    }
    fn enqueue(&self, event: Event) -> Result<(), Error> {
        self.live()?;
        self.sender
            .lock()
            .map_err(|_| Error::Unavailable)?
            .as_mut()
            .ok_or(Error::Unavailable)?
            .try_send(event)
            .map_err(|_| Error::Unavailable)
    }
    fn destination(&self, address: &Address, payload: &[u8]) -> Result<cdht::NodeId, crate::Error> {
        self.live().map_err(|_| refused())?;
        if payload.len() > self.maximum {
            return Err(crate::Error::new(
                ErrorKind::Limit,
                "application payload exceeds limit",
            ));
        }
        let endpoint = address.expect_scheme(&scheme())?;
        // Only an already authenticated advertisement may bind an onion address
        // to its ENV0 node key. Unknown or ambiguous destinations refuse.
        self.discovery
            .node_for_endpoint(endpoint)
            .map_err(|_| refused())
    }
    pub(crate) async fn receive<W: TorWatcher>(
        &self,
        records: &TorRecords<I, V, W>,
        incoming: ctrn::Received,
    ) -> Result<(), Error> {
        match incoming {
            ctrn::Received::Call { payload, reply } => {
                let received = self.discovery.authenticate(payload).await?;
                match operation_kind(received.operation())? {
                    OperationKind::RecordsQuestion => {
                        let answer = records.handle_authenticated_call(received).await?;
                        reply.send(&answer).await.map_err(|_| Error::Unavailable)
                    }
                    OperationKind::ApplicationCall => {
                        self.live()?;
                        if received.operation().signer() != received.sender() {
                            return Err(Error::BadSignature);
                        }
                        let (_, Application::Call(payload)) = application(received.operation())?
                        else {
                            return Err(Error::Encoding);
                        };
                        if payload.len() > self.maximum {
                            return Err(Error::ValueTooLarge);
                        }
                        // A dial-only (sender-less) caller is refused: it may only
                        // look up, read and write records.
                        self.discovery
                            .admit_caller(&received, CallKind::Application)?;
                        let port = ApplicationReply {
                            discovery: self.discovery.clone(),
                            received,
                            reply,
                            closed: self.closed.clone(),
                        };
                        self.enqueue(Event::Call {
                            payload,
                            reply: Reply::new(Box::new(port), self.maximum),
                        })
                    }
                    _ => Err(Error::Encoding),
                }
            }
            ctrn::Received::Message(payload) => {
                let received = self.discovery.authenticate(payload).await?;
                match operation_kind(received.operation())? {
                    OperationKind::RecordsStatement => {
                        records.handle_authenticated_statement(received)
                    }
                    OperationKind::ApplicationMessage => {
                        self.live()?;
                        if received.operation().signer() != received.sender() {
                            return Err(Error::BadSignature);
                        }
                        let (_, Application::Message(payload)) = application(received.operation())?
                        else {
                            return Err(Error::Encoding);
                        };
                        if payload.len() > self.maximum {
                            return Err(Error::ValueTooLarge);
                        }
                        // One-way statements need no routing update or inferred
                        // peer advertisement; only the authenticated payload moves.
                        self.enqueue(Event::Message(payload))
                    }
                    _ => Err(Error::Encoding),
                }
            }
        }
    }
}

pub(crate) struct ApplicationLease<
    'a,
    I: TorNodeIdentity + 'static,
    V: Verifier + crate::MaybeSend + crate::MaybeSync + 'static,
>(&'a TorApplications<I, V>);
impl<I: TorNodeIdentity + 'static, V: Verifier + crate::MaybeSend + crate::MaybeSync + 'static> Drop
    for ApplicationLease<'_, I, V>
{
    fn drop(&mut self) {
        self.0.close();
    }
}

#[cfg_attr(not(target_arch = "wasm32"), crate::async_trait)]
#[cfg_attr(target_arch = "wasm32", crate::async_trait(?Send))]
impl<I: TorNodeIdentity + 'static, V: Verifier + crate::MaybeSend + crate::MaybeSync + 'static>
    Backend for TorApplications<I, V>
{
    fn scheme(&self) -> Scheme {
        scheme()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            anonymous: true,
            datagrams: false,
            offline_delivery: false,
            dht: false,
            latency: LatencyClass::Interactive,
        }
    }
    fn max_payload(&self) -> usize {
        self.maximum
    }
    async fn listen(&self) -> Result<Box<dyn Listener>, crate::Error> {
        self.live().map_err(|_| refused())?;
        let receiver = self
            .receiver
            .lock()
            .map_err(|_| refused())?
            .take()
            .ok_or_else(refused)?;
        Ok(Box::new(ApplicationListener {
            address: self.address.clone(),
            receiver,
            closed: self.closed.clone(),
        }))
    }
    async fn app_call(&self, to: &Address, payload: &[u8]) -> Result<Vec<u8>, crate::Error> {
        let target = self.destination(to, payload)?;
        let question = self
            .discovery
            .with_sender(Question::application(payload).map_err(|_| refused())?)
            .map_err(|_| refused())?;
        let received = self
            .discovery
            .call_record(&target, &question)
            .await
            .map_err(|_| refused())?;
        self.live().map_err(|_| refused())?;
        let response = question
            .application_answer(received.operation())
            .map_err(|_| refused())?;
        if response.len() > self.maximum {
            return Err(refused());
        }
        Ok(response)
    }
    async fn app_message(&self, to: &Address, payload: &[u8]) -> Result<(), crate::Error> {
        let target = self.destination(to, payload)?;
        let statement = Statement::application(payload).map_err(|_| refused())?;
        self.discovery
            .send_statement(&statement, &target)
            .await
            .map_err(|_| refused())?;
        self.live().map_err(|_| refused())
    }
}

struct ApplicationListener {
    address: Address,
    receiver: mpsc::Receiver<Event>,
    closed: Arc<AtomicBool>,
}
impl Drop for ApplicationListener {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.receiver.close();
    }
}
#[cfg_attr(not(target_arch = "wasm32"), crate::async_trait)]
#[cfg_attr(target_arch = "wasm32", crate::async_trait(?Send))]
impl Listener for ApplicationListener {
    fn address(&self) -> &Address {
        &self.address
    }
    async fn next(&mut self) -> Result<Event, crate::Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(refused());
        }
        let event = self.receiver.next().await.ok_or_else(refused)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(refused());
        }
        Ok(event)
    }
}
struct ApplicationReply<I, V> {
    discovery: Arc<TorDiscovery<I, V>>,
    received: AuthenticatedOperation,
    reply: ctrn::Reply,
    closed: Arc<AtomicBool>,
}
#[cfg_attr(not(target_arch = "wasm32"), crate::async_trait)]
#[cfg_attr(target_arch = "wasm32", crate::async_trait(?Send))]
impl<I: TorNodeIdentity + 'static, V: Verifier + crate::MaybeSend + crate::MaybeSync + 'static>
    ReplyPort for ApplicationReply<I, V>
{
    async fn send(self: Box<Self>, payload: &[u8]) -> Result<(), crate::Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(refused());
        }
        let answer =
            Answer::application(self.received.operation(), payload).map_err(|_| refused())?;
        let sealed = self
            .discovery
            .reply_record(&answer, self.received.sender())
            .await
            .map_err(|_| refused())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(refused());
        }
        self.reply.send(&sealed).await.map_err(|_| refused())
    }
}
