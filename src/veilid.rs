//! Native Mesh forwarding adapter. Adoption waits for cvln qualification.
use crate::{
    Address, Backend, Capabilities, Error, ErrorKind, Event, LatencyClass, Listener, Reply,
    ReplyPort, Scheme,
};
use async_trait::async_trait;

/// Shares the CLI's sole Veilid runtime; construction does not start another node.
pub struct Veilid(pub cvln::Messages);

fn scheme() -> Scheme {
    Scheme::new("veilid").expect("constant network")
}
fn failure(error: cvln::Error) -> Error {
    let kind = match error {
        cvln::Error::Unsupported => ErrorKind::Unsupported,
        cvln::Error::AlreadyStarted | cvln::Error::Unavailable => ErrorKind::Network,
        cvln::Error::Invalid => ErrorKind::InvalidAddress,
        cvln::Error::Limit => ErrorKind::Limit,
        cvln::Error::Closed => ErrorKind::Closed,
        cvln::Error::Timeout => ErrorKind::Timeout,
    };
    Error::new(kind, "Veilid message transport failed")
}
#[async_trait]
impl Backend for Veilid {
    fn scheme(&self) -> Scheme {
        scheme()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            anonymous: true, // Both safety and private routes; see adoption gate.
            datagrams: false,
            offline_delivery: false,
            dht: false, // Whole-record candidate awaits network qualification.
            latency: LatencyClass::Interactive,
        }
    }
    fn max_payload(&self) -> usize {
        self.0.max_payload()
    }
    async fn listen(&self) -> Result<Box<dyn Listener>, Error> {
        let inner = self.0.listen().await.map_err(failure)?;
        let address = Address::new(scheme(), inner.address().to_vec())?;
        Ok(Box::new(VeilidListener { inner, address }))
    }
    async fn app_message(&self, to: &Address, payload: &[u8]) -> Result<(), Error> {
        self.0
            .app_message(to.expect_scheme(&scheme())?, payload)
            .await
            .map_err(failure)
    }
    async fn app_call(&self, to: &Address, payload: &[u8]) -> Result<Vec<u8>, Error> {
        self.0
            .app_call(to.expect_scheme(&scheme())?, payload)
            .await
            .map_err(failure)
    }
}
struct VeilidListener {
    inner: cvln::Listener,
    address: Address,
}
#[async_trait]
impl Listener for VeilidListener {
    fn address(&self) -> &Address {
        &self.address
    }
    async fn next(&mut self) -> Result<Event, Error> {
        match self.inner.next().await.map_err(failure)? {
            cvln::Incoming::Message(payload) => Ok(Event::Message(payload)),
            cvln::Incoming::Call { payload, reply } => Ok(Event::Call {
                payload,
                reply: Reply::new(Box::new(VeilidReply(reply)), cvln::MAX_PAYLOAD),
            }),
        }
    }
}
struct VeilidReply(cvln::Reply);
#[async_trait]
impl ReplyPort for VeilidReply {
    async fn send(self: Box<Self>, payload: &[u8]) -> Result<(), Error> {
        self.0.send(payload).await.map_err(failure)
    }
}

/// Bounds for the native message runtime.
pub use cvln::Limits as VeilidLimits;
/// One-use native record initializer; constructed only inside the scoped key owner.
pub use cvln::RecordInitializer as VeilidRecordInitializer;
/// Upstream native configuration; no alternate network stack or wire format.
pub use cvln::core::VeilidConfig;

/// Mesh owns construction of the native transport. The product calls this facade,
/// keeps it alive, and never opens a second Veilid instance in Records or the CLI.
pub struct VeilidRuntime(cvln::Node);
impl VeilidRuntime {
    /// Prepare the sole native runtime without attaching. Restore owners may now
    /// initialize their record capabilities and stage original semantic values.
    /// Readiness remains the actual core state; call `attach` when staging ends.
    pub async fn prepare(config: VeilidConfig, limits: VeilidLimits) -> Result<Self, Error> {
        Ok(Self(
            cvln::Node::start(config, limits).await.map_err(failure)?,
        ))
    }
    /// End startup staging permanently and await actual core attachment.
    /// A failed or cancelled attempt never re-enables offline republication.
    pub async fn attach(&self) -> Result<(), Error> {
        self.0.attach().await.map_err(failure)
    }
    /// Prepare and attach immediately when no startup record restore is needed.
    /// Failure shuts down the partial node.
    pub async fn start(config: VeilidConfig, limits: VeilidLimits) -> Result<Self, Error> {
        let runtime = Self::prepare(config, limits).await?;
        runtime.attach().await?;
        Ok(runtime)
    }
    /// Measure readiness from the actual native runtime, never caller-supplied state.
    pub async fn ready(&self) -> Result<bool, Error> {
        Ok(self
            .0
            .attachment()
            .await
            .map_err(failure)?
            .public_internet_ready)
    }
    /// Register this message port in the ordinary Mesh builder.
    pub fn messages(&self) -> Result<Veilid, Error> {
        Ok(Veilid(self.0.messages().map_err(failure)?))
    }
    /// Whole-record factory sharing exactly the same native core as messages.
    pub fn records(&self, maximum: std::num::NonZeroUsize) -> Result<VeilidRecords, Error> {
        Ok(VeilidRecords(
            self.0.record_backend(maximum).map_err(failure)?,
        ))
    }
    /// Shut down the owned core, invalidating every borrowed message/record port.
    pub async fn shutdown(self) {
        self.0.shutdown().await;
    }
}

/// Whole-record factory; Keys supplies only record-scoped native capabilities.
pub struct VeilidRecords(cvln::Records);
impl VeilidRecords {
    /// Consume an opaque scoped initializer into the shared actual native core.
    /// Keys owns its construction; Mesh and Records never obtain key getters.
    pub async fn initialize(
        &self,
        initializer: VeilidRecordInitializer,
    ) -> Result<cdht::runtime::Record<VeilidRecord>, cdht::Error> {
        bind_record(
            self.0
                .initialize(initializer)
                .await
                .map_err(record_failure)?,
        )
    }
}
/// Forwarding implementation of Records' authenticated-runtime boundary.
/// Authentication, encryption, routing and persistence remain in actual core.
pub struct VeilidRecord {
    inner: cvln::Record,
    identity: cdht::runtime::Identity,
}
fn public_key(key: cvln::core::PublicKey) -> Result<[u8; 32], cdht::Error> {
    key.ref_value()
        .to_vec()
        .try_into()
        .map_err(|_| cdht::Error::Encoding)
}
fn bind_record(inner: cvln::Record) -> Result<cdht::runtime::Record<VeilidRecord>, cdht::Error> {
    let identity = cdht::runtime::Identity {
        key: inner.key().to_string().parse()?,
        owner: public_key(inner.owner())?,
        schema: cdht::Schema::parse(&inner.schema().compile())?,
        writer: inner.writer().map(public_key).transpose()?,
    };
    cdht::runtime::Record::new(VeilidRecord { inner, identity })
}
fn record_failure(error: cvln::Error) -> cdht::Error {
    match error {
        cvln::Error::Invalid => cdht::Error::Encoding,
        cvln::Error::Unsupported => cdht::Error::WrongWriter,
        _ => cdht::Error::Unavailable,
    }
}
fn value(value: cvln::core::ValueData) -> Result<cdht::runtime::Value, cdht::Error> {
    Ok(cdht::runtime::Value {
        seq: u32::from(value.seq()),
        writer: public_key(value.writer())?,
        data: value.data().to_vec().into(),
    })
}
impl VeilidRecord {
    /// Close upstream before releasing this record's runtime reservation.
    pub async fn close(self) -> Result<(), cdht::Error> {
        self.inner.close().await.map_err(record_failure)
    }
}
impl cdht::runtime::Backend for VeilidRecord {
    fn identity(&self) -> cdht::runtime::Identity {
        self.identity.clone()
    }
    async fn get(
        &self,
        subkey: u32,
        refresh: bool,
    ) -> Result<Option<cdht::runtime::Value>, cdht::Error> {
        self.inner
            .get(subkey, refresh)
            .await
            .map_err(record_failure)?
            .map(value)
            .transpose()
    }
    async fn republish(
        &self,
        subkey: u32,
        seq: u32,
        data: &[u8],
    ) -> Result<cdht::runtime::Publication, cdht::Error> {
        Ok(
            match self
                .inner
                .republish(subkey, seq, data)
                .await
                .map_err(record_failure)?
            {
                cvln::Publication::Queued => cdht::runtime::Publication::Queued,
                cvln::Publication::Replicated => cdht::runtime::Publication::Replicated,
                cvln::Publication::Conflict(current) => {
                    cdht::runtime::Publication::Conflict(value(current)?)
                }
            },
        )
    }
    async fn flush(&self) -> Result<bool, cdht::Error> {
        self.inner.flush().await.map_err(record_failure)
    }
    async fn watch(&self) -> Result<bool, cdht::Error> {
        self.inner.watch().await.map_err(record_failure)
    }
    async fn cancel_watch(&self) -> Result<bool, cdht::Error> {
        self.inner.cancel_watch().await.map_err(record_failure)
    }
    fn changes(&self) -> Result<cdht::runtime::Changes, cdht::Error> {
        let changes = self.inner.changes();
        Ok(cdht::runtime::Changes {
            ranges: changes.ranges,
            lost: changes.lost,
        })
    }
}
