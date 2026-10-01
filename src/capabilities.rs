//! What a backend guarantees.

/// Expected latency class of a backend, best first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LatencyClass {
    /// Suitable for live voice (tens of milliseconds one way).
    Realtime,
    /// Suitable for live text (hundreds of milliseconds).
    Interactive,
    /// Seconds or more; store-and-forward.
    Bulk,
}

impl LatencyClass {
    /// All classes, best first.
    pub const ALL: [LatencyClass; 3] = [Self::Realtime, Self::Interactive, Self::Bulk];
}

/// Capabilities a backend declares. The facade trusts these declarations only
/// as far as it trusts the backend code; it checks the hooks for consistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Capabilities {
    /// The backend hides network locations from peers under its documented
    /// threat model. This is not resistance to global timing correlation, nor
    /// a claim that different anonymity networks offer equivalent protection.
    pub anonymous: bool,
    /// Unreliable datagrams, only when independently implemented and qualified.
    pub datagrams: bool,
    /// Delivery while the recipient is offline (store-and-forward).
    pub offline_delivery: bool,
    /// A byte-preserving DHT port, only when independently qualified.
    pub dht: bool,
    /// Expected latency.
    pub latency: LatencyClass,
}
