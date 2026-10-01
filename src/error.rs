//! Coarse, privacy-preserving errors.
use std::fmt;

/// What went wrong, coarse enough to never carry peer or payload data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The address is malformed for its scheme.
    InvalidAddress,
    /// The backend does not serve this scheme or capability.
    Unsupported,
    /// The backend's network is unusable (not bootstrapped, detached, no
    /// circuits). The facade reports this to the fallback executor.
    Network,
    /// This peer or route cannot be reached; the network itself may be fine.
    Unreachable,
    /// A deadline passed.
    Timeout,
    /// The stream, listener or backend was closed.
    Closed,
    /// A bound was exceeded (size, count, capacity).
    Limit,
    /// The peer violated the backend's wire protocol.
    Protocol,
}

/// Error with a kind and a static description.
///
/// The description is a compile-time string: errors never include addresses,
/// keys, payloads or upstream error text, so they are safe to log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    context: &'static str,
}

impl Error {
    /// Create an error.
    pub const fn new(kind: ErrorKind, context: &'static str) -> Self {
        Self { kind, context }
    }

    /// The error kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The static description.
    pub fn context(&self) -> &'static str {
        self.context
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.context)
    }
}

impl std::error::Error for Error {}

impl From<Error> for std::io::Error {
    fn from(error: Error) -> Self {
        let kind = match error.kind {
            ErrorKind::InvalidAddress => std::io::ErrorKind::InvalidInput,
            ErrorKind::Unsupported => std::io::ErrorKind::Unsupported,
            ErrorKind::Network | ErrorKind::Unreachable => std::io::ErrorKind::NotConnected,
            ErrorKind::Timeout => std::io::ErrorKind::TimedOut,
            ErrorKind::Closed => std::io::ErrorKind::BrokenPipe,
            ErrorKind::Limit => std::io::ErrorKind::OutOfMemory,
            ErrorKind::Protocol => std::io::ErrorKind::InvalidData,
        };
        std::io::Error::new(kind, error)
    }
}

#[cfg(test)]
#[path = "../tests/support/error_unit.rs"]
mod tests;
