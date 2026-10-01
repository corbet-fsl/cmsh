//! Abstract addresses: a scheme naming the network plus opaque bytes.
use crate::{Error, ErrorKind};
use std::fmt;

/// Upper bound for address bytes. Veilid private route blobs are the largest
/// known addresses (a few KiB); this leaves room without being unbounded.
pub const MAX_ADDRESS_BYTES: usize = 64 * 1024;

/// Network name, for example `tor` or `veilid`: 1 to 16 characters from
/// `a-z`, `0-9` and `-`, starting with a letter.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scheme(String);

impl Scheme {
    /// Validate a scheme name.
    pub fn new(name: &str) -> Result<Self, Error> {
        let valid = (1..=16).contains(&name.len())
            && name.as_bytes()[0].is_ascii_lowercase()
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if valid {
            Ok(Self(name.to_owned()))
        } else {
            Err(Error::new(ErrorKind::InvalidAddress, "invalid scheme name"))
        }
    }

    /// The scheme name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scheme({})", self.0)
    }
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A backend address: scheme plus opaque, backend-defined bytes.
///
/// Only the backend serving the scheme can interpret the bytes (an onion
/// address for Tor, a private route blob for Veilid). Addresses identify
/// peers, so `Debug` never prints the bytes.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Address {
    scheme: Scheme,
    bytes: Vec<u8>,
}

impl Address {
    /// Build an address; the bytes must be non-empty and bounded.
    pub fn new(scheme: Scheme, bytes: Vec<u8>) -> Result<Self, Error> {
        if bytes.is_empty() || bytes.len() > MAX_ADDRESS_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidAddress,
                "address bytes empty or too long",
            ));
        }
        Ok(Self { scheme, bytes })
    }

    /// The network this address belongs to.
    pub fn scheme(&self) -> &Scheme {
        &self.scheme
    }

    /// Backend-defined address bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Fail with [`ErrorKind::Unsupported`] unless the scheme matches.
    pub fn expect_scheme(&self, scheme: &Scheme) -> Result<&[u8], Error> {
        if &self.scheme == scheme {
            Ok(&self.bytes)
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "address scheme not served by this backend",
            ))
        }
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Address({}, {} bytes)", self.scheme, self.bytes.len())
    }
}

#[cfg(test)]
#[path = "../tests/support/address_unit.rs"]
mod tests;
