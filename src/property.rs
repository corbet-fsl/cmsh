//! The requirement vocabulary: properties derived from capabilities, and the
//! consumer's connection policy.
use crate::{Capabilities, LatencyClass};
use std::collections::BTreeSet;

/// A property a backend can offer and a policy can require.
///
/// Latency is expressed as monotone flags: a backend of class `c` offers
/// `LatencyWithin(d)` for every class `d` at least as slow as `c`, so a
/// requirement "at most interactive" is a plain set inclusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Property {
    /// Location privacy for both ends.
    Anonymous,
    /// Unreliable datagrams.
    Datagrams,
    /// Store-and-forward delivery while the recipient is offline.
    OfflineDelivery,
    /// A DHT hook for `cdht`.
    Dht,
    /// Latency no worse than the given class.
    LatencyWithin(LatencyClass),
}

/// The properties a backend with `capabilities` offers.
pub fn properties(capabilities: &Capabilities) -> BTreeSet<Property> {
    let mut properties = BTreeSet::new();
    let flags = [
        (capabilities.anonymous, Property::Anonymous),
        (capabilities.datagrams, Property::Datagrams),
        (capabilities.offline_delivery, Property::OfflineDelivery),
        (capabilities.dht, Property::Dht),
    ];
    properties.extend(flags.into_iter().filter(|(on, _)| *on).map(|(_, p)| p));
    properties.extend(
        LatencyClass::ALL
            .into_iter()
            .filter(|class| *class >= capabilities.latency)
            .map(Property::LatencyWithin),
    );
    properties
}

/// What the consumer requires of every connection, on top of the operator's
/// minimum standard. The facade never selects a backend below either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    require: BTreeSet<Property>,
}

impl Policy {
    /// Only anonymous backends (the default).
    pub fn anonymous_only() -> Self {
        Self {
            require: BTreeSet::from([Property::Anonymous]),
        }
    }

    /// No additional consumer requirement. Mesh still requires anonymity
    /// and applies the configured minimum guarantees.
    pub fn unrestricted() -> Self {
        Self {
            require: BTreeSet::new(),
        }
    }

    /// Add a requirement.
    pub fn require(mut self, property: Property) -> Self {
        self.require.insert(property);
        self
    }

    /// The required properties.
    pub fn requirements(&self) -> &BTreeSet<Property> {
        &self.require
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::anonymous_only()
    }
}

#[cfg(test)]
#[path = "../tests/support/property_unit.rs"]
mod tests;
