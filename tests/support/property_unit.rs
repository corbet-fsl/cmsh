use super::*;

fn capabilities(latency: LatencyClass) -> Capabilities {
    Capabilities {
        anonymous: true,
        datagrams: false,
        offline_delivery: true,
        dht: true,
        latency,
    }
}

#[test]
fn latency_flags_are_monotone() {
    let interactive = properties(&capabilities(LatencyClass::Interactive));
    assert!(!interactive.contains(&Property::LatencyWithin(LatencyClass::Realtime)));
    assert!(interactive.contains(&Property::LatencyWithin(LatencyClass::Interactive)));
    assert!(interactive.contains(&Property::LatencyWithin(LatencyClass::Bulk)));
    let realtime = properties(&capabilities(LatencyClass::Realtime));
    assert!(realtime.contains(&Property::LatencyWithin(LatencyClass::Realtime)));
}

#[test]
fn flags_follow_capabilities() {
    let offered = properties(&capabilities(LatencyClass::Bulk));
    assert!(offered.contains(&Property::Anonymous));
    assert!(offered.contains(&Property::OfflineDelivery));
    assert!(offered.contains(&Property::Dht));
    assert!(!offered.contains(&Property::Datagrams));
}

#[test]
fn default_policy_is_anonymous_only() {
    assert_eq!(Policy::default(), Policy::anonymous_only());
    assert!(
        Policy::default()
            .requirements()
            .contains(&Property::Anonymous)
    );
    assert!(Policy::unrestricted().requirements().is_empty());
    assert!(
        Policy::unrestricted()
            .require(Property::Dht)
            .requirements()
            .contains(&Property::Dht)
    );
}
