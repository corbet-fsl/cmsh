use super::*;

#[test]
fn scheme_validation() {
    let tor = Scheme::new("tor").unwrap();
    assert_eq!(tor.as_str(), "tor");
    assert_eq!(format!("{tor:?}"), "Scheme(tor)");
    assert!(Scheme::new("veilid").is_ok());
    assert!(Scheme::new("i2p-sam3").is_ok());
    for bad in [
        "",
        "Tor",
        "1tor",
        "tor.onion",
        "a-very-long-scheme-name",
        "t r",
    ] {
        assert!(Scheme::new(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn address_bounds_and_scheme() {
    let tor = Scheme::new("tor").unwrap();
    let veilid = Scheme::new("veilid").unwrap();
    assert!(Address::new(tor.clone(), vec![]).is_err());
    assert!(Address::new(tor.clone(), vec![0; MAX_ADDRESS_BYTES + 1]).is_err());
    let address = Address::new(tor.clone(), b"x".to_vec()).unwrap();
    assert_eq!(address.expect_scheme(&tor).unwrap(), b"x");
    assert_eq!(address.bytes(), b"x");
    assert_eq!(
        address.expect_scheme(&veilid).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
}

#[test]
fn debug_hides_address_bytes() {
    let address = Address::new(Scheme::new("tor").unwrap(), b"secret-peer".to_vec()).unwrap();
    let printed = format!("{address:?}");
    assert!(!printed.contains("secret"));
    assert_eq!(printed, "Address(tor, 11 bytes)");
}
