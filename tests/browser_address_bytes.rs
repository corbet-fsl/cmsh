//! Browser regression: opaque address bytes survive byte-for-byte.
use cmsh::{Address, ErrorKind, MAX_ADDRESS_BYTES, Scheme};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;

fn tor() -> Scheme {
    Scheme::new("tor").unwrap()
}

fn all_byte_values() -> Vec<u8> {
    (0..=255u8).collect()
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn tor_address_preserves_every_opaque_byte() {
    let payload = all_byte_values();
    assert!(payload.contains(&0));
    assert!(payload.iter().any(|b| *b > 127));
    let address = Address::new(tor(), payload.clone()).unwrap();
    assert_eq!(address.bytes(), payload.as_slice());
    assert_eq!(address.expect_scheme(&tor()).unwrap(), payload.as_slice());
    let owned = address.clone();
    assert_eq!(address, owned);
    assert_eq!(owned.bytes(), payload.as_slice());
    assert_eq!(owned.expect_scheme(&tor()).unwrap(), payload.as_slice());
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn tor_address_exact_bound_holds_and_first_overbound_fails() {
    let bounded = vec![0xabu8; MAX_ADDRESS_BYTES];
    let address = Address::new(tor(), bounded.clone()).unwrap();
    assert_eq!(address.bytes().len(), MAX_ADDRESS_BYTES);
    assert_eq!(address.bytes(), bounded.as_slice());
    assert_eq!(address.expect_scheme(&tor()).unwrap(), bounded.as_slice());
    let over = vec![0u8; MAX_ADDRESS_BYTES + 1];
    assert_eq!(
        Address::new(tor(), over).unwrap_err().kind(),
        ErrorKind::InvalidAddress
    );
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn wrong_scheme_fails_without_altering_accepted_address() {
    let payload = all_byte_values();
    let address = Address::new(tor(), payload.clone()).unwrap();
    let other = Scheme::new("veilid").unwrap();
    assert_eq!(
        address.expect_scheme(&other).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(address.bytes(), payload.as_slice());
    assert_eq!(address.expect_scheme(&tor()).unwrap(), payload.as_slice());
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn scheme_boundary_names_accept_only_exact_ascii() {
    assert!(Scheme::new("abcdefghijklmnop").is_ok());
    assert_eq!(
        Scheme::new("abcdefghijklmnopq").unwrap_err().kind(),
        ErrorKind::InvalidAddress
    );
    for bad in ["to\0r", "t\u{43e}r", "t\u{e9}r"] {
        assert_eq!(
            Scheme::new(bad).unwrap_err().kind(),
            ErrorKind::InvalidAddress,
            "{bad:?}"
        );
    }
}

#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn debug_shows_only_scheme_and_length() {
    let marker = b"synthetic-marker-42".to_vec();
    let address = Address::new(tor(), marker.clone()).unwrap();
    let printed = format!("{address:?}");
    assert_eq!(printed, format!("Address(tor, {} bytes)", marker.len()));
    assert!(!printed.contains("synthetic-marker-42"));
    let full = Address::new(tor(), all_byte_values()).unwrap();
    assert_eq!(format!("{full:?}"), "Address(tor, 256 bytes)");
}
