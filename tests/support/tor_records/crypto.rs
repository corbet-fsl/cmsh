//! Real maintained crypto adapters for fixtures with VLD0 semantics (Ed25519ph, context
//! `VLD0_SIGN`). In the vault, `cvlt` adapts `ckmg` instead.
#![allow(dead_code)]

use cdht::crypto::{PublicKey, Signature};
use cdht::{Error, Signer, Verifier};
use ed25519_dalek::{Digest, Sha512, SigningKey, VerifyingKey};

pub struct TestKey(SigningKey);

pub fn key(seed: u8) -> TestKey {
    key_bytes([seed; 32])
}

pub fn key_bytes(seed: [u8; 32]) -> TestKey {
    TestKey(SigningKey::from_bytes(&seed))
}

fn prehash(data: &[u8]) -> Sha512 {
    let mut digest = Sha512::new();
    digest.update(data);
    digest
}

impl Signer for TestKey {
    fn public_key(&self) -> PublicKey {
        self.0.verifying_key().to_bytes()
    }
    fn sign(&self, data: &[u8]) -> Result<Signature, Error> {
        self.0
            .sign_prehashed(prehash(data), Some(&b"VLD0_SIGN"[..]))
            .map(|s| s.to_bytes())
            .map_err(|_| Error::Signer)
    }
}

#[derive(Clone, Copy)]
pub struct TestVerifier;

impl Verifier for TestVerifier {
    fn verify(&self, public_key: &PublicKey, data: &[u8], signature: &Signature) -> bool {
        let Ok(key) = VerifyingKey::from_bytes(public_key) else {
            return false;
        };
        !key.is_weak()
            && key
                .verify_prehashed_strict(
                    prehash(data),
                    Some(&b"VLD0_SIGN"[..]),
                    &ed25519_dalek::Signature::from_bytes(signature),
                )
                .is_ok()
    }
}

impl TestKey {
    /// A second valid signature from maintained dalek for a public test seed.
    /// Change only the nonce prefix, never the signing scalar or public key.
    /// This is an interoperability fixture, not a production signing option.
    pub fn alternative_signature(&self, data: &[u8]) -> Signature {
        use ed25519_dalek::hazmat::{ExpandedSecretKey, raw_sign_prehashed};
        let bytes: [u8; 64] = Sha512::digest(self.0.to_bytes()).into();
        let mut expanded = ExpandedSecretKey::from_bytes(&bytes);
        expanded.hash_prefix[0] ^= 1;
        raw_sign_prehashed::<Sha512, _>(
            &expanded,
            prehash(data),
            &self.0.verifying_key(),
            Some(b"VLD0_SIGN"),
        )
        .unwrap()
        .to_bytes()
    }
}
