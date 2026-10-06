//! Actual Tor routing advancement from signed bootstrap hints only.
#[cfg(not(target_arch = "wasm32"))]
#[path = "../tests/support/tor_discovery/main.rs"]
mod fixture;
#[cfg(not(target_arch = "wasm32"))]
fn main() {
    fixture::main();
}
#[cfg(target_arch = "wasm32")]
fn main() {}
