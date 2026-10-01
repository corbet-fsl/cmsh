//! Bounded private-Tor record integration; no production discovery implementation.
#[cfg(not(target_arch = "wasm32"))]
#[path = "../tests/support/tor_records/main.rs"]
mod fixture;
#[cfg(not(target_arch = "wasm32"))]
fn main() {
    fixture::main();
}
#[cfg(target_arch = "wasm32")]
fn main() {}
