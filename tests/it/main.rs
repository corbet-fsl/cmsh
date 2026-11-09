//! The crate's integration tests in one binary.
//!
//! Every former `tests/<name>.rs` is a module here, so a warm edit loop links one test
//! executable instead of one per file. The test names keep their module prefix
//! (`<name>::<test>`); `tests/support` stays where the library compiles it (`src/*.rs`
//! `#[path]` inputs) and is not part of this binary.
mod contracts;
mod tor_port;
