//! Cleaning an answer of a live broker before it is written to the disk.
//!
//! The rules live in [`brokapi_redact`]: the connectors clean what they log by the same rules. This module
//! keeps the old paths.

pub use brokapi_redact::{REDACTED, remove_secrets, scrub};
