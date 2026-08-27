//! Controlled execution probes and immutable telemetry for Queue-Aware SMDP
//! research.
//!
//! This crate records public/private exchange events and runs a controlled,
//! minimum-size Hyperliquid execution probe. Offline normalization, feature
//! generation, simulation, and SMDP control remain outside this crate.

pub mod arch;
