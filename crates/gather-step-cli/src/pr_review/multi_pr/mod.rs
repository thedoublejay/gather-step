//! Multi-PR `pr-review` support.
//!
//! This module owns the PR-set manifest contract, cache identity, runner,
//! cross-PR analysis, and output rendering for coordinated review sets.

pub mod cache_key;
pub mod coordinator;
pub mod delta_report;
pub mod gh;
pub mod manifest;
