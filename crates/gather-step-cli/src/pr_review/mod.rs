//! `pr_review` — isolated review artifact management for `gather-step pr-review`.
//!
//! This module owns the on-disk layout for a single review run: directory
//! creation, the marker file that proves the directory is review-owned, and the
//! safety-guarded handoff to [`StorageContext`].

pub mod artifact_root;
pub mod cache;
pub mod cleanup;
pub mod delta_report;
pub mod extract;
pub mod index_runner;
