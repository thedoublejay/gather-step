//! Multi-PR `pr-review` support.
//!
//! This module owns the PR-set manifest contract first. Later phases add cache,
//! runner, cross-PR analysis, and output rendering on top of this schema.

pub mod manifest;
