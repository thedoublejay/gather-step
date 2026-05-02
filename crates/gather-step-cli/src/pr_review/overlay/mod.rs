//! Diff overlay module — Phase 5 Task 2 prototype.
//!
//! Provides [`store::DiffOverlayStore`], a read-only [`GraphStore`][gather_step_storage::GraphStore]
//! implementation that layers added/changed/removed nodes and edges over an
//! immutable baseline without mutating it.

pub mod store;
