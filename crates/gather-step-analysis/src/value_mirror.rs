//! Value-mirror convergence (v5.1, Task 4).
//!
//! The convergence logic lives in `gather-step-parser` (next to
//! [`gather_step_parser::ValueMirrorCandidate`]) so the `gather-step-storage`
//! indexer can call it during pass-2 materialization without creating a
//! dependency cycle (analysis depends on storage). This module re-exports it so
//! the public analysis API stays stable for downstream callers.

pub use gather_step_parser::{
    ValueMirrorConvergence, converge_value_mirrors, emit_value_mirrors_per_repo,
};
