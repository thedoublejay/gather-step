//! Delta extractors — one module per surface type.
//!
//! Each extractor takes a baseline `&S: GraphStore` and a review `&S: GraphStore`
//! and returns a typed delta struct for inclusion in [`crate::pr_review::delta_report::DeltaReport`].
//!
//! Phase 2 Task 2 implements `routes`.
//! Tasks 3-6 will implement `symbols`, `payload_contracts`, `events`, and
//! `removed_surfaces`.

pub mod events;
pub mod impact_attach;
pub mod payload_contracts;
pub mod removed_surfaces;
pub mod routes;
pub mod symbols;
