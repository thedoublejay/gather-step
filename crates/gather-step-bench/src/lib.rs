#![forbid(unsafe_code)]

use mimalloc::MiMalloc;

// Keep benchmark allocator behavior aligned with the main CLI binary so
// throughput and RSS measurements reflect production runtime contention.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod compare;
pub mod environment;
pub mod harness;
pub mod link_quality;
pub mod metrics;
pub mod planning_oracle;
pub mod pr_oracle;
pub mod release_gate;
pub mod reliability;
pub mod threshold;
pub mod tool_trace;
