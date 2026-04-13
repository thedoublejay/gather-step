use serde::{Deserialize, Serialize};

pub use gather_step_storage::StorageDaemonMetadata as DaemonPidFile;

use crate::command_render::RenderedCommand;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum DaemonRequest {
    Search {
        query: String,
        limit: usize,
        kind: Option<String>,
        repo_filter: Option<String>,
    },
    Status {
        repo_filter: Option<String>,
    },
    TraceCrud {
        method: Option<String>,
        path: Option<String>,
        symbol_id: Option<String>,
        limit: usize,
        repo_filter: Option<String>,
    },
    Doctor {
        repo_filter: Option<String>,
    },
    Conventions {
        repo_filter: Option<String>,
    },
    EventsTrace {
        subject: String,
        limit: usize,
        repo_filter: Option<String>,
    },
    EventsBlastRadius {
        subject: String,
        limit: usize,
        depth: usize,
        repo_filter: Option<String>,
    },
    EventsOrphans {
        limit: usize,
        repo_filter: Option<String>,
    },
    Impact {
        symbol: String,
        limit: usize,
        repo_filter: Option<String>,
    },
    Pack {
        target: Option<String>,
        symbol: Option<String>,
        route_method: Option<String>,
        route_path: Option<String>,
        event_target: Option<String>,
        mode: String,
        limit: usize,
        depth: usize,
        budget_bytes: Option<usize>,
        repo_filter: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub result: RenderedCommand,
}
