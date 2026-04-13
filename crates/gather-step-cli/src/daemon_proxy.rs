use anyhow::Result;
use gather_step_mcp::output::redact::relativize_to_workspace;
use tracing::warn;

use crate::{
    app::AppContext, command_render::RenderedCommand, daemon_client::DaemonClient,
    daemon_protocol::DaemonRequest,
};

pub fn run_read_only_command<F>(app: &AppContext, request: &DaemonRequest, local: F) -> Result<()>
where
    F: FnOnce(&AppContext) -> Result<RenderedCommand>,
{
    let output = app.output();
    if let Some(rendered) = try_daemon_first(app, request) {
        return rendered.emit(&output);
    }

    local(app)?.emit(&output)
}

fn try_daemon_first(app: &AppContext, request: &DaemonRequest) -> Option<RenderedCommand> {
    let client = match DaemonClient::try_connect(&app.workspace_path) {
        Ok(Some(client)) => client,
        Ok(None) => return None,
        Err(error) => {
            warn!(
                workspace = %relativize_to_workspace(&app.workspace_path, &app.workspace_path),
                request = request_name(request),
                %error,
                "failed to inspect daemon state; falling back to local execution"
            );
            return None;
        }
    };

    match client.call(request) {
        Ok(rendered) => Some(rendered),
        Err(error) => {
            warn!(
                workspace = %app.workspace_path.display(),
                request = request_name(request),
                %error,
                "daemon request failed; falling back to local execution"
            );
            None
        }
    }
}

fn request_name(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Search { .. } => "search",
        DaemonRequest::Status { .. } => "status",
        DaemonRequest::TraceCrud { .. } => "trace_crud",
        DaemonRequest::Doctor { .. } => "doctor",
        DaemonRequest::Conventions { .. } => "conventions",
        DaemonRequest::EventsTrace { .. } => "events_trace",
        DaemonRequest::EventsBlastRadius { .. } => "events_blast_radius",
        DaemonRequest::EventsOrphans { .. } => "events_orphans",
        DaemonRequest::Impact { .. } => "impact",
        DaemonRequest::Pack { .. } => "pack",
    }
}
