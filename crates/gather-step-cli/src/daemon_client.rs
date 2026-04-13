use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result};

use crate::{
    command_render::RenderedCommand,
    daemon_protocol::{DaemonPidFile, DaemonRequest, DaemonResponse},
};

pub struct DaemonClient {
    socket_path: std::path::PathBuf,
}

const DAEMON_RPC_TIMEOUT: Duration = Duration::from_secs(5);

impl DaemonClient {
    pub fn try_connect(workspace_root: &Path) -> Result<Option<Self>> {
        let daemon_dir = workspace_root.join(".gather-step");
        let pid_path = daemon_dir.join("daemon.pid");
        let socket_path = daemon_dir.join("daemon.sock");
        if !pid_path.exists() || !socket_path.exists() {
            return Ok(None);
        }

        let pid_file = fs::read_to_string(&pid_path)
            .with_context(|| format!("reading {}", pid_path.display()))?;
        let pid_meta: DaemonPidFile = serde_json::from_str(&pid_file)
            .with_context(|| format!("parsing {}", pid_path.display()))?;
        if pid_meta.workspace_root != workspace_root.display().to_string() {
            return Ok(None);
        }

        #[cfg(not(unix))]
        {
            return Ok(None);
        }

        Ok(Some(Self { socket_path }))
    }

    pub fn call(&self, request: &DaemonRequest) -> Result<RenderedCommand> {
        #[cfg(unix)]
        {
            let mut stream = std::os::unix::net::UnixStream::connect(&self.socket_path)
                .with_context(|| format!("connecting to {}", self.socket_path.display()))?;
            stream
                .set_read_timeout(Some(DAEMON_RPC_TIMEOUT))
                .context("setting daemon read timeout")?;
            stream
                .set_write_timeout(Some(DAEMON_RPC_TIMEOUT))
                .context("setting daemon write timeout")?;
            let request_json =
                serde_json::to_string(request).context("serializing daemon request")?;
            stream
                .write_all(request_json.as_bytes())
                .context("writing daemon request")?;
            stream
                .write_all(b"\n")
                .context("terminating daemon request")?;
            stream.flush().context("flushing daemon request")?;

            let mut reader = BufReader::new(stream);
            let mut response_line = String::new();
            reader
                .read_line(&mut response_line)
                .context("reading daemon response")?;
            let response: DaemonResponse =
                serde_json::from_str(&response_line).context("parsing daemon response")?;
            Ok(response.result)
        }

        #[cfg(not(unix))]
        {
            let _ = request;
            anyhow::bail!("daemon IPC is unsupported on this platform");
        }
    }
}
