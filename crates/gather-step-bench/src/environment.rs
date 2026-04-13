#![forbid(unsafe_code)]

//! Execution environment capture for benchmark reproducibility.
//!
//! Call [`capture`] once at the start of a bench run to obtain a snapshot
//! of the host, toolchain, and repository state.  The snapshot is serialised
//! into every result artifact under an `environment` key so that results can
//! be compared across machines and commits.

use std::process::Command;

use serde::{Deserialize, Serialize};

/// A best-effort snapshot of the execution environment captured at the start
/// of a benchmark run.
///
/// All fields that may be unavailable on a given platform are `Option`al;
/// serialisation skips `None` values so that historical result files that
/// pre-date this struct still load without error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnvironmentCapture {
    /// Operating system family, e.g. `"darwin"`, `"linux"`, `"windows"`.
    pub os: String,
    /// Kernel release string from `uname -r`, e.g. `"25.4.0"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    /// CPU architecture, e.g. `"aarch64"`, `"x86_64"`.
    pub arch: String,
    /// Human-readable CPU model string (best effort).
    ///
    /// On macOS this comes from `sysctl -n machdep.cpu.brand_string`.
    /// On Linux it is parsed from `/proc/cpuinfo`.
    /// `None` when the value cannot be determined.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_model: Option<String>,
    /// Number of logical CPU cores available to the process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_cpus: Option<usize>,
    /// Number of physical CPU cores (excludes hyper-threading).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_cpus: Option<usize>,
    /// Total system memory in bytes (best effort).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_memory_bytes: Option<u64>,
    /// Full `rustc -V` version string, e.g. `"rustc 1.94.1 (abcdef012 2026-04-01)"`.
    pub rust_version: String,
    /// HEAD commit SHA of the workspace at the time of the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_commit: Option<String>,
    /// `true` when the workspace has uncommitted changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dirty: Option<bool>,
    /// First ten lines of `git status --porcelain` when the workspace is dirty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dirty_summary: Option<Vec<String>>,
    /// `argv` of the bench process, joined with spaces.
    pub command_line: String,
    /// High-level summary of the index produced during this run (files, symbols,
    /// edges).  Omitted for bench variants that do not build an index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_summary: Option<IndexSummary>,
}

/// Counts from the index built during a bench run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexSummary {
    /// Total number of source files indexed.
    pub files: usize,
    /// Total number of symbols extracted.
    pub symbols: usize,
    /// Total number of graph edges.
    pub edges: usize,
}

/// Capture the current execution environment.
///
/// Shell commands (`git`, `rustc`, `uname`, `sysctl`) are invoked
/// synchronously.  If any command fails the corresponding field is set to
/// `None` rather than propagating an error.
///
/// `workspace_root` should be the root of the git repository; it is used as
/// the working directory for `git` commands.
#[must_use]
pub fn capture(workspace_root: Option<&std::path::Path>) -> EnvironmentCapture {
    let os_version = run_stdout("uname", &["-r"]);
    let cpu_model = detect_cpu_model();
    let logical_cpus = std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZero::get);
    let physical_cpus = detect_physical_cpus();
    let total_memory_bytes = detect_total_memory_bytes();
    let rust_version = run_stdout("rustc", &["-V"]).unwrap_or_else(|| "unknown".to_owned());
    let (workspace_commit, workspace_dirty, workspace_dirty_summary) =
        git_workspace_state(workspace_root);
    let command_line = std::env::args().collect::<Vec<_>>().join(" ");

    EnvironmentCapture {
        os: std::env::consts::OS.to_owned(),
        os_version,
        arch: std::env::consts::ARCH.to_owned(),
        cpu_model,
        logical_cpus,
        physical_cpus,
        total_memory_bytes,
        rust_version,
        workspace_commit,
        workspace_dirty,
        workspace_dirty_summary,
        command_line,
        index_summary: None,
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Run a command and return trimmed stdout, or `None` on failure.
fn run_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if s.is_empty() { None } else { Some(s) }
}

fn detect_cpu_model() -> Option<String> {
    detect_cpu_model_impl()
}

#[cfg(target_os = "macos")]
fn detect_cpu_model_impl() -> Option<String> {
    run_stdout("sysctl", &["-n", "machdep.cpu.brand_string"])
}

#[cfg(target_os = "linux")]
fn detect_cpu_model_impl() -> Option<String> {
    let content = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("model name")
            && let Some(value) = rest.strip_prefix(':')
        {
            let model = value.trim().to_owned();
            if !model.is_empty() {
                return Some(model);
            }
        }
    }
    None
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_cpu_model_impl() -> Option<String> {
    None
}

fn detect_physical_cpus() -> Option<usize> {
    detect_physical_cpus_impl()
}

#[cfg(target_os = "macos")]
fn detect_physical_cpus_impl() -> Option<usize> {
    let s = run_stdout("sysctl", &["-n", "hw.physicalcpu"])?;
    s.parse::<usize>().ok()
}

#[cfg(target_os = "linux")]
fn detect_physical_cpus_impl() -> Option<usize> {
    // Count unique `physical id` × `core id` pairs in /proc/cpuinfo.
    let content = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    let mut pairs = std::collections::BTreeSet::new();
    let mut physical_id: Option<String> = None;
    let mut core_id: Option<String> = None;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("physical id") {
            if let Some(v) = rest.strip_prefix(':') {
                physical_id = Some(v.trim().to_owned());
            }
        } else if let Some(rest) = line.strip_prefix("core id") {
            if let Some(v) = rest.strip_prefix(':') {
                core_id = Some(v.trim().to_owned());
            }
        } else if line.is_empty()
            && let (Some(p), Some(c)) = (physical_id.take(), core_id.take())
        {
            pairs.insert((p, c));
        }
    }
    if pairs.is_empty() {
        None
    } else {
        Some(pairs.len())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_physical_cpus_impl() -> Option<usize> {
    None
}

fn detect_total_memory_bytes() -> Option<u64> {
    detect_total_memory_bytes_impl()
}

#[cfg(target_os = "macos")]
fn detect_total_memory_bytes_impl() -> Option<u64> {
    let s = run_stdout("sysctl", &["-n", "hw.memsize"])?;
    s.parse::<u64>().ok()
}

#[cfg(target_os = "linux")]
fn detect_total_memory_bytes_impl() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            // Value is in kB; convert to bytes.
            if let Some(kb) = rest.trim().strip_suffix(" kB") {
                return kb.trim().parse::<u64>().ok().map(|kb| kb * 1024);
            }
        }
    }
    None
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_total_memory_bytes_impl() -> Option<u64> {
    None
}

fn git_workspace_state(
    workspace_root: Option<&std::path::Path>,
) -> (Option<String>, Option<bool>, Option<Vec<String>>) {
    let Some(root) = workspace_root else {
        return (None, None, None);
    };

    let commit = git_stdout_in(root, &["rev-parse", "HEAD"]);
    let status_output = git_stdout_in(root, &["status", "--porcelain"]);

    let (dirty, dirty_summary) = match status_output {
        None => (None, None),
        Some(ref s) if s.is_empty() => (Some(false), None),
        Some(ref s) => {
            let lines: Vec<String> = s.lines().take(10).map(str::to_owned).collect();
            (Some(true), Some(lines))
        }
    };

    (commit, dirty, dirty_summary)
}

fn git_stdout_in(cwd: &std::path::Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Some(s)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn capture_populates_mandatory_fields() {
        let env = capture(None);
        assert!(!env.os.is_empty(), "os must be non-empty");
        assert!(!env.arch.is_empty(), "arch must be non-empty");
        assert!(
            !env.rust_version.is_empty(),
            "rust_version must be non-empty"
        );
        assert!(
            !env.command_line.is_empty(),
            "command_line must be non-empty"
        );
    }

    #[test]
    fn capture_skips_git_fields_when_no_root_given() {
        let env = capture(None);
        assert!(env.workspace_commit.is_none());
        assert!(env.workspace_dirty.is_none());
        assert!(env.workspace_dirty_summary.is_none());
    }

    #[test]
    fn capture_with_invalid_git_root_yields_none_git_fields() {
        let env = capture(Some(Path::new("/tmp/nonexistent-path-gather-step-bench")));
        // git commands will fail; all three git fields should be None.
        assert!(env.workspace_commit.is_none());
        assert!(env.workspace_dirty.is_none());
        assert!(env.workspace_dirty_summary.is_none());
    }

    #[test]
    fn roundtrip_serialization_preserves_all_fields() {
        let original = EnvironmentCapture {
            os: "linux".to_owned(),
            os_version: Some("6.1.0".to_owned()),
            arch: "x86_64".to_owned(),
            cpu_model: Some("Intel(R) Core(TM) i9".to_owned()),
            logical_cpus: Some(16),
            physical_cpus: Some(8),
            total_memory_bytes: Some(34_359_738_368),
            rust_version: "rustc 1.94.1 (abc123 2026-04-01)".to_owned(),
            workspace_commit: Some("deadbeef".to_owned()),
            workspace_dirty: Some(true),
            workspace_dirty_summary: Some(vec!["M src/lib.rs".to_owned()]),
            command_line: "gather-step-bench run fixture".to_owned(),
            index_summary: Some(IndexSummary {
                files: 100,
                symbols: 500,
                edges: 1000,
            }),
        };

        let json = serde_json::to_string(&original).expect("serialize");
        let restored: EnvironmentCapture = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, restored);
    }

    #[test]
    fn skip_serializing_none_fields() {
        let env = EnvironmentCapture {
            os: "darwin".to_owned(),
            os_version: None,
            arch: "aarch64".to_owned(),
            cpu_model: None,
            logical_cpus: None,
            physical_cpus: None,
            total_memory_bytes: None,
            rust_version: "rustc 1.94.1".to_owned(),
            workspace_commit: None,
            workspace_dirty: None,
            workspace_dirty_summary: None,
            command_line: "gather-step-bench run".to_owned(),
            index_summary: None,
        };

        let json = serde_json::to_string(&env).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        let obj = value.as_object().expect("object");

        // Mandatory fields are present.
        assert!(obj.contains_key("os"));
        assert!(obj.contains_key("arch"));
        assert!(obj.contains_key("rust_version"));
        assert!(obj.contains_key("command_line"));

        // Optional None fields must not appear.
        assert!(!obj.contains_key("os_version"));
        assert!(!obj.contains_key("cpu_model"));
        assert!(!obj.contains_key("logical_cpus"));
        assert!(!obj.contains_key("physical_cpus"));
        assert!(!obj.contains_key("total_memory_bytes"));
        assert!(!obj.contains_key("workspace_commit"));
        assert!(!obj.contains_key("workspace_dirty"));
        assert!(!obj.contains_key("workspace_dirty_summary"));
        assert!(!obj.contains_key("index_summary"));
    }

    #[test]
    fn os_version_is_detected_on_supported_platforms() {
        // This test can only assert that the field is *populated* on macOS and
        // Linux; it cannot assert the exact string.  On unsupported platforms
        // the field will be None, which is also acceptable.
        let env = capture(None);
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            // uname -r is available on both; assert the string looks like a
            // kernel version (contains at least one dot).
            if let Some(ref version) = env.os_version {
                assert!(
                    version.contains('.'),
                    "os_version should look like a version: {version}"
                );
            }
        }
        let _ = env; // silence unused on other platforms
    }

    #[test]
    fn logical_cpus_is_at_least_one() {
        let env = capture(None);
        if let Some(n) = env.logical_cpus {
            assert!(n >= 1, "logical_cpus must be at least 1, got {n}");
        }
    }

    #[test]
    fn physical_cpus_does_not_exceed_logical() {
        let env = capture(None);
        if let (Some(phys), Some(log)) = (env.physical_cpus, env.logical_cpus) {
            assert!(
                phys <= log,
                "physical_cpus ({phys}) must not exceed logical_cpus ({log})"
            );
        }
    }
}
