#![forbid(unsafe_code)]

/// Capture the current process RSS (resident set size) in bytes.
///
/// On Linux this reads `/proc/self/status` via the `procfs` crate.
/// On macOS and other platforms this returns `None` because the required
/// Mach task info API is not yet wired up.
///
/// # Errors
///
/// Returns `None` when the platform is unsupported or the measurement fails.
pub fn capture_rss() -> Option<u64> {
    capture_rss_impl()
}

#[cfg(target_os = "linux")]
fn capture_rss_impl() -> Option<u64> {
    use procfs::process::Process;
    let proc = Process::myself().ok()?;
    let status = proc.status().ok()?;
    // VmRSS is reported in kibibytes by the kernel.
    status.vmrss.map(|kb| kb * 1024)
}

#[cfg(not(target_os = "linux"))]
fn capture_rss_impl() -> Option<u64> {
    // TODO: implement via mach_task_basic_info on macOS and GetProcessMemoryInfo on Windows.
    None
}
