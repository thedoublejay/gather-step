/// Capture the current process RSS (resident set size) in bytes.
///
/// Returns `None` when the platform is unsupported or the measurement fails.
#[must_use]
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
    None
}
