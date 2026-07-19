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

#[cfg(target_os = "macos")]
fn capture_rss_impl() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let kibibytes = std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    kibibytes.checked_mul(1024)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn capture_rss_impl() -> Option<u64> {
    None
}
