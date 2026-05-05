#![forbid(unsafe_code)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

const DEFAULT_RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(25);

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

/// Count currently open file descriptors for this process when the platform
/// exposes a file-descriptor directory.
#[must_use]
pub fn capture_open_fds() -> Option<u64> {
    capture_open_fds_impl()
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ResourceSnapshot {
    pub rss_bytes: Option<u64>,
    pub open_fds: Option<u64>,
}

impl ResourceSnapshot {
    #[must_use]
    pub fn capture() -> Self {
        Self {
            rss_bytes: capture_rss(),
            open_fds: capture_open_fds(),
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ResourcePeaks {
    pub start_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub end_rss_bytes: Option<u64>,
    pub start_open_fds: Option<u64>,
    pub peak_open_fds: Option<u64>,
    pub end_open_fds: Option<u64>,
}

impl ResourcePeaks {
    #[must_use]
    pub fn from_start(start: &ResourceSnapshot) -> Self {
        Self {
            start_rss_bytes: start.rss_bytes,
            peak_rss_bytes: start.rss_bytes,
            end_rss_bytes: start.rss_bytes,
            start_open_fds: start.open_fds,
            peak_open_fds: start.open_fds,
            end_open_fds: start.open_fds,
        }
    }

    pub fn observe(&mut self, sample: &ResourceSnapshot) {
        if let Some(rss) = sample.rss_bytes {
            self.peak_rss_bytes = Some(self.peak_rss_bytes.map_or(rss, |peak| peak.max(rss)));
            self.end_rss_bytes = Some(rss);
        }
        if let Some(open_fds) = sample.open_fds {
            self.peak_open_fds = Some(
                self.peak_open_fds
                    .map_or(open_fds, |peak| peak.max(open_fds)),
            );
            self.end_open_fds = Some(open_fds);
        }
    }

    #[must_use]
    pub fn rss_growth_bytes(&self) -> Option<u64> {
        match (self.start_rss_bytes, self.end_rss_bytes) {
            (Some(start), Some(end)) => Some(end.saturating_sub(start)),
            _ => None,
        }
    }
}

/// Background sampler for peak RSS and open-FD counts during benchmark phases.
pub struct ResourceSampler {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<ResourcePeaks>>,
    start: ResourceSnapshot,
}

impl ResourceSampler {
    #[must_use]
    pub fn start() -> Self {
        Self::start_with_interval(DEFAULT_RESOURCE_SAMPLE_INTERVAL)
    }

    #[must_use]
    pub fn start_with_interval(interval: Duration) -> Self {
        let start = ResourceSnapshot::capture();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_start = start.clone();
        let handle = thread::spawn(move || {
            let mut peaks = ResourcePeaks::from_start(&thread_start);
            while !thread_stop.load(Ordering::Relaxed) {
                peaks.observe(&ResourceSnapshot::capture());
                thread::sleep(interval);
            }
            peaks.observe(&ResourceSnapshot::capture());
            peaks
        });
        Self {
            stop,
            handle: Some(handle),
            start,
        }
    }

    #[must_use]
    pub fn finish(mut self) -> ResourcePeaks {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> ResourcePeaks {
        self.stop.store(true, Ordering::Relaxed);
        let Some(handle) = self.handle.take() else {
            return ResourcePeaks::from_start(&self.start);
        };
        match handle.join() {
            Ok(mut peaks) => {
                peaks.observe(&ResourceSnapshot::capture());
                peaks
            }
            Err(_) => ResourcePeaks::from_start(&self.start),
        }
    }
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        if self.handle.is_some() {
            let _ = self.finish_inner();
        }
    }
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

#[cfg(unix)]
fn capture_open_fds_impl() -> Option<u64> {
    for path in ["/proc/self/fd", "/dev/fd"] {
        let Ok(entries) = std::fs::read_dir(path) else {
            continue;
        };
        let count = entries.flatten().count();
        return Some(u64::try_from(count).unwrap_or(u64::MAX));
    }
    None
}

#[cfg(not(unix))]
fn capture_open_fds_impl() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::ResourceSampler;

    #[test]
    fn resource_sampler_returns_a_snapshot() {
        let sampler = ResourceSampler::start_with_interval(std::time::Duration::from_millis(1));
        let peaks = sampler.finish();

        assert_eq!(
            peaks.start_rss_bytes.is_some(),
            peaks.peak_rss_bytes.is_some()
        );
        assert_eq!(
            peaks.start_open_fds.is_some(),
            peaks.peak_open_fds.is_some()
        );
    }

    #[test]
    #[cfg(unix)]
    fn resource_sampler_observes_open_fd_peak() {
        let sampler = ResourceSampler::start_with_interval(std::time::Duration::from_millis(1));
        let file = std::fs::File::open("/dev/null").expect("/dev/null should open");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let peaks = sampler.finish();
        drop(file);

        if let (Some(start), Some(peak)) = (peaks.start_open_fds, peaks.peak_open_fds) {
            assert!(peak >= start, "peak={peak} start={start}");
        }
    }
}
