#![forbid(unsafe_code)]

use std::io::Write;
use std::process::ExitCode;

use anyhow::{Error, Result};
use clap::Parser;
#[cfg(feature = "dhat-heap")]
use dhat::Alloc;
use gather_step::app::{self, AppContext};
use gather_step::commands::{self, Cli};
use gather_step::errors::format_operator_error;
#[cfg(not(feature = "dhat-heap"))]
use mimalloc::MiMalloc;

// mimalloc provides a concurrent-friendly allocator; under rayon's parallel
// parse workload it reduces contention in the default system allocator
// (especially on macOS libmalloc) for a measurable speedup.
#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static GLOBAL: Alloc = Alloc;

// Use tokio's default multi-thread runtime (worker_threads = num_cpus).
// An earlier revision limited it to 2 workers under the theory that rayon
// handles the CPU-bound parse pool on its own and tokio only needs enough
// threads for signal/progress plumbing — but that starves blocking work
// done from the synchronous indexer body (git analysis, progress refresh,
// tracing flushes) and hurts end-to-end wall clock.
#[tokio::main]
async fn main() -> ExitCode {
    match run_main().await {
        Ok(code) => ExitCode::from(code),
        Err(error) => print_operator_error_and_code(&error),
    }
}

async fn run_main() -> Result<u8> {
    #[cfg(feature = "dhat-heap")]
    let _heap_profiler = dhat::Profiler::new_heap();

    let cli = Cli::parse();
    let multi_progress = app::init_tracing(&cli)?;
    let app = AppContext::from_cli(&cli, multi_progress)?;
    app::maybe_print_banner(&app);
    commands::run(cli, app).await
}

/// Print the operator-facing error to stderr and return exit code 1.
///
/// Returning `ExitCode` rather than calling `std::process::exit(1)` lets
/// tokio's runtime tear down cleanly and lets stdio buffers flush — important
/// for `pr-review` (which prints a structured report on stdout) and any
/// command that emits trailing tracing lines.
fn print_operator_error_and_code(error: &Error) -> ExitCode {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{}", format_operator_error(error));
    ExitCode::from(1)
}
