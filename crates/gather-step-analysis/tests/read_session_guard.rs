//! Guard: analysis walkers must do per-hop graph lookups through a held
//! `GraphReadSession`, never via per-call `get_outgoing`/`get_incoming` on the
//! store (each of those opens a fresh read transaction per node visited).
//!
//! Scans every `src/*.rs` file up to its first `#[cfg(test)]` marker. Justified
//! exceptions go in `ALLOWED` with the file name and the exact expected count.

use std::fs;
use std::path::Path;

/// (file name, allowed occurrences before the first `#[cfg(test)]`).
const ALLOWED: &[(&str, usize)] = &[];

#[test]
fn analysis_sources_use_read_sessions_for_per_hop_lookups() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();

    let mut entries: Vec<_> = fs::read_dir(&src_dir)
        .expect("src dir should be readable")
        .map(|entry| entry.expect("dir entry should be readable").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    entries.sort();

    for path in entries {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("source file name should be utf-8")
            .to_owned();
        let source = fs::read_to_string(&path).expect("source file should be readable");

        let mut hits = 0_usize;
        for line in source.lines() {
            if line.trim_start().starts_with("#[cfg(test)]") {
                break;
            }
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            hits += line.matches(".get_outgoing(").count();
            hits += line.matches(".get_incoming(").count();
        }

        let allowed = ALLOWED
            .iter()
            .find(|(name, _)| *name == file_name)
            .map_or(0, |(_, count)| *count);
        if hits != allowed {
            violations.push(format!(
                "{file_name}: {hits} direct per-hop lookup(s) (allowed: {allowed})"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "per-hop graph lookups outside GraphReadSession (route them through \
         store.read_session(), or add a justified ALLOWED entry):\n{}",
        violations.join("\n")
    );
}
