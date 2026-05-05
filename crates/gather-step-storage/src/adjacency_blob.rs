//! Zero-copy adjacency blob format backed by rkyv.
//!
//! # What this is
//!
//! A serialization format for the offset / index arrays of
//! [`crate::GraphCsrSnapshot`]. The arrays are pure `Vec<u64>` data —
//! ideal for rkyv's zero-copy archive format because the on-disk bytes can
//! be cast directly to the `Archived<…>` view without parsing.
//!
//! # Why not just bitcode / serde?
//!
//! `bitcode` and `serde_json` both copy bytes through Rust-side allocators
//! to materialize a `Vec<u64>`. For the largest production graphs, the
//! offset array runs into millions of entries; rkyv lets the read path
//! borrow the on-disk view through a single page-faulted read, which is
//! the v3.3 bucket's "rkyv zero-copy adjacency blobs" goal.
//!
//! # What is NOT in here yet
//!
//! - On-disk persistence path (writing the blob alongside the redb file
//!   and reading it back during `csr_snapshot()`).
//! - Production wiring: callers still build the CSR snapshot from
//!   `redb` reads on every call. The blob format below is exercised by
//!   tests and is the prerequisite for the persistence path.

use rkyv::{Archive, Deserialize, Serialize, rancor::Error as RkyvError};

/// rkyv-archived adjacency arrays for [`crate::GraphCsrSnapshot`].
///
/// All six arrays mirror the in-memory snapshot one-to-one. Node and edge
/// payloads are intentionally **not** included — those are stored in
/// `redb` and reconstructed on load. The blob captures only the dense
/// adjacency layout that benefits from zero-copy bytes.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct AdjacencyBlob {
    /// Number of nodes the offsets index. Used to validate that
    /// `outgoing_offsets.len() == node_count + 1` etc.
    pub node_count: u64,
    /// Number of edges the indices reference.
    pub edge_count: u64,
    pub outgoing_offsets: Vec<u64>,
    pub outgoing_edge_indices: Vec<u64>,
    pub incoming_offsets: Vec<u64>,
    pub incoming_edge_indices: Vec<u64>,
    pub owner_offsets: Vec<u64>,
    pub owner_edge_indices: Vec<u64>,
}

impl AdjacencyBlob {
    /// Serialize this blob to a byte buffer suitable for writing to disk
    /// or sending over the wire.  The buffer is aligned for direct
    /// archive access on read.
    pub fn to_bytes(&self) -> Result<Vec<u8>, RkyvError> {
        let aligned = rkyv::to_bytes::<RkyvError>(self)?;
        Ok(aligned.to_vec())
    }

    /// Validate `bytes` and decode into an owned [`AdjacencyBlob`].
    ///
    /// The validation step (`bytecheck`) enforces that pointer offsets
    /// and length fields are within bounds before any field is read,
    /// which lets `forbid(unsafe_code)` callers consume untrusted input
    /// safely.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RkyvError> {
        let archived = rkyv::access::<ArchivedAdjacencyBlob, RkyvError>(bytes)?;
        rkyv::deserialize::<Self, RkyvError>(archived)
    }

    /// Borrow an [`ArchivedAdjacencyBlob`] view of `bytes` without
    /// allocating a Rust-owned copy. Returns the validated archive on
    /// success.
    ///
    /// Use this on the read path when the caller only needs to look up a
    /// few offsets — paying the deserialize cost would defeat the
    /// zero-copy benefit.
    pub fn access(bytes: &[u8]) -> Result<&ArchivedAdjacencyBlob, RkyvError> {
        rkyv::access::<ArchivedAdjacencyBlob, RkyvError>(bytes)
    }

    /// Self-consistency check used by tests and (eventually) by the
    /// load path before handing the blob to the snapshot constructor.
    pub fn validate_lengths(&self) -> Result<(), AdjacencyBlobError> {
        let node_count_usize: usize = self
            .node_count
            .try_into()
            .map_err(|_| AdjacencyBlobError::NodeCountOverflow {
                node_count: self.node_count,
            })?;

        let expected_offsets = node_count_usize + 1;
        for (label, offsets) in [
            ("outgoing_offsets", &self.outgoing_offsets),
            ("incoming_offsets", &self.incoming_offsets),
            ("owner_offsets", &self.owner_offsets),
        ] {
            if offsets.len() != expected_offsets {
                return Err(AdjacencyBlobError::OffsetLengthMismatch {
                    field: label,
                    actual: offsets.len(),
                    expected: expected_offsets,
                });
            }
        }
        Ok(())
    }
}

/// Errors produced by [`AdjacencyBlob`] validation.
#[derive(Debug, thiserror::Error)]
pub enum AdjacencyBlobError {
    #[error("adjacency blob node_count {node_count} does not fit in usize on this target")]
    NodeCountOverflow { node_count: u64 },
    #[error(
        "adjacency blob `{field}` has length {actual} but expected {expected} (node_count + 1)"
    )]
    OffsetLengthMismatch {
        field: &'static str,
        actual: usize,
        expected: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_blob() -> AdjacencyBlob {
        AdjacencyBlob {
            node_count: 3,
            edge_count: 2,
            outgoing_offsets: vec![0, 1, 2, 2],
            outgoing_edge_indices: vec![0, 1],
            incoming_offsets: vec![0, 0, 1, 2],
            incoming_edge_indices: vec![0, 1],
            owner_offsets: vec![0, 2, 2, 2],
            owner_edge_indices: vec![0, 1],
        }
    }

    #[test]
    fn round_trips_through_to_bytes_from_bytes() {
        let blob = sample_blob();
        let bytes = blob.to_bytes().expect("serialize");
        let recovered = AdjacencyBlob::from_bytes(&bytes).expect("deserialize");
        assert_eq!(blob, recovered);
    }

    #[test]
    fn access_returns_archived_view_without_owned_copy() {
        let blob = sample_blob();
        let bytes = blob.to_bytes().expect("serialize");
        let archived = AdjacencyBlob::access(&bytes).expect("access");
        // ArchivedAdjacencyBlob exposes the same fields by value.
        assert_eq!(u64::from(archived.node_count), blob.node_count);
        assert_eq!(u64::from(archived.edge_count), blob.edge_count);
        assert_eq!(archived.outgoing_offsets.len(), blob.outgoing_offsets.len());
    }

    #[test]
    fn validate_lengths_rejects_mismatched_offsets() {
        let mut blob = sample_blob();
        blob.outgoing_offsets.pop();
        let err = blob.validate_lengths().expect_err("should reject");
        assert!(matches!(
            err,
            AdjacencyBlobError::OffsetLengthMismatch {
                field: "outgoing_offsets",
                ..
            }
        ));
    }

    #[test]
    fn from_bytes_rejects_truncated_input() {
        let blob = sample_blob();
        let bytes = blob.to_bytes().expect("serialize");
        // Lop off the last 16 bytes — bytecheck should catch this.
        let truncated = &bytes[..bytes.len() - 16];
        AdjacencyBlob::from_bytes(truncated).expect_err("truncated input must be rejected");
    }
}
