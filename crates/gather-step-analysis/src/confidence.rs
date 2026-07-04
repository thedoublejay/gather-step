//! Confidence banding for graph edges.
//!
//! Edges carry a numeric `confidence` (`u16`) in their metadata. Query output
//! surfaces a coarse, human-facing band alongside the raw number so agents can
//! tell an extracted fact from an inferred hint without memorising the scale.

/// Lower bound (inclusive) for the [`ConfidenceBand::Extracted`] band.
pub const BAND_EXTRACTED_MIN: u16 = 900;
/// Lower bound (inclusive) for the [`ConfidenceBand::Inferred`] band.
pub const BAND_INFERRED_MIN: u16 = 500;

/// A coarse, human-facing band derived from a numeric edge confidence.
///
/// Query-time only; never persisted. The raw numeric confidence is always
/// serialized alongside this band, so the mapping stays informational.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfidenceBand {
    /// Directly extracted from source (`>= 900`).
    Extracted,
    /// Inferred from a strong heuristic (`500..=899`).
    Inferred,
    /// A weak hint (`< 500`).
    Hint,
}

/// Map a numeric edge confidence onto its [`ConfidenceBand`].
pub fn band_of(confidence: u16) -> ConfidenceBand {
    if confidence >= BAND_EXTRACTED_MIN {
        ConfidenceBand::Extracted
    } else if confidence >= BAND_INFERRED_MIN {
        ConfidenceBand::Inferred
    } else {
        ConfidenceBand::Hint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bands_partition_at_the_documented_boundaries() {
        assert_eq!(band_of(0), ConfidenceBand::Hint);
        assert_eq!(band_of(499), ConfidenceBand::Hint);
        assert_eq!(band_of(500), ConfidenceBand::Inferred);
        assert_eq!(band_of(899), ConfidenceBand::Inferred);
        assert_eq!(band_of(900), ConfidenceBand::Extracted);
        assert_eq!(band_of(1000), ConfidenceBand::Extracted);
        assert_eq!(band_of(u16::MAX), ConfidenceBand::Extracted);
    }

    #[test]
    fn band_serializes_lowercase() {
        let json = serde_json::to_string(&ConfidenceBand::Inferred).unwrap();
        assert_eq!(json, "\"inferred\"");
    }
}
