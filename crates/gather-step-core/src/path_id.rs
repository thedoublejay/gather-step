/// Convert backslashes to forward slashes without allocating when the input
/// contains no backslashes.
///
/// Windows-style path separators (`\`) must be normalised to `/` before being
/// stored as display strings or passed to Tantivy.  This function returns a
/// [`std::borrow::Cow::Borrowed`] reference when no replacement is needed and
/// a [`std::borrow::Cow::Owned`] `String` only when at least one `\` is found.
///
/// # Examples
///
/// ```
/// use gather_step_core::normalize_path_separators;
///
/// assert_eq!(normalize_path_separators("src/main.rs"), "src/main.rs");
/// assert_eq!(normalize_path_separators("src\\main.rs"), "src/main.rs");
/// ```
pub fn normalize_path_separators(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains('\\') {
        std::borrow::Cow::Owned(s.replace('\\', "/"))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Lossless path identity type.
///
/// On Unix, the underlying representation is the raw `OsStr` bytes, so paths
/// containing arbitrary byte sequences (including non-UTF-8) round-trip
/// perfectly through the type.
///
/// On non-Unix targets the bytes are the UTF-8 encoding of `path.to_string_lossy()`.
/// This is a known limitation tracked as a future follow-up: those targets
/// should route through a WTF-8 or wide-char representation instead.
///
/// # Identity vs. display
///
/// `PathId` is for **identity** use (map keys, row keys, graph node keys,
/// reconcile inputs). Call [`PathId::to_display`] for log lines, CLI output,
/// and markdown rendering where lossy UTF-8 is acceptable.
///
/// # Serde
///
/// Serialized as a transparent byte sequence so it can be round-tripped
/// through formats that support raw bytes (e.g. bincode, bitcode). JSON
/// serialization will Base64-encode the bytes; prefer display-only paths
/// in JSON output.
///
/// # Examples
///
/// ```
/// # use gather_step_core::PathId;
/// # use std::path::Path;
/// let path = Path::new("src/main.rs");
/// let id = PathId::from_path(path);
/// assert_eq!(id.as_bytes(), b"src/main.rs");
/// ```
#[derive(Clone, Eq, PartialEq, Hash, Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PathId(Vec<u8>);

impl PathId {
    /// Construct a `PathId` from a [`std::path::Path`], preserving the raw
    /// bytes on Unix. On non-Unix targets the path is converted via
    /// `to_string_lossy()` pending a WTF-8 follow-up.
    pub fn from_path(path: &std::path::Path) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Self(path.as_os_str().as_bytes().to_vec())
        }
        #[cfg(not(unix))]
        {
            // Portable fallback: UTF-8 lossy is acceptable ONLY when
            // OsStrExt is unavailable. A future follow-up should route
            // through a WTF-8 or wide-char representation on Windows.
            Self(path.to_string_lossy().into_owned().into_bytes())
        }
    }

    /// Construct a `PathId` directly from raw bytes.
    ///
    /// Prefer [`PathId::from_path`] when starting from a
    /// [`std::path::Path`].  This constructor is provided for callers that
    /// have already extracted the bytes (e.g. after reading from a BLOB
    /// column or a binary serialization format).
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The raw bytes of the path, as stored.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Lossily convert to a displayable string for log lines, CLI output, and
    /// markdown rendering where valid UTF-8 cannot be guaranteed.
    ///
    /// **Note:** this conversion is lossy — non-UTF-8 bytes are replaced with
    /// the Unicode replacement character U+FFFD.  Do not use the result as an
    /// identity key; use [`PathId::as_bytes`] instead.
    pub fn to_display(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::PathId;

    #[test]
    fn ascii_path_roundtrips() {
        let path = Path::new("src/main.rs");
        let id = PathId::from_path(path);
        assert_eq!(id.as_bytes(), b"src/main.rs");
    }

    #[test]
    fn display_returns_utf8_string_for_ascii_path() {
        let path = Path::new("src/lib.rs");
        let id = PathId::from_path(path);
        assert_eq!(id.to_display().as_ref(), "src/lib.rs");
    }

    #[test]
    fn from_bytes_roundtrips() {
        let bytes = b"src/foo.ts".to_vec();
        let id = PathId::from_bytes(bytes.clone());
        assert_eq!(id.as_bytes(), bytes);
    }

    #[test]
    #[cfg(unix)]
    fn non_utf8_path_preserves_bytes() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let raw = b"bad-\xff-bytes.ts";
        let os_str = OsStr::from_bytes(raw);
        let path = Path::new(os_str);
        let id = PathId::from_path(path);
        assert_eq!(
            id.as_bytes(),
            raw,
            "non-UTF-8 bytes must round-trip unchanged"
        );
    }
}
