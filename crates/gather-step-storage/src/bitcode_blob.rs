use thiserror::Error;

const BITCODE_BLOB_SCHEMA: u8 = 1;
const HEADER_LEN: usize = 1 + blake3::OUT_LEN;

#[derive(Debug, Error)]
pub(crate) enum BitcodeBlobError {
    #[error("corrupt bitcode blob: payload is too short ({len} bytes)")]
    TooShort { len: usize },
    #[error("corrupt bitcode blob: schema byte {stored} is unsupported (expected {expected})")]
    UnsupportedSchema { stored: u8, expected: u8 },
    #[error("corrupt bitcode blob: checksum mismatch")]
    ChecksumMismatch,
}

pub(crate) fn wrap(payload: Vec<u8>) -> Vec<u8> {
    let checksum = blake3::hash(&payload);
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(BITCODE_BLOB_SCHEMA);
    out.extend_from_slice(checksum.as_bytes());
    out.extend_from_slice(&payload);
    out
}

pub(crate) fn unwrap(bytes: &[u8]) -> Result<&[u8], BitcodeBlobError> {
    if bytes.len() < HEADER_LEN {
        return Err(BitcodeBlobError::TooShort { len: bytes.len() });
    }
    let schema = bytes[0];
    if schema != BITCODE_BLOB_SCHEMA {
        return Err(BitcodeBlobError::UnsupportedSchema {
            stored: schema,
            expected: BITCODE_BLOB_SCHEMA,
        });
    }
    let checksum = &bytes[1..HEADER_LEN];
    let payload = &bytes[HEADER_LEN..];
    let actual = blake3::hash(payload);
    if checksum != actual.as_bytes().as_slice() {
        return Err(BitcodeBlobError::ChecksumMismatch);
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    #[test]
    fn checked_blob_rejects_tampered_payload() {
        let mut wrapped = super::wrap(bitcode::encode(&"payload"));
        let last = wrapped.last_mut().expect("payload byte should exist");
        *last ^= 0xff;

        assert!(matches!(
            super::unwrap(&wrapped),
            Err(super::BitcodeBlobError::ChecksumMismatch)
        ));
    }

    #[test]
    fn checked_blob_rejects_wrong_schema_byte() {
        let mut wrapped = super::wrap(bitcode::encode(&"payload"));
        wrapped[0] = 2;

        assert!(matches!(
            super::unwrap(&wrapped),
            Err(super::BitcodeBlobError::UnsupportedSchema { .. })
        ));
    }
}
