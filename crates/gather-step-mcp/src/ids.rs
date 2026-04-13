use gather_step_core::NodeId;

pub fn encode_node_id(node_id: NodeId) -> String {
    let mut output = String::with_capacity(32);
    for byte in node_id.as_bytes() {
        output.push(nibble_to_hex(byte >> 4));
        output.push(nibble_to_hex(byte & 0x0f));
    }
    output
}

pub fn decode_node_id(input: &str) -> Result<NodeId, String> {
    if input.len() != 32 {
        return Err(format!(
            "symbol_id must be a 32-character lowercase hex string, got length {}",
            input.len()
        ));
    }

    let mut bytes = [0_u8; 16];
    let chars = input.as_bytes();
    for (index, chunk) in chars.chunks_exact(2).enumerate() {
        let high = hex_to_nibble(chunk[0])?;
        let low = hex_to_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }

    Ok(NodeId(bytes))
}

fn nibble_to_hex(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + (value - 10)),
        _ => unreachable!("nibble out of range"),
    }
}

fn hex_to_nibble(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(format!(
            "invalid hex character `{}` in symbol_id",
            char::from(value)
        )),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{decode_node_id, encode_node_id};
    use gather_step_core::NodeId;

    #[test]
    fn round_trips_node_id_hex() {
        let node_id = NodeId([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba,
            0xdc, 0xfe,
        ]);
        let encoded = encode_node_id(node_id);
        assert_eq!(encoded, "0123456789abcdef1032547698badcfe");
        assert_eq!(
            decode_node_id(&encoded).expect("hex should decode"),
            node_id
        );
    }
}
