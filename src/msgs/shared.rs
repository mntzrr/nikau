use serde::{Deserialize, Serialize};

/// The protocol version exchanged between client and server on each stream.
/// This is compared on initial connection between client and server.
/// If the event/bulk definitions change, then this should change.
pub const PROTOCOL_VERSION: u64 = 7;

/// An initial handshake message exchanged between client and server on each stream.
/// If the peer doesn't support the provided version value, it can cut off the connection early.
/// The intent is for the structure of this message to never change.
#[derive(Debug, Deserialize, Serialize)]
pub struct VersionBootstrapMessage {
    pub version: u64,
}

/// Returns true if `buf` contains at least one complete COBS frame.
/// COBS-encoded frames never contain a 0x00 byte and are terminated by one 0x00,
/// so the first 0x00 marks the end of the current frame.
pub fn has_complete_cobs_frame(buf: &[u8]) -> bool {
    buf.contains(&0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cobs_frame_detection() {
        assert!(!has_complete_cobs_frame(&[]));
        assert!(!has_complete_cobs_frame(&[1, 2, 3]));
        assert!(has_complete_cobs_frame(&[1, 2, 0]));
        assert!(has_complete_cobs_frame(&[1, 0, 5, 6, 0]));
    }
}
