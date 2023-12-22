use serde::{Deserialize, Serialize};

/// The protocol version sent from the client to the server on the events stream.
/// This is compared on initial connection between client and server.
/// If the event/bulk definitions change, then this should change.
pub const PROTOCOL_VERSION: u64 = 4;

/// An initial handshake message sent from the client to the server on the events stream.
/// If the server doesn't support the provided version value, it can cut off the connection early.
/// The intent is for the structure of this message to never change.
#[derive(Debug, Deserialize, Serialize)]
pub struct VersionBootstrapMessage {
    pub version: u64,
}
