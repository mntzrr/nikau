use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Capacity (in whole frames) of the per-connection bulk writer queue, on
/// both the server (per client) and the client. Each queued blob is one
/// complete frame — a serialized header glued to its payload — so nothing is
/// ever dropped mid-message. The bound keeps a peer that stops draining from
/// queueing clipboard payloads (potentially megabytes each) without limit;
/// a full queue fails the send fast, and the sender drops the CONNECTION,
/// exactly like a write failure: a peer that isn't draining would die on the
/// QUIC idle timeout anyway. Event loops never block on the queue.
pub const BULK_QUEUE_CAPACITY: usize = 4;

/// A serialized bulk message sent from the server to the client.
/// This is sent on a separate 'bulk' stream from the main 'events' stream, to avoid blocking events.
#[derive(Debug, Deserialize, Serialize)]
pub enum ServerBulk<'a> {
    /// Request for clipboard content of the specified type from the client.
    #[serde(borrow)]
    ClipboardRequest(ServerClipboardRequest<'a>),

    /// Sends requested clipboard contents to the client.
    #[serde(borrow)]
    ClipboardHeader(ServerClipboardHeader<'a>),
}

impl<'a> std::fmt::Display for ServerBulk<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ServerBulk::ClipboardRequest(e) => e.fmt(f),
            ServerBulk::ClipboardHeader(e) => e.fmt(f),
        }
    }
}

/// A serialized bulk message sent either from the client to the server.
/// This is sent on a separate 'bulk' stream from the main 'events' stream, to avoid blocking events.
#[derive(Debug, Deserialize, Serialize)]
pub enum ClientBulk<'a> {
    /// Request for clipboard content of the specified type from the server (may then route to a client).
    #[serde(borrow)]
    ClipboardRequest(ClientClipboardRequest<'a>),

    /// Sends requested clipboard contents to the server.
    #[serde(borrow)]
    ClipboardHeader(ClientClipboardHeader<'a>),
}

impl<'a> std::fmt::Display for ClientBulk<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClientBulk::ClipboardRequest(e) => e.fmt(f),
            ClientBulk::ClipboardHeader(e) => e.fmt(f),
        }
    }
}

// ServerClipboardRequest

/// Request to retrieve a previously advertised clipboard, sent from the server to a client
#[derive(Debug, Deserialize, Serialize)]
pub struct ServerClipboardRequest<'a> {
    /// The desired type to be retrieved from the client,
    /// from a prior ClipboardTypes event advertised by the client
    pub requested_type: &'a str,

    /// Request that any sent clipboards not exceed this size
    pub max_size_bytes: u64,

    /// The client that requested the clipboard, or None if it was the server.
    /// Used by the server to route the clipboard back to the requestor.
    pub request_client: Option<SocketAddr>,

    /// Correlates this request with its response.
    /// A plain per-originator counter: the goal is accidental-misdelivery
    /// protection, not adversarial resistance.
    pub request_id: u64,
}

impl<'a> std::fmt::Display for ServerClipboardRequest<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ServerClipboardRequest(requested_type={}, max_size_bytes={}, request_client={:?}, request_id={})",
                self.requested_type, self.max_size_bytes, self.request_client, self.request_id,
            )
            .as_str(),
        )
    }
}

// ServerClipboardHeader

/// Metadata about requested clipboard content which follows, sent from the server to a client
#[derive(Debug, Deserialize, Serialize)]
pub struct ServerClipboardHeader<'a> {
    /// The mime type that had originally been requested in the ClientClipboardRequest
    pub requested_type: &'a str,

    /// The actual type being returned, or None if it matches requested_type.
    /// This is used for sending compressed or packaged payloads as needed for some types.
    pub data_type: Option<&'a str>,

    /// The length of the clipboard content that follows this header
    pub content_len_bytes: u64,

    /// Correlates this response with its request.
    /// A plain per-originator counter: the goal is accidental-misdelivery
    /// protection, not adversarial resistance.
    pub request_id: u64,
}

impl<'a> std::fmt::Display for ServerClipboardHeader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ServerClipboardHeader(requested_type={}, data_type={:?}, content_len_bytes={}, request_id={})",
                self.requested_type, self.data_type, self.content_len_bytes, self.request_id,
            )
            .as_str(),
        )
    }
}

// ClientClipboardRequest

/// Request to retrieve a previously advertised clipboard, sent from a client to the server
#[derive(Debug, Deserialize, Serialize)]
pub struct ClientClipboardRequest<'a> {
    /// The desired type to be retrieved from the server,
    /// from a prior ClipboardTypes event adverstised by the server
    pub requested_type: &'a str,

    /// Request that any sent clipboards not exceed this size
    pub max_size_bytes: u64,

    /// Correlates this request with its response.
    /// A plain per-originator counter: the goal is accidental-misdelivery
    /// protection, not adversarial resistance.
    pub request_id: u64,
}

impl<'a> std::fmt::Display for ClientClipboardRequest<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ClientClipboardRequest(requested_type={}, max_size_bytes={}, request_id={})",
                self.requested_type, self.max_size_bytes, self.request_id,
            )
            .as_str(),
        )
    }
}

// ClientClipboardHeader

/// Metadata about requested clipboard content which follows, sent from a client to the server
#[derive(Debug, Deserialize, Serialize)]
pub struct ClientClipboardHeader<'a> {
    /// The mime type that had originally been requested in the ServerClipboardRequest
    pub requested_type: &'a str,

    /// The actual type being returned, or None if it matches requested_type.
    /// This is used for sending compressed or packaged payloads as needed for some types.
    pub data_type: Option<&'a str>,

    /// The length of the clipboard content that follows this header
    pub content_len_bytes: u64,

    /// The client that requested the clipboard, or None if it was the server.
    /// Copied from the preceding ServerClipboardRequest
    pub request_client: Option<SocketAddr>,

    /// Correlates this response with its request.
    /// Copied from the preceding ServerClipboardRequest.
    pub request_id: u64,
}

impl<'a> std::fmt::Display for ClientClipboardHeader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ClientClipboardHeader(requested_type={}, data_type={:?}, content_len_bytes={}, request_client={:?}, request_id={})",
                self.requested_type, self.data_type, self.content_len_bytes, self.request_client, self.request_id,
            )
            .as_str(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trips all four clipboard messages, checking that request_id survives.
    #[test]
    fn clipboard_messages_roundtrip_request_id() {
        let client: SocketAddr = "192.0.2.1:1213".parse().unwrap();

        let msg = ClientBulk::ClipboardRequest(ClientClipboardRequest {
            requested_type: "text/plain",
            max_size_bytes: 1024,
            request_id: 7,
        });
        let mut bytes = postcard::to_stdvec_cobs(&msg).unwrap();
        let (decoded, _) = postcard::take_from_bytes_cobs::<ClientBulk>(&mut bytes).unwrap();
        match decoded {
            ClientBulk::ClipboardRequest(c) => {
                assert_eq!(c.requested_type, "text/plain");
                assert_eq!(c.max_size_bytes, 1024);
                assert_eq!(c.request_id, 7);
            }
            other => panic!("wrong variant: {}", other),
        }

        let msg = ClientBulk::ClipboardHeader(ClientClipboardHeader {
            requested_type: "text/plain",
            data_type: Some("zstd"),
            content_len_bytes: 100,
            request_client: Some(client),
            request_id: 8,
        });
        let mut bytes = postcard::to_stdvec_cobs(&msg).unwrap();
        let (decoded, _) = postcard::take_from_bytes_cobs::<ClientBulk>(&mut bytes).unwrap();
        match decoded {
            ClientBulk::ClipboardHeader(c) => {
                assert_eq!(c.data_type, Some("zstd"));
                assert_eq!(c.content_len_bytes, 100);
                assert_eq!(c.request_client, Some(client));
                assert_eq!(c.request_id, 8);
            }
            other => panic!("wrong variant: {}", other),
        }

        let msg = ServerBulk::ClipboardRequest(ServerClipboardRequest {
            requested_type: "text/plain",
            max_size_bytes: 2048,
            request_client: None,
            request_id: 9,
        });
        let mut bytes = postcard::to_stdvec_cobs(&msg).unwrap();
        let (decoded, _) = postcard::take_from_bytes_cobs::<ServerBulk>(&mut bytes).unwrap();
        match decoded {
            ServerBulk::ClipboardRequest(c) => {
                assert_eq!(c.max_size_bytes, 2048);
                assert_eq!(c.request_client, None);
                assert_eq!(c.request_id, 9);
            }
            other => panic!("wrong variant: {}", other),
        }

        let msg = ServerBulk::ClipboardHeader(ServerClipboardHeader {
            requested_type: "text/plain",
            data_type: None,
            content_len_bytes: 50,
            request_id: 10,
        });
        let mut bytes = postcard::to_stdvec_cobs(&msg).unwrap();
        let (decoded, _) = postcard::take_from_bytes_cobs::<ServerBulk>(&mut bytes).unwrap();
        match decoded {
            ServerBulk::ClipboardHeader(c) => {
                assert_eq!(c.data_type, None);
                assert_eq!(c.content_len_bytes, 50);
                assert_eq!(c.request_id, 10);
            }
            other => panic!("wrong variant: {}", other),
        }
    }
}
