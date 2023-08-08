use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

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
}

impl<'a> std::fmt::Display for ServerClipboardRequest<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ServerClipboardRequest(requested_type={}, max_size_bytes={}, request_client={:?})",
                self.requested_type, self.max_size_bytes, self.request_client,
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
}

impl<'a> std::fmt::Display for ServerClipboardHeader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ServerClipboardHeader(requested_type={}, data_type={:?}, content_len_bytes={})",
                self.requested_type, self.data_type, self.content_len_bytes,
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
}

impl<'a> std::fmt::Display for ClientClipboardRequest<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ClientClipboardRequest(requested_type={}, max_size_bytes={})",
                self.requested_type, self.max_size_bytes,
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
}

impl<'a> std::fmt::Display for ClientClipboardHeader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ClientClipboardHeader(requested_type={}, data_type={:?}, content_len_bytes={}, request_client={:?})",
                self.requested_type, self.data_type, self.content_len_bytes, self.request_client,
            )
            .as_str(),
        )
    }
}
