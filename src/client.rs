use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use quinn::{RecvStream, SendStream};
use tokio::sync::{mpsc, watch};
use tracing::{info, trace, warn};

use crate::device::output;
use crate::msgs::{bulk, event};
use crate::network::{approval, transport};
use crate::x11clipboard::{
    reader::{ClipboardReader, ClipboardTypeWatcher},
    writer::{ClipboardFetch, ClipboardWriter},
    ClipboardData,
};

pub struct LocalClipboard {
    reader: ClipboardReader,
    pub writer: ClipboardWriter,
    fetch_rx: mpsc::Receiver<ClipboardFetch>,
    local_types_rx: watch::Receiver<Vec<String>>,
    local_types: Option<Vec<String>>,
    pub serving_remote_clipboard: bool,
}

impl LocalClipboard {
    pub async fn new() -> Result<Self> {
        let (fetch_tx, fetch_rx) = mpsc::channel(32);
        let reader = ClipboardReader::new().await?;
        let (local_types_tx, local_types_rx) = watch::channel(vec![]);
        ClipboardTypeWatcher::start(local_types_tx).await?;
        let writer = ClipboardWriter::start(fetch_tx).await?;
        Ok(Self {
            reader,
            writer,
            fetch_rx,
            local_types_rx,
            local_types: None,
            serving_remote_clipboard: false,
        })
    }

    pub async fn clear_remote_clipboard(&mut self) -> Result<()> {
        if self.serving_remote_clipboard {
            self.local_types = None;
            self.serving_remote_clipboard = false;
            self.writer.store_types(vec![])?;
        }
        Ok(())
    }
}

/// Initializes a new client connection and runs its event loop.
/// Returns an error on connection failure or other logic error, in which case a new connection can be tried.
pub async fn run(
    server_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
    max_clipboard_size_bytes: u64,
    local_clipboard: &mut Option<LocalClipboard>,
    virtual_devices: &mut output::VirtualDevices,
) -> Result<()> {
    let (mut client, connect_time) =
        Connection::new(server_addr, cert_verifier, max_clipboard_size_bytes).await?;
    loop {
        client
            .step(local_clipboard, virtual_devices, &connect_time)
            .await?
    }
}

struct Connection {
    events_send: SendStream,
    events_recv: RecvStream,
    bulk_send: SendStream,
    bulk_recv: RecvStream,
    max_clipboard_size_bytes: u64,

    active: bool,

    /// Reusable buffer for receiving keyboard events.
    event_bytes: Vec<u8>,
    /// Reusable buffer for receiving bulk data (clipboards).
    bulk_recv_bytes: Vec<u8>,

    /// Accumulator of raw clipboard data streamed from the server.
    /// Cleared when the clipboard data has all been received.
    incoming_clipboard_data: Option<ClipboardData>,

    /// Pending fetch request for this connection, if any.
    waiting_clipboard_fetch: Option<ClipboardFetch>,
}

impl Connection {
    /// Connects to the specified server, or returns an error if the connection fails.
    async fn new(
        server_addr: &SocketAddr,
        cert_verifier: Arc<approval::NikauCertVerification>,
        max_clipboard_size_bytes: u64,
    ) -> Result<(Self, Instant)> {
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("Failed to parse 0.0.0.0:0");
        let client_endpoint = transport::build_client(&bind_addr, cert_verifier)?;
        // Connect to server, our custom cert verifiers result in server_name being ignored
        let conn = client_endpoint
            .connect(*server_addr, "__ignored__")?
            .await?;
        info!(
            "Connected to server: {} (from local endpoint {})",
            conn.remote_address(),
            // IP is typically 0.0.0.0 but the local port should be there at least
            client_endpoint
                .local_addr()
                .map_or("<unknown endpoint>".to_string(), |s| s.to_string())
        );
        let connect_time = Instant::now();
        let (mut events_send, events_recv) = conn
            .open_bi()
            .await
            .context("Failed to initialize events stream")?;

        // Send version to server, who will close the connection if they can't support it.
        transport::send_version(&mut events_send).await?;

        let (mut bulk_send, bulk_recv) = conn
            .open_bi()
            .await
            .context("Failed to initialize bulk stream")?;

        // Send version to server again via the bulk stream.
        // This is required in order to initialize the bulk stream,
        // otherwise the server times out waiting for the stream to open.
        transport::send_version(&mut bulk_send).await?;

        Ok((
            Self {
                events_send,
                events_recv,
                bulk_send,
                bulk_recv,
                max_clipboard_size_bytes,
                active: false,
                event_bytes: Vec::with_capacity(1024),
                bulk_recv_bytes: Vec::with_capacity(65536),
                incoming_clipboard_data: None,
                waiting_clipboard_fetch: None,
            },
            connect_time,
        ))
    }

    /// Performs a step of the client event loop, returning an error if the connection should be retried.
    async fn step(
        &mut self,
        local_clipboard: &mut Option<LocalClipboard>,
        virtual_devices: &mut output::VirtualDevices,
        connect_time: &Instant,
    ) -> Result<()> {
        if let Some(local_clipboard) = local_clipboard {
            // Local clipboard enabled: Include watching for local clipboard events
            tokio::select! {
                local_fetch_request = local_clipboard.fetch_rx.recv() => {
                    // Send fetch request to server, keep request and its nested oneshot for handling the response
                    if let Some(local_fetch_request) = local_fetch_request {
                        let msg = bulk::ClientBulk::ClipboardRequest(bulk::ClientClipboardRequest{
                            requested_type: &local_fetch_request.requested_type,
                            max_size_bytes: self.max_clipboard_size_bytes as u64,
                        });
                        let serializedmsg = postcard::to_stdvec_cobs(&msg)
                            .map_err(|e| anyhow!("Failed to serialize clipboard request message: {:?}", e))?;
                        trace!(
                            "Sending {} byte clipboard content request: {:X?}",
                            serializedmsg.len(),
                            &serializedmsg
                        );
                        if let Some(old_fetch_request) = &self.waiting_clipboard_fetch.replace(local_fetch_request) {
                            warn!("Overwriting prior fetch request for type={}", old_fetch_request.requested_type);
                        }
                        self.bulk_send.write_all(&serializedmsg)
                            .await
                            .context("Failed to send clipboard request message")?;
                    } else {
                        bail!("Clipboard fetch request queue has closed");
                    }
                },
                types_notify = local_clipboard.local_types_rx.changed() => {
                    // Local machine has a new clipboard entry.
                    // If we're currently active, then store it until we're deactivated by a switch.
                    // Ignore clipboard changes when inactive: Avoid polluting the rotation with "external" clipboards.
                    if let Err(e) = types_notify {
                        warn!("local_types_rx is closed: {:?}", e);
                        return Err(anyhow!(e));
                    }
                    if self.active {
                        local_clipboard.local_types.replace(local_clipboard.local_types_rx.borrow().clone());
                        // Now that we have a local clipboard, don't fetch clipboards from the server.
                        local_clipboard.serving_remote_clipboard = false;
                    }
                },
                event_result = self.events_recv.read_chunk(1024, true) => {
                    // Incoming data may contain one or more messages, but I've never seen fragments of messages.
                    let resp = event_result
                        .with_context(|| if is_new_connection(connect_time) {
                            // Additional help for typical behavior when setting things up
                            "Lost events connection, does server need to approve our fingerprint?"
                        } else {
                            "Lost events connection"
                        })?
                        .context("Server closed events connection")?;
                    trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                    // Copy the immutable response data into a mutable buffer
                    self.event_bytes.extend_from_slice(&resp.bytes);
                    self.handle_event_messages(Some(local_clipboard), virtual_devices).await?;
                    self.event_bytes.clear();
                },
                bulk_result = self.bulk_recv.read_chunk(65536, true) => {
                    let resp = bulk_result
                        .with_context(|| if is_new_connection(connect_time) {
                            // Additional help for typical behavior when setting things up
                            "Lost bulk connection, does server need to approve our fingerprint?"
                        } else {
                            "Lost bulk connection"
                        })?
                        .context("Server closed bulk connection")?;
                    trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                    self.handle_bulk_data_or_messages(Some(local_clipboard), resp.bytes).await?;
                },
            }
        } else {
            // Local clipboard disabled: Don't select on local clipboard events
            tokio::select! {
                event_result = self.events_recv.read_chunk(1024, true) => {
                    // Incoming data may contain one or more messages, but I've never seen fragments of messages.
                    let resp = event_result
                        .with_context(|| if is_new_connection(connect_time) {
                            "Lost events connection, does server need to approve our fingerprint?"
                        } else {
                            "Lost events connection"
                        })?
                        .context("Server closed events connection")?;
                    trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                    // Copy the immutable response data into a mutable buffer
                    self.event_bytes.extend_from_slice(&resp.bytes);
                    self.handle_event_messages(None, virtual_devices).await?;
                    self.event_bytes.clear();
                },
                bulk_result = self.bulk_recv.read_chunk(65536, true) => {
                    let resp = bulk_result
                        .with_context(|| if is_new_connection(connect_time) {
                            "Lost bulk connection, does server need to approve our fingerprint?"
                        } else {
                            "Lost bulk connection"
                        })?
                        .context("Server closed bulk connection")?;
                    trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                    self.handle_bulk_data_or_messages(None, resp.bytes).await?;
                },
            }
        }
        Ok(())
    }

    async fn handle_event_messages(
        &mut self,
        mut local_clipboard: Option<&mut LocalClipboard>,
        virtual_devices: &mut output::VirtualDevices,
    ) -> Result<()> {
        let mut offset = 0;
        let bytes_len = self.event_bytes.len();
        while offset < self.event_bytes.len() {
            // Assumption: We shouldn't be getting a ServerMessage that's broken up into separate fragments
            let (msg, resp_remainder) = postcard::take_from_bytes_cobs::<event::ServerEvent>(
                &mut self.event_bytes[offset..],
            )
            .map_err(|e| anyhow!("Failed to deserialize server message: {:?}", e))?;
            let consumed = bytes_len - resp_remainder.len() - offset;
            trace!(
                "Consumed event at offset={}: {} ({} bytes)",
                offset,
                msg,
                consumed
            );
            match msg {
                event::ServerEvent::Switch(e) => {
                    info!(
                        "This client is {}",
                        if e.enabled { "active" } else { "inactive" }
                    );
                    virtual_devices.switch()?;
                    self.active = e.enabled;
                    if let Some(local_clipboard) = &mut local_clipboard {
                        if let Some(types) = &local_clipboard.local_types {
                            if !e.enabled && !types.is_empty() {
                                // We're being disabled and we have a clipboard from a local app.
                                // It may be from when we were disabled, or from a prior enabled session. That's fine.
                                // Keep announcing the local clipboard until/unless it gets overridden by a new one from the server.
                                let types = types.join(" ");
                                info!("Sending clipboard types to server: {}", types);
                                let msg =
                                    event::ClientEvent::ClipboardTypes(event::ClipboardTypes {
                                        types: &types,
                                        max_size_bytes: self.max_clipboard_size_bytes,
                                    });
                                let serializedmsg =
                                    postcard::to_stdvec_cobs(&msg).map_err(|e| {
                                        anyhow!(
                                            "Failed to serialize clipboard types message: {:?}",
                                            e
                                        )
                                    })?;
                                self.events_send
                                    .write_all(&serializedmsg)
                                    .await
                                    .context("Failed to send clipboard types message")?;
                            }
                        }
                    }
                }
                event::ServerEvent::Input(input) => {
                    // User input event
                    virtual_devices.add_event(input)?;
                }
                event::ServerEvent::ClipboardTypes(types) => {
                    // Receiving types announcement from server (following recent activation)
                    // Announce the types to X11 for local apps to see, and clear any prior types from local apps.
                    if let Some(local_clipboard) = &mut local_clipboard {
                        info!("Got clipboard types from server: {}", types.types);
                        local_clipboard.local_types = None;
                        local_clipboard.serving_remote_clipboard = true;
                        let types: Vec<String> =
                            types.types.split(' ').map(|s| s.to_string()).collect();
                        local_clipboard.writer.store_types(types)?;
                    } else {
                        info!("Ignoring clipboard types from server: {}", types.types);
                    }
                }
            }
            offset += consumed;
        }
        Ok(())
    }

    async fn handle_bulk_data_or_messages(
        &mut self,
        local_clipboard: Option<&mut LocalClipboard>,
        resp_bytes: Bytes,
    ) -> Result<()> {
        if let Some(c) = &mut self.incoming_clipboard_data {
            // Clipboard data streaming is in progress. The message should be raw clipboard data.
            if c.remaining_bytes >= resp_bytes.len() {
                // This chunk should entirely be raw clipboard data.
                c.data.extend_from_slice(&resp_bytes);
                c.remaining_bytes -= resp_bytes.len();
            } else {
                // Chunk contains more data than expected for the clipboard entry.
                // Finish the clipboard entry, then save the remainder to bulk_recv_bytes for processing below.
                c.data
                    .extend_from_slice(&(*resp_bytes)[..c.remaining_bytes]);
                self.bulk_recv_bytes
                    .extend_from_slice(&(*resp_bytes)[c.remaining_bytes..]);
                c.remaining_bytes = 0;
            }

            if c.remaining_bytes == 0 {
                // Raw clipboard data has all been accumulated, send it to the pending fetch.
                // Pass ownership of the data to the writer and clear local state.
                if let Some(waiting_clipboard_fetch) = self.waiting_clipboard_fetch.take() {
                    let d = self
                        .incoming_clipboard_data
                        .take()
                        .expect("Just checked data was present");
                    if let Err(_d_again) = waiting_clipboard_fetch.fetch_result_tx.send(d) {
                        warn!("Discarding clipboard data from server: no pending clipboard request (timed out?)");
                    }
                } else {
                    warn!("Discarding unexpected clipboard data from server: no clipboard request is pending");
                    self.incoming_clipboard_data = None;
                }
            }

            if !self.bulk_recv_bytes.is_empty() {
                // Handle any data/messages following the raw clipboard dump.
                if let Some(updated_clipboard_data) =
                    self.handle_bulk_messages(local_clipboard).await?
                {
                    self.incoming_clipboard_data.replace(updated_clipboard_data);
                }
                self.bulk_recv_bytes.clear();
            }
        } else {
            // Not in the middle of a raw clipboard dump. Must be a postcard message.
            // Copy the immutable response data into a mutable buffer for parsing.
            self.bulk_recv_bytes.extend_from_slice(&resp_bytes);
            if let Some(updated_clipboard_data) = self.handle_bulk_messages(local_clipboard).await?
            {
                self.incoming_clipboard_data.replace(updated_clipboard_data);
            }
            self.bulk_recv_bytes.clear();
        }
        Ok(())
    }

    async fn handle_bulk_messages(
        &mut self,
        mut local_clipboard: Option<&mut LocalClipboard>,
    ) -> Result<Option<ClipboardData>> {
        let mut offset = 0;
        let bytes_len = self.bulk_recv_bytes.len();
        while offset < bytes_len {
            // Assumption: We shouldn't be getting a BulkMessage that's broken up into separate fragments
            let (msg, resp_remainder) = postcard::take_from_bytes_cobs::<bulk::ServerBulk>(
                &mut self.bulk_recv_bytes[offset..],
            )
            .map_err(|e| anyhow!("Failed to deserialize bulk message: {:?}", e))?;
            let consumed = bytes_len - resp_remainder.len() - offset;
            trace!(
                "Consumed event at offset={}: {} ({} bytes)",
                offset,
                msg,
                consumed
            );
            offset += consumed;

            match msg {
                bulk::ServerBulk::ClipboardRequest(c) => {
                    let local_clipboard = match &mut local_clipboard {
                        Some(lc) => lc,
                        None => {
                            bail!("Got ClipboardRequest event from server when we don't support clipboards, resetting connection");
                        }
                    };
                    info!(
                        "Reading clipboard data for requested type {} to {}",
                        c.requested_type,
                        if let Some(c) = c.request_client {
                            format!("server for {}", c)
                        } else {
                            "server".to_string()
                        }
                    );
                    // Read the clipboard data from the local application.
                    let (local_clipboard_data, data_type) = local_clipboard
                        .reader
                        .read(c.requested_type, c.max_size_bytes, &c.request_client)
                        .await?;
                    let msg = bulk::ClientBulk::ClipboardHeader(bulk::ClientClipboardHeader {
                        requested_type: c.requested_type,
                        data_type: data_type.as_ref().map(|t| t.as_str()),
                        content_len_bytes: local_clipboard_data.len() as u64,
                        request_client: c.request_client,
                    });
                    let serializedmsg = postcard::to_stdvec_cobs(&msg).map_err(|e| {
                        anyhow!("Failed to serialize clipboard types message: {:?}", e)
                    })?;
                    self.bulk_send
                        .write_all(&serializedmsg)
                        .await
                        .context("Failed to send clipboard content header")?;
                    self.bulk_send
                        .write_all(&local_clipboard_data)
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to send {} byte clipboard content",
                                local_clipboard_data.len()
                            )
                        })?;
                }
                bulk::ServerBulk::ClipboardHeader(c) => {
                    if c.content_len_bytes > self.max_clipboard_size_bytes {
                        // The content length from the server is bigger than what we advertised.
                        // Reset the connection since this shouldn't happen to begin with.
                        bail!(
                            "Received clipboard size {} exceeds max size {}, resetting connection",
                            c.content_len_bytes,
                            self.max_clipboard_size_bytes
                        );
                    } else if c.content_len_bytes as usize <= resp_remainder.len() {
                        // The clipboard content fits fully within resp_remainder, send it to the pending fetch.
                        // Mark content as consumed and continue looping in case another message follows.
                        if let Some(waiting_clipboard_fetch) = self.waiting_clipboard_fetch.take() {
                            let mut data = Vec::with_capacity(c.content_len_bytes as usize);
                            data.extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                            let d = ClipboardData {
                                requested_type: c.requested_type.to_string(),
                                data_type: c.data_type.map(|t| t.to_string()),
                                data,
                                remaining_bytes: 0,
                            };
                            if let Err(_d_again) = waiting_clipboard_fetch.fetch_result_tx.send(d) {
                                warn!("Discarding clipboard data from server: no pending clipboard request (previous request timed out?)");
                            }
                        } else {
                            warn!("Ignoring unexpected clipboard data from server: no clipboard request is pending");
                        }
                        offset += c.content_len_bytes as usize;
                    } else {
                        // Need to collect more data.
                        // Save what we've got so far, and assign remaining_bytes to what's left.
                        let mut data = Vec::with_capacity(c.content_len_bytes as usize);
                        data.extend_from_slice(resp_remainder);
                        return Ok(Some(ClipboardData {
                            requested_type: c.requested_type.to_string(),
                            data_type: c.data_type.map(|t| t.to_string()),
                            data,
                            remaining_bytes: c.content_len_bytes as usize - resp_remainder.len(),
                        }));
                    }
                }
            }
        }
        Ok(None)
    }
}

fn is_new_connection(connect_time: &Instant) -> bool {
    Instant::now().duration_since(*connect_time) < Duration::from_secs(5)
}
