use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use quinn::{RecvStream, SendStream};
use tokio::sync::Mutex;
use tokio::task;
use tracing::{debug, error, info, trace, warn};

use crate::clipboard::{client, data};
use crate::device::output;
use crate::msgs::{bulk, event, shared};
use crate::network::{approval, transport};

/// Initializes a new client connection and runs its event loop.
/// Returns an error on connection failure or other logic error, in which case a new connection can be tried.
pub async fn run<O: output::OutputHandler>(
    server_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification<'static>>,
    max_clipboard_size_bytes: u64,
    local_clipboard: &mut Option<client::LocalClipboard>,
    output_handler: &mut O,
    mode: transport::NetworkMode,
) -> Result<()> {
    let (mut client, connect_time) =
        Connection::new(server_addr, cert_verifier, max_clipboard_size_bytes, mode).await?;
    loop {
        client
            .step(local_clipboard, output_handler, &connect_time)
            .await?
    }
}

struct Connection {
    events_send: SendStream,
    events_recv: RecvStream,
    /// Shared with spawned clipboard-serving tasks so that large clipboard
    /// writes never block this loop from applying incoming input events.
    /// The lock is always held across header+payload writes, keeping the
    /// stream framing intact when transfers overlap.
    bulk_send: Arc<Mutex<SendStream>>,
    bulk_recv: RecvStream,
    max_clipboard_size_bytes: u64,

    active: bool,

    /// Reusable buffer for receiving input events.
    event_bytes: Vec<u8>,
    /// Reusable buffer for receiving bulk data (clipboards).
    bulk_recv_bytes: Vec<u8>,

    /// Accumulator of raw clipboard data streamed from the server, plus the fetch
    /// it answers (None when the header's request_id was unknown: the payload is
    /// still swallowed to keep stream framing, but never delivered to a fetch).
    /// Cleared when the clipboard data has all been received.
    incoming_clipboard_data: Option<(data::ClipboardData, Option<data::ClipboardFetch>)>,

    /// Pending fetch requests for this connection, keyed by request id.
    pending_fetches: HashMap<u64, data::ClipboardFetch>,
    /// Next fetch request id. Wrapping is fine: ids only need to correlate a
    /// reply with its request, not resist adversaries.
    next_fetch_id: u64,
}

impl Connection {
    /// Connects to the specified server, or returns an error if the connection fails.
    async fn new(
        server_addr: &SocketAddr,
        cert_verifier: Arc<approval::NikauCertVerification<'static>>,
        max_clipboard_size_bytes: u64,
        mode: transport::NetworkMode,
    ) -> Result<(Self, Instant)> {
        let bind_addr: SocketAddr = match server_addr {
            SocketAddr::V4(_) => "0.0.0.0:0".parse().expect("Failed to parse 0.0.0.0:0"),
            SocketAddr::V6(_) => "[::]:0".parse().expect("Failed to parse [::]:0"),
        };
        let client_endpoint = transport::build_client(&bind_addr, cert_verifier, mode)?;
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
        let (mut events_send, mut events_recv) = conn
            .open_bi()
            .await
            .context("Failed to initialize events stream")?;

        // Exchange versions with the server; either side closes the connection
        // if it can't support the other's version.
        let mut event_bytes = Vec::with_capacity(1024);
        transport::send_version(&mut events_send).await?;
        transport::recv_version(&mut events_recv, &mut event_bytes).await?;

        let (mut bulk_send, mut bulk_recv) = conn
            .open_bi()
            .await
            .context("Failed to initialize bulk stream")?;

        // Exchange versions again via the bulk stream.
        // This is required in order to initialize the bulk stream,
        // otherwise the server times out waiting for the stream to open.
        transport::send_version(&mut bulk_send).await?;
        transport::recv_version(&mut bulk_recv, &mut event_bytes).await?;

        Ok((
            Self {
                events_send,
                events_recv,
                bulk_send: Arc::new(Mutex::new(bulk_send)),
                bulk_recv,
                max_clipboard_size_bytes,
                active: false,
                event_bytes,
                bulk_recv_bytes: Vec::with_capacity(65536),
                incoming_clipboard_data: None,
                pending_fetches: HashMap::new(),
                next_fetch_id: 0,
            },
            connect_time,
        ))
    }

    /// Performs a step of the client event loop, returning an error if the connection should be retried.
    async fn step<O: output::OutputHandler>(
        &mut self,
        local_clipboard: &mut Option<client::LocalClipboard>,
        output_handler: &mut O,
        connect_time: &Instant,
    ) -> Result<()> {
        if let Some(local_clipboard) = local_clipboard {
            // Local clipboard enabled: Include watching for local clipboard events
            tokio::select! {
                local_fetch_request = local_clipboard.clipboard_fetch_rx.recv() => {
                    // Send fetch request to server, keep request and its nested oneshot for handling the response
                    if let Some(local_fetch_request) = local_fetch_request {
                        let request_id = self.next_fetch_id;
                        self.next_fetch_id = self.next_fetch_id.wrapping_add(1);
                        let msg = bulk::ClientBulk::ClipboardRequest(bulk::ClientClipboardRequest{
                            requested_type: &local_fetch_request.requested_type,
                            max_size_bytes: self.max_clipboard_size_bytes as u64,
                            request_id,
                        });
                        let serializedmsg = postcard::to_stdvec_cobs(&msg)
                            .map_err(|e| anyhow!("Failed to serialize clipboard request message: {:?}", e))?;
                        trace!(
                            "Sending {} byte clipboard content request: {:X?}",
                            serializedmsg.len(),
                            &serializedmsg
                        );
                        // Drop fetches whose requester already timed out, then track this one.
                        self.pending_fetches.retain(|_, f| !f.fetch_result_tx.is_closed());
                        self.pending_fetches.insert(request_id, local_fetch_request);
                        // May wait briefly behind an in-flight clipboard payload
                        // write; that delays only clipboard traffic, never input.
                        self.bulk_send.lock().await.write_all(&serializedmsg)
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
                        local_clipboard.set_local_clipboard().await;
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
                    self.handle_event_messages(Some(local_clipboard), output_handler).await?;
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
                    // Don't log the bytes themselves, there can be a lot for larger file copies
                    trace!("Received {} bytes from bulk stream", resp.bytes.len());
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
                    self.handle_event_messages(None, output_handler).await?;
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

    async fn handle_event_messages<O: output::OutputHandler>(
        &mut self,
        mut local_clipboard: Option<&mut client::LocalClipboard>,
        output_handler: &mut O,
    ) -> Result<()> {
        let mut offset = 0;
        let bytes_len = self.event_bytes.len();
        while offset < self.event_bytes.len() {
            // A partial frame (no COBS terminator yet) is kept for the next chunk.
            if !shared::has_complete_cobs_frame(&self.event_bytes[offset..]) {
                break;
            }
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
                    self.active = e.enabled;
                    if !e.enabled {
                        // This client was deactivated: release any held keys so they
                        // don't stay stuck on the virtual devices.
                        output_handler.release_all().await?;
                    }
                    if let Some(local_clipboard) = &mut local_clipboard {
                        if let Some(types) = &local_clipboard.get_local_clipboard_types() {
                            if !e.enabled && !types.is_empty() {
                                // We're being disabled and we have a clipboard from a local app.
                                // It may be from when we were disabled, or from a prior enabled session. That's fine.
                                // Keep announcing the local clipboard until/unless it gets overridden by a new one from the server.
                                let types = types.join(" ");
                                debug!("Sending clipboard types to server: {}", types);
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
                event::ServerEvent::Input(events) => {
                    // User input events
                    output_handler.write(events).await?;
                }
                event::ServerEvent::ClipboardTypes(types) => {
                    // Receiving types announcement from server (following recent activation)
                    // Announce the types to X11 for local apps to see, and clear any prior types from local apps.
                    if let Some(local_clipboard) = &mut local_clipboard {
                        debug!("Got clipboard types from server: {}", types.types);
                        let types: Vec<String> =
                            types.types.split(' ').map(|s| s.to_string()).collect();
                        local_clipboard.set_remote_clipboard(types)?;
                    } else {
                        debug!("Ignoring clipboard types from server: {}", types.types);
                    }
                }
            }
            offset += consumed;
        }
        // Retain any unconsumed partial frame for the next chunk.
        self.event_bytes.drain(..offset);
        Ok(())
    }

    async fn handle_bulk_data_or_messages(
        &mut self,
        local_clipboard: Option<&mut client::LocalClipboard>,
        resp_bytes: Bytes,
    ) -> Result<()> {
        if let Some((c, _fetch)) = &mut self.incoming_clipboard_data {
            // Clipboard data streaming is in progress. The message should be raw clipboard data.
            if c.remaining_bytes >= resp_bytes.len() {
                // This chunk should entirely be raw clipboard data.
                c.bytes.extend_from_slice(&resp_bytes);
                c.remaining_bytes -= resp_bytes.len();
            } else {
                // Chunk contains more data than expected for the clipboard entry.
                // Finish the clipboard entry, then save the remainder to bulk_recv_bytes for processing below.
                c.bytes
                    .extend_from_slice(&(*resp_bytes)[..c.remaining_bytes]);
                self.bulk_recv_bytes
                    .extend_from_slice(&(*resp_bytes)[c.remaining_bytes..]);
                c.remaining_bytes = 0;
            }

            if c.remaining_bytes == 0 {
                // Raw clipboard data has all been accumulated, send it to the pending fetch.
                // Pass ownership of the data to the writer and clear local state.
                let (d, fetch) = self
                    .incoming_clipboard_data
                    .take()
                    .expect("Just checked data was present");
                if let Some(waiting_clipboard_fetch) = fetch {
                    if let Err(_d_again) = waiting_clipboard_fetch.fetch_result_tx.send(d) {
                        warn!("Discarding clipboard data from server: the requesting paste already timed out");
                    }
                }
                // fetch=None: unknown request_id (debug-logged when the header arrived);
                // the payload was only swallowed to keep the stream framed.
            }

            if !self.bulk_recv_bytes.is_empty() {
                // Handle any data/messages following the raw clipboard dump.
                if let Some(updated_clipboard_data) =
                    self.handle_bulk_messages(local_clipboard).await?
                {
                    self.incoming_clipboard_data.replace(updated_clipboard_data);
                }
            }
        } else {
            // Not in the middle of a raw clipboard dump. Must be a postcard message.
            // Copy the immutable response data into a mutable buffer for parsing.
            self.bulk_recv_bytes.extend_from_slice(&resp_bytes);
            if let Some(updated_clipboard_data) = self.handle_bulk_messages(local_clipboard).await?
            {
                self.incoming_clipboard_data.replace(updated_clipboard_data);
            }
        }
        Ok(())
    }

    async fn handle_bulk_messages(
        &mut self,
        mut local_clipboard: Option<&mut client::LocalClipboard>,
    ) -> Result<Option<(data::ClipboardData, Option<data::ClipboardFetch>)>> {
        let mut offset = 0;
        let bytes_len = self.bulk_recv_bytes.len();
        while offset < bytes_len {
            // A partial frame (no COBS terminator yet) is kept for the next chunk.
            if !shared::has_complete_cobs_frame(&self.bulk_recv_bytes[offset..]) {
                break;
            }
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
                    // Serve the data from a spawned task: reading the local
                    // clipboard (zipping large copied files can take seconds) and
                    // writing a big payload must not stall input event handling.
                    let reader = local_clipboard.reader_handle();
                    let bulk_send = self.bulk_send.clone();
                    let requested_type = c.requested_type.to_string();
                    let max_size_bytes = c.max_size_bytes;
                    let request_client = c.request_client;
                    let request_id = c.request_id;
                    task::spawn(async move {
                        // Read the clipboard data from the local application.
                        // On read failure or timeout, reply with an empty header so that the requester just sets nothing.
                        // The read must always answer within the 5s fetch timeout on the requester side.
                        let (local_clipboard_data, data_type) = match tokio::time::timeout(
                            Duration::from_secs(4),
                            client::LocalClipboard::read(&reader, &requested_type, max_size_bytes, request_client),
                        )
                        .await
                        {
                            Ok(Ok(result)) => result,
                            Ok(Err(e)) => {
                                warn!("Failed to read local clipboard of type {}: {:?}", requested_type, e);
                                (Vec::new(), None)
                            }
                            Err(_) => {
                                warn!("Timed out after 4s reading local clipboard of type {}", requested_type);
                                (Vec::new(), None)
                            }
                        };
                        let msg = bulk::ClientBulk::ClipboardHeader(bulk::ClientClipboardHeader {
                            requested_type: &requested_type,
                            data_type: data_type.as_ref().map(|t| t.as_str()),
                            content_len_bytes: local_clipboard_data.len() as u64,
                            request_client,
                            request_id,
                        });
                        let mut bytes = match postcard::to_stdvec_cobs(&msg) {
                            Ok(b) => b,
                            Err(e) => {
                                error!("Failed to serialize clipboard header message: {:?}", e);
                                return;
                            }
                        };
                        bytes.extend_from_slice(&local_clipboard_data);
                        // Hold the lock across header+payload so overlapping
                        // transfers can't interleave on the stream.
                        let mut send = bulk_send.lock().await;
                        if let Err(e) = send.write_all(&bytes).await {
                            // A broken stream also fails the step loop's read
                            // side, which resets the connection.
                            error!("Failed to send {} byte clipboard content: {:?}", bytes.len(), e);
                        }
                    });
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
                    }
                    // Correlate the response with its request. A response with an unknown
                    // id (e.g. its fetch already timed out) is still consumed to keep the
                    // stream framed, but is never delivered to a different fetch.
                    let fetch = self.pending_fetches.remove(&c.request_id);
                    if fetch.is_none() {
                        debug!("Discarding clipboard data for unknown request_id={}", c.request_id);
                    }
                    if c.content_len_bytes as usize <= resp_remainder.len() {
                        // The clipboard content fits fully within resp_remainder, send it to the pending fetch.
                        // Mark content as consumed and continue looping in case another message follows.
                        if let Some(waiting_clipboard_fetch) = fetch {
                            let mut bytes = Vec::with_capacity(c.content_len_bytes as usize);
                            bytes
                                .extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                            let d = data::ClipboardData {
                                requested_type: c.requested_type.to_string(),
                                data_type: c.data_type.map(|t| t.to_string()),
                                bytes,
                                remaining_bytes: 0,
                            };
                            if let Err(_d_again) = waiting_clipboard_fetch.fetch_result_tx.send(d) {
                                warn!("Discarding clipboard data from server: the requesting paste already timed out");
                            }
                        }
                        offset += c.content_len_bytes as usize;
                    } else {
                        // Need to collect more data.
                        // Save what we've got so far, and assign remaining_bytes to what's left.
                        let mut payload = Vec::with_capacity(c.content_len_bytes as usize);
                        payload.extend_from_slice(resp_remainder);
                        let d = data::ClipboardData {
                            requested_type: c.requested_type.to_string(),
                            data_type: c.data_type.map(|t| t.to_string()),
                            bytes: payload,
                            remaining_bytes: c.content_len_bytes as usize - resp_remainder.len(),
                        };
                        // All bytes were consumed (into the pending clipboard data).
                        self.bulk_recv_bytes.clear();
                        return Ok(Some((d, fetch)));
                    }
                }
            }
        }
        // Retain any unconsumed partial frame for the next chunk.
        self.bulk_recv_bytes.drain(..offset);
        Ok(None)
    }
}

fn is_new_connection(connect_time: &Instant) -> bool {
    Instant::now().duration_since(*connect_time) < Duration::from_secs(5)
}
