use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures::{select, FutureExt, StreamExt};
use quinn::SendStream;
use tracing::{info, trace};

use crate::{approval, bulkmsgs, deviceoutput, eventmsgs, transport, x11clipboard};

pub struct LocalClipboard {
    reader: x11clipboard::reader::ClipboardReader,
    pub writer: x11clipboard::writer::ClipboardWriter,
    fetch_rx: async_channel::Receiver<x11clipboard::writer::ClipboardFetch>,
    types_rx: async_channel::Receiver<Vec<String>>,
    local_types: Option<Vec<String>>,
    pub serving_remote_clipboard: bool,
}

impl LocalClipboard {
    pub async fn new() -> Result<Self> {
        let (fetch_tx, fetch_rx) = async_channel::bounded(32);
        let reader = x11clipboard::reader::ClipboardReader::new().await?;
        let (types_tx, types_rx) = async_channel::bounded(32);
        x11clipboard::reader::ClipboardTypeWatcher::start(types_tx).await?;
        let writer = x11clipboard::writer::ClipboardWriter::new(fetch_tx).await?;
        Ok(Self {
            reader,
            writer,
            fetch_rx,
            types_rx,
            local_types: None,
            serving_remote_clipboard: false,
        })
    }

    pub async fn clear_remote_clipboard(&mut self) -> Result<()> {
        if self.serving_remote_clipboard {
            self.local_types = None;
            self.serving_remote_clipboard = false;
            self.writer.store_types(vec![]).await?;
        }
        Ok(())
    }
}

pub async fn run_client(
    bind_addr: &SocketAddr,
    server_addr: &SocketAddr,
    virtual_devices: &mut deviceoutput::VirtualDevices,
    cert_verifier: Arc<approval::NikauCertVerification>,
    max_clipboard_size_bytes: u64,
    local_clipboard: &mut LocalClipboard,
) -> Result<()> {
    let client_endpoint = transport::build_client(bind_addr, cert_verifier)?;
    // Connect to server, our custom cert verifiers result in server_name being ignored
    let conn = client_endpoint
        .connect(server_addr.clone(), "__ignored__")?
        .await?;
    info!("Connected to server: {}", conn.remote_address());
    let connect_time = Instant::now();
    let is_new_connection =
        move || Instant::now().duration_since(connect_time) < Duration::from_secs(5);
    let (mut events_send, mut events_recv) = conn
        .open_bi()
        .await
        .context("Failed to initialize events stream")?;

    // Send version to server, who will close the connection if they can't support it.
    transport::send_version(&mut events_send).await?;

    let (mut bulk_send, mut bulk_recv) = conn
        .open_bi()
        .await
        .context("Failed to initialize bulk stream")?;

    // Send version to server again via the bulk stream.
    // This is required in order to initialize the bulk stream,
    // otherwise the server times out waiting for the stream to open.
    transport::send_version(&mut bulk_send).await?;

    // Reusable buffer for receiving keyboard events.
    let mut event_bytes = Vec::with_capacity(1024);
    // Reusable buffer for receiving bulk data (clipboards).
    let mut bulk_recv_bytes = Vec::with_capacity(1024); // TODO(later) 65536 here and below once chunking is known-good

    // Accumulator of raw clipboard data streamed from the server.
    // Cleared when the clipboard data has all been received.
    let mut incoming_clipboard_data: Option<x11clipboard::ClipboardData> = None;
    let mut active = false;
    info!("Waiting to be activated by server...");
    loop {
        let mut event_fut = pin!(events_recv.read_chunk(1024, true).fuse());
        let mut bulk_fut = pin!(bulk_recv.read_chunk(1024, true).fuse());
        select! {
            local_fetch_request = local_clipboard.fetch_rx.next() => {
                // Send fetch request to server
                if let Some(local_fetch_request) = local_fetch_request {
                    let msg = bulkmsgs::ClientBulk::ClipboardRequest(bulkmsgs::ClientClipboardRequest{
                        type_: &local_fetch_request.type_,
                        max_size_bytes: max_clipboard_size_bytes as u64,
                    });
                    let serializedmsg = postcard::to_stdvec_cobs(&msg)
                        .map_err(|e| anyhow!("Failed to serialize clipboard request message: {:?}", e))?;
                    trace!(
                        "Sending {} byte clipboard content request: {:X?}",
                        serializedmsg.len(),
                        &serializedmsg
                    );
                    bulk_send.write_all(&serializedmsg)
                        .await
                        .context("Failed to send clipboard request message")?;
                }
            },
            types = local_clipboard.types_rx.next() => {
                if active {
                    // New clipboard entry on local machine, and we're active.
                    // We'll advertise it to the server when there's a switch.
                    // Avoid polluting the rotation with "external" clipboards: only collect info if we're active.
                    local_clipboard.local_types = types;
                    local_clipboard.serving_remote_clipboard = false;
                }
            },
            event_result = event_fut => {
                // Incoming data may contain one or more messages, but I've never seen fragments of messages.
                let resp = event_result
                    .with_context(|| if is_new_connection() {
                        "Lost events connection, does server need to approve our fingerprint?"
                    } else {
                        "Lost events connection"
                    })?
                    .context("Server closed events connection")?;
                trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                // Copy the immutable response data into a mutable buffer
                event_bytes.extend_from_slice(&*resp.bytes);
                handle_event_messages(&mut events_send, &mut event_bytes, virtual_devices, local_clipboard, max_clipboard_size_bytes, &mut active).await?;
                event_bytes.clear();
            },
            bulk_result = bulk_fut => {
                let resp = bulk_result
                    .with_context(|| if is_new_connection() {
                        "Lost bulk connection, does server need to approve our fingerprint?"
                    } else {
                        "Lost bulk connection"
                    })?
                    .context("Server closed bulk connection")?;
                trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                if let Some(c) = &mut incoming_clipboard_data {
                    // Clipboard data streaming is in progress. Interpret as raw clipboard data.
                    if c.remaining_bytes >= resp.bytes.len() {
                        // This chunk should entirely be raw clipboard data.
                        c.data.extend_from_slice(&*resp.bytes);
                        c.remaining_bytes -= resp.bytes.len();
                    } else {
                        // Chunk contains more data than expected for the clipboard entry.
                        // Finish the clipboard entry, then pass the rest to bulk_recv_bytes for processing below.
                        c.data.extend_from_slice(&(*resp.bytes)[..c.remaining_bytes]);
                        bulk_recv_bytes.extend_from_slice(&(*resp.bytes)[c.remaining_bytes..]);
                        c.remaining_bytes = 0;
                    }

                    if c.remaining_bytes == 0 {
                        // Raw clipboard data has all been accumulated, flush and clear.
                        // Pass ownership of the data to the writer and clear local state.
                        // unwrap(): We just checked above that incoming_clipboard_data is present
                        local_clipboard.writer.store_data(incoming_clipboard_data.take().unwrap()).await?;
                    }

                    if bulk_recv_bytes.len() > 0 {
                        // Handle any data/messages following the raw clipboard dump.
                        incoming_clipboard_data = handle_bulk_messages(&mut bulk_send, &mut bulk_recv_bytes, &mut local_clipboard.reader, &local_clipboard.writer, max_clipboard_size_bytes).await?;
                        bulk_recv_bytes.clear();
                    }
                } else {
                    // Not in the middle of a raw clipboard dump. Must be a postcard message.
                    // Copy the immutable response data into a mutable buffer for parsing.
                    bulk_recv_bytes.extend_from_slice(&*resp.bytes);
                    incoming_clipboard_data = handle_bulk_messages(&mut bulk_send, &mut bulk_recv_bytes, &mut local_clipboard.reader, &local_clipboard.writer, max_clipboard_size_bytes).await?;
                    bulk_recv_bytes.clear();
                }
            },
        }
    }
}

async fn handle_event_messages(
    event_send: &mut SendStream,
    bytes: &mut Vec<u8>,
    virtual_devices: &mut deviceoutput::VirtualDevices,
    local_clipboard: &mut LocalClipboard,
    max_clipboard_size_bytes: u64,
    active: &mut bool,
) -> Result<()> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes.len() {
        // Assumption: We shouldn't be getting a ServerMessage that's broken up into separate fragments
        let (msg, resp_remainder) =
            postcard::take_from_bytes_cobs::<eventmsgs::ServerEvent>(&mut bytes[offset..])
                .map_err(|e| anyhow!("Failed to deserialize server message: {:?}", e))?;
        let consumed = bytes_len - resp_remainder.len() - offset;
        trace!(
            "Consumed event at offset={}: {} ({} bytes)",
            offset,
            msg,
            consumed
        );
        match msg {
            eventmsgs::ServerEvent::Switch(e) => {
                info!(
                    "This client is {}",
                    if e.enabled { "active" } else { "inactive" }
                );
                virtual_devices.switch()?;
                *active = e.enabled;
                if let Some(types) = &local_clipboard.local_types {
                    if !e.enabled && !types.is_empty() {
                        // We're being disabled and we have a clipboard from a local app.
                        // It may be from when we were disabled, or from a prior enabled session. That's fine.
                        // Keep announcing the local clipboard until/unless it gets overridden by a new one from the server.
                        let types = types.join(" ");
                        info!("Sending clipboard types to server: {}", types);
                        let msg =
                            eventmsgs::ClientEvent::ClipboardTypes(eventmsgs::ClipboardTypes {
                                types: &types,
                                max_size_bytes: max_clipboard_size_bytes,
                            });
                        let serializedmsg = postcard::to_stdvec_cobs(&msg).map_err(|e| {
                            anyhow!("Failed to serialize clipboard types message: {:?}", e)
                        })?;
                        event_send
                            .write_all(&serializedmsg)
                            .await
                            .context("Failed to send clipboard types message")?;
                    }
                }
            }
            eventmsgs::ServerEvent::Input(input) => {
                // User input event
                virtual_devices.add_event(input)?;
            }
            eventmsgs::ServerEvent::ClipboardTypes(types) => {
                // Receiving types announcement from server (following recent activation)
                // Announce the types to X11 for local apps to see, and clear any prior types from local apps.
                info!("Got clipboard types from server: {}", types.types);
                local_clipboard.local_types = None;
                local_clipboard.serving_remote_clipboard = true;
                let types: Vec<String> = types.types.split(" ").map(|s| s.to_string()).collect();
                local_clipboard.writer.store_types(types).await?;
            }
        }
        offset += consumed;
    }
    Ok(())
}

async fn handle_bulk_messages(
    bulk_send: &mut SendStream,
    bytes: &mut Vec<u8>,
    local_clipboard_reader: &mut x11clipboard::reader::ClipboardReader,
    local_clipboard_writer: &x11clipboard::writer::ClipboardWriter,
    max_clipboard_size_bytes: u64,
) -> Result<Option<x11clipboard::ClipboardData>> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes_len {
        // Assumption: We shouldn't be getting a BulkMessage that's broken up into separate fragments
        let (msg, resp_remainder) =
            postcard::take_from_bytes_cobs::<bulkmsgs::ServerBulk>(&mut bytes[offset..])
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
            bulkmsgs::ServerBulk::ClipboardRequest(c) => {
                info!(
                    "Sending clipboard data with type={} to {}",
                    c.type_,
                    if let Some(c) = c.request_client {
                        format!("server for {}", c)
                    } else {
                        "server".to_string()
                    }
                );
                // Read the clipboard data from the local application.
                let local_clipboard_data = local_clipboard_reader
                    .read(c.type_, c.max_size_bytes, &c.request_client)
                    .await?;
                let msg = bulkmsgs::ClientBulk::ClipboardHeader(bulkmsgs::ClientClipboardHeader {
                    type_: c.type_,
                    content_len_bytes: local_clipboard_data.len() as u64,
                    request_client: c.request_client,
                });
                let serializedmsg = postcard::to_stdvec_cobs(&msg)
                    .map_err(|e| anyhow!("Failed to serialize clipboard types message: {:?}", e))?;
                bulk_send
                    .write_all(&serializedmsg)
                    .await
                    .context("Failed to send clipboard content header")?;
                bulk_send
                    .write_all(&local_clipboard_data)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to send {} byte clipboard content",
                            local_clipboard_data.len()
                        )
                    })?;
            }
            bulkmsgs::ServerBulk::ClipboardHeader(c) => {
                if c.content_len_bytes > max_clipboard_size_bytes {
                    // The content length from the server is bigger than what we advertised.
                    // Reset the connection since this shouldn't happen to begin with.
                    bail!(
                        "Received clipboard size {} exceeds max size {}, resetting connection",
                        c.content_len_bytes,
                        max_clipboard_size_bytes
                    );
                } else if c.content_len_bytes as usize <= resp_remainder.len() {
                    // The clipboard content fits fully within resp_remainder.
                    // Mark content as consumed and continue looping in case another message follows.
                    let mut data = Vec::with_capacity(c.content_len_bytes as usize);
                    data.extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                    let d = x11clipboard::ClipboardData {
                        type_: c.type_.to_string(),
                        data,
                        remaining_bytes: 0,
                    };
                    local_clipboard_writer.store_data(d).await?;
                    offset += c.content_len_bytes as usize;
                } else {
                    // Need to collect more data.
                    // Save what we've got so far, and assign remaining_bytes to what's left.
                    let mut data = Vec::with_capacity(c.content_len_bytes as usize);
                    data.extend_from_slice(resp_remainder);
                    return Ok(Some(x11clipboard::ClipboardData {
                        type_: c.type_.to_string(),
                        data,
                        remaining_bytes: c.content_len_bytes as usize - resp_remainder.len(),
                    }));
                }
            }
        }
    }
    Ok(None)
}
