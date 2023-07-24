use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use futures::{select, FutureExt, StreamExt};
use quinn::SendStream;
use tracing::{info, trace};

use crate::{approval, deviceoutput, messages, transport, x11clipboard};

pub struct ClientClipboard {
    reader: x11clipboard::reader::ClipboardReader,
    writer: x11clipboard::writer::ClipboardWriter,
    fetch_rx: async_channel::Receiver<x11clipboard::writer::ClipboardFetch>,
    types_rx: async_channel::Receiver<Vec<String>>,
}

impl ClientClipboard {
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
        })
    }
}

pub async fn run_client(
    bind_addr: &SocketAddr,
    server_addr: &SocketAddr,
    virtual_devices: &mut deviceoutput::VirtualDevices,
    cert_verifier: Arc<approval::NikauCertVerification>,
    max_clipboard_size_bytes: u64,
    clipboard: &mut ClientClipboard,
) -> Result<()> {
    let client_endpoint = transport::build_client(bind_addr, cert_verifier)?;
    // Connect to server, our custom cert verifiers result in server_name being ignored
    let conn = client_endpoint
        .connect(server_addr.clone(), "__ignored__")?
        .await?;
    info!("Connected to server: {}", conn.remote_address());
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
    let mut clipboard_data: Option<x11clipboard::ClipboardData> = None;
    let mut clipboard_types: Option<Vec<String>> = None;
    info!("Waiting to be activated by server...");
    loop {
        let mut event_fut = pin!(events_recv.read_chunk(1024, true).fuse());
        let mut bulk_fut = pin!(bulk_recv.read_chunk(1024, true).fuse());
        select! {
            fetch_request = clipboard.fetch_rx.next() => {
                if let Some(fetch_request) = fetch_request {
                    let msg = messages::BulkMessage::ClipboardContentRequest(messages::ClipboardContentRequest{
                        type_: &fetch_request.type_,
                        max_size_bytes: max_clipboard_size_bytes as u64,
                    });
                    let serializedmsg = postcard::to_stdvec_cobs(&msg)
                        .map_err(|e| anyhow!("Failed to serialize clipboard request message: {:?}", e))?;
                    trace!(
                        "Sending {} byte event: {:X?}",
                        serializedmsg.len(),
                        &serializedmsg
                    );
                    bulk_send.write_all(&serializedmsg)
                        .await
                        .context("Failed to send clipboard request message")?;
                }
            },
            types = clipboard.types_rx.next() => {
                // Save recently received types
                info!("Received updated clipboard types: {:?}", types);
                clipboard_types = types;
            },
            event_result = event_fut => {
                // Incoming data may contain one or more messages, but I've never seen fragments of messages.
                let resp = event_result
                    .context("Lost events connection, does server need to approve our fingerprint?")?
                    .context("Server closed events connection")?;
                trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                // Copy the immutable response data into a mutable buffer
                event_bytes.extend_from_slice(&*resp.bytes);
                handle_event_messages(&mut events_send, &mut event_bytes, virtual_devices, &mut clipboard_types, &mut clipboard.writer, max_clipboard_size_bytes).await?;
                event_bytes.clear();
            },
            bulk_result = bulk_fut => {
                let resp = bulk_result
                    .context("Lost server bulk connection, does server need to approve our fingerprint?")?
                    .context("Server closed bulk connection")?;
                trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                if let Some(c) = &mut clipboard_data {
                    if c.remaining_bytes >= resp.bytes.len() {
                        // Chunk is all clipboard data.
                        c.data.extend_from_slice(&*resp.bytes);
                        c.remaining_bytes -= resp.bytes.len();
                    } else {
                        // Chunk contains additional data past the clipboard entry.
                        c.data.extend_from_slice(&(*resp.bytes)[..c.remaining_bytes]);
                        bulk_recv_bytes.extend_from_slice(&(*resp.bytes)[c.remaining_bytes..]);
                        c.remaining_bytes = 0;
                    }

                    if c.remaining_bytes == 0 {
                        // Clipboard data is all accumulated, flush and clear
                        clipboard.writer.store_data(clipboard_data.take().expect("missing clipboard")).await?;
                    }

                    if bulk_recv_bytes.len() > 0 {
                        // Handle any data following the clipboard entry.
                        // Hopefully it's not fragmented too since we don't really support that
                        clipboard_data = handle_bulk_messages(&mut bulk_send, &mut bulk_recv_bytes, &mut clipboard.reader, &clipboard.writer, max_clipboard_size_bytes).await?;
                        bulk_recv_bytes.clear();
                    }
                } else {
                    // Copy the immutable response data into a mutable buffer
                    bulk_recv_bytes.extend_from_slice(&*resp.bytes);
                    clipboard_data = handle_bulk_messages(&mut bulk_send, &mut bulk_recv_bytes, &mut clipboard.reader, &clipboard.writer, max_clipboard_size_bytes).await?;
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
    latest_clipboard_types: &mut Option<Vec<String>>,
    clipboard_writer: &mut x11clipboard::writer::ClipboardWriter,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes.len() {
        let (msg, resp_remainder) =
            postcard::take_from_bytes_cobs::<messages::ServerMessage>(&mut bytes[offset..])
                .map_err(|e| anyhow!("Failed to deserialize server message: {:?}", e))?;
        let consumed = bytes_len - resp_remainder.len() - offset;
        trace!(
            "Consumed event at offset={}: {} ({} bytes)",
            offset,
            msg,
            consumed
        );
        match msg {
            messages::ServerMessage::Switch(e) => {
                virtual_devices.switch(e.enabled)?;
                // We're being closed, send clipboard types if we have any
                // TODO only send the types if they ORIGINATED FROM US
                if let Some(types) = latest_clipboard_types {
                    if !e.enabled && !types.is_empty() {
                        let types = types.join(" ");
                        let msg =
                            messages::ClientMessage::ClipboardTypes(messages::ClipboardTypes {
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
                // If we're being opened or closed, we should discard any previously received clipboard types.
                // In the disabled case, we just sent it above.
                // In the enabled case, we should only pay attention to clipboards received while enabled.
                let _ = latest_clipboard_types.take();
            }
            messages::ServerMessage::Input(input) => {
                virtual_devices.add_event(input)?;
            }
            messages::ServerMessage::ClipboardTypes(types) => {
                let types: Vec<String> = types.types.split(" ").map(|s| s.to_string()).collect();
                clipboard_writer.store_types(types).await?;
            }
        }
        offset += consumed;
    }
    Ok(())
}

async fn handle_bulk_messages(
    bulk_send: &mut SendStream,
    bytes: &mut Vec<u8>,
    clipboard_reader: &mut x11clipboard::reader::ClipboardReader,
    clipboard_writer: &x11clipboard::writer::ClipboardWriter,
    max_clipboard_size_bytes: u64,
) -> Result<Option<x11clipboard::ClipboardData>> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes_len {
        let (msg, resp_remainder) =
            postcard::take_from_bytes_cobs::<messages::BulkMessage>(&mut bytes[offset..])
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
            messages::BulkMessage::ClipboardContentRequest(c) => {
                let clipboard_data = clipboard_reader.read(c.type_, c.max_size_bytes).await?;
                let msg = messages::BulkMessage::ClipboardContentHeader(
                    messages::ClipboardContentHeader {
                        type_: c.type_,
                        content_len_bytes: clipboard_data.len() as u64,
                    },
                );
                let serializedmsg = postcard::to_stdvec_cobs(&msg)
                    .map_err(|e| anyhow!("Failed to serialize clipboard types message: {:?}", e))?;
                bulk_send
                    .write_all(&serializedmsg)
                    .await
                    .context("Failed to send clipboard content header")?;
                bulk_send
                    .write_all(&clipboard_data)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to send {} byte clipboard content",
                            clipboard_data.len()
                        )
                    })?;
            }
            messages::BulkMessage::ClipboardContentHeader(c) => {
                if c.content_len_bytes > max_clipboard_size_bytes {
                    // The content length from the server is bigger than what we advertised.
                    // Reset the connection since this shouldn't happen to begin with.
                    bail!(
                        "Received clipboard size {} exceeds max size {}, resetting connection",
                        c.content_len_bytes,
                        max_clipboard_size_bytes
                    );
                } else if c.content_len_bytes as usize <= resp_remainder.len() {
                    // The clipboard content fits fully within resp_remainder
                    // Mark content as consumed and continue looping in case another message follows?
                    let mut data = Vec::with_capacity(c.content_len_bytes as usize);
                    data.extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                    let d = x11clipboard::ClipboardData {
                        type_: c.type_.to_string(),
                        data,
                        remaining_bytes: 0,
                    };
                    clipboard_writer.store_data(d).await?;
                    offset += c.content_len_bytes as usize;
                } else {
                    // Need to collect more data.
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
