use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_lock::Mutex;
use async_std::task;
use futures::{select, FutureExt, StreamExt};
use tracing::{error, trace, warn};

use crate::{approval, deviceinput, devicewatch, messages, rotation, transport, x11clipboard};

pub async fn run_server(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
    mut input_rx: async_channel::Receiver<deviceinput::Event>,
    grab_tx: async_channel::Sender<devicewatch::GrabEvent>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    // TODO(later) rotation just accepts inputs without responses? if so then maybe put it in a separate task behind a channel, with an enum for the different request types
    let (local_clipboard_fetch_tx, mut local_clipboard_fetch_rx) = async_channel::bounded(32);
    let rotation: Arc<Mutex<rotation::Rotation>> =
        Arc::new(Mutex::new(rotation::Rotation::new(grab_tx, local_clipboard_fetch_tx).await?));

    let rotation_cpy = rotation.clone();
    // Task: listen to server device input events
    task::spawn(async move {
        while let Some(event) = input_rx.next().await {
            match event {
                deviceinput::Event::Input(evt) => {
                    // rotation handles and logs failed sends internally
                    let _result = rotation_cpy
                        .lock()
                        .await
                        .send_event(messages::ServerMessage::Input(evt))
                        .await;
                }
                deviceinput::Event::SwitchNext => {
                    rotation_cpy.lock().await.next_client().await;
                }
                deviceinput::Event::SwitchPrev => {
                    rotation_cpy.lock().await.prev_client().await;
                }
            }
        }
    });

    let rotation_cpy = rotation.clone();
    // Task: listen to server machine requests to get the clipboard
    task::spawn(async move {
        while let Some(fetch_request) = local_clipboard_fetch_rx.next().await {
            // Got clipboard paste request from the server machine.
            if let Err(e) = rotation_cpy
                .lock()
                .await
                .clipboard_request_content(None, &fetch_request.type_, max_clipboard_size_bytes)
                .await {
                warn!("Failed to retrieve clipboard content for server: {:?}", e);
            }
        }
    });

    let (local_clipboard_types_tx, mut local_clipboard_types_rx) = async_channel::bounded(32);
    x11clipboard::reader::ClipboardTypeWatcher::start(local_clipboard_types_tx).await?;
    let rotation_cpy = rotation.clone();
    // Task: listen to server machine updates to the clipboard types
    task::spawn(async move {
        while let Some(clipboard_types) = local_clipboard_types_rx.next().await {
            // Got updated clipboard types from the server machine.
            if let Err(e) = rotation_cpy
                .lock()
                .await
                .clipboard_update_source(None, clipboard_types, max_clipboard_size_bytes)
                .await {
                warn!("Failed to update clipboard source to server: {:?}", e);
            }
        }
    });

    let server_endpoint = transport::build_server(listen_addr, cert_verifier)?;
    while let Some(conn) = server_endpoint.accept().await {
        let rotation_cpy = rotation.clone();
        // Task: handle this client connection
        task::spawn(async move {
            match conn.await {
                Ok(conn) => {
                    let remote_addr = conn.remote_address();
                    if let Err(e) =
                        handle_connection(conn, rotation_cpy.clone(), max_clipboard_size_bytes).await
                    {
                        // Always try to remove the client from rotation, even if it wasn't added yet.
                        rotation_cpy.lock().await.remove_client(remote_addr).await;
                        error!("Client connection error: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Client failed to connect: {}", e);
                }
            }
        });
    }
    error!("Exiting server");
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connection,
    rotation: Arc<Mutex<rotation::Rotation>>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    let (events_send, mut events_recv) = conn
        .accept_bi()
        .await
        .context("Failed to initialize events stream")?;

    // Receive version from client and close the connection if it's not supported.
    // Future versions could follow the version message with more data. We ignore/discard it here.
    {
        let mut version_buf = vec![];
        transport::recv_version(&mut events_recv, &mut version_buf).await?;
    }

    // Start second stream for bulk messages
    let (bulk_send, mut bulk_recv) = conn
        .accept_bi()
        .await
        .context("Failed to initialize bulk stream")?;

    // Add client to the rotation after a successful init
    rotation
        .lock()
        .await
        .add_client(conn.remote_address(), events_send, bulk_send)
        .await;

    let mut event_bytes = Vec::with_capacity(1024);
    let mut bulk_bytes = Vec::with_capacity(1024); // TODO(later) 65536 here and below once chunking is known-good
    let mut clipboard: Option<x11clipboard::ClipboardData> = None;
    loop {
        let mut event_fut = pin!(events_recv.read_chunk(1024, true).fuse());
        let mut bulk_fut = pin!(bulk_recv.read_chunk(1024, true).fuse());
        select! {
            event_result = event_fut => {
                let resp = event_result
                    .context("Lost client events connection")?
                    .context("Client closed events connection")?;
                trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                // Copy the immutable response data into a mutable buffer
                event_bytes.extend_from_slice(&*resp.bytes);
                handle_event_messages(conn.remote_address(), &rotation, &mut event_bytes, max_clipboard_size_bytes).await?;
                event_bytes.clear();
            },
            bulk_result = bulk_fut => {
                let resp = bulk_result
                    .context("Lost client bulk connection")?
                    .context("Client closed bulk connection")?;
                trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                if let Some(c) = &mut clipboard {
                    if c.remaining_bytes >= resp.bytes.len() {
                        // Chunk is all clipboard data.
                        c.data.extend_from_slice(&*resp.bytes);
                        c.remaining_bytes -= resp.bytes.len();
                    } else {
                        // Chunk contains additional data past the clipboard entry.
                        c.data.extend_from_slice(&(*resp.bytes)[..c.remaining_bytes]);
                        bulk_bytes.extend_from_slice(&(*resp.bytes)[c.remaining_bytes..]);
                        c.remaining_bytes = 0;
                    }

                    if c.remaining_bytes == 0 {
                        // Clipboard data is all accumulated, flush and clear
                        rotation
                            .lock()
                            .await
                            .clipboard_send_content(conn.remote_address(), clipboard.take().unwrap())
                            .await?;
                    }

                    if bulk_bytes.len() > 0 {
                        // Handle any data following the clipboard entry.
                        // Hopefully it's not fragmented too since we don't really support that
                        clipboard = handle_bulk_messages(conn.remote_address(), &rotation, &mut bulk_bytes, max_clipboard_size_bytes).await?;
                        bulk_bytes.clear();
                    }
                } else {
                    // Copy the immutable response data into a mutable buffer
                    bulk_bytes.extend_from_slice(&*resp.bytes);
                    clipboard = handle_bulk_messages(conn.remote_address(), &rotation, &mut bulk_bytes, max_clipboard_size_bytes).await?;
                    bulk_bytes.clear();
                }
            },
        }
    }
}

async fn handle_event_messages(
    source: SocketAddr,
    rotation: &Arc<Mutex<rotation::Rotation>>,
    bytes: &mut Vec<u8>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes_len {
        let (msg, resp_remainder) =
            postcard::take_from_bytes_cobs::<messages::ClientMessage>(&mut bytes[offset..])
                .map_err(|e| anyhow!("Failed to deserialize client message: {:?}", e))?;
        let consumed = bytes_len - resp_remainder.len() - offset;
        trace!(
            "Consumed event at offset={}: {} ({} bytes)",
            offset,
            msg,
            consumed
        );
        match msg {
            messages::ClientMessage::ClipboardTypes(t) => {
                let types: Vec<String> = t.types.split(",").map(|t| t.to_string()).collect();
                rotation
                    .lock()
                    .await
                    .clipboard_update_source(
                        Some(source),
                        types,
                        // Advertise min(advertising client max, server max)
                        std::cmp::min(t.max_size_bytes, max_clipboard_size_bytes),
                    )
                    .await?;
            }
        }
        offset += consumed;
    }
    Ok(())
}

async fn handle_bulk_messages(
    source: SocketAddr,
    rotation: &Arc<Mutex<rotation::Rotation>>,
    bytes: &mut Vec<u8>,
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
                rotation
                    .lock()
                    .await
                    .clipboard_request_content(
                        Some(source),
                        c.type_,
                        // Advertise min(advertising client max, server max)
                        std::cmp::min(c.max_size_bytes, max_clipboard_size_bytes),
                    )
                    .await?;
            }
            messages::BulkMessage::ClipboardContentHeader(c) => {
                if c.content_len_bytes > max_clipboard_size_bytes {
                    // The content length from the client is bigger than what we advertised.
                    // Reset the client connection since this shouldn't happen to begin with.
                    bail!(
                        "Received clipboard size {} exceeds max size {}, resetting connection",
                        c.content_len_bytes,
                        max_clipboard_size_bytes
                    );
                } else if c.content_len_bytes as usize <= resp_remainder.len() {
                    // The clipboard content fits fully within resp_remainder
                    // Mark content as consumed and continue looping in case another message follows?
                    let mut data = Vec::new();
                    data.extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                    rotation
                        .lock()
                        .await
                        .clipboard_send_content(
                            source,
                            x11clipboard::ClipboardData {
                                type_: c.type_.to_string(),
                                data,
                                remaining_bytes: 0,
                            },
                        )
                        .await?;
                    offset += c.content_len_bytes as usize;
                } else {
                    // Need to collect more data. Save what we've got so far, and assign remaining_bytes to what's left
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
