use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{broadcast, mpsc, watch as watchchan, Mutex};
use tokio::task;
use tracing::{error, trace, warn};

use crate::device::{input, watch};
use crate::msgs::{bulk, event};
use crate::network::{approval, transport};
use crate::{rotation, x11clipboard};

pub async fn run_server(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
    mut input_rx: mpsc::Receiver<input::Event>,
    grab_tx: broadcast::Sender<watch::GrabEvent>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    // TODO(later) rotation just accepts inputs without responses? if so then maybe put it in a separate task behind a channel, with an enum for the different request types. basically let's see if we can avoid mutex locking on input events.
    let (local_clipboard_fetch_tx, mut local_clipboard_fetch_rx) = mpsc::channel(32);
    let rotation: Arc<Mutex<rotation::Rotation>> = Arc::new(Mutex::new(
        rotation::Rotation::new(grab_tx, local_clipboard_fetch_tx).await?,
    ));

    let rotation_cpy = rotation.clone();
    // Task: listen to server device input events
    task::spawn(async move {
        while let Some(event) = input_rx.recv().await {
            match event {
                input::Event::Input(evt) => {
                    // rotation handles and logs failed sends internally
                    let _result = rotation_cpy
                        .lock()
                        .await
                        .send_event_current(event::ServerEvent::Input(evt))
                        .await;
                }
                input::Event::SwitchNext => {
                    rotation_cpy.lock().await.next_client().await;
                }
                input::Event::SwitchPrev => {
                    rotation_cpy.lock().await.prev_client().await;
                }
            }
        }
    });

    let rotation_cpy = rotation.clone();
    // Task: listen to local host requests to get the clipboard
    task::spawn(async move {
        while let Some(fetch_request) = local_clipboard_fetch_rx.recv().await {
            // Got clipboard paste request from the local machine.
            if let Err(e) = rotation_cpy
                .lock()
                .await
                .clipboard_request_content(None, &fetch_request.type_, max_clipboard_size_bytes)
                .await
            {
                warn!("Failed to retrieve clipboard content for server: {:?}", e);
            }
        }
    });

    // TODO(later) allow missing clipboard support
    let (local_clipboard_types_tx, mut local_clipboard_types_rx) = watchchan::channel(vec![]);
    x11clipboard::reader::ClipboardTypeWatcher::start(local_clipboard_types_tx).await?;
    let rotation_cpy = rotation.clone();
    // Task: listen to server machine updates to the clipboard types
    task::spawn(async move {
        loop {
            if let Err(e) = local_clipboard_types_rx.changed().await {
                warn!("local_clipboard_types_rx has closed: {}", e);
                break;
            }
            // Another application on the server machine has a clipboard entry.
            let clipboard_types = local_clipboard_types_rx.borrow().clone();
            if let Err(e) = rotation_cpy
                .lock()
                .await
                .clipboard_update_source(None, clipboard_types, max_clipboard_size_bytes)
                .await
            {
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
                        handle_connection(conn, rotation_cpy.clone(), max_clipboard_size_bytes)
                            .await
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
    let mut event_bytes = Vec::with_capacity(1024);
    transport::recv_version(&mut events_recv, &mut event_bytes).await?;

    // Start second stream for bulk messages
    let (bulk_send, mut bulk_recv) = conn
        .accept_bi()
        .await
        .context("Failed to initialize bulk stream")?;

    // Receive the version a second time, on the bulk stream.
    // Sending some data is required to initialize the bulk stream, so let's just repeat ourselves.
    // Maybe we'll want to have different per-stream versions someday? Probably not.
    transport::recv_version(&mut bulk_recv, &mut event_bytes).await?;

    // Add client to the rotation after a successful init
    rotation
        .lock()
        .await
        .add_client(conn.remote_address(), events_send, bulk_send)
        .await;

    let mut bulk_bytes = Vec::with_capacity(65536);
    let mut incoming_clipboard_data: Option<(x11clipboard::ClipboardData, Option<SocketAddr>)> =
        None;
    loop {
        tokio::select! {
            event_result = events_recv.read_chunk(1024, true) => {
                let resp = event_result
                    .context("Lost client events connection")?
                    .context("Client closed events connection")?;
                trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                // Copy the immutable response data into a mutable buffer
                event_bytes.extend_from_slice(&*resp.bytes);
                handle_event_messages(conn.remote_address(), &rotation, &mut event_bytes, max_clipboard_size_bytes).await?;
                event_bytes.clear();
            },
            bulk_result = bulk_recv.read_chunk(65536, true) => {
                let resp = bulk_result
                    .context("Lost client bulk connection")?
                    .context("Client closed bulk connection")?;
                trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                if let Some((c, request_client)) = &mut incoming_clipboard_data {
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
                        // Streamed clipboard data is all accumulated, flush and clear
                        rotation
                            .lock()
                            .await
                            .clipboard_send_content(
                                conn.remote_address(),
                                *request_client,
                                incoming_clipboard_data.take().unwrap().0
                            )
                            .await?;
                    }

                    if bulk_bytes.len() > 0 {
                        // Handle any data following the clipboard entry.
                        incoming_clipboard_data = handle_bulk_messages(conn.remote_address(), &rotation, &mut bulk_bytes, max_clipboard_size_bytes).await?;
                        bulk_bytes.clear();
                    }
                } else {
                    // Copy the immutable response data into a mutable buffer
                    bulk_bytes.extend_from_slice(&*resp.bytes);
                    incoming_clipboard_data = handle_bulk_messages(conn.remote_address(), &rotation, &mut bulk_bytes, max_clipboard_size_bytes).await?;
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
        let bytes2 = bytes.clone();
        let (msg, resp_remainder) = postcard::take_from_bytes_cobs::<event::ClientEvent>(
            &mut bytes[offset..],
        )
        .map_err(|e| {
            anyhow!(
                "Failed to deserialize client message: {:?} bytes(off={})={:X?}",
                e,
                offset,
                bytes2
            )
        })?;
        let consumed = bytes_len - resp_remainder.len() - offset;
        trace!(
            "Consumed event at offset={}: {} ({} bytes)",
            offset,
            msg,
            consumed
        );
        match msg {
            event::ClientEvent::ClipboardTypes(t) => {
                // Client broadcasted new clipboard types for server (and other clients) to advertise
                let types: Vec<String> = t.types.split(" ").map(|t| t.to_string()).collect();
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
) -> Result<Option<(x11clipboard::ClipboardData, Option<SocketAddr>)>> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes_len {
        let (msg, resp_remainder) =
            postcard::take_from_bytes_cobs::<bulk::ClientBulk>(&mut bytes[offset..])
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
            bulk::ClientBulk::ClipboardRequest(c) => {
                // Forward the request to rotation, which tracks where to get it from.
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
            bulk::ClientBulk::ClipboardHeader(c) => {
                if c.content_len_bytes > max_clipboard_size_bytes {
                    // The content length from the client is bigger than what we advertised.
                    // Reset the client connection since this shouldn't happen to begin with.
                    bail!(
                        "Received clipboard size {} exceeds max size {}, resetting connection",
                        c.content_len_bytes,
                        max_clipboard_size_bytes
                    );
                } else if c.content_len_bytes as usize <= resp_remainder.len() {
                    // The clipboard content fits fully within resp_remainder.
                    // Mark content as consumed and continue looping in case another message follows.
                    let mut data = Vec::new();
                    data.extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                    rotation
                        .lock()
                        .await
                        .clipboard_send_content(
                            source,
                            c.request_client,
                            x11clipboard::ClipboardData {
                                type_: c.type_.to_string(),
                                data,
                                remaining_bytes: 0,
                            },
                        )
                        .await?;
                    offset += c.content_len_bytes as usize;
                } else {
                    // Need to collect more data.
                    // Save what we've got so far, and assign remaining_bytes to what's left.
                    let mut data = Vec::with_capacity(c.content_len_bytes as usize);
                    data.extend_from_slice(resp_remainder);
                    return Ok(Some((
                        x11clipboard::ClipboardData {
                            type_: c.type_.to_string(),
                            data,
                            remaining_bytes: c.content_len_bytes as usize - resp_remainder.len(),
                        },
                        c.request_client,
                    )));
                }
            }
        }
    }
    Ok(None)
}
