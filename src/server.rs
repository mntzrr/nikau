use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio::time;
use tracing::{debug, error, trace, warn};

use crate::clipboard::data::ClipboardData;
use crate::clipboard::server::LocalClipboard;
use crate::device::{output, Event, GrabState};
use crate::msgs::{bulk, event, shared};
use crate::network::{approval, transport};
use crate::rotation;

pub async fn run_server_events_loop<O: output::OutputHandler>(
    config_dir: PathBuf,
    mut event_rx: mpsc::Receiver<Event>,
    grab_tx: watch::Sender<GrabState>,
    output_handler: O,
    max_clipboard_size_bytes: u64,
    max_uncompressed_size_bytes: u64,
    rotation_tx: mpsc::Sender<rotation::RotationEvent>,
    mut rotation_rx: mpsc::Receiver<rotation::RotationEvent>,
    motion_flush_interval: Option<Duration>,
    diagnostics: Arc<rotation::DiagnosticsMirror>,
) -> Result<()> {
    let local_clipboard = LocalClipboard::start(
        config_dir.clone(),
        rotation_tx.clone(),
        max_clipboard_size_bytes,
        max_uncompressed_size_bytes,
    ).await;

    let mut rotation =
        rotation::Rotation::new(grab_tx, output_handler, local_clipboard, &config_dir, rotation_tx, motion_flush_interval, diagnostics).await?;
    // Input-flow heartbeat: makes "user is typing but nothing arrives anywhere"
    // visible in the log, instead of silent (the dead-Enter investigations).
    let mut status_tick = time::interval(Duration::from_secs(10));
    // Skip the immediate first tick; the first heartbeat lands 10s in.
    status_tick.tick().await;
    // Pointer-motion coalescing flush timer (office mode, see --motion-hz).
    // The branch guard keeps it inert until motion has accumulated; after a
    // long idle the first tick fires immediately, so the first delta goes out
    // without added delay and only sustained streams are coalesced.
    let mut motion_tick =
        time::interval(motion_flush_interval.unwrap_or(Duration::from_secs(3600)));
    // The tick is only polled while motion is pending, so after an idle stretch
    // many periods count as "missed". Delay (not the default Burst) skips the
    // catch-up: one immediate flush after idle, then one per interval. With
    // Burst, the backlog of catch-up ticks would fire on every frame and
    // silently defeat the coalescing.
    motion_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    // Seed the diagnostics mirror so a SIGHUP before the first event still dumps.
    rotation.update_diagnostics();
    loop {
        tokio::select! {
            // Listen and forward rotation events to rotation
            event = rotation_rx.recv() => {
                let event = match event {
                    Some(e) => e,
                    None => bail!("rotation_rx is closed, exiting server"),
                };
                rotation.accept(event).await;
            },
            // Listen to local system device input events
            event = event_rx.recv() => {
                let event = match event {
                    Some(e) => e,
                    None => bail!("event_rx is closed, exiting server"),
                };
                match event {
                    Event::Input(batch) => {
                        if let Err(e) = rotation.send_input_events(batch).await {
                            warn!("Failed to send input events to current client: {:?}", e);
                        }
                    }
                    Event::SwitchNext => {
                        rotation.next_client().await;
                    }
                    Event::SwitchPrev => {
                        rotation.prev_client().await;
                    }
                    Event::SwitchTo(fingerprint) => {
                        rotation.set_client(fingerprint).await;
                    }
                    Event::PauseToggle => {
                        rotation.toggle_pause().await;
                    }
                }
            },
            _ = status_tick.tick() => {
                rotation.log_input_status();
            },
            _ = motion_tick.tick(), if rotation.motion_dirty() => {
                rotation.flush_pending_motion().await;
            },
        }
        // Refresh the mirrored state after every iteration: the SIGHUP handler
        // reads it directly from the signal thread, so the dump must not
        // depend on this loop being alive.
        rotation.update_diagnostics();
    }
}

pub async fn run_server_connections_loop(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::MonuxCertVerification<'static>>,
    fingerprint: Arc<Mutex<Option<String>>>,
    max_clipboard_size_bytes: u64,
    rotation_tx: mpsc::Sender<rotation::RotationEvent>,
    mode: transport::NetworkMode,
) -> Result<()> {
    let server_endpoint = transport::build_server(listen_addr, cert_verifier, mode)
        .context("Failed to set up server endpoint")?;
    // How long a single connection handshake may take before it is dropped.
    // Local mode must outlast the interactive approval prompt (60s), which runs
    // inside the handshake. Www mode never prompts, so it can be much stricter.
    let handshake_timeout = match mode {
        transport::NetworkMode::Local => Duration::from_secs(75),
        transport::NetworkMode::Www => Duration::from_secs(15),
    };
    // Task launcher for new client connections
    // Monotonic token tagging each accepted connection: a reconnect can reuse
    // the same addr:port, and the token lets the rotation tell a late removal
    // from the old (dead) connection apart from the healthy new entry.
    let mut next_conn_token: u64 = 1;
    loop {
        let conn = server_endpoint.accept().await;
        let conn = match conn {
            Some(c) => c,
            None => bail!("Server endpoint is closed, exiting server"),
        };
        let conn_token = next_conn_token;
        next_conn_token += 1;
        let remote_addr = conn.remote_address();
        if mode == transport::NetworkMode::Www && !conn.remote_address_validated() {
            // On the public internet, require the client to validate its source
            // address via a QUIC retry packet before we spend resources on a
            // TLS handshake (spoofed-source amplification/DoS mitigation).
            // The client will come back with a validated address.
            if let Err(e) = conn.retry() {
                error!("Failed to request address validation from {}: {}", remote_addr, e);
            }
            continue;
        }
        let connecting = match conn.accept() {
            Ok(connecting) => connecting,
            Err(e) => {
                error!("Client failed to connect: {}", e);
                continue;
            }
        };
        let rotation_tx_cpy = rotation_tx.clone();
        let fingerprint_cpy = fingerprint.clone();
        // Complete the handshake in a spawned task so that a slow or stuck peer
        // cannot block the accept loop for other clients. We still wait to spawn
        // the connection task until we've gotten the fingerprint, to avoid
        // fingerprint mismatch issues.
        task::spawn(async move {
            let conn = match tokio::time::timeout(handshake_timeout, connecting).await {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    error!("Client failed to connect: {}", e);
                    return;
                }
                Err(_) => {
                    warn!(
                        "Dropping connection from {}: handshake timed out after {}s",
                        remote_addr,
                        handshake_timeout.as_secs()
                    );
                    return;
                }
            };
            // HACK: This is retrieving the fingerprint stored by approval.rs
            // See more about this in approval.rs.
            match fingerprint_cpy.lock() {
                Ok(mut opt) => {
                    if let Some(fingerprint) = opt.take() {
                        debug!("Got fingerprint: {}", fingerprint);
                        // Now that we have extracted the client cert fingerprint, spawn.
                        task::spawn(async move {
                            if let Err(e) =
                                handle_connection(conn, fingerprint, rotation_tx_cpy.clone(), max_clipboard_size_bytes, conn_token)
                                .await
                            {
                                // Always try to remove the client from rotation, even if it wasn't added yet.
                                // The token lets the rotation ignore this removal if the endpoint
                                // was since reused by a newer connection.
                                if let Err(e) = rotation_tx_cpy
                                    .send(rotation::RotationEvent::RemoveClient {
                                        endpoint: remote_addr,
                                        conn_token,
                                    })
                                    .await {
                                        error!("Failed to send remove client event: {:?}", e);
                                    };
                                error!("Client connection error: {:?}", e);
                            }
                        });
                    } else {
                        // In theory, this could happen if there was a race which approval.rs cleaned up.
                        // Drop the connection and make the client try again.
                        warn!("BUG: Fingerprint missing for new connection, dropping connection so that client can retry");
                    }
                },
                Err(e) => {
                    error!("Failed to lock fingerprint for new connection: {}", e);
                },
            };
        });
    }
}

async fn handle_connection(
    conn: quinn::Connection,
    fingerprint: String,
    rotation_tx: mpsc::Sender<rotation::RotationEvent>,
    max_clipboard_size_bytes: u64,
    conn_token: u64,
) -> Result<()> {
    let (mut events_send, mut events_recv) = conn
        .accept_bi()
        .await
        .context("Failed to initialize events stream")?;

    // Receive version from client and close the connection if it's not supported.
    // Future versions could follow the version message with more data. We ignore/discard it here.
    let mut event_bytes = Vec::with_capacity(1024);
    let client_version = transport::recv_version(&mut events_recv, &mut event_bytes).await?;
    // Reply with our own version BEFORE rejecting a mismatch, so that the
    // client learns it (its update gate needs it to catch up after we upgrade).
    transport::send_version(&mut events_send).await?;
    transport::ensure_compatible_version(client_version)?;

    // Start second stream for bulk messages
    let (mut bulk_send, mut bulk_recv) = conn
        .accept_bi()
        .await
        .context("Failed to initialize bulk stream")?;
    // Clipboard bulk yields to the events stream (priority 0) when the
    // connection is congested, so a big transfer can't starve input.
    let _ = bulk_send.set_priority(-1);

    // Receive the version a second time, on the bulk stream.
    // Sending some data is required to initialize the bulk stream, so let's just repeat ourselves.
    // Maybe we'll want to have different per-stream versions someday? Probably not.
    let client_version = transport::recv_version(&mut bulk_recv, &mut event_bytes).await?;
    transport::send_version(&mut bulk_send).await?;
    transport::ensure_compatible_version(client_version)?;

    // Add client to the rotation after a successful init
    rotation_tx
        .send(rotation::RotationEvent::AddClient(
            rotation::AddClientArgs {
                endpoint: conn.remote_address(),
                fingerprint,
                events_send,
                bulk_send,
                conn: conn.clone(),
                conn_token,
            },
        ))
        .await?;

    let mut bulk_bytes = Vec::with_capacity(65536);
    let mut incoming_clipboard_data: Option<(ClipboardData, Option<SocketAddr>, u64)> =
        None;
    loop {
        tokio::select! {
            event_result = events_recv.read_chunk(16384, true) => {
                let resp = match event_result {
                    Ok(chunk) => chunk.context("Client closed events connection")?,
                    Err(e) => {
                        transport::log_conn_stats(&conn);
                        Err(e).context("Lost client events connection")?
                    }
                };
                trace!("Received {} bytes from events stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                // Copy the immutable response data into a mutable buffer
                event_bytes.extend_from_slice(&resp.bytes);
                handle_event_messages(conn.remote_address(), &rotation_tx, &mut event_bytes, max_clipboard_size_bytes).await?;
            },
            bulk_result = bulk_recv.read_chunk(65536, true) => {
                let resp = match bulk_result {
                    Ok(chunk) => chunk.context("Client closed bulk connection")?,
                    Err(e) => {
                        transport::log_conn_stats(&conn);
                        Err(e).context("Lost client bulk connection")?
                    }
                };
                trace!("Received {} bytes from bulk stream: {:X?}", resp.bytes.len(), &*resp.bytes);
                if let Some((c, request_client, request_id)) = &mut incoming_clipboard_data {
                    if c.remaining_bytes >= resp.bytes.len() {
                        // Chunk is all clipboard data.
                        c.bytes.extend_from_slice(&resp.bytes);
                        c.remaining_bytes -= resp.bytes.len();
                    } else {
                        // Chunk contains additional data past the clipboard entry.
                        c.bytes.extend_from_slice(&(*resp.bytes)[..c.remaining_bytes]);
                        bulk_bytes.extend_from_slice(&(*resp.bytes)[c.remaining_bytes..]);
                        c.remaining_bytes = 0;
                    }

                    if c.remaining_bytes == 0 {
                        // Streamed clipboard data is all accumulated, flush and clear
                        rotation_tx.send(rotation::RotationEvent::ClipboardSendContent(rotation::ClipboardSendContentArgs{
                            data_source: conn.remote_address(),
                            request_client: *request_client,
                            request_id: *request_id,
                            data: incoming_clipboard_data.take().unwrap().0
                        })).await?;
                    }

                    if !bulk_bytes.is_empty() {
                        // Handle any data following the clipboard entry.
                        incoming_clipboard_data = handle_bulk_messages(conn.remote_address(), &rotation_tx, &mut bulk_bytes, max_clipboard_size_bytes).await?;
                    }
                } else {
                    // Copy the immutable response data into a mutable buffer
                    bulk_bytes.extend_from_slice(&resp.bytes);
                    incoming_clipboard_data = handle_bulk_messages(conn.remote_address(), &rotation_tx, &mut bulk_bytes, max_clipboard_size_bytes).await?;
                }
            },
        }
    }
}

async fn handle_event_messages(
    source: SocketAddr,
    rotation_tx: &mpsc::Sender<rotation::RotationEvent>,
    bytes: &mut Vec<u8>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes_len {
        // A partial frame (no COBS terminator yet) is kept for the next chunk.
        if !shared::has_complete_cobs_frame(&bytes[offset..]) {
            break;
        }
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
                let types: Vec<String> = t.types.split(' ').map(|t| t.to_string()).collect();
                debug!("Got clipboard type advertisement from client {}: {:?}", source, types);
                rotation_tx
                    .send(rotation::RotationEvent::ClipboardUpdateSource(
                        rotation::ClipboardUpdateSourceArgs {
                            source: Some(source),
                            types,
                            // Advertise min(advertising client max, server max)
                            max_size_bytes: std::cmp::min(
                                t.max_size_bytes,
                                max_clipboard_size_bytes,
                            ),
                        },
                    ))
                    .await?;
            }
        }
        offset += consumed;
    }
    // Retain any unconsumed partial frame for the next chunk.
    bytes.drain(..offset);
    Ok(())
}

async fn handle_bulk_messages(
    source: SocketAddr,
    rotation_tx: &mpsc::Sender<rotation::RotationEvent>,
    bytes: &mut Vec<u8>,
    max_clipboard_size_bytes: u64,
) -> Result<Option<(ClipboardData, Option<SocketAddr>, u64)>> {
    let mut offset = 0;
    let bytes_len = bytes.len();
    while offset < bytes_len {
        // A partial frame (no COBS terminator yet) is kept for the next chunk.
        if !shared::has_complete_cobs_frame(&bytes[offset..]) {
            break;
        }
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
                rotation_tx
                    .send(rotation::RotationEvent::ClipboardRequestContent(
                        rotation::ClipboardRequestContentArgs {
                            request_source: rotation::ClipboardRequestSource::Remote(source),
                            requested_type: c.requested_type.to_string(),
                            // Advertise min(advertising client max, server max)
                            max_size_bytes: std::cmp::min(
                                c.max_size_bytes,
                                max_clipboard_size_bytes,
                            ),
                            request_id: Some(c.request_id),
                        },
                    ))
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
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(&resp_remainder[..c.content_len_bytes as usize]);
                    rotation_tx
                        .send(rotation::RotationEvent::ClipboardSendContent(
                            rotation::ClipboardSendContentArgs {
                                data_source: source,
                                request_client: c.request_client,
                                request_id: c.request_id,
                                data: ClipboardData {
                                    requested_type: c.requested_type.to_string(),
                                    data_type: c.data_type.map(|t| t.to_string()),
                                    bytes,
                                    remaining_bytes: 0,
                                },
                            },
                        ))
                        .await?;
                    offset += c.content_len_bytes as usize;
                } else {
                    // Need to collect more data.
                    // Save what we've got so far, and assign remaining_bytes to what's left.
                    let mut payload = Vec::with_capacity(c.content_len_bytes as usize);
                    payload.extend_from_slice(resp_remainder);
                    let d = (
                        ClipboardData {
                            requested_type: c.requested_type.to_string(),
                            data_type: c.data_type.map(|t| t.to_string()),
                            bytes: payload,
                            remaining_bytes: c.content_len_bytes as usize - resp_remainder.len(),
                        },
                        c.request_client,
                        c.request_id,
                    );
                    // All bytes were consumed (into the pending clipboard data).
                    bytes.clear();
                    return Ok(Some(d));
                }
            }
        }
    }
    // Retain any unconsumed partial frame for the next chunk.
    bytes.drain(..offset);
    Ok(None)
}
