use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio::time;
use tracing::{debug, error, info, trace, warn};

use crate::clipboard::data::ClipboardData;
use crate::clipboard::server::LocalClipboard;
use crate::device::{output, Event, GrabState};
use crate::msgs::{bulk, event, shared};
use crate::network::{approval, transport};
use crate::rotation;

/// Marker for a refused protocol mismatch. The refusal was already logged
/// with full context by PeerVersions, so the accept loop logs it at debug
/// instead of erroring again per retry.
#[derive(Debug)]
struct VersionMismatch;

impl std::fmt::Display for VersionMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("protocol version mismatch (already logged)")
    }
}

impl std::error::Error for VersionMismatch {}

/// How often to repeat the friendly refusal log for the same client.
const REFUSAL_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Tracks per-client protocol versions so a version-mismatched client reads
/// like the self-healing flow it is (it will auto-update and return), not
/// like a broken connection erroring every few seconds — and so the moment
/// it catches up is visible too.
#[derive(Default)]
struct PeerVersions {
    /// addr -> version from the last successful exchange (for the update note).
    seen: HashMap<SocketAddr, u64>,
    /// addr -> when we last logged a refusal for it (rate limit).
    last_refusal_log: HashMap<SocketAddr, Instant>,
}

impl PeerVersions {
    /// Judges a client version from the exchange: Ok when compatible (and a
    /// log line when the client just upgraded), Err when it must be refused
    /// (a rate-limited, self-healing-framed warning on the first/log-worthy
    /// refusal, silence otherwise).
    fn check(&mut self, addr: SocketAddr, version: u64) -> Result<(), ()> {
        let ours = shared::PROTOCOL_VERSION;
        if version == ours {
            if let Some(old) = self.seen.insert(addr, version) {
                if old < version {
                    info!(
                        "Client {} updated protocol v{} -> v{} and reconnected",
                        addr, old, version
                    );
                }
            }
            return Ok(());
        }
        // Refuse, but keep the last seen version for the eventual upgrade note.
        self.seen.insert(addr, version);
        let should_log = self
            .last_refusal_log
            .get(&addr)
            .is_none_or(|last| last.elapsed() >= REFUSAL_LOG_INTERVAL);
        if should_log {
            self.last_refusal_log.insert(addr, Instant::now());
            if version < ours {
                warn!(
                    "Client {} speaks protocol v{} but we speak v{}: it's outdated and will auto-update and reconnect shortly (refusing until then)",
                    addr, version, ours
                );
            } else {
                warn!(
                    "Client {} speaks protocol v{} but we speak v{}: THIS server is outdated — update it so the versions line up",
                    addr, version, ours
                );
            }
        }
        Err(())
    }
}

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
    bulk_throttle_mbps: Option<f64>,
    mode: transport::NetworkMode,
    diagnostics: Arc<rotation::DiagnosticsMirror>,
    edge_client_tx: Option<watch::Sender<Vec<(SocketAddr, String)>>>,
) -> Result<()> {
    let local_clipboard = LocalClipboard::start(
        config_dir.clone(),
        rotation_tx.clone(),
        max_clipboard_size_bytes,
        max_uncompressed_size_bytes,
    ).await;

    let mut rotation =
        rotation::Rotation::new(grab_tx, output_handler, local_clipboard, &config_dir, rotation_tx, motion_flush_interval, bulk_throttle_mbps, mode, diagnostics).await?;
    if let Some(tx) = edge_client_tx {
        rotation.set_edge_client_publisher(tx);
    }
    // Input-flow heartbeat: makes "user is typing but nothing arrives anywhere"
    // visible in the log, instead of silent (the dead-Enter investigations).
    let mut status_tick = time::interval(Duration::from_secs(10));
    // Skip the immediate first tick; the first heartbeat lands 10s in.
    status_tick.tick().await;
    // App-level liveness check (see ServerEvent::Ping): pings the current
    // client so a black-holed link ungrabs within ~6s instead of silently
    // swallowing input until the QUIC idle timeout fires.
    let mut ping_tick = time::interval(rotation::PING_INTERVAL);
    // Skip the immediate first tick; the first ping lands one interval in.
    ping_tick.tick().await;
    // Delay (not the default Burst): after the loop was blocked, don't fire
    // catch-up pings back to back — ping_tick's own stall guard handles the
    // late tick, and a burst would only multiply the load on a busy loop.
    ping_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
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
        // Snapshot per iteration (Copy, so no borrow of rotation crosses the
        // select): the trailing edge of the local clipboard debounce window,
        // if an update is currently held (see CLIPBOARD_UPDATE_DEBOUNCE).
        let clipboard_debounce_deadline = rotation.pending_local_clipboard_deadline();
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
                    Event::SetPaused(paused) => {
                        rotation.set_paused(paused).await;
                    }
                }
            },
            _ = status_tick.tick() => {
                rotation.log_input_status();
                // Prune fetch bookkeeping whose requester already gave up, so
                // dead entries don't linger until the next request arrives.
                rotation.prune_pending_clipboard_requests();
            },
            _ = ping_tick.tick() => {
                rotation.ping_tick().await;
            },
            _ = motion_tick.tick(), if rotation.motion_dirty() => {
                rotation.flush_pending_motion().await;
            },
            // Trailing edge of the local clipboard debounce: apply the newest
            // update held during the window. Pends forever when nothing is held.
            _ = async {
                match clipboard_debounce_deadline {
                    Some(deadline) => time::sleep_until(time::Instant::from_std(deadline)).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                rotation.flush_pending_local_clipboard().await;
            },
        }
        // Refresh the mirrored state after every iteration: the SIGHUP handler
        // reads it directly from the signal thread, so the dump must not
        // depend on this loop being alive.
        rotation.update_diagnostics();
    }
}

/// A previous instance releases the single-instance lock before its endpoint
/// socket finishes closing (teardown ordering), so a takeover can arrive
/// while the port is still draining: retry briefly on EADDRINUSE instead of
/// dying (seen in the wild when a manual start took over from an auto-update
/// restart).
async fn bind_server_with_retry(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::MonuxCertVerification<'static>>,
    mode: transport::NetworkMode,
) -> Result<quinn::Endpoint> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0u32;
    loop {
        match transport::build_server(listen_addr, cert_verifier.clone(), mode) {
            Ok(endpoint) => return Ok(endpoint),
            Err(e) if is_addr_in_use(&e) && attempt < MAX_RETRIES => {
                attempt += 1;
                info!(
                    "Port {} is still held (previous instance finishing teardown?), retrying bind in 500ms (attempt {}/{})",
                    listen_addr.port(),
                    attempt,
                    MAX_RETRIES
                );
                time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e).context("Failed to set up server endpoint"),
        }
    }
}

/// Whether the error chain contains an EADDRINUSE io error.
fn is_addr_in_use(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.raw_os_error() == Some(libc::EADDRINUSE))
    })
}

pub async fn run_server_connections_loop(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::MonuxCertVerification<'static>>,
    fingerprint: Arc<Mutex<Option<String>>>,
    max_clipboard_size_bytes: u64,
    rotation_tx: mpsc::Sender<rotation::RotationEvent>,
    mode: transport::NetworkMode,
) -> Result<()> {
    let server_endpoint = bind_server_with_retry(listen_addr, cert_verifier, mode).await?;
    // Protocol-version tracker: turns refusal spam into the self-healing
    // story (outdated client auto-updating) and notes when it catches up.
    let peer_versions = Arc::new(Mutex::new(PeerVersions::default()));
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
        let peer_versions_cpy = peer_versions.clone();
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
                                handle_connection(conn, fingerprint, rotation_tx_cpy.clone(), max_clipboard_size_bytes, conn_token, peer_versions_cpy)
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
                                if e.downcast_ref::<VersionMismatch>().is_some() {
                                    // Already logged with full context by the
                                    // version check; don't error per retry.
                                    debug!("Refused client connection from {}: protocol mismatch", remote_addr);
                                } else {
                                    error!("Client connection error: {:?}", e);
                                }
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
    peer_versions: Arc<Mutex<PeerVersions>>,
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
    // Judge the client's version with context (an upgrade note on success, a
    // friendly rate-limited refusal otherwise) before the hard gate.
    if peer_versions
        .lock()
        .expect("peer versions lock poisoned")
        .check(conn.remote_address(), client_version)
        .is_err()
    {
        return Err(VersionMismatch.into());
    }
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
                // Anything received from the client is proof of liveness (see
                // ServerEvent::Ping): reported per chunk, so raw clipboard
                // payload counts too — a large upload taking >6s must not
                // look like a silent client.
                rotation_tx
                    .send(rotation::RotationEvent::ClientHeardFrom {
                        endpoint: conn.remote_address(),
                    })
                    .await?;
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
                // Proof of liveness, same as the events stream above.
                rotation_tx
                    .send(rotation::RotationEvent::ClientHeardFrom {
                        endpoint: conn.remote_address(),
                    })
                    .await?;
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
        let (msg, resp_remainder) = match postcard::take_from_bytes_cobs::<event::ClientEvent>(
            &mut bytes[offset..],
        ) {
            Ok(parsed) => parsed,
            // The buffer is only copied (into this message) on the error
            // path, not for every successfully parsed message.
            Err(e) => bail!(
                "Failed to deserialize client message: {:?} bytes(off={})={:X?}",
                e,
                offset,
                bytes
            ),
        };
        let consumed = bytes_len - resp_remainder.len() - offset;
        trace!(
            "Consumed event at offset={}: {} ({} bytes)",
            offset,
            msg,
            consumed
        );
        match msg {
            event::ClientEvent::Pong => {
                // Answer to the server's Ping (see ServerEvent::Ping). The
                // liveness bookkeeping already happened per-chunk in
                // handle_connection (ClientHeardFrom); nothing else to do.
                trace!("Got pong from client {}", source);
            }
            event::ClientEvent::SwitchRequest { .. } => {
                // Client-initiated return to the local machine (screen-edge
                // detection on the client). y_fraction is reserved for future
                // cursor warping and ignored for now; the rotation honors the
                // request only when this client is the current one.
                debug!("Got switch request from client {}", source);
                rotation_tx
                    .send(rotation::RotationEvent::SwitchRequest { endpoint: source })
                    .await?;
            }
            event::ClientEvent::ClipboardTypes(t) => {
                // Client broadcasted new clipboard types for server (and other clients) to advertise.
                // An empty types string (the client's clipboard was cleared) splits
                // to no types — a phantom "" type must never reach the rotation.
                let types: Vec<String> = t.types_vec();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> SocketAddr {
        "127.0.0.1:12345".parse().unwrap()
    }

    #[test]
    fn matching_version_passes_and_records() {
        let mut pv = PeerVersions::default();
        assert!(pv.check(addr(), shared::PROTOCOL_VERSION).is_ok());
        assert!(pv.check(addr(), shared::PROTOCOL_VERSION).is_ok());
    }

    #[test]
    fn older_version_refuses_with_rate_limited_log() {
        let mut pv = PeerVersions::default();
        let old = shared::PROTOCOL_VERSION - 1;
        assert!(pv.check(addr(), old).is_err());
        assert!(pv.last_refusal_log.contains_key(&addr()));
        // A second refusal inside the window is not logged again.
        let first = *pv.last_refusal_log.get(&addr()).unwrap();
        assert!(pv.check(addr(), old).is_err());
        assert_eq!(*pv.last_refusal_log.get(&addr()).unwrap(), first);
        // After the window passes, it logs again (new timestamp).
        *pv.last_refusal_log.get_mut(&addr()).unwrap() =
            Instant::now() - REFUSAL_LOG_INTERVAL - Duration::from_secs(1);
        assert!(pv.check(addr(), old).is_err());
        assert_ne!(*pv.last_refusal_log.get(&addr()).unwrap(), first);
    }

    #[test]
    fn newer_version_refuses_as_server_outdated() {
        let mut pv = PeerVersions::default();
        assert!(pv.check(addr(), shared::PROTOCOL_VERSION + 1).is_err());
    }

    #[test]
    fn upgrade_is_noted_on_reconnect() {
        let mut pv = PeerVersions::default();
        let old = shared::PROTOCOL_VERSION - 1;
        // Refused while old, then comes back matching: the upgrade note fires.
        assert!(pv.check(addr(), old).is_err());
        assert!(pv.check(addr(), shared::PROTOCOL_VERSION).is_ok());
        // No note when nothing changed since the last seen version.
        assert!(pv.check(addr(), shared::PROTOCOL_VERSION).is_ok());
        assert_eq!(pv.seen.get(&addr()), Some(&shared::PROTOCOL_VERSION));
    }
}
