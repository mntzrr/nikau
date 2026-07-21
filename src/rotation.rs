use std::collections::{HashMap, VecDeque};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use quinn::{SendDatagramError, SendStream};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task;
use tracing::{debug, error, info, trace, warn};

use crate::clipboard::{data, server};
use crate::device;
use crate::msgs::{bulk, event};

/// If the selected client reconnects within this long after being removed, then reselect it
/// automatically. This is intended to help with fast recovery following networking flakes.
/// Sized against the LAN QUIC idle timeout (transport.rs): a client that only learns of the
/// drop via the 25s idle timeout needs ~25s to detect it plus an immediate first reconnect
/// attempt; 45s leaves margin for a couple of backoff steps on top of that worst case.
const REMOVED_CLIENT_RECOVERY_DEADLINE: Duration = Duration::from_secs(45);

/// Name of the file (inside the config dir) recording the fingerprint of the
/// client currently switched active. Written on every switch to a client and
/// removed on switch back to the local machine. It deliberately survives
/// shutdown, graceful or not: the next server instance uses it to re-activate
/// that client when it reconnects, making restarts (e.g. after an update)
/// seamless. Staleness is bounded by ACTIVE_CLIENT_MAX_AGE.
pub const ACTIVE_CLIENT_STATE_FILE: &str = "active_client";

/// How old the active-client state may be before it is ignored on startup.
/// Resumption is expected soon after the previous stop (crash or update);
/// resuming a days-old session would be surprising.
const ACTIVE_CLIENT_MAX_AGE: Duration = Duration::from_secs(3600);

/// Minimum spacing between processed clipboard source updates. Clipboard
/// managers (wl-clip-persist, wl-paste --watch) can turn one copy into dozens
/// of updates per second; each processed update costs a fresh wayland
/// connection and data source on the compositor, so bursts are collapsed.
const CLIPBOARD_UPDATE_DEBOUNCE: Duration = Duration::from_millis(300);

/// Minimum spacing between processed rotation switches (next/prev). When the
/// rotation loop is briefly blocked (e.g. a network hiccup delaying a write),
/// every frustrated shortcut press queues another switch; without a debounce
/// they then execute back-to-back and the rotation ends up on a random side.
const SWITCH_DEBOUNCE: Duration = Duration::from_millis(500);

/// Channels for communicating with a connected client.
#[derive(Debug)]
struct ClientInfo {
    /// The primary identifier for a client. We can have multiple clients with the same fingerprint:
    /// - When the user is sharing certificates between clients (they are free to do so)
    /// - When a client has reconnected without the old connection timing out yet
    endpoint: SocketAddr,
    /// Cert fingerprint used to select clients via --shortcut-goto keyboard shortcuts
    fingerprint: String,
    events_send: SendStream,
    /// Queue for the client's bulk writer task, which owns the actual bulk
    /// stream. Keeping large clipboard writes out of the rotation loop means
    /// they never stall input forwarding.
    bulk_tx: mpsc::UnboundedSender<Vec<u8>>,
    /// Connection handle for sending unreliable/unordered QUIC datagrams
    /// (used for high-rate pointer motion; see MotionDatagram).
    conn: quinn::Connection,
    /// Whether the peer accepts QUIC datagrams. Disabled permanently on the
    /// first UnsupportedByPeer/Disabled error, falling back to the stream.
    datagrams_ok: bool,
}

/// Keeps track of the most recently disconnected client,
/// used for automatically reactivating clients if they reconnect quickly.
#[derive(Debug)]
struct DefunctClientInfo {
    /// Use the endpoint, not the fingerprint, to identify recently disconnected clients.
    /// This reduces the likelihood of weird behavior if e.g. clients are sharing certificates.
    /// In practice we only address clients by certificate with certain keyboard shortcuts.
    endpoint: SocketAddr,
    removed_at: Instant,
}

impl DefunctClientInfo {
    /// Returns whether the specified endpoint should be reenabled as the selected client.
    /// true is returned if the IPs match and if the defunct client was disconnected <= N seconds ago.
    fn recoverable(&self, endpoint: SocketAddr, now: &Instant) -> bool {
        // Only check IP, port is expected to change
        endpoint.ip() == self.endpoint.ip() && !self.expired(now)
    }

    /// Returns whether this defunct client info has expired, in which case it can be cleared.
    fn expired(&self, now: &Instant) -> bool {
        now.duration_since(self.removed_at) > REMOVED_CLIENT_RECOVERY_DEADLINE
    }
}

/// Tracks the location and type of the current clipboard
#[derive(Debug)]
struct ClipboardTarget {
    /// None if the clipboard is at the server
    source: Option<SocketAddr>,
    types: Vec<String>,
    max_size_bytes: u64,
}

pub enum RotationEvent {
    /// Request to add a client to the rotation
    AddClient(AddClientArgs),
    /// Request to remove a disconnected client from the rotation
    /// If the client currently owns the clipboard, that status is cleared
    RemoveClient(SocketAddr),
    /// Request to update the current clipboard location and info
    ClipboardUpdateSource(ClipboardUpdateSourceArgs),
    /// Request to fetch a current clipboard's content
    ClipboardRequestContent(ClipboardRequestContentArgs),
    /// Request to send a current clipboard's content in response to a prior request
    ClipboardSendContent(ClipboardSendContentArgs),
}

pub struct AddClientArgs {
    pub endpoint: SocketAddr,
    pub fingerprint: String,
    pub events_send: SendStream,
    pub bulk_send: SendStream,
    pub conn: quinn::Connection,
}

/// Outcome of a pointer-motion datagram send attempt.
enum MotionSend {
    Sent,
    /// The peer can't do datagrams (permanently disabled); use the stream.
    Fallback,
    /// Not queued right now (see SendDatagramError::TooLarge); the caller
    /// keeps the deltas pending and retries on the next opportunity.
    Retry,
}

/// How many recent coalesced motion deltas each datagram repeats (see
/// MotionDatagram.history). At the default 250 Hz flush rate, 32 frames cover
/// a 128 ms loss burst — far longer than a typical WiFi blip — for ~300 extra
/// bytes per datagram (each frame is ≤10 postcard bytes). Full-rate mode sends
/// no redundancy (lost = skipped).
const MOTION_HISTORY_LEN: usize = 32;

/// Returns true if the batch consists solely of relative X/Y pointer motion,
/// which is safe to send over unreliable datagrams: each update is a delta that
/// is immediately superseded by the next one. Buttons, wheel, and absolute axes
/// must NOT be lost or reordered and always stay on the ordered stream.
fn is_pure_pointer_motion(events: &[event::InputEvent]) -> bool {
    const EV_REL: u16 = evdev::EventType::RELATIVE.0;
    const REL_X: u16 = evdev::RelativeAxisCode::REL_X.0;
    const REL_Y: u16 = evdev::RelativeAxisCode::REL_Y.0;
    !events.is_empty()
        && events.iter().all(|e| {
            e.inputf64.is_none()
                && matches!(&e.inputi32, Some(i) if i.type_ == EV_REL && (i.code == REL_X || i.code == REL_Y))
        })
}

pub struct ClipboardUpdateSourceArgs {
    pub source: Option<SocketAddr>,
    pub types: Vec<String>,
    // min of source_client_max (if any), and server_max:
    pub max_size_bytes: u64,
}

pub struct ClipboardRequestContentArgs {
    pub request_source: ClipboardRequestSource,
    pub requested_type: String,
    pub max_size_bytes: u64,
    /// The request id assigned by the originator.
    /// None when the request originates locally on the server (an id is assigned
    /// during routing); Some(id) when forwarded from a client's request.
    pub request_id: Option<u64>,
}

/// Pointer to where clipboard data should be sent once it's been fetched
pub enum ClipboardRequestSource {
    /// The clipboard is being requested from the local (server) machine.
    /// The oneshot can be used for sending back the clipboard result.
    Local(oneshot::Sender<data::ClipboardData>),

    /// The clipboard is being requested from a remote client.
    /// The data should be sent to the client's address.
    Remote(SocketAddr),
}

impl<'a> std::fmt::Display for ClipboardRequestSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClipboardRequestSource::Local(_) => f.write_str("Local"),
            ClipboardRequestSource::Remote(addr) => {
                f.write_str(format!("Remote({})", addr).as_str())
            }
        }
    }
}

pub struct ClipboardSendContentArgs {
    /// The client sending the clipboard data
    pub data_source: SocketAddr,
    /// Copied from the ServerClipboardRequest, indicates where the clipboard data should be sent
    pub request_client: Option<SocketAddr>,
    /// Copied from the ClientClipboardHeader, correlates the content with its request
    pub request_id: u64,
    pub data: data::ClipboardData,
}

/// Input-flow counters for the current status window (see log_input_status).
/// They exist to make "the user is typing but nothing arrives anywhere"
/// observable instead of silent.
#[derive(Default)]
struct InputCounts {
    /// Physical events read from local devices.
    physical: u64,
    /// Events forwarded to the remote client.
    forwarded: u64,
    /// Events emitted to local virtual devices.
    emitted_local: u64,
}

/// Mirror of the rotation loop's diagnostic state, read directly by the
/// SIGHUP handler on the signal thread. The dump must work when the loop
/// itself is stalled — that scenario is exactly what it exists to debug — so
/// nothing here touches the loop's channels: an atomic liveness timestamp
/// plus a pre-formatted state string behind a std Mutex that is only held for
/// a swap/clone. The rotation loop refreshes it after every iteration (see
/// Rotation::update_diagnostics).
pub struct DiagnosticsMirror {
    /// Base for the liveness timestamp.
    started: Instant,
    /// Milliseconds since `started` when the rotation loop last completed an
    /// iteration. The loop wakes at least every 10s (input-status heartbeat),
    /// so a value much older than that in a dump means the loop is stuck.
    last_iteration_ms: AtomicU64,
    /// The dumpable state, formatted by the loop after each iteration.
    state: Mutex<String>,
}

impl DiagnosticsMirror {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            last_iteration_ms: AtomicU64::new(0),
            state: Mutex::new("<rotation loop has not completed an iteration yet>".to_string()),
        }
    }

    /// Stamps loop liveness and swaps in the latest formatted state.
    /// Rotation-loop side only.
    fn update(&self, state: String) {
        self.last_iteration_ms.store(
            self.started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        if let Ok(mut s) = self.state.lock() {
            *s = state;
        }
    }

    /// Logs the full state dump for SIGHUP. Runs on the signal thread, so it
    /// must never wait on the rotation loop: it only reads this mirror.
    pub fn dump(&self) {
        let age_ms = self
            .started
            .elapsed()
            .as_millis()
            .saturating_sub(self.last_iteration_ms.load(Ordering::Relaxed) as u128);
        let state = match self.state.lock() {
            Ok(s) => s.clone(),
            Err(_) => "<diagnostics state lock poisoned>".to_string(),
        };
        info!(
            "Diagnostics dump (SIGHUP): rotation loop last completed an iteration {}ms ago (a healthy loop iterates at least every 10s); {}",
            age_ms,
            state
        );
    }
}

pub struct Rotation<O: device::output::OutputHandler> {
    grab_tx: watch::Sender<device::GrabEvent>,
    output_handler: O,
    clients: Vec<ClientInfo>,
    /// Use the endpoint, not the fingerprint, to uniquely identify clients.
    /// This allows situations like a client reconnecting before the old socket has closed.
    current_client: Option<SocketAddr>,
    removed_current_client: Option<DefunctClientInfo>,
    /// Path of the file recording the active client's fingerprint for
    /// crash recovery (see ACTIVE_CLIENT_STATE_FILE).
    active_client_path: PathBuf,
    /// Fingerprint of the client that was active when the previous server
    /// instance exited unexpectedly. That client is re-activated
    /// automatically when it reconnects.
    pending_resume_fingerprint: Option<String>,

    /// Tracking the current clipboard owner, whether it's at the server or a client.
    clipboard_target: Option<ClipboardTarget>,
    /// Access to the local system clipboard on the server.
    local_clipboard: Option<server::LocalClipboard>,
    /// Pending clipboard fetches for the local server machine, keyed by request id.
    pending_clipboard_requests: HashMap<u64, oneshot::Sender<data::ClipboardData>>,
    /// Next server-originated clipboard request id. Wrapping is fine: ids only
    /// need to correlate a reply with its request, not resist adversaries.
    next_clipboard_request_id: u64,
    /// Self-handle for spawned tasks (e.g. per-client bulk writers) to report
    /// events back to the rotation loop, such as client removal on stream failure.
    rotation_tx: mpsc::Sender<RotationEvent>,
    /// When the last clipboard source update was processed; used to debounce
    /// machine-paced update bursts from clipboard managers (see
    /// CLIPBOARD_UPDATE_DEBOUNCE).
    last_clipboard_update: Option<Instant>,
    /// Sequence number for the next pointer-motion datagram (per server; only
    /// monotonicity within a client connection matters for stale-drop).
    motion_seq: u64,
    /// Set once we've logged that pointer motion is using QUIC datagrams.
    motion_datagram_announced: bool,
    /// Input-flow counters for the current status window (see log_input_status).
    status_counts: InputCounts,
    /// When the current status window started.
    status_window_start: Instant,
    /// Coalescing accumulator for relative pointer motion (dx, dy, source event
    /// count), flushed on a timer at the --motion-hz rate. Deltas are summed
    /// losslessly: the cursor ends up in the same place with far less traffic.
    pending_motion: (i32, i32, u64),
    /// Whether pending_motion holds unsent deltas.
    motion_dirty: bool,
    /// Recently flushed motion deltas, newest first. Each coalesced datagram
    /// repeats up to MOTION_HISTORY_LEN of them so the client can heal frames
    /// lost on the wire (see MotionDatagram.history). Cleared on every switch:
    /// deltas flushed to one client are moot for another.
    motion_history: VecDeque<(i32, i32)>,
    /// Flush interval for motion coalescing; None = forward every batch
    /// immediately (e.g. --motion-hz 0 for gaming).
    motion_flush_interval: Option<Duration>,
    /// When the last next/prev switch was processed (see SWITCH_DEBOUNCE).
    last_switch_at: Option<Instant>,
    /// Loop-independent mirror of this rotation's diagnostic state, dumped by
    /// the SIGHUP handler without involving the loop (see DiagnosticsMirror).
    diagnostics: Arc<DiagnosticsMirror>,
}

impl<O: device::output::OutputHandler> Rotation<O> {
    pub async fn new(
        grab_tx: watch::Sender<device::GrabEvent>,
        output_handler: O,
        local_clipboard: Option<server::LocalClipboard>,
        config_dir: &Path,
        rotation_tx: mpsc::Sender<RotationEvent>,
        motion_flush_interval: Option<Duration>,
        diagnostics: Arc<DiagnosticsMirror>,
    ) -> Result<Self> {
        let active_client_path = active_client_state_path(config_dir);
        let pending_resume_fingerprint = load_pending_resume(&active_client_path);
        if let Some(pending) = &pending_resume_fingerprint {
            info!(
                "A client ({}) was active when the server last stopped; it will be re-activated when it reconnects",
                pending
            );
        }
        Ok(Rotation {
            grab_tx,
            output_handler,
            clients: Vec::new(),
            current_client: None,
            removed_current_client: None,
            active_client_path,
            pending_resume_fingerprint,
            clipboard_target: None,
            local_clipboard,
            pending_clipboard_requests: HashMap::new(),
            next_clipboard_request_id: 0,
            rotation_tx,
            last_clipboard_update: None,
            motion_seq: 0,
            motion_datagram_announced: false,
            status_counts: InputCounts::default(),
            status_window_start: Instant::now(),
            pending_motion: (0, 0, 0),
            motion_dirty: false,
            motion_history: VecDeque::new(),
            motion_flush_interval,
            last_switch_at: None,
            diagnostics,
        })
    }

    pub async fn accept(&mut self, event: RotationEvent) {
        match event {
            RotationEvent::AddClient(args) => {
                self.add_client(
                    args.endpoint,
                    args.fingerprint,
                    args.events_send,
                    args.bulk_send,
                    args.conn,
                )
                .await
            }
            RotationEvent::RemoveClient(endpoint) => {
                self.remove_client_and_clear_clipboard(endpoint).await
            }
            RotationEvent::ClipboardUpdateSource(args) => {
                if let Err(e) = self
                    .clipboard_update_source(args.source, args.types, args.max_size_bytes)
                    .await
                {
                    warn!("Failed to update clipboard source to server: {:?}", e);
                }
            }
            RotationEvent::ClipboardRequestContent(args) => {
                if let Err(e) = self
                    .clipboard_request_content(
                        args.request_source,
                        &args.requested_type,
                        args.max_size_bytes,
                        args.request_id,
                    )
                    .await
                {
                    warn!("Failed to retrieve clipboard content for server: {:?}", e);
                }
            }
            RotationEvent::ClipboardSendContent(args) => {
                if let Err(e) = self
                    .clipboard_send_content_from_client(
                        args.data_source,
                        args.request_client,
                        args.request_id,
                        args.data,
                    )
                    .await
                {
                    warn!("Failed to send clipboard content to client: {:?}", e);
                }
            }
        }
    }

    async fn add_client(
        &mut self,
        endpoint: SocketAddr,
        fingerprint: String,
        events_send: SendStream,
        bulk_send: SendStream,
        conn: quinn::Connection,
    ) {
        // Dedicated writer task for this client's bulk stream: clipboard payloads
        // can be megabytes, and writing them inline would stall input forwarding
        // for the whole rotation. The task also keeps each header glued to its
        // payload by writing queued byte blobs sequentially.
        let (bulk_tx, mut bulk_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        {
            let rotation_tx = self.rotation_tx.clone();
            let mut bulk_send = bulk_send;
            task::spawn(async move {
                while let Some(bytes) = bulk_rx.recv().await {
                    trace!("Sending {} byte bulk message to {}", bytes.len(), endpoint);
                    if let Err(e) = bulk_send.write_all(&bytes).await {
                        warn!("Bulk stream to {} failed, removing client: {:?}", endpoint, e);
                        let _ = rotation_tx
                            .send(RotationEvent::RemoveClient(endpoint))
                            .await;
                        return;
                    }
                }
            });
        }
        let info = ClientInfo {
            endpoint,
            fingerprint: fingerprint.clone(),
            events_send,
            bulk_tx,
            conn,
            datagrams_ok: true,
        };
        // Clients stay sorted by endpoint as an arbitrary consistent order across
        // sessions. An identical endpoint can already be present when a reconnect
        // lands before the old connection's removal: update that entry in place
        // instead of inserting a duplicate (a later removal would clear only the
        // first copy, leaving a dead one behind).
        match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => self.clients[idx] = info,
            Err(idx) => self.clients.insert(idx, info),
        }

        info!(
            "Added client {} @ {} to rotation: {}",
            fingerprint,
            endpoint,
            self.clients
                .iter()
                .map(|c| c.endpoint.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        // If the new client has the same IP as the currently enabled client, it's probably a fast retry
        // where we haven't removed the prior session yet. Mark the new client as enabled/current.
        // If two clients were connected from the same IP then this will result in spurious switches,
        // but that shouldn't be the case in practice.
        if let Some(current_client) = &self.current_client {
            // Only check IP: port is expected to change between sessions
            if current_client.ip() == endpoint.ip() {
                self.update_current_client(Some(endpoint)).await;
            }
        }

        // If the new client has the same IP as a recently disconnected client that was enabled,
        // it's probably a slow reconnect. Mark the new client as enabled/current.
        if let Some(removed_current_client) = &self.removed_current_client {
            // Only check IP: port is expected to change between sessions
            let now = Instant::now();
            if removed_current_client.recoverable(endpoint, &now) {
                // Enable this client automatically since it was recently disconnected
                // This automatically unsets self.removed_current_client
                self.update_current_client(Some(endpoint)).await;
            } else if removed_current_client.expired(&now) {
                // Clean up expired client info
                self.removed_current_client = None;
            }
        }

        // Session resumption: this client was active when the previous server
        // instance stopped (crash or intentional restart, e.g. after an update).
        // Re-activate it immediately.
        if let Some(pending) = &self.pending_resume_fingerprint {
            if *pending == fingerprint {
                self.pending_resume_fingerprint = None;
                info!(
                    "Resuming session: re-activating client {} that was active when the previous server stopped",
                    endpoint
                );
                self.update_current_client(Some(endpoint)).await;
            }
        }

        // Announce clipboard to client, if its IP doesn't match the clipboard owner's IP.
        // Matching IP would indicate that the client is reconnecting but we haven't disconnected the old one yet.
        if let Some(clipboard_target) = &self.clipboard_target {
            if match clipboard_target.source {
                // Client has clipboard. Make sure it's not the same client IP.
                Some(clipboard_source) => clipboard_source.ip() != endpoint.ip(),
                // Server has clipboard.
                None => true,
            } {
                // Tell the new client about the current clipboard status.
                let types_str = clipboard_target.types.join(" ");
                let types_msg = event::ServerEvent::ClipboardTypes(event::ClipboardTypes {
                    types: &types_str,
                    max_size_bytes: clipboard_target.max_size_bytes,
                });
                if let Err(e) = self.send_event(&endpoint, types_msg).await {
                    // This shouldn't happen in practice, given we just added the client...
                    warn!("Newly added client already failed and was removed: {:?}", e);
                }
            }
        }
    }

    async fn remove_client_and_clear_clipboard(&mut self, endpoint: SocketAddr) {
        if self.handle_client_removal(&endpoint).await {
            self.clipboard_clear().await;
        }
    }

    /// Returns true if a next/prev switch request should be ignored because one
    /// was just processed (see SWITCH_DEBOUNCE); otherwise records it and
    /// returns false.
    fn switch_debounced(&mut self) -> bool {
        if let Some(last) = self.last_switch_at {
            if last.elapsed() < SWITCH_DEBOUNCE {
                debug!(
                    "Ignoring switch request: a switch happened {:?} ago",
                    last.elapsed()
                );
                return true;
            }
        }
        self.last_switch_at = Some(Instant::now());
        false
    }

    /// Switches to the previous client (or to the server) in the arbitrary rotation.
    pub async fn prev_client(&mut self) {
        if self.switch_debounced() {
            return;
        }
        if let Some(current_client) = &self.current_client {
            // Currently on remote machine, find its entry in the list and go to the prev one
            let idx = match self
                .clients
                .binary_search_by(|c| c.endpoint.cmp(current_client))
            {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            if idx == 0 {
                // At start of vec or vec is empty - switch to local machine
                self.update_current_client(None).await;
            } else {
                // Go to prev entry in vec
                self.update_current_client(self.clients.get(idx - 1).map(|c| c.endpoint))
                    .await;
            }
        } else {
            // Currently on local machine, go to last entry on vec (if any)
            self.update_current_client(self.clients.last().map(|c| c.endpoint))
                .await;
        }
    }

    /// Switches to the next client (or to the server) in the arbitrary rotation.
    pub async fn next_client(&mut self) {
        if self.switch_debounced() {
            return;
        }
        if let Some(current_client) = &self.current_client {
            // Currently on remote machine, find its entry in the list and go to the next one
            let idx = match self
                .clients
                .binary_search_by(|c| c.endpoint.cmp(current_client))
            {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            // Go to next entry in vec, or fall back to local machine if vec is empty or we're off the end
            self.update_current_client(self.clients.get(idx + 1).map(|c| c.endpoint))
                .await;
        } else {
            // Currently on local machine, go to first entry on vec (if any)
            self.update_current_client(self.clients.first().map(|c| c.endpoint))
                .await;
        }
    }

    /// Switches to the specified client by fingerprint, or to the server if the fingerprint is empty.
    /// If a matching client isn't connected, does nothing.
    pub async fn set_client(&mut self, fingerprint: String) {
        if fingerprint.is_empty() {
            // Empty fingerprint means "go to server"
            self.update_current_client(None).await;
        } else {
            // Find the matching client, if any. Allow "abcd123" to match client with "abcd12345[...]"
            let matching_clients: Vec<&ClientInfo> = self
                .clients
                .iter()
                .filter(|c| c.fingerprint.starts_with(&fingerprint))
                .collect();
            match matching_clients.len() {
                0 => {
                    warn!(
                        "Missing client with fingerprint {}, doing nothing",
                        fingerprint
                    );
                }
                1 => {
                    let endpoint = matching_clients
                        .first()
                        .expect("matching_clients has len=1")
                        .endpoint;
                    self.update_current_client(Some(endpoint)).await;
                }
                _ => {
                    warn!(
                        "Multiple clients match fingerprint {}, doing nothing: {:?}",
                        fingerprint, matching_clients
                    );
                }
            }
        }
    }

    /// Updates the tracked location for the current clipboard,
    /// whether on the server host or on a remote client.
    async fn clipboard_update_source(
        &mut self,
        source: Option<SocketAddr>,
        types: Vec<String>,
        // min of source_client_max (if any), and server_max:
        max_size_bytes: u64,
    ) -> Result<()> {
        debug!("Announcing new clipboard source: source={:?} current={:?} with max_size_bytes={} has types={:?}", source, self.current_client, max_size_bytes, types);
        // A local update with no types means the compositor revoked the
        // selection (the owning app exited and no clipboard manager persisted
        // it): the tracked target is stale and must stop being announced, or
        // every fetch against it fails. Clear right away, bypassing the
        // debounce, and reset its timestamp so a clipboard manager re-owning
        // the content immediately after isn't debounced away.
        if source.is_none() && types.is_empty() {
            self.last_clipboard_update = None;
            self.clipboard_clear().await;
            return Ok(());
        }
        // The clipboard changed hands: drop any cached served payload so
        // stale contents are never served. Lock-free (an epoch bump), so it
        // never waits on a serve in progress. This must happen even when the
        // update is debounced away below: a debounced update still means the
        // clipboard changed, and the old cache would otherwise keep being
        // served.
        if let Some(reader) = self.local_clipboard.as_ref().map(|lc| lc.reader_handle()) {
            reader.invalidate();
        }
        // Debounce machine-paced bursts: clipboard managers (wl-clip-persist,
        // wl-paste --watch) can turn one copy into dozens of source updates per
        // second, and each processed update costs a fresh wayland connection
        // and source on the compositor. Collapse bursts to one update per
        // CLIPBOARD_UPDATE_DEBOUNCE; legit copies are human-paced and unaffected.
        if let Some(last) = self.last_clipboard_update {
            if last.elapsed() < CLIPBOARD_UPDATE_DEBOUNCE {
                debug!("Debouncing rapid clipboard source update");
                return Ok(());
            }
        }
        // Break clipboard-manager ping-pong: an update identical to the current
        // target (e.g. wl-clip-persist re-owning the same clipboard, or a
        // wl-paste --watch echo of it) must not trigger another round of
        // type advertisements, or the two machines churn each other forever.
        // The serve cache was still invalidated above: content may differ.
        if let Some(current) = &self.clipboard_target {
            if current.source == source && types_equal(&current.types, &types) {
                debug!("Ignoring duplicate clipboard source update (unchanged source and types)");
                return Ok(());
            }
        }
        self.last_clipboard_update = Some(Instant::now());
        // Save the clipboard types/source for future retrievals and client switches
        self.clipboard_target = Some(ClipboardTarget {
            source,
            types,
            max_size_bytes,
        });

        // Notify the active client (or server) about the clipboard info we just received.
        // In practice we should be getting this shortly after a client switch.
        self.update_current_client_clipboard().await?;

        Ok(())
    }

    /// Routes a request for clipboard content to a remote client or a local application.
    /// Fetches that can't be served get an immediate empty reply, so the
    /// requester's paste fails fast instead of waiting out its fetch timeout.
    async fn clipboard_request_content(
        &mut self,
        request_source: ClipboardRequestSource,
        requested_type: &str,
        max_size_bytes: u64,
        request_id: Option<u64>,
    ) -> Result<()> {
        debug!("Handling clipboard content request from source={} with max_size_bytes={} for requested type {}: have {:?}", request_source, max_size_bytes, requested_type, self.clipboard_target);

        let target = match &self.clipboard_target {
            Some(c) => c,
            None => {
                if let ClipboardRequestSource::Remote(client) = &request_source {
                    let client = *client;
                    self.reply_empty_clipboard_fetch(&client, requested_type, request_id.unwrap_or(0))
                        .await;
                }
                bail!(
                    "No clipboard types available: request from {} for requested type {}",
                    request_source,
                    requested_type
                );
            }
        };
        // Sanity check: Is the requested type among the list of supported types?
        if !target.types.contains(&requested_type.to_string()) {
            if let ClipboardRequestSource::Remote(client) = &request_source {
                let client = *client;
                self.reply_empty_clipboard_fetch(&client, requested_type, request_id.unwrap_or(0))
                    .await;
            }
            bail!(
                "Requested clipboard type {} from source {} isn't among available types: {:?}",
                requested_type,
                request_source,
                target.types
            );
        }

        // Figure out where the requested clipboard can be found
        if let Some(clipboard_source) = target.source.clone() {
            // A client has the clipboard: route request to them.
            // Request ids correlate a response with its request. A plain
            // per-originator counter is enough: the goal is
            // accidental-misdelivery protection, not adversarial resistance.
            let (msg, local_request_id, on_behalf_of) = match request_source {
                ClipboardRequestSource::Local(waiting_clipboard_tx) => {
                    // Clipboard request is from the server itself.
                    // Keep the oneshot for replying later, keyed by a fresh request id.
                    let request_id = self.next_clipboard_request_id;
                    self.next_clipboard_request_id =
                        self.next_clipboard_request_id.wrapping_add(1);
                    // Drop entries whose requester already gave up (timed out).
                    self.pending_clipboard_requests
                        .retain(|_, tx| !tx.is_closed());
                    self.pending_clipboard_requests
                        .insert(request_id, waiting_clipboard_tx);
                    let msg = bulk::ServerBulk::ClipboardRequest(bulk::ServerClipboardRequest {
                        requested_type,
                        max_size_bytes,
                        request_client: None,
                        request_id,
                    });
                    (msg, Some(request_id), None)
                }
                ClipboardRequestSource::Remote(client) => {
                    // Clipboard request is from a client: forward its request id.
                    let request_id = match request_id {
                        Some(id) => id,
                        None => {
                            warn!("Clipboard request from {} is missing a request_id, using 0", client);
                            0
                        }
                    };
                    let msg = bulk::ServerBulk::ClipboardRequest(bulk::ServerClipboardRequest {
                        requested_type,
                        max_size_bytes,
                        request_client: Some(client),
                        request_id,
                    });
                    (msg, None, Some(client))
                }
            };
            debug!(
                "Requesting clipboard data with type {} from {}{}",
                requested_type,
                clipboard_source,
                match on_behalf_of {
                    Some(client) => format!(" on behalf of {}", client),
                    None => "".to_string(),
                }
            );
            let sent = self.send_bulk(&clipboard_source, msg, None).await;
            if let Some(request_id) = local_request_id {
                if !matches!(sent, Ok(true)) {
                    // The request couldn't be sent: drop the pending fetch so that
                    // it fails fast instead of waiting out the 5s timeout.
                    self.pending_clipboard_requests.remove(&request_id);
                }
            }
            match sent {
                Ok(true) => {}
                Ok(false) => {
                    if let Some(client) = on_behalf_of {
                        // The owning peer is gone: fail the requester's fetch
                        // fast instead of letting it wait out its timeout.
                        self.reply_empty_clipboard_fetch(&client, requested_type, request_id.unwrap_or(0))
                            .await;
                        warn!(
                            "Unable to send request for clipboard to {} on behalf of {}: not connected (clients: {:?})",
                            clipboard_source,
                            client,
                            self.clients,
                        );
                    } else {
                        warn!(
                            "Unable to send request for clipboard to {}: not connected (clients: {:?})",
                            clipboard_source,
                            self.clients,
                        );
                    }
                }
                Err(e) => return Err(e),
            }
            Ok(())
        } else {
            // The server has the clipboard: serve via X11 from local app
            let request_client = if let ClipboardRequestSource::Remote(c) = &request_source {
                c
            } else {
                // The monux server process is getting asked for a clipboard from itself.
                // The server should only locally serve clipboards from remote clients, but there isn't one.
                // This may mean that the serving client disconnected, but we should have cleared the status.
                bail!(
                    "Server got local clipboard request against itself? current_clipboard={:?}",
                    target
                );
            };
            // Echo the requesting client's id back in the response.
            let request_id = match request_id {
                Some(id) => id,
                None => {
                    warn!("Clipboard request from {} is missing a request_id, using 0", request_client);
                    0
                }
            };
            let local_clipboard = match &self.local_clipboard {
                Some(c) => c,
                None => {
                    self.reply_empty_clipboard_fetch(request_client, requested_type, request_id)
                        .await;
                    bail!("Fetch for local server clipboard but server clipboard is disabled");
                }
            };
            let reader = local_clipboard.reader_handle();
            // Look up the requesting client's bulk queue before spawning.
            let bulk_tx = match self
                .clients
                .binary_search_by(|c| c.endpoint.cmp(request_client))
            {
                Ok(idx) => self
                    .clients
                    .get(idx)
                    .expect("missing request_client")
                    .bulk_tx
                    .clone(),
                Err(_idx) => {
                    warn!(
                        "Unable to send server clipboard data to {}: not connected (clients: {:?})",
                        request_client, self.clients
                    );
                    return Ok(());
                }
            };
            // Reading the clipboard can take seconds for large copies (files get
            // zipped from disk), so serve it from a spawned task: the rotation
            // loop must keep forwarding input meanwhile.
            let request_client = *request_client;
            let requested_type = requested_type.to_string();
            task::spawn(async move {
                // A failed read (clipboard gone, hung source app) still gets an
                // immediate reply — empty content — so the requester's paste
                // completes right away instead of waiting out its fetch
                // timeout. The next paste simply re-requests.
                let (content, data_type) = match server::LocalClipboard::read(
                    &reader,
                    &requested_type,
                    max_size_bytes,
                    &request_client,
                )
                .await
                {
                    Ok(ok) => ok,
                    Err(e) => {
                        warn!(
                            "Failed to read server clipboard for {}: {:?}",
                            request_client, e
                        );
                        (Vec::new(), None)
                    }
                };
                let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
                    requested_type: &requested_type,
                    data_type: data_type.as_ref().map(|t| t.as_str()),
                    content_len_bytes: content.len() as u64,
                    request_id,
                });
                match postcard::to_stdvec_cobs(&msg) {
                    Ok(mut bytes) => {
                        bytes.extend_from_slice(&content);
                        if bulk_tx.send(bytes).is_err() {
                            warn!(
                                "Unable to send server clipboard data to {}: bulk queue closed",
                                request_client
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize clipboard header: {:?}", e);
                    }
                }
            });
            Ok(())
        }
    }

    /// Replies to a remote client's clipboard fetch with empty content, so its
    /// paste completes (with nothing) immediately instead of waiting out its
    /// fetch timeout. Sent whenever a fetch can't be served: the clipboard is
    /// gone, the requested type isn't offered, or the owning peer is gone.
    /// Best-effort: an unconnected requester simply gets nothing, as before.
    async fn reply_empty_clipboard_fetch(
        &self,
        request_client: &SocketAddr,
        requested_type: &str,
        request_id: u64,
    ) {
        let bulk_tx = match self
            .clients
            .binary_search_by(|c| c.endpoint.cmp(request_client))
        {
            Ok(idx) => self
                .clients
                .get(idx)
                .expect("missing request_client")
                .bulk_tx
                .clone(),
            Err(_idx) => return,
        };
        let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
            requested_type,
            data_type: None,
            content_len_bytes: 0,
            request_id,
        });
        match postcard::to_stdvec_cobs(&msg) {
            Ok(bytes) => {
                if bulk_tx.send(bytes).is_err() {
                    warn!(
                        "Unable to send empty clipboard reply to {}: bulk queue closed",
                        request_client
                    );
                }
            }
            Err(e) => {
                error!("Failed to serialize empty clipboard header: {:?}", e);
            }
        }
    }

    /// Sends clipboard content in response to a prior request via clipboard_request_content.
    async fn clipboard_send_content_from_client(
        &mut self,
        // The client sending the clipboard data
        data_source: SocketAddr,
        // Copied from the ServerClipboardRequest, indicates where the clipboard data should be sent
        request_client: Option<SocketAddr>,
        // Copied from the ClientClipboardHeader, correlates the content with its request
        request_id: u64,
        data: data::ClipboardData,
    ) -> Result<()> {
        debug!(
            "Sending clipboard content of requested_type={} data_type={:?} with len={} from source={} to dest={:?}",
            data.requested_type,
            data.data_type,
            data.bytes.len(),
            data_source,
            request_client
        );
        if let Some(request_client) = request_client {
            // Send to specified remote client (assuming it's still available etc...)
            let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
                requested_type: &data.requested_type,
                data_type: data.data_type.as_ref().map(|t| t.as_str()),
                content_len_bytes: data.bytes.len() as u64,
                request_id,
            });
            // If send_bulk returns Ok(false), the client wasn't found. In that case just ignore the request,
            // don't try to reset state since the client should already be removed.
            if !(self
                .send_bulk(&request_client, msg, Some(data.bytes))
                .await?)
            {
                warn!("Unable to send clipboard data received from {} to {}: not connected (clients: {:?})",
                      data_source, request_client, self.clients);
            }
        } else {
            // Send to local X11 clipboard, completing the pending fetch that made the request.
            match self.pending_clipboard_requests.remove(&request_id) {
                Some(waiting_clipboard_tx) => {
                    if let Err(_d_again) = waiting_clipboard_tx.send(data) {
                        warn!(
                            "Discarding clipboard data for request_id={}: the requester already gave up (timed out?)",
                            request_id
                        );
                    }
                }
                None => {
                    warn!(
                        "Discarding clipboard data for unknown/timed-out request_id={}",
                        request_id
                    );
                }
            }
        }
        Ok(())
    }

    /// Updates internal state to route future events to the new client (or to the server).
    /// Goes through the steps of notifying the new client that it's active (if new_client is Some),
    /// then notifying any old client that it's inactive (if old_client is Some).
    async fn update_current_client(&mut self, new_client: Option<SocketAddr>) {
        // Either we automatically reenabled a client, or the user manually did.
        // In either case, clear up any history of previously enabled disconnected clients.
        self.removed_current_client = None;

        // Check if the client is already assigned, treat as a no-op if so
        match (&new_client, &self.current_client) {
            (Some(new_client), Some(current_client)) => {
                if new_client == current_client {
                    debug!("Already switched to client: {}", current_client);
                    return;
                }
            }
            (None, None) => {
                debug!("Already switched to local machine");
                return;
            }
            (_, _) => {}
        }

        // Save the old client for sending enabled=false below
        let old_client = self.current_client;

        self.set_and_grab_current_client(new_client).await;

        if let Some(new_client) = new_client {
            // Try to send switch{true} to the newly assigned current_client.
            // If it fails then current_client is cleaned up.
            if let Ok(()) = self
                .send_event_to_remote_client(event::ServerEvent::Switch(event::SwitchEvent {
                    enabled: true,
                }))
                .await
            {
                info!(
                    "Switched to client: {} (clients: {})",
                    new_client,
                    self.clients
                        .iter()
                        .map(|c| c.endpoint.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                notify_switch(&format!("Input on {}", new_client.ip()));
            }
        } else {
            info!(
                "Switched to local machine (clients: {})",
                self.clients
                    .iter()
                    .map(|c| c.endpoint.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            notify_switch("Input on this machine");
        }

        // Notify the new client (or server) about any current clipboard info, or a noop if it fails.
        // This may be overridden if the old client sends a clipboard update following the switch,
        // or it won't, if the old client doesn't have a clipboard update to send.
        // log_info=false: Avoid duplicate info-level logging of clipboard types, between the server
        // switch and then (potentially) an update from the client that's being deactivated.
        if let Err(e) = self.update_current_client_clipboard().await {
            warn!(
                "Failed to send clipboard update to active client/server: {:?}",
                e
            );
        }

        // AFTER setting up the new client, lets send enabled=false to the old client.
        // This avoids a potential race between the above clipboard update for current data
        // vs the old client sending a new clipboard update when it's marked inactive.
        if let Some(old_client) = old_client {
            // Try to send switch{false} to last current_client.
            // If it fails then the client is cleaned up.
            let _ = self
                .send_event(
                    &old_client,
                    event::ServerEvent::Switch(event::SwitchEvent { enabled: false }),
                )
                .await;
        }
    }

    /// Updates and announces the current clipboard source for handling any future paste requests.
    /// In practice this occurs when a client broadcasts its clipboard shortly after being told its no longer active.
    async fn update_current_client_clipboard(&mut self) -> Result<()> {
        let c = match &self.clipboard_target {
            Some(c) => c,
            // No clipboard to announce
            None => return Ok(()),
        };

        if let Some(clipboard_source) = &c.source {
            // The clipboard is from a client.
            if let Some(current_client) = self.current_client {
                // A remote client is active. Tell it about the clipboard, if it isn't the source of the clipboard.
                if current_client != *clipboard_source {
                    let types_str = c.types.join(" ");
                    let types_msg = event::ServerEvent::ClipboardTypes(event::ClipboardTypes {
                        types: &types_str,
                        max_size_bytes: c.max_size_bytes,
                    });
                    debug!(
                        "Sending clipboard types for {} to {}: {}",
                        clipboard_source, current_client, types_str
                    );
                    self.send_event_to_remote_client(types_msg).await?;
                }
            } else if let Some(local_clipboard) = &mut self.local_clipboard {
                // The server is active and its clipboard support is enabled.
                // Tell it about the client clipbard.
                debug!(
                    "Storing clipboard types for {} on server: {}",
                    clipboard_source,
                    c.types.join(" ")
                );
                local_clipboard.store_types(c.types.clone())?;
            } else {
                debug!("Ignoring clipboard types sent by client: Server clipboard is disabled");
            }
        } else {
            // The clipboard is from the server.
            if let Some(current_client) = self.current_client {
                // A remote client is active. Tell it about the clipboard.
                let types_str = c.types.join(" ");
                let types_msg = event::ServerEvent::ClipboardTypes(event::ClipboardTypes {
                    types: &types_str,
                    max_size_bytes: c.max_size_bytes,
                });
                debug!(
                    "Sending clipboard types for server to {}: {}",
                    current_client, types_str
                );
                self.send_event_to_remote_client(types_msg).await?;
            }
        }
        Ok(())
    }

    /// Sends an event to all connected clients, removing any where sending fails.
    /// If this returns true, then clipboard_clear() should also be called.
    async fn send_event_all<F>(&mut self, msg: event::ServerEvent<'_>, test_fn: F) -> Result<bool>
    where
        F: Fn(&ClientInfo) -> bool,
    {
        let mut clients_to_remove = vec![];
        let mut last_err = None;
        for client in self.clients.iter_mut() {
            if test_fn(client) {
                if let Err(e) = send_message_to_client(&mut client.events_send, &msg).await {
                    clients_to_remove.push(client.endpoint);
                    last_err = Some(e);
                }
            }
        }
        // Reverse: Avoid issues with idx moving as entries are removed
        clients_to_remove.reverse();
        let mut should_clear_clipboard = false;
        for endpoint in clients_to_remove {
            if self.handle_client_removal(&endpoint).await {
                should_clear_clipboard = true;
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(should_clear_clipboard)
        }
    }

    /// Periodic INFO snapshot of input flow, plus warnings for the two ways
    /// input can silently die: grabbed locally but nothing emitted, or a client
    /// is active but nothing is forwarded. Called on a timer from the server
    /// events loop; counters reset each call.
    pub fn log_input_status(&mut self) {
        let counts = std::mem::take(&mut self.status_counts);
        let secs = self.status_window_start.elapsed().as_secs_f64().max(0.1);
        self.status_window_start = Instant::now();
        let grab = format!("{:?}", *self.grab_tx.borrow());
        // Stay silent when completely idle on the local machine: a freeze
        // window always has non-zero counts, so silence loses no evidence.
        let idle_local = self.current_client.is_none()
            && counts.physical == 0
            && counts.forwarded == 0
            && counts.emitted_local == 0;
        if idle_local {
            return;
        }
        match self.current_client {
            Some(endpoint) => info!(
                "Input status: switched to {} ({}): {} events in, {} forwarded ({:.1}/s)",
                endpoint,
                grab,
                counts.physical,
                counts.forwarded,
                counts.forwarded as f64 / secs
            ),
            None => info!(
                "Input status: local ({}): {} events in, {} emitted locally ({:.1}/s)",
                grab,
                counts.physical,
                counts.emitted_local,
                counts.emitted_local as f64 / secs
            ),
        }
        // Swallow detection: physical input arrived but had nowhere to go.
        // The event threshold avoids false positives from a consumed switch combo.
        if counts.physical >= 8 {
            if self.current_client.is_some() && counts.forwarded == 0 {
                warn!(
                    "INPUT SWALLOWED: {} physical events seen while switched to a client, but none were forwarded!",
                    counts.physical
                );
            } else if self.current_client.is_none()
                && matches!(&*self.grab_tx.borrow(), device::GrabEvent::Grab)
                && counts.emitted_local == 0
            {
                warn!(
                    "INPUT SWALLOWED: {} physical events seen while local with devices grabbed, but none were emitted to the virtual devices!",
                    counts.physical
                );
            }
        }
    }

    /// Refreshes the shared diagnostics mirror with the current state. Called
    /// once per rotation loop iteration, so the SIGHUP dump (which reads the
    /// mirror directly from the signal thread) keeps working when this loop
    /// is stalled: the dump then shows the state as of the last completed
    /// iteration plus how long the loop has been stuck.
    pub fn update_diagnostics(&self) {
        let grab = format!("{:?}", *self.grab_tx.borrow());
        let mut state = format!(
            "current_client={:?} grab={} clients={:?} removed_current_client={:?} pending_resume_fingerprint={:?} clipboard_target={:?} pending_clipboard_requests={} motion_seq={} datagrams_ok={} counts={{physical={} forwarded={} emitted_local={}}}",
            self.current_client,
            grab,
            self.clients,
            self.removed_current_client,
            self.pending_resume_fingerprint,
            self.clipboard_target,
            self.pending_clipboard_requests.len(),
            self.motion_seq,
            self.clients
                .iter()
                .map(|c| format!("{}:{}", c.endpoint, c.datagrams_ok))
                .collect::<Vec<_>>()
                .join(", "),
            self.status_counts.physical,
            self.status_counts.forwarded,
            self.status_counts.emitted_local,
        );
        if self.motion_dirty {
            state.push_str(&format!(
                " coalesced_motion_pending={{dx={} dy={} events={}}}",
                self.pending_motion.0, self.pending_motion.1, self.pending_motion.2
            ));
        }
        self.diagnostics.update(state);
    }

    /// Adds a pure-motion batch to the coalescing accumulator (see --motion-hz).
    fn accumulate_motion(&mut self, events: &[event::InputEvent]) {
        for e in events {
            if let Some(i) = &e.inputi32 {
                if i.code == evdev::RelativeAxisCode::REL_X.0 {
                    self.pending_motion.0 = self.pending_motion.0.saturating_add(i.value);
                } else {
                    self.pending_motion.1 = self.pending_motion.1.saturating_add(i.value);
                }
            }
        }
        self.pending_motion.2 += events.len() as u64;
        self.motion_dirty = true;
        trace!(
            "Accumulated motion: dx={} dy={} ({} events pending)",
            self.pending_motion.0, self.pending_motion.1, self.pending_motion.2
        );
    }

    /// Whether coalesced motion is waiting for the flush timer (see --motion-hz).
    pub fn motion_dirty(&self) -> bool {
        self.motion_dirty
    }

    /// Sends any coalesced pointer motion to the active client as a single
    /// batch (see --motion-hz). No-op when nothing is pending.
    pub async fn flush_pending_motion(&mut self) {
        if !self.motion_dirty {
            return;
        }
        self.motion_dirty = false;
        let (dx, dy, source_count) = std::mem::replace(&mut self.pending_motion, (0, 0, 0));
        let endpoint = match self.current_client {
            Some(c) => c,
            // Switched away meanwhile; the pending deltas are moot.
            None => return,
        };
        if dx == 0 && dy == 0 {
            return;
        }
        // Coalesced flushes go as datagrams, not over the ordered stream: a
        // reliable stream retransmits and replays stale motion in order after
        // a WiFi blip, which presents as the cursor sluggishly replaying a
        // backlog. Datagrams never retransmit, and quinn drops the oldest
        // queued datagram when its buffer is full, so no stale-motion backlog
        // can ever pile up. Lost frames are healed position-losslessly via the
        // repeated history (see MotionDatagram).
        let mut history = Vec::with_capacity(MOTION_HISTORY_LEN + 1);
        history.push((dx, dy));
        history.extend(self.motion_history.iter().copied());
        match self.try_send_motion_datagram(&endpoint, history) {
            MotionSend::Sent => {
                self.motion_history.push_front((dx, dy));
                self.motion_history.truncate(MOTION_HISTORY_LEN);
                self.status_counts.forwarded += source_count;
                return;
            }
            MotionSend::Retry => {
                // Keep the deltas pending; they retry (with any newer motion
                // accumulated on top) at the next flush opportunity.
                self.pending_motion = (dx, dy, source_count);
                self.motion_dirty = true;
                return;
            }
            MotionSend::Fallback => {}
        }
        // Stream fallback (peer can't do datagrams): ordered and lossless.
        let mut events = Vec::with_capacity(2);
        if dx != 0 {
            events.push(event::motion_event(evdev::RelativeAxisCode::REL_X.0, dx));
        }
        if dy != 0 {
            events.push(event::motion_event(evdev::RelativeAxisCode::REL_Y.0, dy));
        }
        if let Err(e) = self
            .send_event_to_remote_client(event::ServerEvent::Input(events))
            .await
        {
            warn!("Failed to forward coalesced motion: {:?}", e);
        } else {
            self.status_counts.forwarded += source_count;
        }
    }

    /// Attempts to send a motion frame as a QUIC datagram. `history` is
    /// newest-first: entry 0 is this frame, followed by recent frames for
    /// loss healing (see MotionDatagram). Fallback means the peer can't do
    /// datagrams at all (permanently); Retry means the send buffer is
    /// momentarily full and the caller should keep the deltas pending.
    fn try_send_motion_datagram(
        &mut self,
        endpoint: &SocketAddr,
        history: Vec<(i32, i32)>,
    ) -> MotionSend {
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(endpoint)) {
            Ok(idx) => idx,
            Err(_) => return MotionSend::Fallback,
        };
        if !self.clients[idx].datagrams_ok {
            return MotionSend::Fallback;
        }
        let seq = self.motion_seq.wrapping_add(1);
        let msg = event::MotionDatagram { seq, history };
        let serialized = match postcard::to_stdvec(&msg) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to serialize motion datagram: {:?}", e);
                return MotionSend::Fallback;
            }
        };
        let history_len = msg.history.len();
        match self.clients[idx].conn.send_datagram(Bytes::from(serialized)) {
            Ok(()) => {
                self.motion_seq = seq;
                if !self.motion_datagram_announced {
                    self.motion_datagram_announced = true;
                    info!(
                        "Sending pointer motion to {} as QUIC datagrams (lost frames are healed from repeated history, not retransmitted)",
                        endpoint
                    );
                }
                trace!(
                    "Sent motion datagram seq={} ({} frames) to {}",
                    self.motion_seq,
                    history_len,
                    endpoint
                );
                MotionSend::Sent
            }
            Err(e @ (SendDatagramError::UnsupportedByPeer | SendDatagramError::Disabled)) => {
                debug!(
                    "QUIC datagrams unsupported by {} ({}), using the ordered stream for motion",
                    endpoint, e
                );
                self.clients[idx].datagrams_ok = false;
                MotionSend::Fallback
            }
            Err(e @ SendDatagramError::TooLarge) => {
                // Unreachable for our tiny frames; treated as "not queued" so
                // the caller keeps the deltas pending rather than losing them.
                trace!("Motion datagram to {} not queued ({}), retrying later", endpoint, e);
                MotionSend::Retry
            }
            Err(e) => {
                // ConnectionLost: stream-write instead; a dead connection
                // fails there properly and removes the client.
                trace!(
                    "Motion datagram to {} not sent ({}), using the stream",
                    endpoint, e
                );
                MotionSend::Fallback
            }
        }
    }

    /// Sends an event to the currently active client, removing it if sending fails.
    /// If no client is active, this does nothing.
    async fn send_event_to_remote_client(&mut self, msg: event::ServerEvent<'_>) -> Result<()> {
        let current_client = match self.current_client {
            Some(client) => client,
            None => {
                // On local machine, nothing to do
                return Ok(());
            }
        };
        if !(self.send_event(&current_client, msg).await?) {
            // Active client not found?
            // Shouldn't happen, but recover by switching to local machine and ungrabbing.
            // Otherwise we're leaving the server stuck in a grabbed state.
            self.set_and_grab_current_client(None).await;
        }
        Ok(())
    }

    /// Handles an input event collected from the server.
    pub async fn send_input_events(&mut self, batch: device::InputBatch) -> Result<()> {
        let event_count = batch.events.len() as u64;
        self.status_counts.physical += event_count;
        if let Some(endpoint) = self.current_client {
            // Remote client is active, send all input to client and not to local machine.
            let events = batch.events;
            if is_pure_pointer_motion(&events) {
                if self.motion_flush_interval.is_some() {
                    // Office-mode coalescing (--motion-hz): sum the deltas into
                    // the accumulator; the flush timer forwards them at the
                    // configured rate as datagrams. Lossless for the
                    // cursor position, far less network/CPU load than one
                    // message per 8kHz poll.
                    self.accumulate_motion(&events);
                    return Ok(());
                }
                // Full-rate motion (--motion-hz 0) goes over unreliable/unordered
                // QUIC datagrams: a lost motion update is instantly superseded by
                // the next one, so skipping it beats stalling all later input
                // behind a stream retransmission (the cause of visible
                // micro-stutter). The batch is pure REL_X/REL_Y, so summing it
                // into one frame is lossless.
                let mut dx = 0i32;
                let mut dy = 0i32;
                for e in &events {
                    if let Some(i) = &e.inputi32 {
                        if i.code == evdev::RelativeAxisCode::REL_X.0 {
                            dx = dx.saturating_add(i.value);
                        } else {
                            dy = dy.saturating_add(i.value);
                        }
                    }
                }
                match self.try_send_motion_datagram(&endpoint, vec![(dx, dy)]) {
                    MotionSend::Sent => {
                        self.status_counts.forwarded += event_count;
                        return Ok(());
                    }
                    MotionSend::Retry => {
                        // Send buffer full: skip this update entirely; the next
                        // poll supersedes it (full-rate motion is lossy by design).
                        return Ok(());
                    }
                    MotionSend::Fallback => {}
                }
            }
            // Ordering: coalesced motion must reach the client before this
            // batch (e.g. a click lands after the motion that preceded it).
            self.flush_pending_motion().await;
            self.send_event_to_remote_client(event::ServerEvent::Input(events))
                .await?;
            self.status_counts.forwarded += event_count;
            Ok(())
        } else if batch.is_grabbed {
            // Local machine is active and device is grabbed, write input to local virtual devices.
            // For example, we grab keyboards so that we can skip sending switch combos to the local system.
            self.output_handler.write(batch.events).await?;
            self.status_counts.emitted_local += event_count;
            Ok(())
        } else {
            // Local machine is active and device isn't grabbed (passthrough), drop input event.
            // For example, we don't grab mice/touchpads since they aren't relevant to switch combos.
            // If we send their input to the handler, the input is duplicated between the passthrough
            // and the virtual device.
            Ok(())
        }
    }

    /// Sends an event to the specified client, removing it if sending fails.
    /// If the client isn't found, returns Ok(false)
    /// If sending the message fails, removes the client and returns Err
    async fn send_event(
        &mut self,
        endpoint: &SocketAddr,
        msg: event::ServerEvent<'_>,
    ) -> Result<bool> {
        // Serialize up front: a serialization failure is a problem with the message,
        // not with the client's connection, so it shouldn't kick the client out.
        let serializedmsg = match postcard::to_stdvec_cobs(&msg) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to serialize event message: {:?}", e);
                return Err(anyhow!("Failed to serialize event message: {:?}", e));
            }
        };
        match self.clients.binary_search_by(|c| c.endpoint.cmp(endpoint)) {
            Ok(idx) => {
                let events_send = &mut self
                    .clients
                    .get_mut(idx)
                    .expect("missing current_client")
                    .events_send;
                trace!(
                    "Sending {} byte serialized message: {:X?}",
                    serializedmsg.len(),
                    &serializedmsg
                );
                if let Err(e) = events_send
                    .write_all(&serializedmsg)
                    .await
                    .context("Failed to send serialized message")
                {
                    if self.handle_client_removal(endpoint).await {
                        self.clipboard_clear().await;
                    }
                    Err(e)
                } else {
                    Ok(true)
                }
            }
            Err(_idx) => {
                warn!(
                    "Event client {} not found in clients map: {:?}",
                    endpoint, self.clients
                );
                Ok(false)
            }
        }
    }

    async fn send_bulk(
        &mut self,
        endpoint: &SocketAddr,
        msg: bulk::ServerBulk<'_>,
        payload: Option<Vec<u8>>,
    ) -> Result<bool> {
        // Serialize up front: a serialization failure is a problem with the message,
        // not with the client's connection, so it shouldn't kick the client out.
        let mut bytes = postcard::to_stdvec_cobs(&msg)
            .map_err(|e| anyhow!("Failed to serialize bulk message: {:?}", e))?;
        if let Some(payload) = payload {
            trace!("Queueing {} byte payload for {}", payload.len(), endpoint);
            bytes.extend_from_slice(&payload);
        }
        match self.clients.binary_search_by(|c| c.endpoint.cmp(endpoint)) {
            Ok(idx) => {
                let bulk_tx = &self
                    .clients
                    .get(idx)
                    .expect("missing current_client")
                    .bulk_tx;
                // The network write happens in the client's bulk writer task, so
                // large payloads never block the rotation loop. A closed queue
                // means the writer task died; it reports the removal itself.
                match bulk_tx.send(bytes) {
                    Ok(()) => Ok(true),
                    Err(_) => Ok(false),
                }
            }
            Err(_idx) => {
                warn!(
                    "Bulk client {} not found in clients map: {:?}",
                    endpoint, self.clients
                );
                Ok(false)
            }
        }
    }

    /// Removes the client and switches to the server if it was the active client.
    /// If this returns true, then clipboard_clear() should also be called.
    async fn handle_client_removal(&mut self, endpoint: &SocketAddr) -> bool {
        // Always refetch the idx to avoid issues if there was an await in which the client was
        // removed behind our back.
        match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => {
                self.clients.remove(idx);
            }
            Err(_e) => {
                // Noop. Can happen if we're cleaning up for a client that wasn't added yet.
                debug!("Client to remove not found in rotation: {}", endpoint);
                return false;
            }
        }
        let client_list = self
            .clients
            .iter()
            .map(|c| c.endpoint.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        let mut should_clear_clipboard = false;
        if let Some(clipboard_info) = &self.clipboard_target {
            if let Some(clipboard_source) = &clipboard_info.source {
                if clipboard_source == endpoint {
                    // The removed client owned the clipboard. Remove the clipboard.
                    should_clear_clipboard = true;
                }
            }
        }

        if let Some(current_client) = self.current_client {
            if current_client == *endpoint {
                // This is the active client. Remove it AND switch to local machine.
                info!(
                    "Removing client {} from rotation and switching to local machine (clients: {})",
                    endpoint,
                    if client_list.is_empty() {
                        "none".to_string()
                    } else {
                        client_list
                    }
                );

                // Current client is being removed. If it comes back soon, we can mark it current again.
                self.removed_current_client = Some(DefunctClientInfo {
                    endpoint: current_client,
                    removed_at: Instant::now(),
                });

                self.set_and_grab_current_client(None).await;
                return should_clear_clipboard;
            }
        }

        info!(
            "Removing client {} from client rotation: {}",
            endpoint,
            if client_list.is_empty() {
                "empty".to_string()
            } else {
                client_list
            }
        );
        should_clear_clipboard
    }

    async fn set_and_grab_current_client(&mut self, client: Option<SocketAddr>) {
        if self.current_client.is_none() && client.is_some() {
            // Switching away from the local machine: release any keys held on the
            // local virtual devices so they don't get stuck pressed.
            if let Err(e) = self.output_handler.release_all().await {
                warn!("Failed to release held keys on local virtual devices: {:?}", e);
            }
        }
        // Motion accumulated (or already flushed) for the previous target is
        // moot after a switch.
        self.pending_motion = (0, 0, 0);
        self.motion_dirty = false;
        self.motion_history.clear();
        self.current_client = client;
        // Record which client is active (or none) so that an unexpected exit
        // mid-session can be recovered on the next server start. This is the
        // single funnel for current_client changes, incl. client removal.
        match client {
            Some(endpoint) => {
                if let Some(fingerprint) = self
                    .clients
                    .iter()
                    .find(|c| c.endpoint == endpoint)
                    .map(|c| c.fingerprint.clone())
                {
                    if let Err(e) = fs::write(&self.active_client_path, &fingerprint) {
                        warn!("Failed to record active client state: {:?}", e);
                    }
                }
            }
            None => clear_active_client(&self.active_client_path),
        }
        let grab = if client.is_some() {
            device::GrabEvent::Grab
        } else {
            device::GrabEvent::Ungrab
        };
        if let Err(e) = self.grab_tx.send(grab) {
            // Avoid leaving devices in a bad grabbed state
            panic!(
                "Failed to update device grab, exiting server to avoid bad grab state: {}",
                e
            );
        }
    }

    /// Ensures that all clients and the server have their clipboard state cleared.
    /// To be called when handle_client_removal() returns true, when a client holding the clipboard has disconnected.
    /// Broken into a separate function to avoid recursive async calls.
    async fn clipboard_clear(&mut self) {
        debug!("Clearing clipboard on server and all clients");
        self.clipboard_target = None;

        // Clear the server's host clipboard status
        if let Some(c) = &mut self.local_clipboard {
            if let Err(e) = c.store_types(vec![]) {
                // Keep going with the clients...
                warn!("Failed to clear server clipboard: {}", e);
            }
        }

        // Clear all clients' host clipboard statuses (the client was already removed)
        let types_msg = event::ServerEvent::ClipboardTypes(event::ClipboardTypes {
            types: "",
            // Size shouldn't matter for clearing clipboard...
            max_size_bytes: 0,
        });
        // Treat this as best-effort to tidy up the clients, they should reset locally when disconnected.
        if let Err(e) = self
            .send_event_all(types_msg, |_client: &ClientInfo| true)
            .await
        {
            warn!("Failed to clear clipboard on all clients: {}", e);
        }
    }
}

async fn send_message_to_client<T>(send: &mut quinn::SendStream, msg: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    // Serialize message data: postcard with cobs encoding for event framing
    let serializedmsg = postcard::to_stdvec_cobs(&msg)
        .map_err(|e| anyhow!("Failed to serialize message: {:?}", e))?;
    trace!(
        "Sending {} byte serialized message: {:X?}",
        serializedmsg.len(),
        &serializedmsg
    );
    send.write_all(&serializedmsg)
        .await
        .context("Failed to send serialized message")
}

/// Compares two clipboard mime-type lists as sets (order- and
/// duplicate-insensitive), since different sources advertise the same
/// clipboard with slightly different lists (e.g. wl-copy repeating text/plain).
fn types_equal(a: &[String], b: &[String]) -> bool {
    let mut a: Vec<&str> = a.iter().map(|s| s.as_str()).collect();
    let mut b: Vec<&str> = b.iter().map(|s| s.as_str()).collect();
    a.sort_unstable();
    a.dedup();
    b.sort_unstable();
    b.dedup();
    a == b
}

/// Shows a best-effort desktop notification about an input switch, so that an
/// accidental switch (e.g. a switch shortcut colliding with a compositor bind)
/// is visible at a glance instead of looking like dead keys. Uses notify-send
/// (libnotify); any failure (missing binary, no session bus, root without -E)
/// is ignored. The spawned child is reaped by the tokio runtime.
fn notify_switch(body: &str) {
    let _ = tokio::process::Command::new("notify-send")
        .args([
            "-a",
            "monux",
            "-u",
            "low",
            "-t",
            "2000",
            // Replace the previous switch notification instead of stacking.
            "-h",
            "string:x-canonical-private-synchronous:monux-switch",
            "monux",
            body,
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// Path of the file recording the active client's fingerprint (see
/// ACTIVE_CLIENT_STATE_FILE).
pub fn active_client_state_path(config_dir: &Path) -> PathBuf {
    config_dir.join(ACTIVE_CLIENT_STATE_FILE)
}

/// Reads the fingerprint of the client that was active when the previous
/// server instance exited unexpectedly. Returns None when there is nothing to
/// resume: no state file, a stale one, or an empty one (stale and empty files
/// are removed as junk). A fresh file is LEFT IN PLACE: the resume may span
/// several restarts before the client manages to reconnect (e.g. chained
/// auto-update restarts), and consuming it at load would lose the state after
/// the first one. It is rewritten on the next switch to a client and removed
/// on switch back to the local machine.
fn load_pending_resume(path: &Path) -> Option<String> {
    let metadata = fs::metadata(path).ok()?;
    let stale = match metadata.modified().ok().and_then(|m| m.elapsed().ok()) {
        Some(age) => age > ACTIVE_CLIENT_MAX_AGE,
        // Unreadable mtime or an mtime in the future (clock skew): treat as
        // fresh, resuming is the safer direction for crash recovery.
        None => false,
    };
    if stale {
        debug!("Ignoring stale active-client state file: {}", path.display());
        let _ = fs::remove_file(path);
        return None;
    }
    let fingerprint = fs::read_to_string(path).ok()?.trim().to_string();
    if fingerprint.is_empty() {
        let _ = fs::remove_file(path);
        None
    } else {
        Some(fingerprint)
    }
}

/// Removes the active-client state file, if present. Called on switches back
/// to the local machine. The file deliberately survives shutdown (graceful or
/// not): the next server instance uses it to resume the session.
pub fn clear_active_client(path: &Path) {
    if let Err(e) = fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!("Failed to clear active client state: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("monux-test-{}-{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn i32_event(type_: u16, code: u16, value: i32) -> event::InputEvent {
        event::InputEvent {
            inputi32: Some(event::InputI32 { type_, code, value }),
            inputf64: None,
        }
    }

    #[test]
    fn pure_pointer_motion_detection() {
        let ev_rel = evdev::EventType::RELATIVE.0;
        let rel_x = evdev::RelativeAxisCode::REL_X.0;
        let rel_y = evdev::RelativeAxisCode::REL_Y.0;

        // Pure X/Y motion in one or several events: datagram-worthy.
        assert!(is_pure_pointer_motion(&[i32_event(ev_rel, rel_x, 3)]));
        assert!(is_pure_pointer_motion(&[
            i32_event(ev_rel, rel_x, 3),
            i32_event(ev_rel, rel_y, -2)
        ]));

        // Empty batches are not sent as datagrams.
        assert!(!is_pure_pointer_motion(&[]));

        // Wheel, buttons, keys, and absolute axes must stay on the ordered stream.
        let rel_wheel = evdev::RelativeAxisCode::REL_WHEEL.0;
        assert!(!is_pure_pointer_motion(&[i32_event(ev_rel, rel_wheel, 1)]));
        assert!(!is_pure_pointer_motion(&[
            i32_event(ev_rel, rel_x, 3),
            i32_event(evdev::EventType::KEY.0, 0x110, 1) // BTN_LEFT press
        ]));
        assert!(!is_pure_pointer_motion(&[event::InputEvent {
            inputi32: None,
            inputf64: Some(event::InputF64 {
                type_: evdev::EventType::ABSOLUTE.0,
                code: evdev::AbsoluteAxisCode::ABS_X.0,
                value: 0.5,
            }),
        }]));
    }

    #[test]
    fn pending_resume_roundtrip() {
        let dir = temp_dir("roundtrip");
        let path = active_client_state_path(&dir);
        fs::write(&path, "deadbeef").unwrap();
        assert_eq!(load_pending_resume(&path), Some("deadbeef".to_string()));
        // A fresh file survives the load: the resume may span several
        // restarts before the client manages to reconnect.
        assert!(path.exists());
        assert_eq!(load_pending_resume(&path), Some("deadbeef".to_string()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pending_resume_missing_or_empty() {
        let dir = temp_dir("empty");
        let path = active_client_state_path(&dir);
        assert_eq!(load_pending_resume(&path), None);
        fs::write(&path, "  \n").unwrap();
        assert_eq!(load_pending_resume(&path), None);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pending_resume_stale_is_ignored() {
        let dir = temp_dir("stale");
        let path = active_client_state_path(&dir);
        fs::write(&path, "deadbeef").unwrap();
        let stale_mtime =
            std::time::SystemTime::now() - ACTIVE_CLIENT_MAX_AGE - Duration::from_secs(60);
        let file = fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(stale_mtime).unwrap();
        drop(file);
        assert_eq!(load_pending_resume(&path), None);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn clear_active_client_is_idempotent() {
        let dir = temp_dir("clear");
        let path = active_client_state_path(&dir);
        // Missing file: no-op, no warning-worthy error.
        clear_active_client(&path);
        fs::write(&path, "deadbeef").unwrap();
        clear_active_client(&path);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    /// Stub output handler that just counts what it's asked to write.
    struct StubOutput {
        written: usize,
    }

    #[async_trait::async_trait]
    impl device::output::OutputHandler for StubOutput {
        async fn release_all(&mut self) -> Result<()> {
            Ok(())
        }
        async fn write(&mut self, events: Vec<event::InputEvent>) -> Result<()> {
            self.written += events.len();
            Ok(())
        }
    }

    #[tokio::test]
    async fn input_status_counts_flow_and_reset() {
        let dir = temp_dir("status");
        let (grab_tx, _grab_rx) = watch::channel(device::GrabEvent::Ungrab);
        let (rotation_tx, _rotation_rx) = mpsc::channel(8);
        let mut rotation = Rotation::new(
            grab_tx,
            StubOutput { written: 0 },
            None,
            &dir,
            rotation_tx,
            None,
            Arc::new(DiagnosticsMirror::new()),
        )
        .await
        .unwrap();

        let batch = device::InputBatch {
            events: vec![
                i32_event(evdev::EventType::KEY.0, 28, 1),
                i32_event(evdev::EventType::KEY.0, 28, 0),
            ],
            is_grabbed: true,
        };
        rotation.send_input_events(batch).await.unwrap();
        assert_eq!(rotation.status_counts.physical, 2);
        assert_eq!(rotation.status_counts.emitted_local, 2);
        assert_eq!(rotation.output_handler.written, 2);

        // The status log resets the window for the next interval.
        rotation.log_input_status();
        assert_eq!(rotation.status_counts.physical, 0);
        assert_eq!(rotation.status_counts.emitted_local, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn motion_coalescing_accumulates_and_clears() {
        let dir = temp_dir("coalesce");
        let (grab_tx, _grab_rx) = watch::channel(device::GrabEvent::Ungrab);
        let (rotation_tx, _rotation_rx) = mpsc::channel(8);
        let mut rotation = Rotation::new(
            grab_tx,
            StubOutput { written: 0 },
            None,
            &dir,
            rotation_tx,
            Some(Duration::from_millis(8)),
            Arc::new(DiagnosticsMirror::new()),
        )
        .await
        .unwrap();

        // With a client "active" (no network attached), pure motion batches are
        // accumulated instead of forwarded.
        rotation.current_client = Some("127.0.0.1:1234".parse().unwrap());
        let rel = evdev::EventType::RELATIVE.0;
        let rel_x = evdev::RelativeAxisCode::REL_X.0;
        let rel_y = evdev::RelativeAxisCode::REL_Y.0;
        for (dx, dy) in [(3, -2), (1, 0), (-2, 5)] {
            rotation
                .send_input_events(device::InputBatch {
                    events: vec![i32_event(rel, rel_x, dx), i32_event(rel, rel_y, dy)],
                    is_grabbed: false,
                })
                .await
                .unwrap();
        }
        assert_eq!(rotation.pending_motion, (2, 3, 6));
        assert!(rotation.motion_dirty());
        // Nothing was forwarded yet; the physical side was counted.
        assert_eq!(rotation.status_counts.physical, 6);
        assert_eq!(rotation.status_counts.forwarded, 0);

        // Switching away clears the accumulator without sending.
        rotation.current_client = None;
        rotation.flush_pending_motion().await;
        assert!(!rotation.motion_dirty());
        assert_eq!(rotation.pending_motion, (0, 0, 0));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn switch_requests_are_debounced() {
        let dir = temp_dir("debounce");
        let (grab_tx, _grab_rx) = watch::channel(device::GrabEvent::Ungrab);
        let (rotation_tx, _rotation_rx) = mpsc::channel(8);
        let mut rotation = Rotation::new(
            grab_tx,
            StubOutput { written: 0 },
            None,
            &dir,
            rotation_tx,
            None,
            Arc::new(DiagnosticsMirror::new()),
        )
        .await
        .unwrap();

        // The first switch request is processed; an immediate repeat (e.g. a
        // queued frustrated press after a stall) is dropped.
        assert!(!rotation.switch_debounced());
        assert!(rotation.switch_debounced());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_local_types_update_clears_clipboard_target() {
        let dir = temp_dir("clipclear");
        let (grab_tx, _grab_rx) = watch::channel(device::GrabEvent::Ungrab);
        let (rotation_tx, _rotation_rx) = mpsc::channel(8);
        let mut rotation = Rotation::new(
            grab_tx,
            StubOutput { written: 0 },
            None,
            &dir,
            rotation_tx,
            None,
            Arc::new(DiagnosticsMirror::new()),
        )
        .await
        .unwrap();

        let types = vec!["text/plain".to_string()];
        rotation
            .clipboard_update_source(None, types.clone(), 1024)
            .await
            .unwrap();
        assert!(rotation.clipboard_target.is_some());

        // The compositor revoked the selection (owner exited, nothing
        // persisted it): the tracked target must be cleared immediately...
        rotation
            .clipboard_update_source(None, vec![], 1024)
            .await
            .unwrap();
        assert!(rotation.clipboard_target.is_none());

        // ...and the debounce timestamp is reset, so a clipboard manager
        // re-owning the content right after is processed, not debounced away.
        rotation
            .clipboard_update_source(None, types, 1024)
            .await
            .unwrap();
        assert!(rotation.clipboard_target.is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn types_equal_is_set_based() {
        let six = vec![
            "text/plain".to_string(),
            "text/plain".to_string(),
            "text/plain;charset=utf-8".to_string(),
            "TEXT".to_string(),
            "STRING".to_string(),
            "UTF8_STRING".to_string(),
        ];
        let five = vec![
            "text/plain".to_string(),
            "text/plain;charset=utf-8".to_string(),
            "TEXT".to_string(),
            "STRING".to_string(),
            "UTF8_STRING".to_string(),
        ];
        // Same set, despite the duplicate entry and different lengths
        assert!(types_equal(&six, &five));
        // Order-insensitive
        let mut reordered = five.clone();
        reordered.reverse();
        assert!(types_equal(&five, &reordered));
        // Genuinely different types
        let other = vec!["image/png".to_string()];
        assert!(!types_equal(&five, &other));
    }
}
