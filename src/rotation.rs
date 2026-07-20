use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use quinn::SendStream;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task;
use tracing::{debug, error, info, trace, warn};

use crate::clipboard::{data, server};
use crate::device;
use crate::msgs::{bulk, event};

/// If the selected client reconnects within 10 seconds of being removed, then reselect it automatically.
/// This is intended to help with fast recovery following networking flakes.
const REMOVED_CLIENT_RECOVERY_DEADLINE: Duration = Duration::from_secs(10);

/// Name of the file (inside the config dir) recording the fingerprint of the
/// client currently switched active. Written on every switch to a client,
/// removed on switch back to the local machine and on graceful shutdown.
/// When the server exits unexpectedly (crash, kill -9) the file survives, and
/// the next server instance uses it to re-activate that client on reconnect.
pub const ACTIVE_CLIENT_STATE_FILE: &str = "active_client";

/// How old the active-client state may be before it is ignored on startup.
/// Crash recovery is expected to happen soon after the crash; resuming a
/// days-old session would be surprising.
const ACTIVE_CLIENT_MAX_AGE: Duration = Duration::from_secs(3600);

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
}

impl<O: device::output::OutputHandler> Rotation<O> {
    pub async fn new(
        grab_tx: watch::Sender<device::GrabEvent>,
        output_handler: O,
        local_clipboard: Option<server::LocalClipboard>,
        config_dir: &Path,
        rotation_tx: mpsc::Sender<RotationEvent>,
    ) -> Result<Self> {
        let active_client_path = active_client_state_path(config_dir);
        let pending_resume_fingerprint = load_pending_resume(&active_client_path);
        if let Some(pending) = &pending_resume_fingerprint {
            info!(
                "A client ({}) was active when the server last exited unexpectedly; it will be re-activated when it reconnects",
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
    ) {
        // Sort clients by their endpoints as an arbitrary consistent order across sessions
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
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
        self.clients.insert(
            idx,
            ClientInfo {
                endpoint,
                fingerprint: fingerprint.clone(),
                events_send,
                bulk_tx,
            },
        );

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

        // Crash recovery: this client was active when the previous server
        // instance exited unexpectedly. Re-activate it immediately.
        if let Some(pending) = &self.pending_resume_fingerprint {
            if *pending == fingerprint {
                self.pending_resume_fingerprint = None;
                info!(
                    "Resuming session: re-activating client {} that was active before the unexpected server exit",
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

    /// Switches to the previous client (or to the server) in the arbitrary rotation.
    pub async fn prev_client(&mut self) {
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

    /// Routes a request for clipboard content to a remote client or a local application
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
                bail!(
                    "No clipboard types available: request from {} for requested type {}",
                    request_source,
                    requested_type
                );
            }
        };
        // Sanity check: Is the requested type among the list of supported types?
        if !target.types.contains(&requested_type.to_string()) {
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
                // The nikau server process is getting asked for a clipboard from itself.
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
                None => bail!("Fetch for local server clipboard but server clipboard is disabled"),
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
                match server::LocalClipboard::read(
                    &reader,
                    &requested_type,
                    max_size_bytes,
                    &request_client,
                )
                .await
                {
                    Ok((content, data_type)) => {
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
                    }
                    // No reply is sent: as before, the requester's fetch times
                    // out and it can retry later.
                    Err(e) => {
                        warn!(
                            "Failed to read server clipboard for {}: {:?}",
                            request_client, e
                        );
                    }
                }
            });
            Ok(())
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
        if let Some(_) = self.current_client {
            // Remote client is active, send all input to client and not to local machine.
            self.send_event_to_remote_client(event::ServerEvent::Input(batch.events))
                .await
        } else if batch.is_grabbed {
            // Local machine is active and device is grabbed, write input to local virtual devices.
            // For example, we grab keyboards so that we can skip sending switch combos to the local system.
            self.output_handler.write(batch.events).await
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

/// Path of the file recording the active client's fingerprint (see
/// ACTIVE_CLIENT_STATE_FILE).
pub fn active_client_state_path(config_dir: &Path) -> PathBuf {
    config_dir.join(ACTIVE_CLIENT_STATE_FILE)
}

/// Reads the fingerprint of the client that was active when the previous
/// server instance exited unexpectedly. Returns None when there is nothing to
/// resume: no state file, a stale one, or an empty one. The file is removed in
/// any case; it is rewritten on the next switch to a client.
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
    let _ = fs::remove_file(path);
    if fingerprint.is_empty() {
        None
    } else {
        Some(fingerprint)
    }
}

/// Removes the active-client state file, if present. Called on switches back
/// to the local machine and on graceful server shutdown, so that only an
/// unexpected exit (crash, kill -9) leaves a session behind to resume.
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
        let dir = std::env::temp_dir().join(format!("nikau-test-{}-{}", std::process::id(), name));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn pending_resume_roundtrip() {
        let dir = temp_dir("roundtrip");
        let path = active_client_state_path(&dir);
        fs::write(&path, "deadbeef").unwrap();
        assert_eq!(load_pending_resume(&path), Some("deadbeef".to_string()));
        // The file is consumed by the load.
        assert!(!path.exists());
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
}
