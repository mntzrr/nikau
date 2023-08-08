use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use quinn::SendStream;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot, watch as watchchan};
use tokio::task;
use tracing::{debug, error, info, trace, warn};

use crate::device::watch;
use crate::msgs::{bulk, event};
use crate::x11clipboard::{
    reader::{ClipboardReader, ClipboardTypeWatcher},
    writer::{ClipboardFetch, ClipboardWriter},
    ClipboardData,
};

/// If the selected client reconnects within 5 seconds of being removed, then reselect it automatically.
/// This is intended to help with fast recovery following networking flakes.
const REMOVED_CLIENT_RECOVERY_DEADLINE: Duration = Duration::from_secs(5);

/// Channels for communicating with a connected client.
#[derive(Debug)]
struct ClientInfo {
    endpoint: SocketAddr,
    events_send: SendStream,
    bulk_send: SendStream,
}

/// Keeps track of the most recently disconnected client,
/// used for automatically reactivating clients if they reconnect quickly.
#[derive(Debug)]
struct DefunctClientInfo {
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

/// Wrapper around server-local clipboard storage, if available.
/// Clipboard contents can still be transferred by the server among clients if this is unavailable.
pub struct LocalClipboard {
    reader: ClipboardReader,
    writer: ClipboardWriter,

    /// Pending fetch request for the local server clipboard.
    waiting_clipboard_tx: Option<oneshot::Sender<ClipboardData>>,
}

impl LocalClipboard {
    pub async fn start(
        rotation_tx: mpsc::Sender<RotationEvent>,
        max_clipboard_size_bytes: u64,
    ) -> Result<Self> {
        let (clipboard_fetch_tx, mut clipboard_fetch_rx) = mpsc::channel::<ClipboardFetch>(32);
        let (local_types_tx, mut local_types_rx) = watchchan::channel(vec![]);
        ClipboardTypeWatcher::start(local_types_tx).await?;

        task::spawn(async move {
            loop {
                tokio::select! {
                    // Listen to local host requests to get the clipboard
                    fetch_request = clipboard_fetch_rx.recv() => {
                        if let Some(fetch_request) = fetch_request {
                            // Got clipboard paste request from the local machine.
                            // Pass the request through to the main rotation event handler.
                            let event = RotationEvent::ClipboardRequestContent(ClipboardRequestContentArgs {
                                request_source: ClipboardRequestSource::Local(fetch_request.fetch_result_tx),
                                requested_type: fetch_request.requested_type,
                                max_size_bytes: max_clipboard_size_bytes,
                            });
                            if let Err(e) = rotation_tx.send(event).await {
                                error!("Failed to queue local clipboard request event: {:?}", e);
                                break;
                            }
                        } else {
                            error!("Clipboard fetch request queue has closed, exiting clipboard loop");
                            break;
                        }
                    },
                    // Listen to local host updates to the clipboard types
                    types_notify = local_types_rx.changed() => {
                        if let Err(e) = types_notify {
                            error!("local_types_rx has closed: {}", e);
                            break;
                        }
                        // Another application on the server machine has a clipboard entry.
                        let event = RotationEvent::ClipboardUpdateSource(ClipboardUpdateSourceArgs {
                            source: None,
                            types: local_types_rx.borrow().clone(),
                            max_size_bytes: max_clipboard_size_bytes,
                        });
                        if let Err(e) = rotation_tx.send(event).await {
                            error!("Failed to queue update source event: {:?}", e);
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self {
            reader: ClipboardReader::new().await?,
            writer: ClipboardWriter::start(clipboard_fetch_tx).await?,
            waiting_clipboard_tx: None,
        })
    }
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
}

/// Pointer to where clipboard data should be sent once it's been fetched
pub enum ClipboardRequestSource {
    /// The clipboard is being requested from the local (server) machine.
    /// The oneshot can be used for sending back the clipboard result.
    Local(oneshot::Sender<ClipboardData>),

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
    pub data: ClipboardData,
}

pub struct Rotation {
    grab_tx: broadcast::Sender<watch::GrabEvent>,
    clients: Vec<ClientInfo>,
    current_client: Option<SocketAddr>,
    removed_current_client: Option<DefunctClientInfo>,
    buf: Vec<u8>,
    /// Access to the local system clipboard on the server.
    local_clipboard: Option<LocalClipboard>,
    /// Tracking the current clipboard owner, whether it's at the server or a client.
    clipboard_target: Option<ClipboardTarget>,
}

impl Rotation {
    pub async fn new(
        grab_tx: broadcast::Sender<watch::GrabEvent>,
        local_clipboard: Option<LocalClipboard>,
    ) -> Result<Self> {
        // Init required for space to be usable
        let buf = vec![0; 1024];
        Ok(Rotation {
            grab_tx,
            clients: Vec::new(),
            current_client: None,
            removed_current_client: None,
            buf,
            local_clipboard,
            clipboard_target: None,
        })
    }

    pub async fn accept(&mut self, event: RotationEvent) {
        match event {
            RotationEvent::AddClient(args) => {
                self.add_client(args.endpoint, args.events_send, args.bulk_send)
                    .await
            }
            RotationEvent::RemoveClient(endpoint) => self.remove_client(endpoint).await,
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
        events_send: SendStream,
        bulk_send: SendStream,
    ) {
        // Sort clients by their endpoints as an arbitrary consistent order across sessions
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.clients.insert(
            idx,
            ClientInfo {
                endpoint,
                events_send,
                bulk_send,
            },
        );

        info!(
            "Added client {} to rotation: {}",
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

    async fn remove_client(&mut self, endpoint: SocketAddr) {
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => idx,
            Err(_e) => {
                // Noop. Can happen if we're cleaning up for a client that wasn't added yet.
                debug!("Client {} not found in rotation", endpoint);
                return;
            }
        };
        if self.handle_client_removal(&endpoint, idx).await {
            self.clipboard_clear().await;
        }
    }

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
            // Currently on local machine, go to last entry on vec (if any)
            self.update_current_client(self.clients.first().map(|c| c.endpoint))
                .await;
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
        self.update_current_client_clipboard(true).await?;

        Ok(())
    }

    /// Routes a request for clipboard content to a remote client or a local application
    async fn clipboard_request_content(
        &mut self,
        request_source: ClipboardRequestSource,
        requested_type: &str,
        max_size_bytes: u64,
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
        if let Some(clipboard_source) = &target.source.clone() {
            // A client has the clipboard: route request to them
            let msg = bulk::ServerBulk::ClipboardRequest(bulk::ServerClipboardRequest {
                requested_type,
                max_size_bytes,
                request_client: if let ClipboardRequestSource::Remote(client) = &request_source {
                    Some(*client)
                } else {
                    None
                },
            });
            info!(
                "Requesting clipboard data with type {} from {}{}",
                requested_type,
                clipboard_source,
                if let ClipboardRequestSource::Remote(client) = &request_source {
                    format!(" on behalf of {}", client)
                } else {
                    "".to_string()
                }
            );

            // If the request is coming from the server itself, keep the oneshot for handling the reply.
            if let ClipboardRequestSource::Local(waiting_clipboard_tx) = request_source {
                // Clipboard request is from the server itself.
                // Keep the oneshot for replying later.
                if let Some(local_clipboard) = &mut self.local_clipboard {
                    local_clipboard.waiting_clipboard_tx = Some(waiting_clipboard_tx);
                } else {
                    bail!(
                        "Got request for clipboard from server, but server clipboard is disabled"
                    );
                }
            }

            self.send_bulk(clipboard_source, msg, None).await
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
            let local_clipboard = match &mut self.local_clipboard {
                Some(c) => c,
                None => bail!("Fetch for local server clipboard but server clipboard is disabled"),
            };
            // Read and send the clipboard content
            let (content, data_type) = local_clipboard
                .reader
                .read(requested_type, max_size_bytes, &Some(*request_client))
                .await?;
            let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
                requested_type,
                data_type: data_type.as_ref().map(|t| t.as_str()),
                content_len_bytes: content.len() as u64,
            });
            if let Some(data_type) = &data_type {
                info!(
                    "Sending clipboard data for requested type {} (data type {}) from server to {}",
                    requested_type, data_type, request_client
                );
            } else {
                info!(
                    "Sending clipboard data for requested type {} from server to {}",
                    requested_type, request_client
                );
            }
            self.send_bulk(request_client, msg, Some(content)).await
        }
    }

    /// Sends clipboard content in response to a prior request via clipboard_request_content.
    async fn clipboard_send_content_from_client(
        &mut self,
        // The client sending the clipboard data
        data_source: SocketAddr,
        // Copied from the ServerClipboardRequest, indicates where the clipboard data should be sent
        request_client: Option<SocketAddr>,
        data: ClipboardData,
    ) -> Result<()> {
        debug!(
            "Sending clipboard content of requested_type={} data_type={:?} with len={} from source={:?} to dest={:?}",
            data.requested_type,
            data.data_type,
            data.data.len(),
            data_source,
            request_client
        );
        if let Some(request_client) = request_client {
            // Send to specified remote client (assuming it's still available etc...)
            let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
                requested_type: &data.requested_type,
                data_type: data.data_type.as_ref().map(|t| t.as_str()),
                content_len_bytes: data.data.len() as u64,
            });
            self.send_bulk(&request_client, msg, Some(data.data)).await
        } else if let Some(local_clipboard) = &mut self.local_clipboard {
            // Send to local X11 clipboard, using response oneshot that we'd gotten with the request.
            if let Some(waiting_clipboard_tx) = local_clipboard.waiting_clipboard_tx.take() {
                if let Err(_d_again) = waiting_clipboard_tx.send(data) {
                    warn!("Discarding clipboard data from client: no pending clipboard request (previous request timed out?)");
                }
                Ok(())
            } else {
                warn!(
                    "Ignoring unexpected clipboard data from client: no clipboard fetch is pending"
                );
                Ok(())
            }
        } else {
            warn!(
                "Ignoring unexpected clipboard data from client: clipboard is disabled at server"
            );
            Ok(())
        }
    }

    /// Updates internal state to route future events to the new client.
    /// Goes through the steps of notifying the new client that it's active (if new_client is Some),
    /// then notifying any old client that it's inactive (if old_client is Some).
    async fn update_current_client(&mut self, new_client: Option<SocketAddr>) {
        // Either we automatically reenabled a client, or the user manually did.
        // In either case, clear up any history of previously enabled disconnected clients.
        self.removed_current_client = None;

        // Save the old client for sending enabled=false below
        let old_client = self.current_client;

        self.set_and_grab_current_client(new_client).await;

        if let Some(new_client) = new_client {
            // Try to send switch{true} to the newly assigned current_client.
            // If it fails then current_client is cleaned up.
            if let Ok(()) = self
                .send_event_current(event::ServerEvent::Switch(event::SwitchEvent {
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
        if let Err(e) = self.update_current_client_clipboard(false).await {
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
    async fn update_current_client_clipboard(&mut self, log_info: bool) -> Result<()> {
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
                    if log_info {
                        info!(
                            "Sending clipboard types for {} to {}: {}",
                            clipboard_source, current_client, types_str
                        );
                    } else {
                        debug!(
                            "Sending clipboard types for {} to {}: {}",
                            clipboard_source, current_client, types_str
                        );
                    }
                    self.send_event_current(types_msg).await?;
                }
            } else if let Some(local_clipboard) = &mut self.local_clipboard {
                // The server is active and its clipboard support is enabled.
                // Tell it about the client clipbard.
                if log_info {
                    info!(
                        "Storing clipboard types for {} on server: {}",
                        clipboard_source,
                        c.types.join(" ")
                    );
                } else {
                    debug!(
                        "Storing clipboard types for {} on server: {}",
                        clipboard_source,
                        c.types.join(" ")
                    );
                }
                local_clipboard.writer.store_types(c.types.clone())?;
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
                info!(
                    "Sending clipboard types for server to {}: {}",
                    current_client, types_str
                );
                self.send_event_current(types_msg).await?;
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
        for (idx, client) in self.clients.iter_mut().enumerate() {
            if test_fn(client) {
                if let Err(e) =
                    send_message_to_client(&mut client.events_send, &msg, &mut self.buf).await
                {
                    clients_to_remove.push((idx, client.endpoint));
                    return Err(e);
                }
            }
        }
        // Reverse: Avoid issues with idx moving as entries are removed
        clients_to_remove.reverse();
        let mut should_clear_clipboard = false;
        for (idx, endpoint) in clients_to_remove {
            if self.handle_client_removal(&endpoint, idx).await {
                should_clear_clipboard = true;
            }
        }
        Ok(should_clear_clipboard)
    }

    /// Sends an event to the currently active client, removing it if sending fails.
    /// If no client is active, this does nothing.
    pub async fn send_event_current(&mut self, msg: event::ServerEvent<'_>) -> Result<()> {
        let current_client = match self.current_client {
            Some(client) => client,
            None => {
                // Ignore input when using the local machine.
                // We continue reading the input to detect combo presses but that's it.
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

    /// Sends an event to the specified client, removing it if sending fails.
    /// If the client isn't found, returns Ok(false)
    /// If sending the message fails, removes the client and returns Err
    async fn send_event(
        &mut self,
        client: &SocketAddr,
        msg: event::ServerEvent<'_>,
    ) -> Result<bool> {
        match self.clients.binary_search_by(|c| c.endpoint.cmp(client)) {
            Ok(idx) => {
                let events_send = &mut self
                    .clients
                    .get_mut(idx)
                    .expect("missing current_client")
                    .events_send;
                if let Err(e) = send_message_to_client(events_send, &msg, &mut self.buf).await {
                    let current_client = &self
                        .current_client
                        .expect("Should have exited if current_client was none");
                    if self.handle_client_removal(current_client, idx).await {
                        self.clipboard_clear().await;
                    }
                    Err(e)
                } else {
                    Ok(true)
                }
            }
            Err(_idx) => {
                warn!("Client {} not found in clients map", client);
                Ok(false)
            }
        }
    }

    async fn send_bulk(
        &mut self,
        endpoint: &SocketAddr,
        msg: bulk::ServerBulk<'_>,
        payload: Option<Vec<u8>>,
    ) -> Result<()> {
        match self.clients.binary_search_by(|c| c.endpoint.cmp(endpoint)) {
            Ok(idx) => {
                let bulk_send = &mut self
                    .clients
                    .get_mut(idx)
                    .expect("missing current_client")
                    .bulk_send;
                // Try sending the message, then the payload. Stop on the first failure, to handle below.
                if let Err(e) = send_message_to_client(bulk_send, &msg, &mut self.buf).await {
                    if self.handle_client_removal(endpoint, idx).await {
                        self.clipboard_clear().await;
                    }
                    return Err(e);
                }
                if let Some(payload) = payload {
                    trace!("Sending {} byte payload", payload.len());
                    if let Err(e) = bulk_send.write_all(&payload).await {
                        if self.handle_client_removal(endpoint, idx).await {
                            self.clipboard_clear().await;
                        }
                        return Err(e.into());
                    }
                }
            }
            Err(_idx) => {
                // Shouldn't happen, but recover by setting to local machine and ungrabbing
                warn!(
                    "Requested bulk client {} not found in clients map",
                    endpoint
                );
                self.set_and_grab_current_client(None).await;
            }
        }
        Ok(())
    }

    /// Removes the client and switches to the server if it was the active client.
    /// If this returns true, then clipboard_clear() should also be called.
    async fn handle_client_removal(&mut self, endpoint: &SocketAddr, idx: usize) -> bool {
        self.clients.remove(idx);
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
        self.current_client = client;
        let grab = if client.is_some() {
            watch::GrabEvent::Grab
        } else {
            watch::GrabEvent::Ungrab
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
            if let Err(e) = c.writer.store_types(vec![]) {
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

async fn send_message_to_client<T>(
    send: &mut quinn::SendStream,
    msg: &T,
    buf: &mut Vec<u8>,
) -> Result<()>
where
    T: Serialize + ?Sized,
{
    // Serialize message data: postcard with cobs encoding for event framing
    let buf_len = buf.len();
    let serializedmsg = postcard::to_slice_cobs(&msg, buf).map_err(|e| {
        anyhow!(
            "Failed to serialize message into buf.len={}: {:?}",
            buf_len,
            e
        )
    })?;
    trace!(
        "Sending {} byte serialized message: {:X?}",
        serializedmsg.len(),
        &serializedmsg
    );
    send.write_all(serializedmsg)
        .await
        .context("Failed to send serialized message")
}
