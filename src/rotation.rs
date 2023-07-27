use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use quinn::SendStream;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, trace, warn};

use crate::device::watch;
use crate::msgs::{bulk, event};
use crate::x11clipboard;

/// If the selected client reconnects within 5 seconds of being removed, then reselect it automatically.
/// This is intended to help with fast recovery following networking flakes.
const REMOVED_CLIENT_RECOVERY_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct ClientInfo {
    endpoint: SocketAddr,
    events_send: SendStream,
    bulk_send: SendStream,
}

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

#[derive(Debug)]
struct ClipboardTarget {
    /// None if the clipboard is at the server
    source: Option<SocketAddr>,
    types: Vec<String>,
    max_size_bytes: u64,
}

pub struct Rotation {
    grab_tx: broadcast::Sender<watch::GrabEvent>,
    clients: Vec<ClientInfo>,
    current_client: Option<SocketAddr>,
    removed_current_client: Option<DefunctClientInfo>,
    buf: Vec<u8>,
    clipboard_reader: x11clipboard::reader::ClipboardReader,
    clipboard_writer: x11clipboard::writer::ClipboardWriter,
    clipboard_target: Option<ClipboardTarget>,
}

impl Rotation {
    pub async fn new(
        grab_tx: broadcast::Sender<watch::GrabEvent>,
        clipboard_fetch_tx: mpsc::Sender<x11clipboard::writer::ClipboardFetch>,
    ) -> Result<Self> {
        let mut buf = Vec::with_capacity(1024);
        // Init required for space to be usable
        buf.resize(buf.capacity(), 0);
        Ok(Rotation {
            grab_tx,
            clients: Vec::new(),
            current_client: None,
            removed_current_client: None,
            buf,
            clipboard_reader: x11clipboard::reader::ClipboardReader::new().await?,
            clipboard_writer: x11clipboard::writer::ClipboardWriter::new(clipboard_fetch_tx)
                .await?,
            clipboard_target: None,
        })
    }

    pub async fn add_client(
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

    pub async fn remove_client(&mut self, endpoint: SocketAddr) {
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
                .binary_search_by(|c| c.endpoint.cmp(&current_client))
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
                .binary_search_by(|c| c.endpoint.cmp(&current_client))
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
    pub async fn clipboard_update_source(
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
            types: types.clone(),
            max_size_bytes,
        });

        // Notify the active client (or server) about the clipboard info we just received.
        // In practice we should be getting this shortly after a client switch.
        self.update_current_client_clipboard().await?;

        Ok(())
    }

    /// Routes a request for clipboard content to a remote client or a local application
    pub async fn clipboard_request_content(
        &mut self,
        request_client: Option<SocketAddr>,
        type_: &str,
        max_size_bytes: u64,
    ) -> Result<()> {
        debug!("Handling clipboard content request from source={:?} with max_size_bytes={} for type={}: have {:?}", request_client, max_size_bytes, type_, self.clipboard_target);
        let target = match &self.clipboard_target {
            Some(c) => c,
            None => {
                bail!(
                    "No clipboard types available: request from {:?} for type {}",
                    request_client,
                    type_
                );
            }
        };
        // Sanity check: Is the requested type among the list of supported types?
        if !target.types.contains(&type_.to_string()) {
            bail!(
                "Requested clipboard type {} isn't among available types: {:?}",
                type_,
                target.types
            );
        }

        if let Some(clipboard_source) = &target.source.clone() {
            // A client has the clipboard: route request to them
            let msg = bulk::ServerBulk::ClipboardRequest(bulk::ServerClipboardRequest {
                type_,
                max_size_bytes,
                request_client,
            });
            info!(
                "Requesting clipboard data with type={} from {}{}",
                type_,
                clipboard_source,
                if let Some(c) = request_client {
                    format!(" on behalf of {}", c)
                } else {
                    "".to_string()
                }
            );
            self.send_bulk(clipboard_source, msg, None).await
        } else {
            // The server has the clipboard: serve via X11 from local app
            if let Some(request_client) = &request_client {
                // Read and send the clipboard content
                let content = self
                    .clipboard_reader
                    .read(type_, max_size_bytes, &Some(*request_client))
                    .await?;
                let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
                    type_,
                    content_len_bytes: content.len() as u64,
                });
                info!(
                    "Sending clipboard data with type={} from server to {}",
                    type_, request_client
                );
                self.send_bulk(request_client, msg, Some(content)).await
            } else {
                // We're getting a request for the clipboard from the server host.
                // We should only be serving clipboards for remote clients, but there isn't one.
                // This may mean that the serving client disconnected, but we should have cleared the status.
                bail!(
                    "Server got local clipboard request against itself? current_clipboard={:?}",
                    target
                );
            }
        }
    }

    /// Sends clipboard content in response to a prior request via clipboard_request_content.
    pub async fn clipboard_send_content(
        &mut self,
        // The client sending the clipboard data
        data_source: SocketAddr,
        // Copied from the ServerClipboardRequest, indicates where the clipboard data should be sent
        request_client: Option<SocketAddr>,
        data: x11clipboard::ClipboardData,
    ) -> Result<()> {
        debug!(
            "Sending clipboard content of type={} with len={} from source={:?} to dest={:?}",
            data.type_,
            data.data.len(),
            data_source,
            request_client
        );
        if let Some(request_client) = request_client {
            // Send to specified remote client (assuming it's still available etc...)
            let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
                type_: &data.type_,
                content_len_bytes: data.data.len() as u64,
            });
            self.send_bulk(&request_client, msg, Some(data.data)).await
        } else {
            // Send to local X11
            self.clipboard_writer.store_data(data).await
        }
    }

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

    // TODO(later): this can get called/logged twice in a switch, between us proactively updating and the client sending a clipboard types announce after being marked inactive. doesn't hurt anything though.
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
                    info!(
                        "Sending clipboard types for {} to {}: {}",
                        clipboard_source, current_client, types_str
                    );
                    self.send_event_current(types_msg).await?;
                }
            } else {
                // The server is active. Tell it about the client clipbard.
                info!(
                    "Storing clipboard types for {} on server: {}",
                    clipboard_source,
                    c.types.join(" ")
                );
                self.clipboard_writer.store_types(c.types.clone())?;
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
            if test_fn(&client) {
                if let Err(e) =
                    send_message_to_client(&mut client.events_send, &msg, &mut self.buf).await
                {
                    clients_to_remove.push((idx, client.endpoint.clone()));
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
        let current_client = match self.current_client.clone() {
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
        match self.clients.binary_search_by(|c| c.endpoint.cmp(&client)) {
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
        match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
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
                    endpoint, client_list
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
            "Removing client {} from rotation: {}",
            endpoint, client_list
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
        if let Err(e) = self.clipboard_writer.store_types(vec![]) {
            // Keep going with the clients...
            warn!("Failed to clear server clipboard: {}", e);
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
    send.write_all(&serializedmsg)
        .await
        .context("Failed to send serialized message")
}
