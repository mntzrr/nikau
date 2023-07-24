use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use quinn::SendStream;
use serde::Serialize;
use tracing::{debug, info, trace, warn};

use crate::{devicewatch, messages, x11clipboard};

/// If the selected client reconnects within 5 seconds of being removed, then reselect it automatically.
/// This is intended to help with fast recovery following networking flakes.
const REMOVED_CLIENT_RECOVERY_LIMIT: Duration = Duration::from_secs(5);

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
        now.duration_since(self.removed_at) > REMOVED_CLIENT_RECOVERY_LIMIT
    }
}

#[derive(Debug)]
struct ClipboardInfo {
    source: Option<SocketAddr>,
    types: Vec<String>,
    max_size_bytes: u64,
}

struct ClipboardRouting {
    reader: x11clipboard::reader::ClipboardReader,
    writer: x11clipboard::writer::ClipboardWriter,
    current_clipboard: Option<ClipboardInfo>,
}

pub struct Rotation {
    grab_tx: async_channel::Sender<devicewatch::GrabEvent>,
    clients: Vec<ClientInfo>,
    current_client: Option<SocketAddr>,
    removed_current_client: Option<DefunctClientInfo>,
    buf: Vec<u8>,
    clipboard_routing: ClipboardRouting,
}

impl Rotation {
    pub async fn new(grab_tx: async_channel::Sender<devicewatch::GrabEvent>, clipboard_fetch_tx: async_channel::Sender<x11clipboard::writer::ClipboardFetch>) -> Result<Self> {
        let mut buf = Vec::with_capacity(1024);
        // Init required for space to be usable
        buf.resize(buf.capacity(), 0);
        Ok(Rotation {
            grab_tx,
            clients: Vec::new(),
            current_client: None,
            removed_current_client: None,
            buf,
            clipboard_routing: ClipboardRouting {
                reader: x11clipboard::reader::ClipboardReader::new().await?,
                writer: x11clipboard::writer::ClipboardWriter::new(clipboard_fetch_tx).await?,
                current_clipboard: None,
            },
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
            "Added client {} to rotation: {:?}",
            endpoint,
            self.clients
                .iter()
                .map(|c| c.endpoint)
                .collect::<Vec<SocketAddr>>()
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

        self.handle_client_removal(&endpoint, idx).await;
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

    pub async fn clipboard_update_source(
        &mut self,
        source: Option<SocketAddr>,
        types: Vec<String>,
        max_size_bytes: u64,
    ) -> Result<()> {
        debug!("Updating target clipboard: source={:?} dest={:?} with max_size_bytes={} has types={:?}", source, self.current_client, max_size_bytes, types);
        // Save the clipboard types/source for future retrievals and client switches
        self.clipboard_routing.current_clipboard = Some(ClipboardInfo {
            source,
            types: types.clone(),
            max_size_bytes,
        });
        // Avoid advertising in these cases:
        // - source=server, dest=server: An app on the server owns the clipboard, don't override it
        // - source=clientA, dest=clientA: An app on the client owns the clipboard, don't override it
        if let Some(remote_client) = self.current_client {
            if let Some(remote_source) = &source {
                if *remote_source != remote_client {
                    // Tell current client about a different client's clipboard
                    // Avoid telling same client to about itself: don't want client overriding local app
                    let types_str = types.join(" ");
                    let types_msg = messages::ServerMessage::ClipboardTypes(messages::ClipboardTypes {
                        types: &types_str,
                        max_size_bytes,
                    });
                    self.send_event(types_msg).await?;
                }
            } else {
                // Tell remote client about server's clipboard
                let types_str = types.join(" ");
                let types_msg = messages::ServerMessage::ClipboardTypes(messages::ClipboardTypes {
                    types: &types_str,
                    max_size_bytes,
                });
                self.send_event(types_msg).await?;
            }
        } else if source.is_some() {
            // Tell server about a client's clipboard
            self.clipboard_routing.writer.store_types(types).await?;
        }
        Ok(())
    }

    pub async fn clipboard_request_content(
        &mut self,
        request_source: Option<SocketAddr>,
        type_: &str,
        max_size_bytes: u64,
    ) -> Result<()> {
        debug!("Handling clipboard content request from source={:?} with max_size_bytes={} for type={}: have {:?}", request_source, max_size_bytes, type_, self.clipboard_routing.current_clipboard);
        let current_clipboard = match &self.clipboard_routing.current_clipboard {
            Some(c) => c,
            None => {
                bail!("No clipboard types available: request from {:?} for type {}", request_source, type_);
            },
        };
        if !current_clipboard.types.contains(&type_.to_string()) {
            bail!("Requested clipboard type {} isn't among available types: {:?}", type_, current_clipboard.types);
        }

        if let Some(clipboard_source) = &current_clipboard.source.clone() {
            // Client has the clipboard: route request to them
            let msg = messages::BulkMessage::ClipboardContentRequest(messages::ClipboardContentRequest {
                type_,
                max_size_bytes,
            });
            self.send_bulk(clipboard_source, msg, None).await
        } else {
            if let Some(request_source) = &request_source {
                // Server has the clipboard: read and send back to request_source immediately.
                let content = self.clipboard_routing.reader.read(type_, max_size_bytes).await?;
                let msg = messages::BulkMessage::ClipboardContentHeader(messages::ClipboardContentHeader {
                    type_,
                    content_len_bytes: content.len() as u64,
                });
                self.send_bulk(request_source, msg, Some(content)).await
            } else {
                // We're getting a request for the clipboard from X11.
                // We should only be advertising clipboards for remote clients, but there isn't one.
                // TODO this might happen if we advertise a client that then disconnects, but we should un-advertise it when that happens
                bail!("Server got local clipboard request against itself? current_clipboard={:?}", current_clipboard);
            }
        }
    }

    pub async fn clipboard_send_content(
        &mut self,
        request_source: SocketAddr,
        data: x11clipboard::ClipboardData,
    ) -> Result<()> {
        debug!("Handling clipboard content of type={} with len={} from source={:?} to current={:?}", data.type_, data.data.len(), request_source, self.current_client);
        if let Some(_current_client) = &self.current_client {
            // Send to remote client
            let msg = messages::BulkMessage::ClipboardContentHeader(messages::ClipboardContentHeader {
                type_: &data.type_,
                content_len_bytes: data.data.len() as u64,
            });
            self.send_bulk(&request_source, msg, Some(data.data)).await
        } else {
            // Send to local X11
            self.clipboard_routing.writer.store_data(data).await
        }
    }

    async fn update_current_client(&mut self, new_client: Option<SocketAddr>) {
        // Either we automatically reenabled a client, or the user manually did.
        // In either case, clear up any history of previously enabled disconnected clients.
        self.removed_current_client = None;

        if let Some(_old_client) = self.current_client {
            // Try to send switch{false} to last current_client.
            // If it fails then current_client is cleaned up.
            let _ = self
                .send_event(messages::ServerMessage::Switch(messages::SwitchEvent {
                    enabled: false,
                }))
                .await;
        }

        self.set_and_grab_current_client(new_client).await;

        if let Some(new_client) = new_client {
            // Try to send switch{true} to the newly assigned current_client.
            // If it fails then current_client is cleaned up.
            if let Ok(()) = self
                .send_event(messages::ServerMessage::Switch(messages::SwitchEvent {
                    enabled: true,
                }))
                .await
            {
                if let Some(clipboard_info) = &self.clipboard_routing.current_clipboard {
                    debug!("Telling new client about clipboard: {:?}", clipboard_info);
                    // Update new client with the clipboard types to be advertised
                    let types_str = clipboard_info.types.join(" ");
                    let types_msg = messages::ServerMessage::ClipboardTypes(messages::ClipboardTypes {
                        types: &types_str,
                        max_size_bytes: clipboard_info.max_size_bytes,
                    });
                    // If the send fails then current_client is cleaned up.
                    if let Err(_e) = self.send_event(types_msg).await {
                        return;
                    }
                }
                info!(
                    "Switched to client: {} (clients: {:?})",
                    new_client,
                    self.clients
                        .iter()
                        .map(|c| c.endpoint)
                        .collect::<Vec<SocketAddr>>()
                );
            }
        } else {
            info!(
                "Switched to local machine (clients: {:?})",
                self.clients
                    .iter()
                    .map(|c| c.endpoint)
                    .collect::<Vec<SocketAddr>>()
            );
        }
    }

    pub async fn send_event(&mut self, msg: messages::ServerMessage<'_>) -> Result<()> {
        let current_client = match &self.current_client {
            Some(client) => client,
            None => {
                // Ignore input when using the local machine.
                // We continue reading the input to detect combo presses but that's it.
                return Ok(());
            },
        };

        match self
            .clients
            .binary_search_by(|c| c.endpoint.cmp(&current_client))
        {
            Ok(idx) => {
                let events_send = &mut self
                    .clients
                    .get_mut(idx)
                    .expect("missing current_client")
                    .events_send;
                if let Err(e) = send_message_to_client(events_send, &msg, &mut self.buf).await {
                    let current_client = &self.current_client.expect("Should have exited if current_client was none");
                    self.handle_client_removal(current_client, idx).await;
                    return Err(e);
                }
            }
            Err(_idx) => {
                // Shouldn't happen, but recover by setting to local machine and ungrabbing
                warn!("Active event client is not found in clients map");
                self.set_and_grab_current_client(None).await;
            }
        }
        Ok(())
    }

    async fn send_bulk(&mut self, endpoint: &SocketAddr, msg: messages::BulkMessage<'_>, payload: Option<Vec<u8>>) -> Result<()> {

        match self
            .clients
            .binary_search_by(|c| c.endpoint.cmp(&endpoint))
        {
            Ok(idx) => {
                let bulk_send = &mut self
                    .clients
                    .get_mut(idx)
                    .expect("missing current_client")
                    .bulk_send;
                // Try sending the message, then the payload. Stop on the first failure, to handle below.
                if let Err(e) = send_message_to_client(bulk_send, &msg, &mut self.buf).await {
                    self.handle_client_removal(endpoint, idx).await;
                    return Err(e);
                }
                if let Some(payload) = payload {
                    trace!("Sending {} byte payload", payload.len());
                    if let Err(e) = bulk_send.write_all(&payload).await {
                        self.handle_client_removal(endpoint, idx).await;
                        return Err(e.into());
                    }
                }
            }
            Err(_idx) => {
                // Shouldn't happen, but recover by setting to local machine and ungrabbing
                warn!("Requested bulk client {} not found in clients map", endpoint);
                self.set_and_grab_current_client(None).await;
            }
        }
        Ok(())
    }

    async fn handle_client_removal(&mut self, endpoint: &SocketAddr, idx: usize) {
        self.clients.remove(idx);
        let client_list = self.clients
            .iter()
            .map(|c| c.endpoint.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        if let Some(clipboard_info) = &self.clipboard_routing.current_clipboard {
            if let Some(clipboard_source) = &clipboard_info.source {
                if clipboard_source == endpoint {
                    // Clear clipboard status owned by removed client
                    // TODO should we clear the clipboard in X11 too?
                    self.clipboard_routing.current_clipboard = None;
                }
            }
        }

        if let Some(current_client) = self.current_client {
            if current_client == *endpoint {
                // This is the active client. Remove it AND switch to local machine.
                info!(
                    "Removing client {} from rotation and switching to local machine (clients: {:?})",
                    endpoint, client_list
                );

                // Current client is being removed. If it comes back soon, we can mark it current again.
                self.removed_current_client = Some(DefunctClientInfo {
                    endpoint: current_client,
                    removed_at: Instant::now(),
                });

                self.set_and_grab_current_client(None).await;
                return;
            }
        }

        info!(
            "Removing client {} from rotation: {:?}",
            endpoint, client_list
        );
    }

    async fn set_and_grab_current_client(&mut self, client: Option<SocketAddr>) {
        self.current_client = client;
        let grab = if client.is_some() {
            devicewatch::GrabEvent::Grab
        } else {
            devicewatch::GrabEvent::Ungrab
        };
        if let Err(e) = self.grab_tx.send(grab).await {
            // Avoid leaving devices in a bad grabbed state
            panic!(
                "Failed to update device grab, exiting server to avoid bad grab state: {}",
                e
            );
        }
    }
}

async fn send_message_to_client<T>(
    send: &mut quinn::SendStream,
    msg: &T,
    buf: &mut Vec<u8>,
) -> Result<()>
where T: Serialize + ?Sized
{
    // Serialize message data: postcard with cobs encoding for event framing
    let buf_len = buf.len();
    let serializedmsg = postcard::to_slice_cobs(&msg, buf)
        .map_err(|e| anyhow!("Failed to serialize message into buf.len={}: {:?}", buf_len, e))?;
    trace!(
        "Sending {} byte serialized message: {:X?}",
        serializedmsg.len(),
        &serializedmsg
    );
    send
        .write_all(&serializedmsg)
        .await
        .context("Failed to send serialized message")
}
