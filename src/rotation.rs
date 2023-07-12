use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use quinn::SendStream;
use tracing::{info, trace, warn};

use crate::{devicewatch, messages};

/// If the selected client reconnects within 5 seconds of being removed, then reselect it automatically.
/// This is intended to help with fast recovery following networking flakes.
const REMOVED_CLIENT_RECOVERY_LIMIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct ClientInfo {
    endpoint: SocketAddr,
    send: SendStream,
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

pub struct Rotation {
    grab_tx: async_channel::Sender<devicewatch::GrabEvent>,
    clients: Vec<ClientInfo>,
    current_client: Option<SocketAddr>,
    removed_current_client: Option<DefunctClientInfo>,
    buf: Vec<u8>,
}

impl Rotation {
    pub fn new(grab_tx: async_channel::Sender<devicewatch::GrabEvent>) -> Rotation {
        let mut buf = Vec::with_capacity(1024);
        // Init required for space to be usable
        buf.resize(buf.capacity(), 0);
        Rotation {
            grab_tx,
            clients: Vec::new(),
            current_client: None,
            removed_current_client: None,
            buf,
        }
    }

    pub async fn add_client(&mut self, endpoint: SocketAddr, send: SendStream) {
        // Sort clients by their endpoints as an arbitrary consistent order across sessions
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.clients.insert(idx, ClientInfo { endpoint, send });

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
        if let Ok(idx) = self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            self.clients.remove(idx);
            if let Some(current_client) = self.current_client {
                if current_client == endpoint {
                    // Current client is being removed. If it comes back soon, we can mark it current again.
                    self.removed_current_client = Some(DefunctClientInfo{
                        endpoint: current_client,
                        removed_at: Instant::now()
                    });
                    // Ensure ungrab is done!
                    self.set_current_client(None).await;
                }
            }
            info!(
                "Removed client {} from rotation: {:?}",
                endpoint,
                self.clients
                    .iter()
                    .map(|c| c.endpoint)
                    .collect::<Vec<SocketAddr>>()
            );
        } else {
            // Shouldn't happen
            warn!("Client {} not found in rotation", endpoint);
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

    async fn update_current_client(&mut self, new_client: Option<SocketAddr>) {
        // Either we automatically reenabled a client, or the user manually did.
        // In either case, clear up any history of previously enabled clients.
        self.removed_current_client = None;

        if let Some(_old_client) = self.current_client {
            // Try to send switch{false} to last current_client.
            // If it fails then current_client is cleaned up.
            let _ = self
                .send(messages::ServerMessage::Switch(messages::SwitchEvent {
                    enabled: false,
                }))
                .await;
        }

        self.set_current_client(new_client).await;

        if let Some(new_client) = new_client {
            // Try to send switch{true} to the newly assigned current_client.
            // If it fails then current_client is cleaned up.
            if let Ok(()) = self
                .send(messages::ServerMessage::Switch(messages::SwitchEvent {
                    enabled: true,
                }))
                .await
            {
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

    pub async fn send(&mut self, netmsg: messages::ServerMessage) -> Result<()> {
        if let Some(current_client) = &self.current_client {
            match self
                .clients
                .binary_search_by(|c| c.endpoint.cmp(&current_client))
            {
                Ok(idx) => {
                    let send = &mut self
                        .clients
                        .get_mut(idx)
                        .expect("missing current_client")
                        .send;
                    if let Err(e) = send_client(send, netmsg, &mut self.buf).await {
                        info!(
                            "Client {} has disconnected, switching to local machine: {:?}",
                            current_client, e
                        );
                        // Client is dead, remove it and switch to local machine
                        self.clients.remove(idx);
                        self.set_current_client(None).await;
                        return Err(e);
                    }
                }
                Err(_idx) => {
                    // Shouldn't happen
                    warn!("Current client is not found in clients map");
                    self.set_current_client(None).await;
                }
            };
        }
        Ok(())
    }

    async fn set_current_client(&mut self, client: Option<SocketAddr>) {
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

async fn send_client(
    send: &mut quinn::SendStream,
    netmsg: messages::ServerMessage,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // Serialize message data: postcard with cobs encoding for event framing
    let serializedmsg = postcard::to_slice_cobs(&netmsg, buf)
        .map_err(|e| anyhow!("Failed to serialize message: {:?}", e))?;
    trace!(
        "Sending {} byte event: {:X?}",
        serializedmsg.len(),
        &serializedmsg
    );
    send.write_all(&serializedmsg)
        .await
        .context("Failed to send network message")
}
