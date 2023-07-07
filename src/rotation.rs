use std::net::SocketAddr;

use tracing::{info, warn};

use crate::messages;

#[derive(Debug)]
struct ClientInfo {
    endpoint: SocketAddr,
    netmsg_tx: async_channel::Sender<messages::NetworkMessageV1>,
}

pub struct Rotation {
    clients: Vec<ClientInfo>,
    current_client: Option<SocketAddr>,
}

impl Rotation {
    pub fn new() -> Rotation {
        Rotation {
            clients: Vec::new(),
            current_client: None,
        }
    }

    pub fn add_client(
        &mut self,
        endpoint: SocketAddr,
        netmsg_tx: async_channel::Sender<messages::NetworkMessageV1>,
    ) {
        // Check for any dead clients before we add new ones
        self.check_dead_clients();

        // Sort clients by their endpoints as an arbitrary consistent order across sessions
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.clients.insert(
            idx,
            ClientInfo {
                endpoint,
                netmsg_tx,
            },
        );

        info!(
            "Added client: current={:?} all={:?}",
            self.current_client, self.clients
        );
    }

    pub async fn prev_client(&mut self) {
        // Check for any dead clients before rotating
        self.check_dead_clients();

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
        // Check for any dead clients before rotating
        self.check_dead_clients();

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

    fn check_dead_clients(&mut self) {
        // Closed channels: Nobody's listening
        let mut to_remove = vec![];
        for client in &self.clients {
            if client.netmsg_tx.is_closed() {
                to_remove.push(client.endpoint);
            }
        }
        // Clean up current_client if it's to_remove
        if let Some(current_client) = &self.current_client {
            if to_remove.contains(current_client) {
                self.current_client = None;
            }
        }
        // Clean up vec entries that are to_remove
        for endpoint in to_remove {
            if let Ok(idx) = self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
                self.clients.remove(idx);
            }
        }
    }

    async fn update_current_client(&mut self, new_client: Option<SocketAddr>) {
        info!(
            "Client switch: {:?} => {:?} (all: {:?})",
            self.current_client, new_client, self.clients.iter().map(|c| c.endpoint).collect::<Vec<SocketAddr>>()
        );
        if let Some(_old_client) = self.current_client {
            // Try to send switch{false} to last current_client.
            // If it fails then current_client is cleaned up.
            self.send(messages::NetworkMessageV1::Switch(
                messages::SwitchEventV1 { enabled: false },
            ))
            .await;
        }
        // TODO stop grab if switching to local machine (or start grab if switching to network)
        self.current_client = new_client;
        if self.current_client.is_some() {
            // Try to send switch{true} to next current_client.
            // If it fails then current_client is cleaned up.
            self.send(messages::NetworkMessageV1::Switch(
                messages::SwitchEventV1 { enabled: true },
            ))
            .await;
        }
    }

    pub async fn send(&mut self, netmsg: messages::NetworkMessageV1) {
        if let Some(current_client) = &self.current_client {
            match self
                .clients
                .binary_search_by(|c| c.endpoint.cmp(&current_client))
            {
                Ok(idx) => {
                    let netmsg_tx = &self
                        .clients
                        .get(idx)
                        .expect("missing current_client")
                        .netmsg_tx;
                    if let Err(_) = netmsg_tx.send(netmsg).await {
                        info!("Client is no longer connected, reverting to local machine");
                        // Client is dead, remove it and switch to local machine
                        self.clients.remove(idx);
                        // TODO stop grab if switching to local machine
                        self.current_client = None;
                    }
                }
                Err(_idx) => {
                    // Shouldn't happen
                    warn!("Current client is not found in clients map");
                    self.current_client = None;
                }
            };
        }
    }
}
