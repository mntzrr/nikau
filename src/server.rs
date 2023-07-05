use anyhow::{anyhow, bail, Context, Result};
use async_lock::Mutex;
use async_std::task;
use futures::StreamExt;
use tracing::{debug, error, info};

use std::net::SocketAddr;
use std::sync::Arc;

use crate::messages;
use crate::transport;

pub enum Event {
    Input(messages::InputEventV1),
    SwitchNext,
    SwitchPrev,
}

struct ClientInfo {
    endpoint: SocketAddr,
    netmsg_tx: async_channel::Sender<messages::NetworkMessageV1>,
}

struct Rotation {
    clients: Vec<ClientInfo>,
    current_client: Option<SocketAddr>,
}

impl Rotation {
    fn new() -> Rotation {
        Rotation {
            clients: Vec::new(),
            current_client: None
        }
    }

    fn add_client(&mut self, endpoint: SocketAddr, netmsg_tx: async_channel::Sender<messages::NetworkMessageV1>) {
        // Check for any dead clients before we add new ones
        self.check_dead_clients();

        // Sort clients by their endpoints as an arbitrary consistent order across sessions
        let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&endpoint)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.clients.insert(idx, ClientInfo{endpoint, netmsg_tx});
    }

    async fn prev_client(&mut self) {
        // Check for any dead clients before rotating
        self.check_dead_clients();

        if let Some(current_client) = &self.current_client {
            // Currently on remote machine, find its entry in the list and go to the prev one
            let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&current_client)) {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            if idx == 0 {
                // At start of vec or vec is empty - switch to local machine
                self.update_current_client(None).await;
            } else {
                // Go to prev entry in vec
                self.update_current_client(self.clients.get(idx - 1).map(|c| c.endpoint)).await;
            }
        } else {
            // Currently on local machine, go to last entry on vec (if any)
            self.update_current_client(self.clients.last().map(|c| c.endpoint)).await;
        }
    }

    async fn next_client(&mut self) {
        // Check for any dead clients before rotating
        self.check_dead_clients();

        if let Some(current_client) = &self.current_client {
            // Currently on remote machine, find its entry in the list and go to the next one
            let idx = match self.clients.binary_search_by(|c| c.endpoint.cmp(&current_client)) {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            // Go to next entry in vec, or fall back to local machine if vec is empty or we're off the end
            self.update_current_client(self.clients.get(idx + 1).map(|c| c.endpoint)).await;
        } else {
            // Currently on local machine, go to last entry on vec (if any)
            self.update_current_client(self.clients.first().map(|c| c.endpoint)).await;
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
        if let Some(_old_client) = self.current_client {
            // Try to send switch{false} to last current_client.
            // If it fails then current_client is cleaned up.
            self.send(messages::NetworkMessageV1::Switch(messages::SwitchEventV1{enabled: false})).await;
        }
        // TODO stop grab if switching to local machine (or start grab if switching to network)
        self.current_client = new_client;
        if self.current_client.is_some() {
            // Try to send switch{true} to next current_client.
            // If it fails then current_client is cleaned up.
            self.send(messages::NetworkMessageV1::Switch(messages::SwitchEventV1{enabled: true})).await;
        }
    }

    async fn send(&mut self, netmsg: messages::NetworkMessageV1) {
        if let Some(current_client) = &self.current_client {
            match self.clients.binary_search_by(|c| c.endpoint.cmp(&current_client)) {
                Ok(idx) => {
                    let netmsg_tx = &self.clients.get(idx).expect("missing current_client").netmsg_tx;
                    if let Err(_) = netmsg_tx.send(netmsg).await {
                        // Client is dead, remove it and switch to local machine
                        self.clients.remove(idx);
                        // TODO stop grab if switching to local machine
                        self.current_client = None;
                    }
                },
                Err(_idx) => {
                    // Shouldn't happen: current_client not found in map
                    self.current_client = None;
                },
            };
        }
    }
}

pub async fn run_server(
    listen_addr: &SocketAddr,
    known_client_certs: Vec<rustls::Certificate>,
    mut input_rx: async_channel::Receiver<Event>
) -> Result<()> {

    let rotation: Arc<Mutex<Rotation>> = Arc::new(Mutex::new(Rotation::new()));
    let rotation2 = rotation.clone();

    task::spawn(async move {
        while let Some(event) = input_rx.next().await {
            match event {
                Event::Input(evt) => {
                    rotation2.lock().await.send(messages::NetworkMessageV1::Input(evt)).await;
                },
                Event::SwitchNext => {
                    rotation2.lock().await.next_client().await;
                },
                Event::SwitchPrev => {
                    rotation2.lock().await.prev_client().await;
                },
            }
        }
    });

    let server_endpoint = transport::build_server(listen_addr, known_client_certs)?;
    while let Some(conn) = server_endpoint.accept().await {
        info!("Client connected: {}", conn.remote_address());
        let (netmsg_tx, netmsg_rx): (
            async_channel::Sender<messages::NetworkMessageV1>,
            async_channel::Receiver<messages::NetworkMessageV1>,
        ) = async_channel::bounded(32);
        rotation.lock().await.add_client(conn.remote_address(), netmsg_tx);
        task::spawn(async move {
            if let Err(e) = handle_connection(conn, netmsg_rx).await {
                error!("Client connection error: {}", e);
            }
        });
    }
    info!("Exiting server");
    Ok(())
}

async fn handle_connection(conn: quinn::Connecting, mut netmsg_rx: async_channel::Receiver<messages::NetworkMessageV1>) -> Result<()> {
    let connection = conn.await?;

    // A single message is around 6-9 bytes, so 64 is plenty for a scratch pad
    let mut buf = Vec::with_capacity(64);
    buf.resize(64, 0);

    loop {
        debug!("Waiting for bidirectional stream to start");
        let stream = connection.accept_bi().await;
        let (mut send, mut recv) = match stream {
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("Client connection closed");
                break;
            }
            Err(e) => {
                bail!("Connection error: {}", e);
            }
            Ok(stream) => stream,
        };

        // Expect client to start with providing a plain string of its current version
        // We currently only support 'v1'.
        debug!("Reading protocol version from client");
        let client_protocol_version = recv.read_chunk(1024, true).await.context("failed reading protocol version from client")?.context("client closed connection before sending initial protocol version")?;
        if client_protocol_version.bytes == messages::PROTOCOL_VERSION {
            debug!("Client protocol version is supported");
        } else {
            bail!("Client version isn't supported, dropping client");
        }

        while let Some(netmsg) = netmsg_rx.next().await {
            // Serialize message data: postcard with cobs encoding for event framing
            let serializedmsg = postcard::to_slice_cobs(&netmsg, &mut buf)
                .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;
            debug!("Sending {} byte event: {:X?}", serializedmsg.len(), &serializedmsg);
            send.write_all(&serializedmsg).await.context("Failed to send network message")?;
        }
    }
    Ok(())
}
