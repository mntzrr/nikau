use anyhow::{Context, Result};
use tracing::info;

use std::net::SocketAddr;

use crate::messages;
use crate::transport;

pub async fn run_client(bind_addr: SocketAddr, server_addr: SocketAddr, known_server_certs: Vec<rustls::Certificate>) -> Result<()> {
    let client_endpoint = transport::build_client(bind_addr, known_server_certs)?;
    // connect to server, our custom cert verifiers result in server_name being ignored
    let conn = client_endpoint.connect(server_addr, "__ignored__")?.await?;
    info!("Connected to server: {}", conn.remote_address());
    let (mut send, mut recv) = conn.open_bi().await.context("failed to open stream")?;
    send.write_all(messages::PROTOCOL_VERSION).await.context("failed to send request")?;
    send.finish().await.context("failed to shutdown send stream")?;
    let resp = recv.read_to_end(usize::max_value()).await.context("failed to read response")?;
    info!("got data, exiting: {}", std::str::from_utf8(&resp)?);
    Ok(())
}
