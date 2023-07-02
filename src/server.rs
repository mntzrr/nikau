use anyhow::{bail, Context, Result};
use async_std::task;
use tracing::{error, info};

use std::net::SocketAddr;

use crate::messages;
use crate::transport;

pub async fn run_server(listen_addr: SocketAddr, known_client_certs: Vec<rustls::Certificate>) -> Result<()> {
    let server_endpoint = transport::build_server(listen_addr, known_client_certs)?;
    while let Some(conn) = server_endpoint.accept().await {
        info!("Client connected: {}", conn.remote_address());
        task::spawn(async move {
            if let Err(e) = handle_connection(conn).await {
                error!("Client connection error: {}", e);
            }
        });
    }
    info!("Exiting server");
    Ok(())
}

async fn handle_connection(conn: quinn::Connecting) -> Result<()> {
    let connection = conn.await?;
    loop {
        info!("[server] waiting for stream to start");
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
        let client_protocol_version = recv.read_to_end(64 * 1024).await.context("failed reading request")?;
        if client_protocol_version != messages::PROTOCOL_VERSION {
            bail!("Client version isn't supported, dropping client");
        }

        // TODO send InputEvents
        send.write_all("hello there".as_bytes()).await.context("Failed to send response")?;
        send.finish().await.context("Failed to close stream")?;
        info!("request complete");
    }
    Ok(())
}
