use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_lock::Mutex;
use async_std::task;
use futures::StreamExt;
use tracing::{debug, error, info, trace};

use crate::{approval, deviceinput, devicewatch, messages, rotation, transport};

pub async fn run_server(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
    mut input_rx: async_channel::Receiver<deviceinput::Event>,
    grab_tx: async_channel::Sender<devicewatch::GrabEvent>,
) -> Result<()> {
    let rotation: Arc<Mutex<rotation::Rotation>> =
        Arc::new(Mutex::new(rotation::Rotation::new(grab_tx)));
    let rotation2 = rotation.clone();

    task::spawn(async move {
        while let Some(event) = input_rx.next().await {
            match event {
                deviceinput::Event::Input(evt) => {
                    rotation2
                        .lock()
                        .await
                        .send(messages::NetworkMessageV1::Input(evt))
                        .await;
                }
                deviceinput::Event::SwitchNext => {
                    rotation2.lock().await.next_client().await;
                }
                deviceinput::Event::SwitchPrev => {
                    rotation2.lock().await.prev_client().await;
                }
            }
        }
    });

    let server_endpoint = transport::build_server(listen_addr, cert_verifier)?;
    while let Some(conn) = server_endpoint.accept().await {
        let rotation3 = rotation.clone();
        task::spawn(async move {
            if let Err(e) = handle_connection(conn, rotation3).await {
                error!("Client connection error: {}", e);
            }
        });
    }
    error!("Exiting server");
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connecting,
    rotation: Arc<Mutex<rotation::Rotation>>,
) -> Result<()> {
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
        let client_protocol_version = recv
            .read_chunk(1024, true)
            .await
            .context("failed reading protocol version from client")?
            .context("client closed connection before sending initial protocol version")?;
        if client_protocol_version.bytes == messages::PROTOCOL_VERSION {
            debug!("Client protocol version is supported");
        } else {
            bail!("Client version isn't supported, dropping client");
        }

        // Add client to the rotation after a successful handshake
        let (netmsg_tx, mut netmsg_rx): (
            async_channel::Sender<messages::NetworkMessageV1>,
            async_channel::Receiver<messages::NetworkMessageV1>,
        ) = async_channel::bounded(32);
        rotation
            .lock()
            .await
            .add_client(connection.remote_address(), netmsg_tx)
            .await;

        while let Some(netmsg) = netmsg_rx.next().await {
            // Serialize message data: postcard with cobs encoding for event framing
            let serializedmsg = postcard::to_slice_cobs(&netmsg, &mut buf)
                .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;
            trace!(
                "Sending {} byte event: {:X?}",
                serializedmsg.len(),
                &serializedmsg
            );
            send.write_all(&serializedmsg)
                .await
                .context("Failed to send network message")?;
        }
    }
    Ok(())
}
