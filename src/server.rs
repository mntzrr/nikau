use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use async_lock::Mutex;
use async_std::task;
use futures::StreamExt;
use tracing::{error, warn};

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
                    // rotation handles and logs failed sends internally
                    let _result = rotation2
                        .lock()
                        .await
                        .send(messages::ServerMessageV1::Input(evt))
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
                error!("Client connection error: {:?}", e);
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
    loop {
        // In practice the client should only open one stream per connection,
        // but looping/blocking on this allows us to detect when the client has disconnected.
        let stream = connection.accept_bi().await;
        let (send, mut recv) = match stream {
            Err(e) => {
                warn!("Connection error to {}: {}", connection.remote_address(), e);
                rotation
                    .lock()
                    .await
                    .remove_client(connection.remote_address())
                    .await;
                break;
            }
            Ok(stream) => stream,
        };

        // Receive version from client and close the connection if it's not supported.
        // Future versions could follow the version message with more data. We ignore/discard it here.
        {
            let mut version_buf = vec![];
            transport::recv_version(&mut recv, &mut version_buf).await?;
        }

        // Add client to the rotation after a successful handshake
        rotation
            .lock()
            .await
            .add_client(connection.remote_address(), send)
            .await;
    }
    Ok(())
}
