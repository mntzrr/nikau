use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tracing::{info, trace};

use crate::{approval, deviceoutput, messages, transport};

pub async fn run_client(
    bind_addr: &SocketAddr,
    server_addr: &SocketAddr,
    virtual_devices: &mut deviceoutput::VirtualDevices,
    cert_verifier: Arc<approval::NikauCertVerification>,
) -> Result<()> {
    let client_endpoint = transport::build_client(bind_addr, cert_verifier)?;
    // Connect to server, our custom cert verifiers result in server_name being ignored
    let conn = client_endpoint
        .connect(server_addr.clone(), "__ignored__")?
        .await?;
    info!("Connected to server: {}", conn.remote_address());
    let (mut send, mut recv) = conn.open_bi().await.context("failed to open stream")?;
    send.write_all(messages::PROTOCOL_VERSION)
        .await
        .context("failed to send protocol version")?;
    let mut bytes = Vec::with_capacity(1024);
    info!("Waiting to be activated by server...");
    loop {
        // Incoming data may contain one or more messages, but I've never seen fragments of messages.
        let resp = recv
            .read_chunk(1024, true)
            .await
            .context("failed reading event")?
            .context("server closed connection")?;
        trace!("Received {} bytes: {:X?}", resp.bytes.len(), &*resp.bytes);
        // Copy the immutable response data into a mutable buffer
        bytes.extend_from_slice(&*resp.bytes);

        let mut offset = 0;
        while offset < bytes.len() {
            let (networkmsg, resp_remainder) =
                postcard::take_from_bytes_cobs::<messages::NetworkMessageV1>(&mut bytes[offset..])
                    .map_err(|e| anyhow!("Failed to deserialize message: {}", e))?;
            let consumed = resp.bytes.len() - resp_remainder.len() - offset;
            trace!(
                "Consumed event at offset={}: {} ({} bytes)",
                offset,
                networkmsg,
                consumed
            );
            match networkmsg {
                messages::NetworkMessageV1::Switch(e) => {
                    virtual_devices.switch(e.enabled)?;
                }
                messages::NetworkMessageV1::Input(input) => {
                    virtual_devices.add_event(input)?;
                }
            }
            offset += consumed;
        }

        bytes.clear();
    }
}
