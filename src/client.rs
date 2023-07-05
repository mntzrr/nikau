use anyhow::{anyhow, Context, Result};
use tracing::{debug, info, trace};

use std::net::SocketAddr;

use crate::messages;
use crate::messages::NetworkMessageV1;
use crate::transport;

pub async fn run_client(bind_addr: &SocketAddr, server_addr: &SocketAddr, known_server_certs: Vec<rustls::Certificate>) -> Result<()> {
    let client_endpoint = transport::build_client(bind_addr, known_server_certs)?;
    // Connect to server, our custom cert verifiers result in server_name being ignored
    let conn = client_endpoint.connect(server_addr.clone(), "__ignored__")?.await?;
    info!("Connected to server: {}", conn.remote_address());
    let (mut send, mut recv) = conn.open_bi().await.context("failed to open stream")?;
    send.write_all(messages::PROTOCOL_VERSION).await.context("failed to send protocol version")?;
    let mut bytes = Vec::with_capacity(1024);
    loop {
        // Incoming data may contain one or more messages, but I've never seen fragments of messages.
        let resp = recv.read_chunk(1024, true).await.context("failed reading event")?.context("server closed connection")?;
        trace!("received {} bytes: {:X?}", resp.bytes.len(), &*resp.bytes);
        // Copy the immutable response data into a mutable buffer
        bytes.extend_from_slice(&*resp.bytes);

        let mut input_events = vec![];
        // if there are multiple, only retain the last one
        let mut latest_switch_event = None;

        let mut offset = 0;
        while offset < bytes.len() {
            let (networkmsg, resp_remainder) = postcard::take_from_bytes_cobs::<messages::NetworkMessageV1>(&mut bytes[offset..])
                .map_err(|e| anyhow!("Failed to deserialize message: {}", e))?;
            let consumed = resp.bytes.len() - resp_remainder.len() - offset;
            debug!("consumed event at offset={}: {} ({} bytes)", offset, networkmsg, consumed);
            match networkmsg {
                NetworkMessageV1::Switch(e) => {
                    latest_switch_event = Some(e);
                },
                NetworkMessageV1::Input(e) => {
                    input_events.push(e)
                },
            }
            offset += consumed;
        }

        // TODO do something with the events: switch should reset devices, input should write to the respective devices (with scaling by SCALED_DIM_SIZE)
        // see also https://www.kernel.org/doc/html/latest/input/uinput.html
        info!("batch: input({})={:?} switch={:?}", input_events.len(), input_events, latest_switch_event);

        bytes.clear();
    }
}
