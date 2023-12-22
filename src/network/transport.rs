use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use quinn::{
    ClientConfig, Endpoint, IdleTimeout, RecvStream, SendStream, ServerConfig, TransportConfig,
    VarInt,
};
use tracing::{debug, trace};

use crate::msgs::shared;
use crate::network::approval;

/// Wireguard for example recommends 25000 for spanning NATs/firewalls, so that'd be optimal.
/// But we need to keep it shorter than TIMEOUT_MILLIS to avoid spurious timeouts.
const KEEPALIVE_MILLIS: u64 = 2000;

/// This is the delay between a client losing connection and a server ungrabbing its input devices.
/// Keep this very short so that connection problems get resolved relatively quickly.
const TIMEOUT_MILLIS: u32 = 3000;

pub fn build_client(
    bind_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
) -> Result<quinn::Endpoint> {
    let mut client_config = ClientConfig::new(approval::rustls_client_config(cert_verifier)?);
    client_config.transport_config(transport_config());

    let mut client_endpoint = Endpoint::client(*bind_addr)?;
    client_endpoint.set_default_client_config(client_config);
    Ok(client_endpoint)
}

pub fn build_server(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
) -> Result<quinn::Endpoint> {
    let mut server_config =
        ServerConfig::with_crypto(approval::rustls_server_config(cert_verifier)?);
    server_config
        .use_retry(true)
        .transport_config(transport_config());
    Endpoint::server(server_config, *listen_addr)
        .with_context(|| format!("Failed to listen on {}", listen_addr))
}

fn transport_config() -> Arc<TransportConfig> {
    let mut transport_config = TransportConfig::default();
    transport_config
        //.max_concurrent_bidi_streams(2_u8.into()) // events + bulk
        .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
        .keep_alive_interval(Some(Duration::from_millis(KEEPALIVE_MILLIS)))
        .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(TIMEOUT_MILLIS))));
    Arc::new(transport_config)
}

pub async fn send_version(send: &mut SendStream) -> Result<()> {
    let msg = shared::VersionBootstrapMessage {
        version: shared::PROTOCOL_VERSION,
    };
    let serializedmsg = postcard::to_stdvec_cobs(&msg)
        .map_err(|e| anyhow!("Failed to serialize version message: {:?}", e))?;
    trace!(
        "Sending {} byte version: {:X?}",
        serializedmsg.len(),
        &serializedmsg
    );
    send.write_all(&serializedmsg)
        .await
        .context("Failed to send protocol version")?;
    Ok(())
}

pub async fn recv_version(recv: &mut RecvStream, buf: &mut Vec<u8>) -> Result<()> {
    debug!("Waiting to receive version");
    let resp = recv
        .read_chunk(1024, true)
        .await
        .context("Failed reading protocol version from server")?
        .context("Server closed connection")?;
    trace!(
        "Received {} byte version: {:X?}",
        resp.bytes.len(),
        &*resp.bytes
    );
    // Copy the immutable response data into a mutable buffer
    buf.extend_from_slice(&resp.bytes);
    let version: u64;
    {
        let (versionmsg, resp_remainder) =
            postcard::take_from_bytes_cobs::<shared::VersionBootstrapMessage>(buf)
                .map_err(|e| anyhow!("Failed to deserialize message: {:?}", e))?;
        version = versionmsg.version;
        // Remove this message from the front of buf
        let consumed = resp.bytes.len() - resp_remainder.len();
        let buf_len = buf.len();
        buf.copy_within(consumed..buf_len, 0);
        buf.truncate(buf_len - consumed);
    }
    if version != shared::PROTOCOL_VERSION {
        bail!(
            "Their protocol version {} doesn't match our expected version {}. You need to update nikau across your server and client(s) so that the protocol versions line up. Use 'nikau -V' to check the version.",
            version,
            shared::PROTOCOL_VERSION
        );
    }
    Ok(())
}
