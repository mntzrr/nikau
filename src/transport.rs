use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use quinn::{ClientConfig, Endpoint, IdleTimeout, ServerConfig, TransportConfig, VarInt};

use crate::approval;

/// Wireguard recommends 25000 for spanning NATs/firewalls.
/// Must be shorter than TIMEOUT_MILLIS to avoid spurious timeouts.
const KEEPALIVE_MILLIS: u64 = 2000;

/// This is the delay between a client losing connection and a server ungrabbing the keys.
/// Keep this very short so that connection problems get resolved relatively quickly.
const TIMEOUT_MILLIS: u32 = 3000;

pub fn build_client(
    bind_addr: &SocketAddr,
    cert_verifier: Arc<approval::NikauCertVerification>,
) -> Result<quinn::Endpoint> {
    let mut client_config = ClientConfig::new(approval::rustls_client_config(cert_verifier)?);
    client_config.transport_config(transport_config());

    let mut client_endpoint = Endpoint::client(bind_addr.clone())?;
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
    Ok(Endpoint::server(server_config, listen_addr.clone())
       .with_context(|| format!("Failed to listen on {}", listen_addr))?)
}

fn transport_config() -> Arc<TransportConfig> {
    let mut transport_config = TransportConfig::default();
    transport_config
        .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
        .keep_alive_interval(Some(Duration::from_millis(KEEPALIVE_MILLIS)))
        .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(TIMEOUT_MILLIS))));
    Arc::new(transport_config)
}
