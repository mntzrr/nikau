use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use quinn::{ClientConfig, Endpoint, IdleTimeout, ServerConfig, TransportConfig, VarInt};

use crate::approval;

const KEEPALIVE_MILLIS: u64 = 25000; // value recommended by wireguard across NATs/firewalls
const TIMEOUT_MILLIS: u32 = 30000; // must be larger than keepalive to avoid spurious timeouts

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
    Ok(Endpoint::server(server_config, listen_addr.clone())?)
}

fn transport_config() -> Arc<TransportConfig> {
    let mut transport_config = TransportConfig::default();
    transport_config
        .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
        .keep_alive_interval(Some(Duration::from_millis(KEEPALIVE_MILLIS)))
        .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(TIMEOUT_MILLIS))));
    Arc::new(transport_config)
}
