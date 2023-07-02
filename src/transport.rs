use anyhow::{Context, Result};
use quinn::{ClientConfig, Endpoint, IdleTimeout, ServerConfig, TransportConfig, VarInt};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::approval;
use crate::certs;

const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];
const KEEPALIVE_MILLIS: u64 = 25000; // value recommended by wireguard across NATs/firewalls
const TIMEOUT_MILLIS: u32 = 65000; // must be larger than keepalive to avoid spurious timeouts

pub fn build_client(bind_addr: SocketAddr, known_server_certs: Vec<rustls::Certificate>) -> Result<quinn::Endpoint> {
    let (client_cert, client_privkey) = certs::load_keypair().context("Failed to load client keypair")?;
    let mut rustls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(approval::ManualServerVerification::new(&client_cert, known_server_certs))
        .with_single_cert(vec![client_cert], client_privkey)?;
    rustls_config.alpn_protocols = ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();
    let mut transport_config = TransportConfig::default();
    transport_config
        .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
        .keep_alive_interval(Some(Duration::from_millis(KEEPALIVE_MILLIS)))
        .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(TIMEOUT_MILLIS))));
    let mut client_config = ClientConfig::new(Arc::new(rustls_config));
    client_config
        .transport_config(Arc::new(transport_config));

    let mut client_endpoint = Endpoint::client(bind_addr)?;
    client_endpoint.set_default_client_config(client_config);
    Ok(client_endpoint)
}

pub fn build_server(listen_addr: SocketAddr, known_client_certs: Vec<rustls::Certificate>) -> Result<quinn::Endpoint> {
    let (server_cert, server_privkey) = certs::load_keypair().context("Failed to load server keypair")?;
    let mut rustls_config = rustls::ServerConfig::builder()
        .with_safe_defaults() // includes TLS1.3 required by QUIC
        .with_client_cert_verifier(approval::ManualClientVerification::new(&server_cert, known_client_certs))
        .with_single_cert(vec![server_cert], server_privkey)?;
    rustls_config.alpn_protocols = ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();
    rustls_config.max_early_data_size = u32::MAX; // required by QUIC
    let mut transport_config = TransportConfig::default();
    transport_config
        .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
        .keep_alive_interval(Some(Duration::from_millis(KEEPALIVE_MILLIS)))
        .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(TIMEOUT_MILLIS))));
    let mut server_config = ServerConfig::with_crypto(Arc::new(rustls_config));
    server_config
        .use_retry(true)
        .transport_config(Arc::new(transport_config));
    Ok(Endpoint::server(server_config, listen_addr)?)
}
