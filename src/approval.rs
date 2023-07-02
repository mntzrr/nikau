use anyhow::{bail, Context, Result};
use async_std::io as aio;
use async_std::task;
use tracing::{info, warn};

use std::io::{self, prelude::*};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::certs;

const PROMPT_TIMEOUT_SECS: u64 = 60;

pub struct ManualServerVerification {
    our_cert_fingerprint: String,
    known_certs: Vec<rustls::Certificate>,
}

impl ManualServerVerification {
    pub fn new(our_cert: &rustls::Certificate, known_certs: Vec<rustls::Certificate>) -> Arc<Self> {
        Arc::new(ManualServerVerification {
            our_cert_fingerprint: certs::fingerprint(our_cert),
            known_certs,
        })
    }
}

impl rustls::client::ServerCertVerifier for ManualServerVerification {
    fn verify_server_cert(
        &self,
        server_cert: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        let server_cert_fingerprint = certs::fingerprint(&server_cert);
        if self.known_certs.contains(server_cert) {
            info!("Server has a saved certificate: {}", server_cert_fingerprint);
            Ok(rustls::client::ServerCertVerified::assertion())
        } else if prompt_unknown_server_cert(server_cert, &self.our_cert_fingerprint) {
            info!("Server approved: {}", server_cert_fingerprint);
            Ok(rustls::client::ServerCertVerified::assertion())
        } else {
            info!("Server denied: {}", server_cert_fingerprint);
            Err(rustls::Error::General(format!("Unknown server cert was denied: {}", server_cert_fingerprint)))
        }
    }
}

fn prompt_unknown_server_cert(server_cert: &rustls::Certificate, client_cert_fingerprint: &String) -> bool {
    let server_cert_fingerprint = certs::fingerprint(&server_cert);
    let message = format!("NEW UNKNOWN SERVER CONNECTION: Approval needed

The client has connected to a new unknown server.
Only approve this if you are expecting to be connecting to a new server.
You will also likely need to confirm this connection on the server as well.

Check that these fingerprints look the same on the server and on the client:
- Server fingerprint: {}
- Client fingerprint: {}

Answering yes will allow the server connection to proceed, saving the server certificate as pre-approved for future connections.
Answering no will deny the new server and close the connection.

Confirm new connection and save certificate as approved? ({}s timeout) [y/N]", server_cert_fingerprint, client_cert_fingerprint, PROMPT_TIMEOUT_SECS);

    let result = prompt_yn(&message, false);
    if result {
        if let Err(e) = certs::write_approved_cert(server_cert) {
            warn!("Couldn't store server cert: {}", e);
        }
    }
    result
}

pub struct ManualClientVerification {
    our_cert_fingerprint: String,
    known_certs: Vec<rustls::Certificate>,
}

impl ManualClientVerification {
    pub fn new(our_cert: &rustls::Certificate, known_certs: Vec<rustls::Certificate>) -> Arc<Self> {
        Arc::new(ManualClientVerification {
            our_cert_fingerprint: certs::fingerprint(our_cert),
            known_certs,
        })
    }
}

impl rustls::server::ClientCertVerifier for ManualClientVerification {
    fn client_auth_root_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        client_cert: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _now: SystemTime,
    ) -> Result<rustls::server::ClientCertVerified, rustls::Error> {
        let client_cert_fingerprint = certs::fingerprint(&client_cert);
        if self.known_certs.contains(client_cert) {
            info!("Client has a saved certificate: {}", client_cert_fingerprint);
            Ok(rustls::server::ClientCertVerified::assertion())
        } else if prompt_unknown_client_cert(client_cert, &self.our_cert_fingerprint) {
            info!("Client approved: {}", client_cert_fingerprint);
            Ok(rustls::server::ClientCertVerified::assertion())
        } else {
            info!("Client denied: {}", client_cert_fingerprint);
            Err(rustls::Error::General(format!("Unknown client cert was denied: {}", client_cert_fingerprint)))
        }
    }
}

fn prompt_unknown_client_cert(client_cert: &rustls::Certificate, server_cert_fingerprint: &String) -> bool {
    let message = format!("NEW UNKNOWN CLIENT CONNECTION: Approval needed

The server has received a new connection from an unknown client.
Only approve this if you are expecting a new client.
You will also likely need to confirm this connection on the client as well.

Check that the following hashes look the same on the server and on the client:
- Server fingerprint: {}
- Client fingerprint: {}

Answering yes will allow the client to join and will save the client certificate as pre-approved for future connections.
Answering no will deny the new client and close the client connection.

Confirm new connection and save certificate as approved? ({}s timeout) [y/N]", server_cert_fingerprint, certs::fingerprint(&client_cert), PROMPT_TIMEOUT_SECS);

    let result = prompt_yn(&message, false);
    if result {
        if let Err(e) = certs::write_approved_cert(client_cert) {
            warn!("Couldn't store client cert: {}", e);
        }
    }
    result
}

fn prompt_yn(msg: &str, default: bool) -> bool {
    match prompt_internal(msg) {
        Ok(input) => {
            if input.is_empty() {
                return default;
            }
            match input.chars().nth(0).expect("Failed to get first char") {
                'y' | 'Y' | 't' | 'T' => true,
                _ => false,
            }
        },
        Err(e) => {
            warn!("Confirmation prompt failed, assuming '{}': {}", if default { "yes" } else { "no" }, e);
            return default;
        }
    }
}

fn prompt_internal(msg: &str) -> Result<String> {
    let msg_formatted = format!("{}: ", msg);
    let mut stdout = io::stdout();
    stdout
        .write_all(msg_formatted.as_bytes())
        .context("Failed to write prompt to stdout")?;
    stdout.flush().expect("Failed to flush stdout");
    let mut response = String::new();
    task::block_on(async {
        match aio::timeout(Duration::from_secs(PROMPT_TIMEOUT_SECS), aio::stdin().read_line(&mut response)).await {
            Ok(_) => {
                return Ok(());
            },
            Err(_e) => {
                // Skip output to next line so that logs don't print on top of the prompt
                println!("");
                bail!("Prompt timed out after {}s", PROMPT_TIMEOUT_SECS)
            },
        }
    })?;
    Ok(response.trim().to_string())
}
