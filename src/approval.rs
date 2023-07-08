use std::io::{self, prelude::*};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use async_std::io as aio;
use async_std::task;
use tracing::{error, info, warn};

use crate::certs;

const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];
const PROMPT_TIMEOUT_SECS: u64 = 60;

pub fn rustls_client_config(
    verifier: Arc<NikauCertVerification>,
) -> Result<Arc<rustls::ClientConfig>> {
    let mut rustls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(verifier.clone())
        .with_single_cert(
            vec![verifier.our_cert.clone()],
            verifier.our_privkey.clone(),
        )?;
    rustls_config.alpn_protocols = ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();
    Ok(Arc::new(rustls_config))
}

pub fn rustls_server_config(
    verifier: Arc<NikauCertVerification>,
) -> Result<Arc<rustls::ServerConfig>> {
    let mut rustls_config = rustls::ServerConfig::builder()
        .with_safe_defaults() // includes TLS1.3 required by QUIC
        .with_client_cert_verifier(verifier.clone())
        .with_single_cert(
            vec![verifier.our_cert.clone()],
            verifier.our_privkey.clone(),
        )?;
    rustls_config.alpn_protocols = ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();
    rustls_config.max_early_data_size = u32::MAX; // required by QUIC
    Ok(Arc::new(rustls_config))
}

pub struct NikauCertVerification {
    /// Used for building rustls configs
    our_cert: rustls::Certificate,
    /// Used for logging, calculated once up-front
    our_cert_fingerprint: String,
    /// Used for building rustls configs
    our_privkey: rustls::PrivateKey,
    /// Pre-approved cert fingerprints provided via commandline argument
    approved_cert_fingerprints: Vec<String>,
    /// Previously-approved certs found on disk, updated internally when approval prompts are confirmed
    known_certs: RwLock<Vec<rustls::Certificate>>,
}

impl NikauCertVerification {
    pub fn new(approved_cert_fingerprints: Vec<String>) -> Result<Arc<Self>> {
        let (our_cert, our_privkey) =
            certs::load_keypair().context("Failed to load our keypair")?;
        let our_cert_fingerprint = certs::fingerprint(&our_cert);
        // Convert e.g. "18:AE:75:F2..." (openssl style) => "18ae75f2..." (our style)
        // Can get the openssl style from: openssl x509 -noout -sha256 -fingerprint -in /path/to/private.pem
        let approved_cert_fingerprints = approved_cert_fingerprints
            .into_iter()
            .map(|fingerprint| fingerprint.to_lowercase().replace(":", ""))
            .collect();
        Ok(Arc::new(NikauCertVerification {
            our_cert,
            our_cert_fingerprint,
            our_privkey,
            approved_cert_fingerprints,
            known_certs: RwLock::new(certs::load_known_certs()?),
        }))
    }

    fn verify_cert<T>(
        &self,
        their_cert: &rustls::Certificate,
        their_name: &str,
        we_are_server: bool,
        approve_response: T,
    ) -> Result<T, rustls::Error> {
        let their_cert_fingerprint = certs::fingerprint(&their_cert);
        if let Ok(mut known_certs) = self.known_certs.write() {
            if known_certs.contains(their_cert) {
                info!(
                    "{} has a known certificate: {}",
                    their_name, their_cert_fingerprint
                );
                Ok(approve_response)
            } else if self
                .approved_cert_fingerprints
                .contains(&their_cert_fingerprint)
            {
                info!(
                    "{} approved via --fingerprints: {}",
                    their_name, their_cert_fingerprint
                );
                // Don't save the cert to disk for --fingerprints.
                // Saving to disk creates weird behavior if the user later changes the certs they approve.
                // Maybe they don't WANT old certs to still be approved if the arg changes? Play it safe.
                known_certs.push(their_cert.clone());
                Ok(approve_response)
            } else if prompt_unknown_cert(their_cert, &self.our_cert_fingerprint, we_are_server) {
                if let Err(e) = certs::write_approved_cert(their_cert) {
                    warn!(
                        "{} approved, but couldn't save cert to disk: {}",
                        their_name, e
                    );
                }
                // Store the approved cert locally so that we don't e.g. reprompt on reconnect later on
                known_certs.push(their_cert.clone());
                Ok(approve_response)
            } else {
                info!("Server denied: {}", their_cert_fingerprint);
                Err(rustls::Error::General(format!(
                    "{} cert was denied: {}",
                    their_name, their_cert_fingerprint
                )))
            }
        } else {
            error!("Failed to get lock on known_certs");
            Err(rustls::Error::General(
                "Failed to lock known certs".to_string(),
            ))
        }
    }
}

impl rustls::client::ServerCertVerifier for NikauCertVerification {
    fn verify_server_cert(
        &self,
        server_cert: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        self.verify_cert(
            server_cert,
            "Server",
            false,
            rustls::client::ServerCertVerified::assertion(),
        )
    }
}

impl rustls::server::ClientCertVerifier for NikauCertVerification {
    fn client_auth_root_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        client_cert: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _now: SystemTime,
    ) -> Result<rustls::server::ClientCertVerified, rustls::Error> {
        self.verify_cert(
            client_cert,
            "Client",
            true,
            rustls::server::ClientCertVerified::assertion(),
        )
    }
}

fn prompt_unknown_cert(
    their_cert: &rustls::Certificate,
    our_cert_fingerprint: &String,
    we_are_server: bool,
) -> bool {
    let their_cert_fingerprint = certs::fingerprint(&their_cert);
    let message = if we_are_server {
        format!(
            "NEW UNKNOWN CLIENT CONNECTION: Approval needed

The server has received a new connection from an unknown client.
Only approve this if you are expecting a new client.
You will also likely need to confirm this connection on the client as well.

Check that the following hashes look the same on the server and on the client:
- Server fingerprint: {}
- Client fingerprint: {}

Answering yes will allow the client to join and will save the client certificate as pre-approved for future connections.
Answering no will deny the new client and close the client connection.

Confirm new connection and save certificate as approved? ({}s timeout) [y/N]",
            our_cert_fingerprint, their_cert_fingerprint, PROMPT_TIMEOUT_SECS)
    } else {
        format!(
            "NEW UNKNOWN SERVER CONNECTION: Approval needed

The client has connected to a new unknown server.
Only approve this if you are expecting to be connecting to a new server.
You will also likely need to confirm this connection on the server as well.

Check that these fingerprints look the same on the server and on the client:
- Server fingerprint: {}
- Client fingerprint: {}

Answering yes will allow the server connection to proceed, saving the server certificate as pre-approved for future connections.
Answering no will deny the new server and close the connection.

Confirm new connection and save certificate as approved? ({}s timeout) [y/N]",
            their_cert_fingerprint, our_cert_fingerprint, PROMPT_TIMEOUT_SECS)
    };
    prompt_yn(&message, false)
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
        }
        Err(e) => {
            warn!(
                "Confirmation prompt failed, assuming '{}': {}",
                if default { "yes" } else { "no" },
                e
            );
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
        match aio::timeout(
            Duration::from_secs(PROMPT_TIMEOUT_SECS),
            aio::stdin().read_line(&mut response),
        )
        .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(_e) => {
                // Skip output to next line so that logs don't print on top of the prompt
                println!("");
                bail!("Prompt timed out after {}s", PROMPT_TIMEOUT_SECS)
            }
        }
    })?;
    Ok(response.trim().to_string())
}
