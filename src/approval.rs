use std::io::{self, prelude::*};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use async_std::{io as aio, task};
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

struct ApprovalState {
    /// Previously-approved certs found on disk, updated internally when approval prompts are confirmed
    known_certs: Vec<rustls::Certificate>,
    /// Whether a cert prompt is currently pending. We only allow one prompt to be pending at a time.
    prompt_active: bool,
}

pub struct NikauCertVerification {
    /// Used for building rustls configs
    our_cert: rustls::Certificate,
    /// Used for building rustls configs
    our_privkey: rustls::PrivateKey,
    /// Pre-approved cert fingerprints provided via commandline argument
    approved_cert_fingerprints: Vec<String>,
    /// Mutable certificate approval state
    approval_state: RwLock<ApprovalState>,
}

impl NikauCertVerification {
    pub fn new(splash_label: &str, approved_cert_fingerprints: Vec<String>) -> Result<Arc<Self>> {
        let (our_cert, our_privkey) = certs::load_keypair(splash_label)
            .with_context(|| format!("Failed to load {} keypair", splash_label))?;
        // Convert e.g. "18:AE:75:F2..." (openssl style) => "18ae75f2..." (our style)
        // Can get the openssl style from: openssl x509 -noout -sha256 -fingerprint -in /path/to/private.pem
        let approved_cert_fingerprints = approved_cert_fingerprints
            .into_iter()
            .map(|fingerprint| fingerprint.to_lowercase().replace(":", ""))
            .collect();
        Ok(Arc::new(NikauCertVerification {
            our_cert,
            our_privkey,
            approved_cert_fingerprints,
            approval_state: RwLock::new(ApprovalState {
                known_certs: certs::load_known_certs()?,
                prompt_active: false,
            }),
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
        if let Ok(mut approval_state) = self.approval_state.write() {
            if approval_state.known_certs.contains(their_cert) {
                info!(
                    "{} cert has been approved before: {}",
                    their_name, their_cert_fingerprint
                );
                return Ok(approve_response);
            } else if self
                .approved_cert_fingerprints
                .contains(&their_cert_fingerprint)
            {
                info!(
                    "{} cert approved via --fingerprints: {}",
                    their_name, their_cert_fingerprint
                );
                // Don't save the cert to disk for --fingerprints.
                // Saving to disk creates weird behavior if the user later changes the certs they approve.
                // Maybe they don't WANT old certs to still be approved if the arg changes? Play it safe.
                approval_state.known_certs.push(their_cert.clone());
                return Ok(approve_response);
            } else if approval_state.prompt_active {
                // Only one prompt at a time, reject other prompts. They will retry connecting anyway.
                return Err(rustls::Error::General(format!(
                    "{} cert rejected: Approval prompt is already pending",
                    their_name
                )));
            } else {
                approval_state.prompt_active = true;
                // Prompt continues below after releasing lock
            }
        } else {
            error!("Failed to get lock on known_certs for check");
            return Err(rustls::Error::General(
                "Failed to lock known certs".to_string(),
            ));
        }

        // Must release lock on self.known_certs during prompt to avoid breaking connectivity,
        // especially on the server, where it can break all clients until the server is restarted.
        if prompt_unknown_cert(their_cert, we_are_server) {
            info!("{} cert approved: {}", their_name, their_cert_fingerprint);
            if let Err(e) = certs::write_approved_cert(their_cert) {
                warn!(
                    "{} was approved, but couldn't save cert to disk: {}",
                    their_name, e
                );
            }
            // Store the approved cert locally so that we don't e.g. reprompt on reconnect later on
            if let Ok(mut approval_state) = self.approval_state.write() {
                approval_state.known_certs.push(their_cert.clone());
                approval_state.prompt_active = false;
            } else {
                error!("Failed to get lock on known_certs for approval");
                return Err(rustls::Error::General(
                    "Failed to lock known certs".to_string(),
                ));
            }
            Ok(approve_response)
        } else {
            info!(
                "{} cert not approved: {}",
                their_name, their_cert_fingerprint
            );
            if let Ok(mut approval_state) = self.approval_state.write() {
                approval_state.prompt_active = false;
            } else {
                error!("Failed to get lock on known_certs for rejection");
                return Err(rustls::Error::General(
                    "Failed to lock mutable state".to_string(),
                ));
            }
            Err(rustls::Error::General(format!(
                "{} cert wasn't approved by user: {}",
                their_name, their_cert_fingerprint
            )))
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

fn prompt_unknown_cert(their_cert: &rustls::Certificate, we_are_server: bool) -> bool {
    let their_cert_fingerprint = certs::fingerprint(&their_cert);
    let message = if we_are_server {
        format!(
            "NEW UNKNOWN CLIENT CONNECTION: Approval needed

The server has received a connection from a new unknown client.
Only approve this if you are expecting a new client.
You will also likely need to confirm this connection on the client as well.

Comfirm that the client startup image has this fingerprint:
    {}

Allow this new client and save its certificate for future connections? ({}s timeout) [y/N]
",
            their_cert_fingerprint, PROMPT_TIMEOUT_SECS
        )
    } else {
        format!(
            "NEW UNKNOWN SERVER CONNECTION: Approval needed

The client has connected to a new unknown server.
Only approve this if you are expecting to be connecting to a new server.
You will also likely need to confirm this connection on the server as well.

Confirm that the server startup image has this fingerprint:
    {}

Allow this new server and save its certificate for future connections? ({}s timeout) [y/N]
",
            their_cert_fingerprint, PROMPT_TIMEOUT_SECS
        )
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
    let msg_formatted = format!("{}", msg);
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
