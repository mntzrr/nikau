use std::io::{self, prelude::*};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};

use crate::network::certs;

const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];
const PROMPT_TIMEOUT_SECS: u64 = 60;

pub fn rustls_client_config(
    verifier: Arc<NikauCertVerification<'static>>,
) -> Result<Arc<dyn quinn::crypto::ClientConfig>> {
    let mut rustls_config = quinn::rustls::ClientConfig::builder_with_provider(verifier.crypto_provider.clone())
        .with_safe_default_protocol_versions()?
        .dangerous().with_custom_certificate_verifier(verifier.clone())
        .with_client_auth_cert(
            vec![verifier.our_cert.clone()],
            verifier.our_privkey.clone_key(),
        )?;
    rustls_config.alpn_protocols = ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();
    Ok(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)?))
}

pub fn rustls_server_config(
    verifier: Arc<NikauCertVerification<'static>>,
) -> Result<Arc<dyn quinn::crypto::ServerConfig>> {
    let mut rustls_config = quinn::rustls::ServerConfig::builder_with_provider(verifier.crypto_provider.clone())
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier.clone())
        .with_single_cert(
            vec![verifier.our_cert.clone()],
            verifier.our_privkey.clone_key(),
        )?;
    rustls_config.alpn_protocols = ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();
    rustls_config.max_early_data_size = u32::MAX; // required by QUIC
    Ok(Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)?))
}

#[derive(Debug)]
struct ApprovalState<'a> {
    /// Previously-approved certs found on disk, updated internally when approval prompts are confirmed
    known_certs: Vec<rustls_pki_types::CertificateDer<'a>>,
    /// Whether a cert prompt is currently pending. We only allow one prompt to be pending at a time.
    prompt_active: bool,
}

#[derive(Debug)]
pub struct NikauCertVerification<'a> {
    /// For storing certs to disk
    config_dir: PathBuf,
    /// Used for building rustls configs
    our_cert: rustls_pki_types::CertificateDer<'a>,
    /// Used for building rustls configs
    our_privkey: rustls_pki_types::PrivateKeyDer<'a>,
    /// Pre-approved cert fingerprints provided via commandline argument
    approved_cert_fingerprints: Vec<String>,
    /// Mutable certificate approval state
    approval_state: RwLock<ApprovalState<'a>>,
    /// Storage for reporting the latest received fingerprint
    fingerprint: Arc<Mutex<Option<String>>>,
    /// For rustls verify calls
    crypto_provider: Arc<rustls::crypto::CryptoProvider>,
}

impl<'a> NikauCertVerification<'a> {
    pub fn new(
        splash_label: &str,
        approved_cert_fingerprints: Vec<String>,
        config_dir: &PathBuf,
        fingerprint: Arc<Mutex<Option<String>>>,
    ) -> Result<Arc<Self>> {
        let (our_cert, our_privkey) = certs::load_keypair(splash_label, config_dir)
            .with_context(|| format!("Failed to load {} keypair", splash_label))?;
        // Convert e.g. "18:AE:75:F2..." (openssl style) => "18ae75f2..." (our style)
        // Can get the openssl style from: openssl x509 -noout -sha256 -fingerprint -in /path/to/private.pem
        let approved_cert_fingerprints: Vec<String> = approved_cert_fingerprints
            .into_iter()
            .map(|fingerprint| fingerprint.to_lowercase().replace(':', ""))
            .collect();
        if !approved_cert_fingerprints.is_empty() {
            info!(
                "Configured {} preapproved fingerprints: {:?}",
                approved_cert_fingerprints.len(),
                approved_cert_fingerprints
            )
        }
        Ok(Arc::new(NikauCertVerification {
            config_dir: config_dir.clone(),
            our_cert,
            our_privkey,
            approved_cert_fingerprints,
            approval_state: RwLock::new(ApprovalState {
                known_certs: certs::load_known_certs(config_dir)?,
                prompt_active: false,
            }),
            fingerprint,
            crypto_provider: Arc::new(rustls::crypto::ring::default_provider()),
        }))
    }

    fn verify_cert(
        &self,
        their_cert: &rustls_pki_types::CertificateDer<'_>,
        their_name: &str,
        we_are_server: bool,
    ) -> Result<String> {
        let their_cert_fingerprint = certs::fingerprint(their_cert);
        if let Ok(mut approval_state) = self.approval_state.write() {
            if approval_state.known_certs.contains(their_cert) {
                info!(
                    "{} cert has been approved before: {}",
                    their_name, their_cert_fingerprint
                );
                return Ok(their_cert_fingerprint);
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
                approval_state.known_certs.push(their_cert.clone().into_owned());
                return Ok(their_cert_fingerprint);
            } else if approval_state.prompt_active {
                // Only one prompt at a time, reject other prompts. They will retry connecting anyway.
                bail!(
                    "{} cert rejected: Approval prompt is already pending",
                    their_name
                );
            } else {
                approval_state.prompt_active = true;
                // Prompt continues below after releasing lock
            }
        } else {
            bail!("Failed to lock known certs for check");
        }

        // Must release lock on self.known_certs during prompt to avoid breaking connectivity,
        // especially on the server, where it can break all clients until the server is restarted.
        if prompt_unknown_cert(their_cert, we_are_server) {
            info!("{} cert approved: {}", their_name, their_cert_fingerprint);
            if let Err(e) =
                certs::write_approved_cert(their_cert, &their_cert_fingerprint, &self.config_dir)
            {
                warn!(
                    "{} approved, but couldn't save cert to disk: {}",
                    their_name, e
                );
            }
            // Store the approved cert locally so that we don't e.g. reprompt on reconnect later on
            if let Ok(mut approval_state) = self.approval_state.write() {
                approval_state.known_certs.push(their_cert.clone().into_owned());
                approval_state.prompt_active = false;
            } else {
                bail!("Failed to lock known certs for approval");
            }
            Ok(their_cert_fingerprint)
        } else {
            info!(
                "{} cert not approved: {}",
                their_name, their_cert_fingerprint
            );
            if let Ok(mut approval_state) = self.approval_state.write() {
                approval_state.prompt_active = false;
            } else {
                bail!("Failed to lock known certs for disapproval");
            }
            bail!(
                "{} cert wasn't approved by user: {}",
                their_name,
                their_cert_fingerprint
            );
        }
    }
}

/// Run by the client to verify servers
impl rustls::client::danger::ServerCertVerifier for NikauCertVerification<'_> {
    fn verify_server_cert(
        &self,
        server_cert: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if let Err(e) = self.verify_cert(server_cert, "Server", false) {
            Err(rustls::Error::General(e.to_string()))
        } else {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // Default call used by WebPkiServerVerifier
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.crypto_provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // Default call used by WebPkiServerVerifier
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.crypto_provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.crypto_provider.signature_verification_algorithms.supported_schemes()
    }
}

/// Run by the server to verify clients
impl<'a> rustls::server::danger::ClientCertVerifier for NikauCertVerification<'a> {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        client_cert: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        match self.verify_cert(client_cert, "Client", true) {
            Err(e) => Err(rustls::Error::General(e.to_string())),
            Ok(their_cert_fingerprint) => {
                // HACK: This is storing the fingerprint for reading by server.rs
                // This code is meant to allow pairing new connections with their cert fingerprints for use by "--shortcut-goto".
                //
                // In an ideal world, the solution for this could be one of the following:
                // - Packing/embedding the fingerprint with the client here somehow
                // - Extracting the cert/fingerprint within the server connection handling somehow
                //
                // But fundamentally we hit these issues:
                // - This rustls API lets us see the cert/fingerprint, but not the client endpoint.
                // - The quinn API lets us see the client endpoint, but not the cert/fingerprint.
                //
                // In order to bridge the gap, we go with this timing-based "message in a bottle" method,
                // where we assume that a rustls cert check will be immediately followed by the quinn connection appearing.
                // This assumption has problems, see below.
                if let Ok(mut fingerprint) = self.fingerprint.lock() {
                    debug!(
                        "Saving fingerprint for connection: {}",
                        their_cert_fingerprint
                    );
                    if let Some(old_fingerprint) =
                        fingerprint.replace(their_cert_fingerprint.clone())
                    {
                        // The fingerprint->connection assumption could fall apart when multiple clients connect at the same time, resulting in fingerprint mismatch:
                        // Scenario A:
                        //   1. client A connects and its fingerprint is saved
                        //   2. very soon after, client B connects and its fingerprint gets saved, overwriting client A's fingerprint <-- you are here
                        //   3. then client A's connection completes and client B's fingerprint is pulled
                        // Scenario B:
                        //   1. client A connects and its fingerprint is saved
                        //   2. very soon after, client B connects and its fingerprint gets saved, overwriting client A's fingerprint <-- you are here
                        //   3. then client B's connection completes and client B's fingerprint is pulled
                        //   3. then client A's connection completes and there is no fingerprint left
                        //
                        // Amelioration:
                        // In both of these cases, if things look off, we reset the fingerprint state and reject client B, so that it can try again.
                        // Meanwhile, server.rs will see the missing fingerprint state and reject client A.
                        // However, another client C could swoop in at this time and store a new fingerprint to be seen by server.rs, this workaround isn't perfect either.
                        // But at the same time, this client-fingerprint pairing is only used for "--shortcut-goto", so it's somewhat less critical that things be perfect.
                        //
                        // Another option would be to put the old fingerprint back and only reject client B, but then that creates a different problem, unrelated to multi-client races:
                        // What if a client finishes cert validation here, but then disconnects before server.rs takes the fingerprint?
                        // In that case, we'd want the unused fingerprint to be overwritten by the next connection attempt.
                        // So it's better to leave things in a recoverable state so that the server isn't rejecting connections indefinitely.
                        warn!("BUG: Obtained new client fingerprint {} but old fingerprint {} is still present, resetting state and rejecting new client (try again)", their_cert_fingerprint, old_fingerprint);
                        // Reject the new client, and clear the fingerprint state. The old client (if any) should then be rejected by server.rs. Both clients can retry.
                        let _ = fingerprint.take();
                        Err(rustls::Error::General("Fingerprint is valid but an existing connection is still in progress, try again".to_string()))
                    } else {
                        Ok(rustls::server::danger::ClientCertVerified::assertion())
                    }
                } else {
                    Err(rustls::Error::General(
                        "Failed to lock fingerprint".to_string(),
                    ))
                }
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // Default call used by WebPkiServerVerifier
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.crypto_provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // Default call used by WebPkiServerVerifier
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.crypto_provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.crypto_provider.signature_verification_algorithms.supported_schemes()
    }
}

fn prompt_unknown_cert(their_cert: &rustls_pki_types::CertificateDer, we_are_server: bool) -> bool {
    let their_cert_fingerprint = certs::fingerprint(their_cert);
    if atty::isnt(atty::Stream::Stdin) {
        warn!("Stdin is not a TTY, skipping user certificate approval prompt. Approve this cert by running the {} with '--fingerprints {}'", if we_are_server { "server" } else { "client" }, their_cert_fingerprint);
        return false;
    }

    let message = if we_are_server {
        format!(
            "APPROVAL NEEDED: New unknown client connection

The server has received a connection from a new unknown client.
Only approve this if you are expecting a new client.
You will also likely need to confirm this connection on the client as well.

Comfirm that the client startup image has this fingerprint:
    {}

Allow this new client and save its certificate for future connections? ({}s timeout) [y/N]
> ",
            their_cert_fingerprint, PROMPT_TIMEOUT_SECS
        )
    } else {
        format!(
            "APPROVAL NEEDED: New unknown server connection

The client has connected to a new unknown server.
Only approve this if you are expecting to be connecting to a new server.
You will also likely need to confirm this connection on the server as well.

Confirm that the server startup image has this fingerprint:
    {}

Allow this new server and save its certificate for future connections? ({}s timeout) [y/N]
> ",
            their_cert_fingerprint, PROMPT_TIMEOUT_SECS
        )
    };
    prompt_yn(&message, false)
}

fn prompt_yn(msg: &str, default: bool) -> bool {
    match prompt_internal(msg) {
        Ok(char_) => {
            match char_ {
                // Check for [yY]es or [tT]rue
                b'y' | b'Y' | b't' | b'T' => true,
                _ => false,
            }
        }
        Err(e) => {
            warn!(
                "Confirmation prompt failed, assuming '{}': {}",
                if default { "yes" } else { "no" },
                e
            );
            default
        }
    }
}

fn prompt_internal(msg: &str) -> Result<u8> {
    // Use nonblock to allow timeout on stdin.
    // Could try to use async, but Tokio docs don't recommend it in this context.
    let mut stdin = nonblock::NonBlockingReader::from_fd(io::stdin())
        .context("Failed to set up nonblocking reader for stdin")?;

    // Flush any preceding input before prompt
    {
        let mut discard = vec![];
        stdin
            .read_available(&mut discard)
            .context("Failed to flush initial input")?;
    }

    // Send the prompt
    let msg_formatted = msg.to_string();
    let mut stdout = io::stdout();
    stdout
        .write_all(msg_formatted.as_bytes())
        .context("Failed to write prompt to stdout")?;
    stdout.flush().expect("Failed to flush stdout");

    // Wait for first char, or time out. Use blocking APIs.
    let end_at = Instant::now()
        .checked_add(Duration::from_secs(PROMPT_TIMEOUT_SECS))
        .expect("Failed to configure timeout");
    let mut content = vec![];
    loop {
        thread::sleep(Duration::from_millis(50));
        stdin
            .read_available(&mut content)
            .context("Failed to check for user input")?;

        // Check and return first character
        if let Some(c) = content.first() {
            return Ok(*c);
        }

        // Still nothing, check for timeout
        if Instant::now() >= end_at {
            println!();
            bail!("Prompt timed out after {}s", PROMPT_TIMEOUT_SECS)
        }
    }
}
