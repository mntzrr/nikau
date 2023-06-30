use anyhow::{bail, Context, Result};
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use std::fs;
use std::io::{BufReader, prelude::*};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub async fn start_server(listen_addr: SocketAddr, known_client_certs: Vec<rustls::Certificate>) -> Result<()> {
    let server_endpoint: Endpoint;
    {
        let (server_cert, server_privkey) = load_keypair()?;
        let mut rustls_config = rustls::ServerConfig::builder()
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_protocol_versions(&[&rustls::version::TLS13]) // required by QUIC
            .expect("default config failed to load")
            .with_client_cert_verifier(ManualClientVerification::new(known_client_certs))
            .with_single_cert(vec![server_cert], server_privkey)?;
        rustls_config.max_early_data_size = u32::MAX; // required by QUIC
        let mut server_config = ServerConfig::with_crypto(Arc::new(rustls_config));
        {
            let mut transport_config = TransportConfig::default();
            transport_config.max_concurrent_uni_streams(0_u8.into());
            transport_config.keep_alive_interval(Some(Duration::from_secs(30)));
            server_config.transport = Arc::new(transport_config);
        }
        server_endpoint = Endpoint::server(server_config, listen_addr)?;
    }
    // accept a single connection
    let incoming_conn = server_endpoint.accept().await.unwrap();
    match incoming_conn.await {
        Ok(conn) => {
            info!(
                "[server] connection accepted: addr={}",
                conn.remote_address()
            );
        },
        Err(e) => {
            error!("[server] connection failed: {}", e);
        }
    }
    Ok(())
}

pub async fn start_client(server_addr: SocketAddr, known_server_certs: Vec<rustls::Certificate>) -> Result<()> {
    let mut client_endpoint: Endpoint;
    {
        client_endpoint = Endpoint::client("127.0.0.1:0".parse()?)?;
        let (client_cert, client_privkey) = load_keypair()?;
        client_endpoint.set_default_client_config(ClientConfig::new(Arc::new(
            rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_custom_certificate_verifier(ManualServerVerification::new(known_server_certs))
                .with_single_cert(vec![client_cert], client_privkey)?
        )));
    }

    // connect to server
    let connection = client_endpoint
        .connect(server_addr, "localhost")?
        .await?;
    info!("[client] connected: addr={}", connection.remote_address());
    // Dropping handles allows the corresponding objects to automatically shut down
    drop(connection);
    // Make sure the server has a chance to clean up
    client_endpoint.wait_idle().await;
    Ok(())
}

struct ManualServerVerification {
    known_certs: Vec<rustls::Certificate>,
    known_cert_sigs: String,
}

impl ManualServerVerification {
    fn new(known_certs: Vec<rustls::Certificate>) -> Arc<Self> {
        let known_cert_sigs: Vec<String> = known_certs.iter()
            .map(|cert| signature(cert))
            .collect();
        Arc::new(ManualServerVerification {
            known_certs,
            known_cert_sigs: format!("{:?}", known_cert_sigs),
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
        if self.known_certs.contains(server_cert) {
            info!("[client] server cert found in known certificates: {}", signature(&server_cert));
            Ok(rustls::client::ServerCertVerified::assertion())
        } else {
            // TODO nice prompt or something, which writes cert to known_certs/ upon approval
            if let Err(e) = write_known_cert(server_cert) {
                warn!("Couldn't store known cert: {}", e);
            }
            Err(rustls::Error::General(format!("[client] server cert to confirm: {} (expected one of: {})", signature(&server_cert), self.known_cert_sigs)))
        }
    }
}

struct ManualClientVerification {
    known_certs: Vec<rustls::Certificate>,
    known_cert_sigs: String,
}

impl ManualClientVerification {
    fn new(known_certs: Vec<rustls::Certificate>) -> Arc<Self> {
        let known_cert_sigs: Vec<String> = known_certs.iter()
            .map(|cert| signature(cert))
            .collect();
        Arc::new(ManualClientVerification {
            known_certs,
            known_cert_sigs: format!("{:?}", known_cert_sigs),
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
        if self.known_certs.contains(client_cert) {
            info!("[server] client cert found in known certificates: {}", signature(&client_cert));
            Ok(rustls::server::ClientCertVerified::assertion())
        } else {
            // TODO nice prompt or something, which writes cert to known_certs/ upon approval
            if let Err(e) = write_known_cert(client_cert) {
                warn!("Couldn't store known cert: {}", e);
            }
            Err(rustls::Error::General(format!("[server] client cert to confirm: {} (expected one of: {})", signature(&client_cert), self.known_cert_sigs)))
        }
    }
}

fn signature(cert: &rustls::Certificate) -> String {
    format!("{:x}", Sha256::digest(&cert))
}

pub fn load_known_certs() -> Result<Vec<rustls::Certificate>> {
    let mut certs = vec![];
    for path in fs::read_dir(init_known_certs_dir()?)? {
        let path = path?;
        let filetype = path.file_type()?;
        if !filetype.is_file() {
            continue;
        }
        certs.push(load_cert(&path.path())?);
    }
    Ok(certs)
}

fn write_known_cert(cert: &rustls::Certificate) -> Result<()> {
    let file_path = init_known_certs_dir().context("Failed to init known_certs dir")?.join(signature(cert));
    let content = pem::encode_config(
        &pem::Pem::new("CERTIFICATE", cert.0.clone()),
        pem::EncodeConfig { line_ending: pem::LineEnding::LF }
    );
    let mut outfile = fs::OpenOptions::new().write(true).create(true).truncate(true).open(&file_path).with_context(|| format!("Failed to open file {:?}", file_path))?;
    ensure_permissions(&file_path, 0o644).with_context(|| format!("Failed to set permissions on {:?}", file_path))?;
    outfile.write_all(content.as_bytes()).with_context(|| format!("Failed to write cert to {:?}", file_path))
}

fn load_keypair() -> Result<(rustls::Certificate, rustls::PrivateKey)> {
    let file_path = init_config_dir()?.join("keypair.pem");
    if file_path.is_file() {
        let mut reader = BufReader::new(fs::File::open(&file_path)?);
        let mut cert: Option<rustls::Certificate> = None;
        let mut key: Option<rustls::PrivateKey> = None;
        for item in rustls_pemfile::read_all(&mut reader)? {
            match item {
                rustls_pemfile::Item::X509Certificate(filecert) => {
                    cert = Some(rustls::Certificate(filecert));
                },
                rustls_pemfile::Item::PKCS8Key(filekey) => {
                    key = Some(rustls::PrivateKey(filekey));
                },
                _ => {
                    warn!("Unexpected item in {:?}", file_path);
                },
            }
        }
        if let (Some(cert), Some(key)) = (cert, key) {
            return Ok((cert, key));
        } else {
            bail!("Incomplete cert/key content in {:?}", file_path);
        }
    } else {
        let cert = rcgen::generate_simple_self_signed(vec![]).context("Failed to generate self-signed cert")?;
        let mut outfile = fs::OpenOptions::new().append(true).create(true).truncate(true).open(&file_path)?;
        ensure_permissions(&file_path, 0o600).with_context(|| format!("Failed to set permissions on {:?}", file_path))?;
        outfile.write_all(cert.serialize_pem()?.as_bytes()).with_context(|| format!("Failed to write cert to {:?}", file_path))?;
        outfile.write_all(cert.serialize_private_key_pem().as_bytes()).with_context(|| format!("Failed to write key to {:?}", file_path))?;
        Ok((rustls::Certificate(cert.serialize_der()?), rustls::PrivateKey(cert.serialize_private_key_der())))
    }
}

fn load_cert(file_path: &PathBuf) -> Result<rustls::Certificate> {
    let mut reader = BufReader::new(fs::File::open(&file_path)?);
    if let Some(rustls_pemfile::Item::X509Certificate(filecert)) = rustls_pemfile::read_one(&mut reader)? {
        return Ok(rustls::Certificate(filecert));
    } else {
        bail!("Public certificate not found in {:?}", file_path);
    }
}

fn init_known_certs_dir() -> Result<PathBuf> {
    let dir_path = init_config_dir()?.join("known_certs");
    fs::create_dir_all(&dir_path).with_context(|| format!("Failed to ensure certs dir exists: {:?}", dir_path))?;
    ensure_permissions(&dir_path, 0o755).with_context(|| format!("Failed to set permissions on {:?}", dir_path))?;
    Ok(dir_path)
}

fn ensure_permissions(path: &PathBuf, perms: u32) -> Result<()> {
    let mut permissions = fs::metadata(path)?.permissions();
    if permissions.mode() != perms {
        permissions.set_mode(perms);
    }
    Ok(())
}

fn init_config_dir() -> Result<PathBuf> {
    let mut homedir = home::home_dir().context("No home dir found: Unable to store certs")?;
    homedir.push(".config");
    homedir.push("nikau");
    fs::create_dir_all(&homedir)?;
    Ok(homedir)
}
