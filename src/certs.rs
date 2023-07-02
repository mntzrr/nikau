use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use std::fs;
use std::io::{self, prelude::*};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

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

pub fn load_keypair() -> Result<(rustls::Certificate, rustls::PrivateKey)> {
    let file_path = init_config_dir()?.join("private.pem");
    if file_path.is_file() {
        let mut reader = io::BufReader::new(fs::File::open(&file_path).with_context(|| format!("Failed to open keypair file: {}", file_path.display()))?);
        let mut cert: Option<rustls::Certificate> = None;
        let mut key: Option<rustls::PrivateKey> = None;
        for item in rustls_pemfile::read_all(&mut reader).with_context(|| format!("Failed to read keypair file: {}", file_path.display()))? {
            match item {
                rustls_pemfile::Item::X509Certificate(filecert) => {
                    cert = Some(rustls::Certificate(filecert));
                },
                rustls_pemfile::Item::PKCS8Key(filekey) => {
                    key = Some(rustls::PrivateKey(filekey));
                },
                _ => {
                    warn!("Unexpected item in {}", file_path.display());
                },
            }
        }
        if let (Some(cert), Some(key)) = (cert, key) {
            return Ok((cert, key));
        } else {
            bail!("Incomplete cert/key content in {}", file_path.display());
        }
    } else {
        let cert = rcgen::generate_simple_self_signed(vec![]).context("Failed to generate self-signed cert")?;
        let mut outfile = fs::File::create(&file_path).with_context(|| format!("Failed to open keypair file for writing: {}", file_path.display()))?;
        ensure_permissions(&file_path, 0o600).with_context(|| format!("Failed to set permissions on keypair file: {}", file_path.display()))?;
        outfile.write_all(cert.serialize_pem()?.as_bytes()).with_context(|| format!("Failed to write cert to keypair file: {}", file_path.display()))?;
        outfile.write_all(cert.serialize_private_key_pem().as_bytes()).with_context(|| format!("Failed to write key to keypair file: {}", file_path.display()))?;
        Ok((rustls::Certificate(cert.serialize_der()?), rustls::PrivateKey(cert.serialize_private_key_der())))
    }
}

/// Returns the sha256 fingerprint of this certificate.
/// We use this for cert filenames and for comparing certs in confirmation prompts.
/// This should match the output of "openssl x509 -in <filename> -noout -sha256 -fingerprint"
pub fn fingerprint(cert: &rustls::Certificate) -> String {
    format!("{:x}", Sha256::digest(&cert))
}

pub fn write_approved_cert(cert: &rustls::Certificate) -> Result<()> {
    let file_path = init_known_certs_dir()
        .context("Failed to init known_certs dir")?
        .join(format!("{}.pem", fingerprint(cert)));
    let content = pem::encode_config(
        &pem::Pem::new("CERTIFICATE", cert.0.clone()),
        pem::EncodeConfig { line_ending: pem::LineEnding::LF }
    );
    let mut outfile = fs::File::create(&file_path).with_context(|| format!("Failed to open known cert file for writing: {}", file_path.display()))?;
    ensure_permissions(&file_path, 0o644).with_context(|| format!("Failed to set permissions on known cert file: {}", file_path.display()))?;
    outfile.write_all(content.as_bytes()).with_context(|| format!("Failed to write known cert to file: {}", file_path.display()))?;
    info!("Wrote approved cert to {}", file_path.display());
    Ok(())
}

fn load_cert(file_path: &PathBuf) -> Result<rustls::Certificate> {
    let mut reader = io::BufReader::new(fs::File::open(&file_path).with_context(|| format!("Failed to open cert file: {}", file_path.display()))?);
    if let Some(rustls_pemfile::Item::X509Certificate(filecert)) = rustls_pemfile::read_one(&mut reader).with_context(|| format!("Failed to read cert file: {}", file_path.display()))? {
        return Ok(rustls::Certificate(filecert));
    } else {
        bail!("Public certificate not found in {}", file_path.display());
    }
}

fn init_known_certs_dir() -> Result<PathBuf> {
    let dir_path = init_config_dir()?.join("known_certs");
    fs::create_dir_all(&dir_path).with_context(|| format!("Failed to ensure certs dir exists: {}", dir_path.display()))?;
    ensure_permissions(&dir_path, 0o755).with_context(|| format!("Failed to set permissions on certs dir: {}", dir_path.display()))?;
    Ok(dir_path)
}

fn ensure_permissions(path: &PathBuf, perms: u32) -> Result<()> {
    let mut permissions = fs::metadata(path).with_context(|| format!("Failed to read file metadata: {}", path.display()))?.permissions();
    if permissions.mode() != perms {
        permissions.set_mode(perms);
    }
    Ok(())
}

fn init_config_dir() -> Result<PathBuf> {
    let mut homedir = home::home_dir().context("No home dir found: Unable to store certs")?;
    homedir.push(".config");
    homedir.push("nikau");
    fs::create_dir_all(&homedir).with_context(|| format!("Failed to create config directory: {}", homedir.display()))?;
    Ok(homedir)
}
