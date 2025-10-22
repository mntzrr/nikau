use std::fs;
use std::io::{self, prelude::*};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

pub fn load_known_certs(config_dir: &PathBuf) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>> {
    let mut certs = vec![];
    for path in fs::read_dir(init_known_certs_dir(config_dir)?)? {
        let path = path?;
        let filetype = path.file_type()?;
        if !filetype.is_file() {
            continue;
        }
        certs.push(load_cert(path.path())?);
    }
    Ok(certs)
}

fn splash(label: &str, fingerprint: &str) {
    println!(
        r"
 \\ //
  \V/
   U
   | nikau {}
   | {}
",
        label, fingerprint
    );
}

pub fn load_keypair<'a>(
    splash_label: &str,
    config_dir: &PathBuf,
) -> Result<(rustls_pki_types::CertificateDer<'a>, rustls_pki_types::PrivateKeyDer<'a>)> {
    let file_path = config_dir.join("private.pem");
    if file_path.is_file() {
        read_existing_keypair(splash_label, &file_path)
    } else {
        write_new_keypair(splash_label, &file_path)
    }
}

fn read_existing_keypair<'a>(
    splash_label: &str,
    file_path: &PathBuf,
) -> Result<(rustls_pki_types::CertificateDer<'a>, rustls_pki_types::PrivateKeyDer<'a>)> {
    let mut reader =
        io::BufReader::new(fs::File::open(&file_path).with_context(|| {
            format!("Failed to open keypair file: {}", file_path.display())
        })?);
    let mut cert: Option<rustls_pki_types::CertificateDer> = None;
    let mut key: Option<rustls_pki_types::PrivateKeyDer> = None;
    for item in rustls_pemfile::read_all(&mut reader) {
        match item.with_context(|| format!("Failed to read keypair file: {}", file_path.display()))? {
            rustls_pemfile::Item::X509Certificate(filecert) => {
                cert = Some(rustls_pki_types::CertificateDer::from(filecert));
            }
            rustls_pemfile::Item::Pkcs8Key(filekey) => {
                key = Some(rustls_pki_types::PrivateKeyDer::from(filekey));
            }
            _ => {
                // Avoid logging the content in case its a privkey
                warn!("Unexpected item in {}", file_path.display());
            }
        }
    }
    if let (Some(cert), Some(key)) = (cert, key) {
        splash(splash_label, &fingerprint(&cert));
        info!("Using keypair from {}", file_path.display());
        Ok((cert, key))
    } else {
        bail!("Incomplete cert/key content in {}", file_path.display());
    }
}

fn write_new_keypair<'a>(
    splash_label: &str,
    file_path: &PathBuf,
) -> Result<(rustls_pki_types::CertificateDer<'a>, rustls_pki_types::PrivateKeyDer<'a>)> {
    let pair = rcgen::generate_simple_self_signed(vec![])
        .context("Failed to generate self-signed cert")?;

    info!("Writing a new keypair to {}", file_path.display());
    let mut outfile = fs::File::create(&file_path).with_context(|| {
        format!(
            "Failed to open keypair file for writing: {}",
            file_path.display()
        )
    })?;
    ensure_permissions(&file_path, 0o600).with_context(|| {
        format!(
            "Failed to set permissions on keypair file: {}",
            file_path.display()
        )
    })?;
    outfile
        .write_all(pair.cert.pem().as_bytes())
        .with_context(|| format!("Failed to write public key to file: {}", file_path.display()))?;
    outfile
        .write_all(pair.signing_key.serialize_pem().as_bytes())
        .with_context(|| format!("Failed to write private key to file: {}", file_path.display()))?;

    let rustls_cert = rustls_pki_types::CertificateDer::from(pair.cert.der().to_vec());
    splash(splash_label, &fingerprint(&rustls_cert));
    Ok((
        rustls_cert,
        rustls_pki_types::PrivateKeyDer::from(rustls_pki_types::PrivatePkcs8KeyDer::from(pair.signing_key.serialize_der())),
    ))
}

/// Returns the sha256 fingerprint of this certificate.
/// We use this for cert filenames and for comparing certs in confirmation prompts.
/// This should match the output of "openssl x509 -in <filename> -noout -sha256 -fingerprint"
pub fn fingerprint(cert: &rustls_pki_types::CertificateDer) -> String {
    format!("{:x}", Sha256::digest(cert))
}

pub fn write_approved_cert(
    cert: &rustls_pki_types::CertificateDer,
    fingerprint: &str,
    config_dir: &PathBuf,
) -> Result<()> {
    let file_path = init_known_certs_dir(config_dir)
        .context("Failed to init known_certs dir")?
        .join(format!("{}.pem", fingerprint));
    let content = pem::encode_config(
        &pem::Pem::new("CERTIFICATE", cert.as_ref()),
        pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
    );
    let mut outfile = fs::File::create(&file_path).with_context(|| {
        format!(
            "Failed to open known cert file for writing: {}",
            file_path.display()
        )
    })?;
    ensure_permissions(&file_path, 0o644).with_context(|| {
        format!(
            "Failed to set permissions on known cert file: {}",
            file_path.display()
        )
    })?;
    outfile.write_all(content.as_bytes()).with_context(|| {
        format!(
            "Failed to write known cert to file: {}",
            file_path.display()
        )
    })?;
    info!("Wrote approved cert to {}", file_path.display());
    Ok(())
}

fn load_cert<'a>(file_path: PathBuf) -> Result<rustls_pki_types::CertificateDer<'a>> {
    let mut reader = io::BufReader::new(
        fs::File::open(&file_path)
            .with_context(|| format!("Failed to open cert file: {}", file_path.display()))?,
    );
    if let Some(rustls_pemfile::Item::X509Certificate(filecert)) =
        rustls_pemfile::read_one(&mut reader)
            .with_context(|| format!("Failed to read cert file: {}", file_path.display()))?
    {
        Ok(rustls_pki_types::CertificateDer::from(filecert))
    } else {
        bail!("Public certificate not found in {}", file_path.display());
    }
}

fn init_known_certs_dir(config_dir: &PathBuf) -> Result<PathBuf> {
    let dir_path = config_dir.join("known_certs");
    fs::create_dir_all(&dir_path)
        .with_context(|| format!("Failed to ensure certs dir exists: {}", dir_path.display()))?;
    ensure_permissions(&dir_path, 0o755).with_context(|| {
        format!(
            "Failed to set permissions on certs dir: {}",
            dir_path.display()
        )
    })?;
    Ok(dir_path)
}

fn ensure_permissions(path: &PathBuf, perms: u32) -> Result<()> {
    let mut permissions = fs::metadata(path)
        .with_context(|| format!("Failed to read file metadata: {}", path.display()))?
        .permissions();
    if permissions.mode() != perms {
        permissions.set_mode(perms);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn can_write_read_keys() {
        let dir = tempfile::tempdir().unwrap();
        // This should automatically write a new keypair
        let (cert1, privkey1) = load_keypair("foo", &dir.path().to_path_buf()).expect("couldn't load");
        // This should read the existing keypair
        let (cert2, privkey2) = load_keypair("foo", &dir.path().to_path_buf()).expect("couldn't load");
        // The results should match
        assert!(fingerprint(&cert1) == fingerprint(&cert2));
        assert!(cert1 == cert2);
        assert!(privkey1 == privkey2);
    }
}
