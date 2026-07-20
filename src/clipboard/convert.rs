use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use tokio::task;
use tracing::{debug, warn};

use crate::clipboard::limited;

/// X11 clipboard types for copying one or more files in a file manager.
/// In this case the payload is a list of paths, which doesn't work over the network.
const PATHS_TARGET_GNOME: &str = "x-special/gnome-copied-files";
const PATHS_TARGET_URIS: &str = "text/uri-list";

/// Clipboard types that should not be compressed by zstd (since it's a waste of time).
/// This is not meant to be an exhaustive list of compressed types, just ones often seen in clipboards.
const UNCOMPRESSIBLE_TYPES: &[&str] = &["image/png"];

/// data_type value for one or more files that are referenced by path.
/// Special handling to support cases where the clipboard is a set of local file paths:
/// The reader combines the file(s) as a single .zip payload to preserve their filenames.
/// The writer extracts the file(s) into a temp directory and advertises the paths in that directory.
const MONUX_COPIED_FILES_DATATYPE: &str = "application/zip+clipboard-paths";

/// data_type value for data that has been compressed using zstandard to improve clipboard transfer performance.
/// In practice this should be used for all payloads that aren't ZIPPED_FILES.
const MONUX_ZSTD_TARGET_DATATYPE: &str = "application/zstd";

/// Converts clipboard data received from a host application
/// to a payload and/or datatype suitable for sending to a Monux peer.
/// If the datatype String is None, then the data is being sent as-is.
pub async fn read(
    buf: Vec<u8>,
    max_compressed_size_bytes: u64,
    requested_type: &str,
) -> Result<(Vec<u8>, Option<String>)> {
    if requested_type == PATHS_TARGET_GNOME {
        let converted =
            task::spawn_blocking(move || read_gnome_file_paths(buf, max_compressed_size_bytes))
                .await??;
        Ok((
            converted,
            Some(MONUX_COPIED_FILES_DATATYPE.to_string()),
        ))
    } else if requested_type == PATHS_TARGET_URIS {
        let converted =
            task::spawn_blocking(move || read_uri_file_paths(buf, max_compressed_size_bytes))
                .await??;
        Ok((
            converted,
            Some(MONUX_COPIED_FILES_DATATYPE.to_string()),
        ))
    } else if buf.len() >= 100 && !UNCOMPRESSIBLE_TYPES.contains(&requested_type) {
        let requested_type = requested_type.to_string();
        let converted = task::spawn_blocking(move || {
            read_zstd(buf, max_compressed_size_bytes, &requested_type)
        })
        .await??;
        Ok((
            converted,
            Some(MONUX_ZSTD_TARGET_DATATYPE.to_string()),
        ))
    } else {
        // Don't bother compressing small or incompressible data
        Ok((buf, None))
    }
}

/// Converts clipboard data received from another Monux peer over the network
/// to a payload suitable for sending to a host application.
pub async fn write(
    buf: Vec<u8>,
    max_uncompressed_size_bytes: u64,
    requested_type: &str,
    data_type: &str,
    config_dir: &PathBuf,
) -> Result<Vec<u8>> {
    debug!("Converting clipboard data from data_type={} to requested_type={}", data_type, requested_type);
    match (requested_type, data_type) {
        (requested_type, MONUX_ZSTD_TARGET_DATATYPE) => {
            let requested_type = requested_type.to_string();
            task::spawn_blocking(move || {
                write_zstd(buf, max_uncompressed_size_bytes, &requested_type)
            })
            .await?
        }
        (PATHS_TARGET_GNOME, MONUX_COPIED_FILES_DATATYPE) => {
            let config_dir = config_dir.clone();
            let paths = task::spawn_blocking(move || {
                unpack_zip_payload(buf, max_uncompressed_size_bytes, &config_dir)
            })
            .await??;
            write_gnome_file_paths(paths)
        }
        (PATHS_TARGET_URIS, MONUX_COPIED_FILES_DATATYPE) => {
            let config_dir = config_dir.clone();
            let paths = task::spawn_blocking(move || {
                unpack_zip_payload(buf, max_uncompressed_size_bytes, &config_dir)
            })
            .await??;
            write_uri_file_paths(paths)
        }
        (requested_type, data_type) => {
            warn!("Clipboard data conversion from data_type={} to requested_type={} isn't supported, writing empty clipboard", data_type, requested_type);
            Ok(vec![])
        }
    }
}

/// Expected format depending on the operation:
///   copy\nfile:///path/to/file1\nfile:///path/to/file2
///   cut\n...
fn read_gnome_file_paths(buf: Vec<u8>, max_compressed_size_bytes: u64) -> Result<Vec<u8>> {
    let buf = String::from_utf8(buf)?;
    let mut lines: Vec<&str> = buf.split("\n").collect();
    if !lines.is_empty() {
        // Remove the "cut" or "copy"
        lines.remove(0);
    }
    build_zip_payload(lines, max_compressed_size_bytes)
}

/// Inverse of read_gnome_file_paths
fn write_gnome_file_paths(paths: Vec<PathBuf>) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = vec![];
    buf.extend_from_slice(b"copy");
    for path in paths {
        let uri = url::Url::from_file_path(&path)
            .map_err(|_e| anyhow!("Failed to format path '{:?}' as uri", path))?;
        buf.extend_from_slice(format!("\n{}", uri).as_bytes());
    }
    Ok(buf)
}

/// Expected format:
///   file:///path/to/file1\r\nfile:///path/to/file2\r\n
fn read_uri_file_paths(buf: Vec<u8>, max_compressed_size_bytes: u64) -> Result<Vec<u8>> {
    let buf = String::from_utf8(buf)?;
    let mut lines: Vec<&str> = buf.split("\r\n").collect();
    if let Some(last) = lines.last() {
        if last.is_empty() {
            // Remove final empty entry from trailing newline
            lines.pop();
        }
    }
    build_zip_payload(lines, max_compressed_size_bytes)
}

/// Inverse of read_uri_file_paths
fn write_uri_file_paths(paths: Vec<PathBuf>) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = vec![];
    for path in paths {
        let uri = url::Url::from_file_path(&path)
            .map_err(|_e| anyhow!("Failed to format path '{:?}' as uri", path))?;
        buf.extend_from_slice(format!("{}\r\n", uri).as_bytes());
    }
    Ok(buf)
}

/// Compresses the provided payload using zstd
fn read_zstd(
    mut buf: Vec<u8>,
    max_compressed_size_bytes: u64,
    requested_type: &str,
) -> Result<Vec<u8>> {
    let orig_len = buf.len();
    let mut limited = limited::LimitedCursor::new(max_compressed_size_bytes);
    zstd::stream::copy_encode(buf.as_slice(), &mut limited, 0)?;
    buf = limited.into_inner();
    debug!(
        "Compressed {}: {} => {} bytes",
        requested_type,
        orig_len,
        buf.len()
    );
    Ok(buf)
}

/// Decompresses the provided payload using zstd
fn write_zstd(
    mut buf: Vec<u8>,
    max_uncompressed_size_bytes: u64,
    requested_type: &str,
) -> Result<Vec<u8>> {
    let compressed_len = buf.len();
    let mut limited = limited::LimitedCursor::new(max_uncompressed_size_bytes);
    zstd::stream::copy_decode(buf.as_slice(), &mut limited)?;
    buf = limited.into_inner();
    debug!(
        "Decompressed {}: {} => {} bytes",
        requested_type,
        compressed_len,
        buf.len()
    );
    Ok(buf)
}

/// Cap on the number of file entries in a clipboard zip payload.
const MAX_ZIP_ENTRIES: usize = 10_000;

/// Counter for giving each unpack its own unique temp directory.
static UNPACK_DIR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Unzips a zip file to a temporary directory under config_dir and returns the list of files.
fn unpack_zip_payload(
    zipdata: Vec<u8>,
    mut max_uncompressed_size_bytes: u64,
    config_dir: &PathBuf,
) -> Result<Vec<PathBuf>> {
    // Use a unique temp directory per unpack rather than wiping a shared one:
    // two unpacks may run at the same time.
    let dir_id = UNPACK_DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let clipboard_dir = config_dir.join(format!(
        "clipboard-{}-{}",
        std::process::id(),
        dir_id
    ));
    debug!("Creating temp directory: {}", clipboard_dir.display());
    std::fs::create_dir_all(&clipboard_dir)?;

    // Remove older temp dirs from this process, keeping the current and previous
    // generation (a paste may still be referencing files from the previous one).
    let dir_prefix = format!("clipboard-{}-", std::process::id());
    if let Ok(entries) = std::fs::read_dir(config_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(id_str) = name.strip_prefix(&dir_prefix) else { continue };
            let Ok(id) = id_str.parse::<usize>() else { continue };
            if id + 1 < dir_id {
                debug!("Removing stale temp directory: {}", entry.path().display());
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    // Unzip payload into temp directory
    let mut ziparchive = zip::read::ZipArchive::new(std::io::Cursor::new(zipdata))?;
    if ziparchive.len() > MAX_ZIP_ENTRIES {
        bail!(
            "Zip payload has {} entries, exceeding limit of {}",
            ziparchive.len(),
            MAX_ZIP_ENTRIES
        );
    }
    let mut files = vec![];
    for i in 0..ziparchive.len() {
        let mut zipfile = ziparchive.by_index(i)?;
        let mut destpath = clipboard_dir.clone();
        for component in Path::new(zipfile.name()).components() {
            if let std::path::Component::Normal(n) = component {
                destpath = destpath.join(n);
            }
        }
        debug!("Unpacking {} to {}", zipfile.name(), destpath.display());
        if destpath == clipboard_dir {
            bail!("Invalid path for file: {}", zipfile.name());
        }
        if let Some(parent) = destpath.parent() {
            std::fs::create_dir_all(&parent).with_context(|| {
                format!("Failed to create temp directory: {}", parent.display())
            })?;
        }
        let outfile = File::create(&destpath)
            .with_context(|| format!("Failed to create temp file: {}", destpath.display()))?;
        let mut limited_outfile = limited::LimitedWrite::new(outfile, max_uncompressed_size_bytes);
        std::io::copy(&mut zipfile, &mut limited_outfile)
            .with_context(|| format!("Failed to unzip file: {}", destpath.display()))?;
        // Update remaining total max to reflect the bytes written by this file
        max_uncompressed_size_bytes = limited_outfile.remaining();
        files.push(destpath);
    }
    Ok(files)
}

fn build_zip_payload(file_uri_strs: Vec<&str>, max_compressed_size_bytes: u64) -> Result<Vec<u8>> {
    // Start by collecting all of the filenames, including any needed recursive scanning.
    let mut files_to_zip = vec![];
    for uri_str in file_uri_strs {
        if files_to_zip.len() >= MAX_ZIP_ENTRIES {
            bail!("Too many files in clipboard: exceeding limit of {}", MAX_ZIP_ENTRIES);
        }
        let uri = url::Url::parse(&uri_str)?;
        let path = uri
            .to_file_path()
            .map_err(|_e| anyhow!("Invalid file entry: {}", uri))?;
        if path.is_dir() {
            // Recursively scan the directory, omitting the directory path itself
            for entry in walkdir::WalkDir::new(path).min_depth(1).into_iter() {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        // Skip entries that vanished or can't be read mid-walk
                        warn!("Skipping unreadable directory entry: {}", e);
                        continue;
                    }
                };
                if entry.path().is_file() {
                    files_to_zip.push(entry.into_path());
                    if files_to_zip.len() >= MAX_ZIP_ENTRIES {
                        bail!("Too many files in clipboard: exceeding limit of {}", MAX_ZIP_ENTRIES);
                    }
                }
            }
        } else if path.is_file() {
            files_to_zip.push(path.to_path_buf());
        } else {
            warn!("Skipping path that isn't a file or directory: {:?}", path);
        }
    }
    // Then write the files to the zip file, aborting internally if the compressed size gets too big
    let (uncompressed_len, zipdata) = zip_files(&files_to_zip, max_compressed_size_bytes)?;
    debug!(
        "Zipped {} files ({} bytes) into {} bytes",
        files_to_zip.len(),
        uncompressed_len,
        zipdata.len()
    );
    Ok(zipdata)
}

fn zip_files(
    files_to_zip: &Vec<PathBuf>,
    max_compressed_size_bytes: u64,
) -> Result<(usize, Vec<u8>)> {
    let mut uncompressed_len = 0;
    let mut cursor = limited::LimitedCursor::new(max_compressed_size_bytes);
    {
        let mut zipwriter = zip::ZipWriter::new(&mut cursor);
        let options =
            zip::write::FileOptions::<()>::default().compression_method(zip::CompressionMethod::ZSTD);
        let mut buf = vec![0; 65536];
        for file_to_zip in files_to_zip {
            let file_name = match file_to_zip.canonicalize() {
                Ok(path) => path.to_string_lossy().to_string(),
                Err(e) => {
                    // File vanished between listing and zipping: skip it instead of aborting
                    warn!("Skipping file that can't be read for zipping: {:?}: {}", file_to_zip, e);
                    continue;
                }
            };
            let mut file = match std::fs::File::open(file_to_zip) {
                Ok(file) => file,
                Err(e) => {
                    // File vanished between listing and zipping: skip it instead of aborting
                    warn!("Skipping file that can't be read for zipping: {:?}: {}", file_to_zip, e);
                    continue;
                }
            };
            zipwriter.start_file(file_name, options)?;
            loop {
                match file.read(&mut buf)? {
                    0 => {
                        // EOF
                        break;
                    }
                    len => {
                        uncompressed_len += len;
                        zipwriter.write_all(&buf[..len])?;
                    }
                }
            }
        }
        zipwriter.finish()?;
    }
    Ok((uncompressed_len, cursor.into_inner()))
}
