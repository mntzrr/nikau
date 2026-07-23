use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use tokio::task;
use tracing::{debug, warn};

use crate::clipboard::limited;

/// Clipboard types for copying one or more files in a file manager.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Big enough that no test payload gets anywhere near the caps.
    const GENEROUS_CAP: u64 = 100_000_000;

    fn temp_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, contents).unwrap();
        path
    }

    fn file_uri(path: &Path) -> String {
        url::Url::from_file_path(path).unwrap().to_string()
    }

    /// Parses a write() gnome-paths result ("copy\nuri\nuri...") back into paths.
    fn gnome_output_paths(buf: &[u8]) -> Vec<PathBuf> {
        let text = String::from_utf8(buf.to_vec()).unwrap();
        let mut lines = text.split('\n');
        assert_eq!(lines.next(), Some("copy"));
        lines
            .map(|l| url::Url::parse(l).unwrap().to_file_path().unwrap())
            .collect()
    }

    /// Parses a write() uri-list result ("uri\r\nuri\r\n") back into paths.
    fn uri_list_output_paths(buf: &[u8]) -> Vec<PathBuf> {
        let text = String::from_utf8(buf.to_vec()).unwrap();
        // Every uri-list line is CRLF-terminated, including the last.
        assert!(text.ends_with("\r\n"));
        text.split("\r\n")
            .filter(|l| !l.is_empty())
            .map(|l| url::Url::parse(l).unwrap().to_file_path().unwrap())
            .collect()
    }

    /// Builds a zip payload in-memory with the given entry names and contents,
    /// bypassing the path canonicalization zip_files does (so traversal entries
    /// can be tested).
    fn build_test_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut cursor = std::io::Cursor::new(vec![]);
        {
            let mut zipwriter = zip::ZipWriter::new(&mut cursor);
            let options = zip::write::FileOptions::<()>::default()
                .compression_method(zip::CompressionMethod::Stored);
            for (name, contents) in entries {
                zipwriter.start_file(*name, options).unwrap();
                zipwriter.write_all(contents).unwrap();
            }
            zipwriter.finish().unwrap();
        }
        cursor.into_inner()
    }

    #[tokio::test]
    async fn zstd_roundtrip_text_payload() {
        let text = "monux clipboard text — unicode ✓ ünïcödé\n".repeat(100);
        let original = text.into_bytes();
        assert!(original.len() >= 100);

        let (payload, data_type) = read(original.clone(), GENEROUS_CAP, "text/plain")
            .await
            .unwrap();
        // The wrapper datatype marks the payload as zstd-compressed, and the
        // repetitive text actually got smaller.
        assert_eq!(data_type.as_deref(), Some(MONUX_ZSTD_TARGET_DATATYPE));
        assert!(payload.len() < original.len());

        let restored = write(
            payload,
            GENEROUS_CAP,
            "text/plain",
            &data_type.unwrap(),
            &PathBuf::from("/nonexistent"),
        )
        .await
        .unwrap();
        assert_eq!(restored, original);
    }

    #[tokio::test]
    async fn zstd_roundtrip_binary_payload() {
        let original: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
        let (payload, data_type) = read(original.clone(), GENEROUS_CAP, "application/octet-stream")
            .await
            .unwrap();
        assert_eq!(data_type.as_deref(), Some(MONUX_ZSTD_TARGET_DATATYPE));

        let restored = write(
            payload,
            GENEROUS_CAP,
            "application/octet-stream",
            &data_type.unwrap(),
            &PathBuf::from("/nonexistent"),
        )
        .await
        .unwrap();
        assert_eq!(restored, original);
    }

    #[tokio::test]
    async fn uncompressible_type_passes_through_uncompressed() {
        // Compressing an already-compressed format is a waste of time.
        let png: Vec<u8> = (0..=255u8).cycle().take(500).collect();
        let (payload, data_type) = read(png.clone(), GENEROUS_CAP, "image/png").await.unwrap();
        assert_eq!(data_type, None);
        assert_eq!(payload, png);
    }

    #[tokio::test]
    async fn small_payload_passes_through_uncompressed() {
        // Under the 100-byte floor, compression doesn't pay for itself.
        let tiny = b"a small clipboard".to_vec();
        let (payload, data_type) = read(tiny.clone(), GENEROUS_CAP, "text/plain").await.unwrap();
        assert_eq!(data_type, None);
        assert_eq!(payload, tiny);
    }

    #[tokio::test]
    async fn compressed_payload_over_cap_errors() {
        // The LimitedCursor must error rather than write past the cap.
        let payload = "some text that will be compressed".repeat(20).into_bytes();
        assert!(read(payload, 4, "text/plain").await.is_err());
    }

    #[tokio::test]
    async fn decompressed_payload_over_cap_errors() {
        let original = "some text that will be compressed".repeat(20).into_bytes();
        let (compressed, data_type) = read(original.clone(), GENEROUS_CAP, "text/plain")
            .await
            .unwrap();
        // Decompressing with a cap smaller than the original errors instead
        // of producing a truncated clipboard.
        assert!(
            write(
                compressed,
                (original.len() - 1) as u64,
                "text/plain",
                &data_type.unwrap(),
                &PathBuf::from("/nonexistent"),
            )
            .await
            .is_err()
        );
    }

    /// A full file-clipboard round trip through one of the paths formats:
    /// source files -> zip payload -> unpack to a fresh dir -> paths output.
    /// Covers spaces and percent signs in filenames (URL encoding), directory
    /// recursion, and byte-exact content preservation.
    async fn file_paths_roundtrip(paths_target: &str) {
        let src = tempfile::tempdir().unwrap();
        let hello = temp_file(src.path(), "hello.txt", b"hello world");
        let spaced = temp_file(
            src.path(),
            "dir with space/100% certain.txt",
            b"bytes \x00\x01\x02 here",
        );
        let nested = temp_file(src.path(), "subdir/nested/deep.txt", b"deep content");

        let input = match paths_target {
            PATHS_TARGET_GNOME => format!(
                "copy\n{}\n{}\n{}",
                file_uri(&hello),
                file_uri(&spaced),
                file_uri(src.path().join("subdir").as_path())
            )
            .into_bytes(),
            PATHS_TARGET_URIS => format!(
                "{}\r\n{}\r\n{}\r\n",
                file_uri(&hello),
                file_uri(&spaced),
                file_uri(src.path().join("subdir").as_path())
            )
            .into_bytes(),
            other => panic!("unexpected paths target: {}", other),
        };

        let (zip_payload, data_type) = read(input, GENEROUS_CAP, paths_target).await.unwrap();
        assert_eq!(
            data_type.as_deref(),
            Some(MONUX_COPIED_FILES_DATATYPE)
        );

        let unpack_root = tempfile::tempdir().unwrap();
        let output = write(
            zip_payload,
            GENEROUS_CAP,
            paths_target,
            &data_type.unwrap(),
            &unpack_root.path().to_path_buf(),
        )
        .await
        .unwrap();

        let paths = match paths_target {
            PATHS_TARGET_GNOME => gnome_output_paths(&output),
            PATHS_TARGET_URIS => uri_list_output_paths(&output),
            other => panic!("unexpected paths target: {}", other),
        };

        // Three files (the directory was scanned recursively), all unpacked
        // under the target dir, with original filenames and exact contents.
        assert_eq!(paths.len(), 3);
        for path in &paths {
            assert!(path.starts_with(unpack_root.path()));
        }
        let by_name: std::collections::HashMap<_, _> = paths
            .iter()
            .map(|p| (p.file_name().unwrap().to_str().unwrap().to_string(), p))
            .collect();
        assert_eq!(
            std::fs::read(by_name["hello.txt"]).unwrap(),
            b"hello world"
        );
        // The '%' and spaces survived the URL decode on both ends.
        assert_eq!(
            std::fs::read(by_name["100% certain.txt"]).unwrap(),
            b"bytes \x00\x01\x02 here"
        );
        // The recursively scanned file matches its source byte-for-byte.
        assert_eq!(
            std::fs::read(by_name["deep.txt"]).unwrap(),
            std::fs::read(&nested).unwrap()
        );
    }

    #[tokio::test]
    async fn gnome_copied_files_roundtrip() {
        file_paths_roundtrip(PATHS_TARGET_GNOME).await;
    }

    #[tokio::test]
    async fn uri_list_roundtrip() {
        file_paths_roundtrip(PATHS_TARGET_URIS).await;
    }

    #[test]
    fn gnome_parse_skips_cut_copy_line() {
        // "cut" is handled the same as "copy" on the read side.
        let src = tempfile::tempdir().unwrap();
        let file = temp_file(src.path(), "a.txt", b"a");
        let zip = read_gnome_file_paths(
            format!("cut\n{}", file_uri(&file)).into_bytes(),
            GENEROUS_CAP,
        )
        .unwrap();
        let unpack_root = tempfile::tempdir().unwrap();
        let paths = unpack_zip_payload(zip, GENEROUS_CAP, &unpack_root.path().to_path_buf())
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(std::fs::read(&paths[0]).unwrap(), b"a");
    }

    #[test]
    fn unpack_sanitizes_traversal_and_absolute_entries() {
        let unpack_root = tempfile::tempdir().unwrap();
        let zip = build_test_zip(&[
            ("../escape.txt", b"evil"),
            ("/absolute/path.txt", b"absolute"),
            ("ok.txt", b"fine"),
        ]);
        let paths = unpack_zip_payload(zip, GENEROUS_CAP, &unpack_root.path().to_path_buf())
            .unwrap();

        // Every entry was unpacked INSIDE the temp dir: the Normal-components
        // guard drops ParentDir/RootDir components, so traversal attempts land
        // harmlessly within the unpack dir instead of escaping it.
        assert_eq!(paths.len(), 3);
        for path in &paths {
            assert!(path.starts_with(unpack_root.path()));
        }
        assert!(
            !unpack_root
                .path()
                .parent()
                .unwrap()
                .join("escape.txt")
                .exists()
        );
        let by_name: std::collections::HashMap<_, _> = paths
            .iter()
            .map(|p| (p.file_name().unwrap().to_str().unwrap().to_string(), p))
            .collect();
        assert_eq!(std::fs::read(by_name["escape.txt"]).unwrap(), b"evil");
        assert_eq!(std::fs::read(by_name["path.txt"]).unwrap(), b"absolute");
        assert_eq!(std::fs::read(by_name["ok.txt"]).unwrap(), b"fine");
    }

    #[test]
    fn unpack_rejects_entry_with_no_normal_components() {
        let unpack_root = tempfile::tempdir().unwrap();
        // An entry whose path is pure traversal has no Normal components at
        // all: it would unpack onto the unpack dir itself, so it is rejected.
        let zip = build_test_zip(&[("..", b"evil"), ("ok.txt", b"fine")]);
        assert!(
            unpack_zip_payload(zip, GENEROUS_CAP, &unpack_root.path().to_path_buf()).is_err()
        );
    }

    #[test]
    fn unpack_enforces_size_cap() {
        // One file over the cap: the LimitedWrite errors mid-extraction
        // instead of writing the full payload.
        let unpack_root = tempfile::tempdir().unwrap();
        let big = vec![b'x'; 1000];
        let zip = build_test_zip(&[("big.txt", &big)]);
        assert!(unpack_zip_payload(zip, 100, &unpack_root.path().to_path_buf()).is_err());

        // The cap is cumulative across entries.
        let unpack_root = tempfile::tempdir().unwrap();
        let zip = build_test_zip(&[("a.txt", &big[..100]), ("b.txt", &big[..100])]);
        assert!(unpack_zip_payload(zip, 150, &unpack_root.path().to_path_buf()).is_err());
    }

    #[tokio::test]
    async fn zip_build_over_cap_errors() {
        // The LimitedCursor caps the COMPRESSED zip size during the build.
        let src = tempfile::tempdir().unwrap();
        let file = temp_file(src.path(), "a.txt", b"some content to zip up");
        assert!(
            read(
                format!("copy\n{}", file_uri(&file)).into_bytes(),
                8,
                PATHS_TARGET_GNOME,
            )
            .await
            .is_err()
        );
    }
}
