use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use tracing::{error, info, warn};

use crate::x11clipboard::{limited_cursor, shared};

pub fn read(buf: Vec<u8>, max_size_bytes: u64, requested_type: &str) -> Result<(Vec<u8>, Option<String>)> {
    if requested_type == shared::PATHS_TARGET_GNOME {
        Ok((
            read_gnome_file_paths(buf, max_size_bytes)?,
            Some(shared::NIKAU_COPIED_FILES_DATATYPE.to_string())
        ))
    } else if requested_type == shared::PATHS_TARGET_URIS {
        Ok((
            read_uri_file_paths(buf, max_size_bytes)?,
            Some(shared::NIKAU_COPIED_FILES_DATATYPE.to_string())
        ))
    } else if buf.len() >= 100 && !shared::UNCOMPRESSIBLE_TYPES.contains(&requested_type) {
        Ok((
            read_zstd(buf, max_size_bytes, requested_type)?,
            Some(shared::NIKAU_ZSTD_TARGET_DATATYPE.to_string())
        ))
    } else {
        // Don't bother compressing small or incompressible data
        Ok((buf, None))
    }
}

pub fn write(buf: Vec<u8>, requested_type: &str, data_type: &str, config_dir: &PathBuf) -> Result<Vec<u8>> {
    match (requested_type, data_type) {
        (requested_type, shared::NIKAU_ZSTD_TARGET_DATATYPE) => write_zstd(buf, requested_type),
        (shared::PATHS_TARGET_GNOME, shared::NIKAU_COPIED_FILES_DATATYPE) => {
            let paths = unpack_zip_payload(buf, config_dir)?;
            write_gnome_file_paths(paths)
        },
        (shared::PATHS_TARGET_URIS, shared::NIKAU_COPIED_FILES_DATATYPE) => {
            let paths = unpack_zip_payload(buf, config_dir)?;
            write_uri_file_paths(paths)
        },
        (requested_type, data_type) => {
            error!("Clipboard data conversion from data_type={} to requested_type={} isn't supported, writing empty clipboard", data_type, requested_type);
            Ok(vec![])
        }
    }
}

/// Expected format depending on the operation:
///   copy\nfile:///path/to/file1\nfile:///path/to/file2
///   cut\n...
fn read_gnome_file_paths(buf: Vec<u8>, max_size_bytes: u64) -> Result<Vec<u8>> {
    let buf = String::from_utf8(buf)?;
    let mut lines: Vec<&str> = buf.split("\n").collect();
    if !lines.is_empty() {
        // Remove the "cut" or "copy"
        lines.remove(0);
    }
    build_zip_payload(lines, max_size_bytes)
}

fn write_gnome_file_paths(paths: Vec<PathBuf>) -> Result<Vec<u8>> {
    bail!("TODO build gnome-format path list");
}

fn read_uri_file_paths(buf: Vec<u8>, max_size_bytes: u64) -> Result<Vec<u8>> {
    // Expected format:
    //   file:///path/to/file1\r\nfile:///path/to/file2\r\n
    let buf = String::from_utf8(buf)?;
    let mut lines: Vec<&str> = buf.split("\r\n").collect();
    if let Some(last) = lines.last() {
        if last.is_empty() {
            // Remove final empty entry from trailing newline
            lines.pop();
        }
    }
    build_zip_payload(lines, max_size_bytes)
}

fn write_uri_file_paths(paths: Vec<PathBuf>) -> Result<Vec<u8>> {
    bail!("TODO build uris-format path list");
}

/// Compresses the provided payload using zstd
fn read_zstd(mut buf: Vec<u8>, max_size_bytes: u64, requested_type: &str) -> Result<Vec<u8>> {
    let orig_len = buf.len();
    buf = zstd::stream::encode_all(buf.as_slice(), 0)?;
    info!("Compressed {}: {} => {} bytes", requested_type, orig_len, buf.len());
    Ok(buf)
}

/// Decompresses the provided payload using zstd
fn write_zstd(mut buf: Vec<u8>, requested_type: &str) -> Result<Vec<u8>> {
    let compressed_len = buf.len();
    buf = zstd::stream::decode_all(buf.as_slice())?;
    info!("Decompressed {}: {} => {} bytes", requested_type, compressed_len, buf.len());
    Ok(buf)
}

// TODO this is all synchronous, wrap to make tokio happy?
fn unpack_zip_payload(zipdata: Vec<u8>, config_dir: &PathBuf) -> Result<Vec<PathBuf>> {
    let tmp_dir = config_dir.join("clipboard");
    // Wipe temp directory to get a clean slate
    if tmp_dir.exists() {
        if tmp_dir.is_dir() {
            std::fs::remove_dir_all(&tmp_dir)?;
        }
        bail!("Temp directory exists, but isn't a directory: {:?}", tmp_dir);
    }
    std::fs::create_dir_all(&tmp_dir)?;

    // Unzip payload into temp directory
    let mut ziparchive = zip::read::ZipArchive::new(std::io::Cursor::new(zipdata))?;
    let mut files = vec![];
    for i in 0..ziparchive.len() {
        let mut zipfile = ziparchive.by_index(i)?;
        let destpath = tmp_dir.join(zipfile.name());
        info!("Unpacking {:?}", destpath);
        // TODO std::io::copy to disk
        files.push(destpath);
    }
    Ok(files)
}

// TODO this is all synchronous, wrap to make tokio happy?
fn build_zip_payload(file_uri_strs: Vec<&str>, max_size_bytes: u64) -> Result<Vec<u8>> {
    // Start by collecting all of the filenames, including any needed recursive scanning.
    let mut files_to_zip = vec![];
    for uri_str in file_uri_strs {
        let uri = url::Url::parse(&uri_str)?;
        if uri.scheme() != "file" {
            warn!("Skipping unsupported file entry: {}", uri);
            continue;
        }
        let path = Path::new(uri.path());
        if path.is_dir() {
            // Recursively scan the directory, omitting the directory path itself
            for entry in walkdir::WalkDir::new(path).min_depth(1).into_iter() {
                let entry = entry?;
                if entry.path().is_file() {
                    files_to_zip.push(entry.into_path());
                }
            }
        } else if path.is_file() {
            files_to_zip.push(path.to_path_buf());
        } else {
            warn!("Skipping path that isn't a file or directory: {:?}", path);
        }
    }
    // Then write the files to the zip file, aborting internally if the compressed size gets too big
    let (uncompressed_len, zipdata) = zip_files(&files_to_zip, max_size_bytes)?;
    info!("Zipped {} files ({} bytes) into {} bytes", files_to_zip.len(), uncompressed_len, zipdata.len());
    Ok(zipdata)
}

fn zip_files(files_to_zip: &Vec<PathBuf>, max_size_bytes: u64) -> Result<(usize, Vec<u8>)> {
    let mut uncompressed_len = 0;
    let mut cursor = limited_cursor::LimitedCursor::new(max_size_bytes);
    {
        let mut zipwriter = zip::ZipWriter::new(&mut cursor);
        let options = zip::write::FileOptions::default().compression_method(zip::CompressionMethod::ZSTD);
        let mut buf = vec![0; 65536];
        for file_to_zip in files_to_zip {
            zipwriter.start_file(file_to_zip.canonicalize()?.to_string_lossy(), options)?;
            let mut file = std::fs::File::open(file_to_zip)?;
            loop {
                match file.read(&mut buf)? {
                    0 => {
                        // EOF
                        break;
                    },
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
