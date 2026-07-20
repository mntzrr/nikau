use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::info;

use monux::clipboard::{
    ClipboardReader as ClipboardReaderTrait,
    ClipboardWriter as ClipboardWriterTrait,
    data,
};
use monux::clipboard::wayland::{reader, type_watcher, writer};
use monux::logging;

/// Smoke test for the wayland clipboard paths on a live session.
/// Drive externally with wl-copy / wl-paste while it runs.
#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();

    // 1. Type watcher: log every clipboard type update seen.
    let (regular_types_tx, mut regular_types_rx) = watch::channel(vec![]);
    tokio::spawn(async move {
        while regular_types_rx.changed().await.is_ok() {
            info!("[TYPES] regular: {:?}", regular_types_rx.borrow().clone());
        }
    });
    type_watcher::start(Some(regular_types_tx))?;

    // 2. Writer: advertise text/plain and serve "hello-from-monux" on paste.
    let (fetch_tx, mut fetch_rx) = mpsc::channel(32);
    writer::ClipboardWriter::new(
        writer::ClipboardType::Regular,
        PathBuf::from("/tmp/monux"),
        1024 * 1024,
        fetch_tx,
    )
    .store_types(vec!["text/plain".to_string()])?;
    info!("[WRITE] advertised text/plain, serving 'hello-from-monux' — try: wl-paste --no-newline");
    tokio::spawn(async move {
        while let Some(fetch) = fetch_rx.recv().await {
            info!("[FETCH] requested_type={}", fetch.requested_type);
            let d = data::ClipboardData {
                requested_type: fetch.requested_type,
                data_type: None,
                bytes: b"hello-from-monux".to_vec(),
                remaining_bytes: 0,
            };
            if fetch.fetch_result_tx.send(d).is_err() {
                info!("[FETCH] failed to answer fetch");
            }
        }
    });

    // 3. Reader: every 15s, read whatever app currently owns the clipboard.
    let mut reader = reader::ClipboardReader::new()?;
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        match reader.read("text/plain", 1024 * 1024, "smoke test").await {
            Ok(bytes) => info!("[READ] {} bytes: {:?}", bytes.len(), String::from_utf8_lossy(&bytes)),
            Err(e) => info!("[READ ERR] {:?}", e),
        }
    }
}
