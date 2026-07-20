use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::{task, time};
use tracing::{error, info};

use monux::logging;
use monux::clipboard::{
    ClipboardReader as ClipboardReaderTrait,
    ClipboardWriter as ClipboardWriterTrait,
};
use monux::clipboard::data::ClipboardData;
use monux::clipboard::x11::{
    reader::ClipboardReader,
    type_watcher::ClipboardTypeWatcher,
    writer::ClipboardWriter,
};

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();

    let (clipboard_types_tx, mut clipboard_types_rx) = watch::channel(vec![]);
    ClipboardTypeWatcher::start(clipboard_types_tx).await?;
    let mut reader = ClipboardReader::new().await?;
    let type_ = "UTF8_STRING";
    let types = vec![
        "text/plain",
        "text/plain;charset=utf-8",
        "STRING",
        "TEXT",
        "COMPOUND_TEXT",
        type_,
    ];
    let (fetch_tx, mut fetch_rx) = mpsc::channel(32);
    let max_uncompressed_bytes = 1024 * 1024;
    let writer = Arc::new(Mutex::new(
        ClipboardWriter::start(
            // For zipping/unzipping files to serve their paths
            PathBuf::from("/tmp/monux"),
            max_uncompressed_bytes,
            fetch_tx,
        )
        .await?,
    ));

    task::spawn(async move {
        loop {
            if let Some(fetch) = fetch_rx.recv().await {
                info!("got clipboard lookup from writer, try pasting");
                // pretend that we're a server fetching a result here...
                let mut bytes = Vec::new();
                bytes.extend_from_slice(b"hello xorg");
                let d = ClipboardData {
                    requested_type: fetch.requested_type,
                    data_type: None,
                    bytes,
                    remaining_bytes: 0,
                };
                if let Err(_d_again) = fetch.fetch_result_tx.send(d) {
                    error!("storing clipboard data failed");
                }
            }
        }
    });

    info!("waiting for new clipboard types...");
    clipboard_types_rx.changed().await?;
    info!("got clipboard types A: {:?}", clipboard_types_rx.borrow());

    x11_fetch_data(&mut reader, type_).await?;

    {
        let mut writer = writer.lock().await;
        // This should get flagged as FROM monux, and so ignored
        x11_store_types(&mut writer, &types).await?;
    }

    info!("waiting for new clipboard types again...");
    clipboard_types_rx.changed().await?;
    info!("got clipboard types B: {:?}", clipboard_types_rx.borrow());

    x11_fetch_data(&mut reader, type_).await?;

    info!("clearing clipboard types");
    {
        let mut writer = writer.lock().await;
        x11_store_types(&mut writer, &vec![]).await?;
    }

    // Sleep a bit to avoid a race between the fetch and the store
    time::sleep(std::time::Duration::from_millis(500)).await;

    info!("trying fetch after clear");
    x11_fetch_data(&mut reader, type_).await?;

    info!("try pasting again in the next 5s, it should do nothing");
    time::sleep(std::time::Duration::from_millis(5000)).await;

    Ok(())
}

async fn x11_store_types(clipboard: &mut ClipboardWriter, types: &Vec<&str>) -> Result<()> {
    let types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    let types_len = types.len();
    clipboard.store_types(types)?;
    info!("stored {} types into clipboard", types_len);
    Ok(())
}

async fn x11_fetch_data(clipboard: &mut ClipboardReader, type_: &str) -> Result<()> {
    let val = clipboard.read(type_, 0, "local").await?;
    if val.len() > 256 {
        info!("got clipboard from x11: {} bytes", val.len());
    } else {
        info!(
            "got clipboard from x11: {} bytes: [{}]",
            val.len(),
            String::from_utf8_lossy(&val)
        );
    }
    Ok(())
}
