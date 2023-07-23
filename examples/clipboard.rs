use async_lock::Mutex;
use std::sync::Arc;

use anyhow::Result;
use async_std::task;
use futures::StreamExt;
use tracing::{error, info};

use nikau::{logging, x11clipboard};

fn main() -> Result<()> {
    logging::init_logging();

    task::block_on(async {
        if let Err(e) = do_thing().await {
            error!("failed: {:?}", e);
        }
    });

    Ok(())
}

async fn do_thing() -> Result<()> {
    let (clipboard_types_tx, mut clipboard_types_rx) = async_channel::bounded(32);
    x11clipboard::reader::ClipboardTypeWatcher::start(clipboard_types_tx).await?;
    let mut reader = x11clipboard::reader::ClipboardReader::new().await?;
    let type_ = "UTF8_STRING";
    let types = vec![
        "text/plain",
        "text/plain;charset=utf-8",
        "STRING",
        "TEXT",
        "COMPOUND_TEXT",
        type_,
    ];
    let (fetch_tx, mut fetch_rx) = async_channel::bounded(32);
    let writer = Arc::new(Mutex::new(
        x11clipboard::writer::ClipboardWriter::new(fetch_tx).await?,
    ));

    let writer2 = writer.clone();
    task::spawn(async move {
        loop {
            if let Some(fetch) = fetch_rx.next().await {
                info!("got fetch from writer");
                // pretend that we're a server fetching a result here...
                let mut data = Vec::new();
                data.extend_from_slice(b"hello xorg");
                let d = x11clipboard::ClipboardData {
                    type_: fetch.type_,
                    data,
                    remaining_bytes: 0,
                };
                if let Err(e) = writer.lock().await.store_data(d).await {
                    error!("storing clipboard data failed: {}", e);
                }
            }
        }
    });


    info!("waiting for new clipboard types...");
    if let Some(clipboard_types) = clipboard_types_rx.next().await {
        info!("got clipboard types: {:?}", clipboard_types);
    }

    x11_fetch_data(&mut reader, type_).await?;

    {
        let mut writer = writer2.lock().await;
        x11_store_types(&mut writer, &types).await?;
    }

    info!("checking for appearance of new clipboard types...");
    if let Some(clipboard_types) = clipboard_types_rx.next().await {
        info!("got clipboard types: {:?}", clipboard_types);
    }

    x11_fetch_data(&mut reader, type_).await?;
    x11_fetch_data(&mut reader, type_).await?;

    Ok(())
}

async fn x11_store_types(
    clipboard: &mut x11clipboard::writer::ClipboardWriter,
    types: &Vec<&str>,
) -> Result<()> {
    let types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    clipboard.store_types(types).await?;
    info!("stored types into clipboard");
    Ok(())
}

async fn x11_fetch_data(
    clipboard: &mut x11clipboard::reader::ClipboardReader,
    type_: &str,
) -> Result<()> {
    let val = clipboard.read(type_, 0).await?;
    if val.len() > 256 {
        info!("x11 fetch clipboard: {} bytes", val.len());
    } else {
        info!(
            "x11 fetch clipboard: {} bytes: [{}]",
            val.len(),
            String::from_utf8_lossy(&val)
        );
    }
    Ok(())
}
