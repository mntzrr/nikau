use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Result};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tracing::{debug, error, info, warn};

use crate::clipboard::{ClipboardReader, ClipboardWriter, data, serve, wayland, x11};
use crate::rotation;

/// Wrapper around server-local clipboard storage, if available.
/// Clipboard contents can still be transferred by the server among clients if this is unavailable.
pub struct LocalClipboard {
    /// Shared with spawned clipboard-serving tasks so that slow reads (e.g.
    /// zipping large copied files) never block the rotation event loop.
    /// Serializes serves and caches the last payload, so request bursts
    /// (e.g. clipboard managers fetching every type) can't pile up CPU work.
    /// Cache invalidation is lock-free (see SharedClipboardReader).
    reader: serve::SharedClipboardReader,
    /// Queue to the writer dispatcher thread (see spawn_writer_dispatcher):
    /// keeps blocking clipboard advertisements off the rotation loop.
    types_tx: std::sync::mpsc::Sender<Vec<String>>,
}

impl LocalClipboard {
    pub async fn start(
        config_dir: PathBuf,
        rotation_tx: mpsc::Sender<rotation::RotationEvent>,
        max_clipboard_size_bytes: u64,
        max_uncompressed_size_bytes: u64,
    ) -> Option<Self> {
        match Self::new_wayland(config_dir.clone(), rotation_tx.clone(), max_clipboard_size_bytes, max_uncompressed_size_bytes).await {
            Ok(Some(c)) => {
                info!("Using wayland clipboard");
                return Some(c);
            }
            Ok(None) => {
                info!("Unable to reach wayland clipboard, trying X11");
            }
            Err(e) => {
                warn!("Failed to reach wayland clipboard, trying X11: {}", e);
            }
        };
        match Self::new_x11(config_dir, rotation_tx, max_clipboard_size_bytes, max_uncompressed_size_bytes).await {
            Ok(c) => {
                info!("Using X11 clipboard");
                Some(c)
            }
            Err(e) => {
                warn!("Unable to reach X11 clipboard: {}", e);
                warn!("CLIPBOARD SHARING DISABLED: no wayland or X11 clipboard is reachable. If monux is running under sudo, start it with 'sudo -E ...' to preserve the session environment (WAYLAND_DISPLAY, XDG_RUNTIME_DIR)");
                None
            }
        }
    }

    async fn new_wayland(
        config_dir: PathBuf,
        rotation_tx: mpsc::Sender<rotation::RotationEvent>,
        max_clipboard_size_bytes: u64,
        max_uncompressed_size_bytes: u64,
    ) -> Result<Option<Self>> {
        // The watcher call is set up to be permissive of missing wayland, so let's try that first
        let (local_regular_types_tx, local_regular_types_rx) = watch::channel(vec![]);
        if wayland::type_watcher::start(Some(local_regular_types_tx))?.is_none() {
            return Ok(None);
        }
        // Wayland should work from here, treat any init issues as an error
        let reader = wayland::reader::ClipboardReader::new()?;
        let (clipboard_fetch_tx, clipboard_fetch_rx) = mpsc::channel::<data::ClipboardFetch>(32);
        let writer = wayland::writer::ClipboardWriter::new(
            wayland::writer::ClipboardType::Regular,
            config_dir,
            max_uncompressed_size_bytes,
            clipboard_fetch_tx,
        );
        Ok(Some(Self::start_impl(
            Box::new(reader),
            Box::new(writer),
            rotation_tx,
            max_clipboard_size_bytes,
            clipboard_fetch_rx,
            local_regular_types_rx,
        ).await?))
    }

    async fn new_x11(
        config_dir: PathBuf,
        rotation_tx: mpsc::Sender<rotation::RotationEvent>,
        max_clipboard_size_bytes: u64,
        max_uncompressed_size_bytes: u64,
    ) -> Result<Self> {
        let (local_types_tx, local_types_rx) = watch::channel(vec![]);
        x11::type_watcher::ClipboardTypeWatcher::start(local_types_tx).await?;
        let reader = x11::reader::ClipboardReader::new().await?;
        let (clipboard_fetch_tx, clipboard_fetch_rx) = mpsc::channel::<data::ClipboardFetch>(32);
        let writer = x11::writer::ClipboardWriter::start(
            config_dir,
            max_uncompressed_size_bytes,
            clipboard_fetch_tx,
        ).await?;
        Self::start_impl(
            Box::new(reader),
            Box::new(writer),
            rotation_tx,
            max_clipboard_size_bytes,
            clipboard_fetch_rx,
            local_types_rx,
        ).await
    }

    async fn start_impl(
        reader: Box<dyn ClipboardReader>,
        writer: Box<dyn ClipboardWriter>,
        rotation_tx: mpsc::Sender<rotation::RotationEvent>,
        max_clipboard_size_bytes: u64,
        mut clipboard_fetch_rx: mpsc::Receiver<data::ClipboardFetch>,
        mut local_types_rx: watch::Receiver<Vec<String>>,
    ) -> Result<Self> {
        task::spawn(async move {
            loop {
                tokio::select! {
                    // Listen to local host requests to get the clipboard
                    fetch_request = clipboard_fetch_rx.recv() => {
                        if let Some(fetch_request) = fetch_request {
                            // Got clipboard paste request from the local machine.
                            // Pass the request through to the main rotation event handler.
                            let event = rotation::RotationEvent::ClipboardRequestContent(rotation::ClipboardRequestContentArgs {
                                request_source: rotation::ClipboardRequestSource::Local(fetch_request.fetch_result_tx),
                                requested_type: fetch_request.requested_type,
                                max_size_bytes: max_clipboard_size_bytes,
                                // The server assigns an id while routing the request.
                                request_id: None,
                            });
                            if let Err(e) = rotation_tx.send(event).await {
                                error!("Failed to queue local clipboard request event: {:?}", e);
                                break;
                            }
                        } else {
                            error!("Clipboard fetch request queue has closed, exiting clipboard loop");
                            break;
                        }
                    },
                    // Listen to local host updates to the clipboard types
                    types_notify = local_types_rx.changed() => {
                        if let Err(e) = types_notify {
                            error!("local_types_rx has closed: {}", e);
                            break;
                        }
                        // Another application on the server machine has a clipboard entry.
                        let event = rotation::RotationEvent::ClipboardUpdateSource(rotation::ClipboardUpdateSourceArgs {
                            source: None,
                            types: local_types_rx.borrow().clone(),
                            max_size_bytes: max_clipboard_size_bytes,
                        });
                        if let Err(e) = rotation_tx.send(event).await {
                            error!("Failed to queue update source event: {:?}", e);
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self {
            reader: serve::SharedClipboardReader::new(reader),
            types_tx: crate::clipboard::spawn_writer_dispatcher(writer),
        })
    }

    /// Handle for sharing the clipboard reader with spawned serving tasks,
    /// so that slow reads never block the rotation event loop.
    pub fn reader_handle(&self) -> serve::SharedClipboardReader {
        self.reader.clone()
    }

    /// Reads the clipboard data for the specified type.
    /// The result may be converted/compressed to a different type for network transfer.
    pub async fn read(
        reader: &serve::SharedClipboardReader,
        requested_type: &str,
        max_size_bytes: u64,
        request_client: &SocketAddr,
    ) -> Result<(Vec<u8>, Option<String>)> {
        let request_source = format!("client {}", request_client);
        let (content, data_type) = reader
            .read(requested_type, max_size_bytes, &request_source)
            .await?;
        if let Some(data_type) = &data_type {
            debug!(
                "Sending clipboard data for requested type {} (data type {}) from server to {}",
                requested_type, data_type, request_source
            );
        } else {
            debug!(
                "Sending clipboard data for requested type {} from server to {}",
                requested_type, request_source
            );
        }
        Ok((content, data_type))
    }

    /// Advertises to the local environment that we have a new clipboard entry
    /// available. Non-blocking: the actual wayland/X11 work happens on the
    /// writer dispatcher thread, so the rotation loop never stalls on it.
    pub fn store_types<K: Into<Vec<String>>>(&self, types: K) -> Result<()> {
        // The dispatcher thread exits (and this send fails) only if the
        // clipboard is being torn down; a failed advertisement is not fatal.
        let _ = self.types_tx.send(types.into());
        Ok(())
    }
}
