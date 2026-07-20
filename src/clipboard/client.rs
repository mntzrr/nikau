use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result};
use tokio::sync::{Mutex, mpsc, watch};
use tracing::{debug, info, warn};

use crate::clipboard::{ClipboardReader, ClipboardWriter, convert, data, wayland, x11};

/// Wrapper around client-local clipboard storage, if available.
pub struct LocalClipboard {
    /// Shared with spawned clipboard-serving tasks so that slow reads (e.g.
    /// zipping large copied files) never block the client event loop.
    reader: Arc<Mutex<Box<dyn ClipboardReader>>>,
    writer: Box<dyn ClipboardWriter>,
    // TODO can we nest a tokio select here instead of exposing these upstream?:
    pub clipboard_fetch_rx: mpsc::Receiver<data::ClipboardFetch>,
    pub local_types_rx: watch::Receiver<Vec<String>>,
    local_types: Option<Vec<String>>,
    serving_remote_clipboard: bool,
}

impl LocalClipboard {
    pub async fn new(config_dir: PathBuf, max_uncompressed_size_bytes: u64) -> Option<Self> {
        match Self::new_wayland(config_dir.clone(), max_uncompressed_size_bytes).await {
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
        match Self::new_x11(config_dir, max_uncompressed_size_bytes).await {
            Ok(c) => {
                info!("Using X11 clipboard");
                Some(c)
            }
            Err(e) => {
                info!("Unable to reach X11 clipboard, disabled system clipboard support: {}", e);
                None
            }
        }
    }

    async fn new_wayland(config_dir: PathBuf, max_uncompressed_size_bytes: u64) -> Result<Option<Self>> {
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
        Ok(Some(Self{
            reader: Arc::new(Mutex::new(Box::new(reader) as Box<dyn ClipboardReader>)),
            writer: Box::new(writer),
            clipboard_fetch_rx,
            local_types_rx: local_regular_types_rx,
            local_types: None,
            serving_remote_clipboard: false,
        }))
    }

    async fn new_x11(config_dir: PathBuf, max_uncompressed_size_bytes: u64) -> Result<Self> {
        let reader = x11::reader::ClipboardReader::new().await?;
        let (local_types_tx, local_types_rx) = watch::channel(vec![]);
        x11::type_watcher::ClipboardTypeWatcher::start(local_types_tx).await?;
        let (clipboard_fetch_tx, clipboard_fetch_rx) = mpsc::channel(32);
        let writer =
            x11::writer::ClipboardWriter::start(config_dir, max_uncompressed_size_bytes, clipboard_fetch_tx).await?;
        Ok(Self {
            reader: Arc::new(Mutex::new(Box::new(reader) as Box<dyn ClipboardReader>)),
            writer: Box::new(writer),
            clipboard_fetch_rx,
            local_types_rx,
            local_types: None,
            serving_remote_clipboard: false,
        })
    }

    /// Handle for sharing the clipboard reader with spawned serving tasks,
    /// so that slow reads never block the client event loop.
    pub fn reader_handle(&self) -> Arc<Mutex<Box<dyn ClipboardReader>>> {
        self.reader.clone()
    }

    /// Reads the clipboard data for the specified type.
    /// The result may be converted/compressed to a different type for network transfer.
    pub async fn read(
        reader: &Arc<Mutex<Box<dyn ClipboardReader>>>,
        requested_type: &str,
        max_size_bytes: u64,
        request_client: Option<SocketAddr>,
    ) -> Result<(Vec<u8>, Option<String>)> {
        let request_source = if let Some(c) = request_client {
            format!("server for {}", c)
        } else {
            "server".to_string()
        };
        debug!(
            "Reading clipboard data for requested type {} to {}",
            requested_type,
            request_source,
        );
        // Hold the reader lock only for the system read itself, not for conversion.
        let original_data = {
            let mut guard = reader.lock().await;
            guard
                .read(requested_type, max_size_bytes, &request_source)
                .await?
        };
        convert::read(original_data, max_size_bytes, requested_type).await
    }

    /// Switches to serving the local clipboard, rather than from the nikau server
    pub fn set_local_clipboard(&mut self) {
        self.local_types.replace(self.local_types_rx.borrow().clone());
        // Now that we have a local clipboard, don't fetch clipboards from the server.
        self.serving_remote_clipboard = false;
    }

    /// Returns the locally available clipboard types
    pub fn get_local_clipboard_types(&mut self) -> Option<Vec<String>> {
        self.local_types.clone()
    }

    /// Clears the clipboard, discarding any types provided by the nikau server
    pub fn clear_remote_clipboard(&mut self) -> Result<()> {
        if self.serving_remote_clipboard {
            self.local_types = None;
            self.serving_remote_clipboard = false;
            self.writer.store_types(vec![])?;
        }
        Ok(())
    }

    /// Sets the clipboard to types provided by the nikau server
    pub fn set_remote_clipboard(&mut self, types: Vec<String>) -> Result<()> {
        self.local_types = None;
        self.serving_remote_clipboard = true;
        self.writer.store_types(types)?;
        Ok(())
    }
}
