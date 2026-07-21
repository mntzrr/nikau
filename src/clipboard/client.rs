use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Result};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::clipboard::{data, serve, wayland, x11};

/// Wrapper around client-local clipboard storage, if available.
pub struct LocalClipboard {
    /// Shared with spawned clipboard-serving tasks so that slow reads (e.g.
    /// zipping large copied files) never block the client event loop.
    /// Serializes serves and caches the last payload, so request bursts
    /// (e.g. clipboard managers fetching every type) can't pile up CPU work.
    /// Cache invalidation is lock-free (see SharedClipboardReader).
    reader: serve::SharedClipboardReader,
    /// Queue to the writer dispatcher thread: keeps blocking clipboard
    /// advertisements off the client event loop (see spawn_writer_dispatcher).
    types_tx: std::sync::mpsc::Sender<Vec<String>>,
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
                warn!("Unable to reach X11 clipboard: {}", e);
                warn!("CLIPBOARD SHARING DISABLED: no wayland or X11 clipboard is reachable. If monux is running under sudo, start it with 'sudo -E ...' to preserve the session environment (WAYLAND_DISPLAY, XDG_RUNTIME_DIR)");
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
            reader: serve::SharedClipboardReader::new(Box::new(reader)),
            types_tx: crate::clipboard::spawn_writer_dispatcher(Box::new(writer)),
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
            reader: serve::SharedClipboardReader::new(Box::new(reader)),
            types_tx: crate::clipboard::spawn_writer_dispatcher(Box::new(writer)),
            clipboard_fetch_rx,
            local_types_rx,
            local_types: None,
            serving_remote_clipboard: false,
        })
    }

    /// Handle for sharing the clipboard reader with spawned serving tasks,
    /// so that slow reads never block the client event loop.
    pub fn reader_handle(&self) -> serve::SharedClipboardReader {
        self.reader.clone()
    }

    /// Reads the clipboard data for the specified type.
    /// The result may be converted/compressed to a different type for network transfer.
    pub async fn read(
        reader: &serve::SharedClipboardReader,
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
        reader
            .read(requested_type, max_size_bytes, &request_source)
            .await
    }

    /// Switches to serving the local clipboard, rather than from the monux server
    pub fn set_local_clipboard(&mut self) {
        self.local_types.replace(self.local_types_rx.borrow().clone());
        // The local clipboard changed: never serve stale cached contents.
        // Lock-free: never waits on a serve in progress.
        self.reader.invalidate();
        // Now that we have a local clipboard, don't fetch clipboards from the server.
        self.serving_remote_clipboard = false;
    }

    /// Returns the locally available clipboard types
    pub fn get_local_clipboard_types(&mut self) -> Option<Vec<String>> {
        self.local_types.clone()
    }

    /// Clears the clipboard, discarding any types provided by the monux server
    pub fn clear_remote_clipboard(&mut self) -> Result<()> {
        if self.serving_remote_clipboard {
            self.local_types = None;
            self.serving_remote_clipboard = false;
            // Non-blocking: the actual advertisement happens on the writer
            // dispatcher thread; a failed send only means we're shutting down.
            let _ = self.types_tx.send(vec![]);
        }
        Ok(())
    }

    /// Sets the clipboard to types provided by the monux server
    pub fn set_remote_clipboard(&mut self, types: Vec<String>) -> Result<()> {
        self.local_types = None;
        self.serving_remote_clipboard = true;
        // Non-blocking: the actual advertisement happens on the writer
        // dispatcher thread; a failed send only means we're shutting down.
        let _ = self.types_tx.send(types);
        Ok(())
    }
}
