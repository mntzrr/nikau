use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use tokio::time;
use tracing::{debug, warn};
use x11rb_async::protocol::xproto::{ConnectionExt, Time};

use crate::clipboard::{
    CLIPBOARD_TIMEOUT_SECS,
    ClipboardReader as ClipboardReaderTrait,
    x11::{events, shared},
};

/// Reads data from the local clipboard for serving to other monux nodes
pub struct ClipboardReader {
    context: shared::XContext,
    atoms: shared::Atoms,
}

impl ClipboardReader {
    pub async fn new() -> Result<Self> {
        let context = shared::XContext::new()
            .await
            .context("Failed to set up X11 API context")?;
        let atoms = shared::Atoms::new(&context.conn)
            .await
            .context("Failed to set up X11 Atoms storage")?;
        Ok(Self { context, atoms })
    }
}

#[async_trait]
impl ClipboardReaderTrait for ClipboardReader {
    /// Reads the clipboard data for the specified type.
    /// The result may be converted/compressed to a different type for network transfer.
    async fn read(
        &mut self,
        requested_type: &str,
        max_size_bytes: u64,
        request_source: &str,
    ) -> Result<Vec<u8>> {
        debug!(
            "Reading local clipboard content as requested by {}: requested_type={} max_size_bytes={}",
            request_source,
            requested_type,
            max_size_bytes
        );
        let type_atom = self
            .atoms
            .get_atom(&self.context.conn, requested_type)
            .await?;

        self.context
            .conn
            .convert_selection(
                self.context.window,
                self.atoms.clipboard,
                type_atom,
                self.atoms.recv_clipboard,
                Time::CURRENT_TIME,
            )
            .await?
            .check()
            .await?;

        let mut buf = Vec::new();
        // If there's a bug in clipboard state management, retrieval can get stuck forever.
        // So just in case let's avoid waiting forever here.
        match time::timeout(
            Duration::from_secs(CLIPBOARD_TIMEOUT_SECS),
            events::process_event(
                &self.context,
                &self.atoms,
                &mut buf,
                max_size_bytes,
                type_atom,
                self.atoms.recv_clipboard,
            ),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                bail!("X11 clipboard read failed: {:?}", e);
            }
            Err(_e) => {
                warn!("X11 clipboard read timed out after {}s", CLIPBOARD_TIMEOUT_SECS);
                buf.clear();
                // Continue below, try to clear the status
            }
        }

        self.context
            .conn
            .delete_property(self.context.window, self.atoms.recv_clipboard)
            .await?
            .check()
            .await?;

        Ok(buf)
    }
}
