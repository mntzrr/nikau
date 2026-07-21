use std::collections::HashMap;
use std::os::fd::AsFd;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use os_pipe::{pipe, PipeReader};
use tokio::{task, time};
use tracing::{debug, trace, warn};
use wayland_client::globals::registry_queue_init;
use wayland_client::{Connection, EventQueue};

use crate::clipboard::{CLIPBOARD_TIMEOUT_SECS, ClipboardReader as ClipboardReaderTrait, limited};
use crate::clipboard::wayland::{common, state};

/// Retrieves clipboard data from other applications on the system.
/// Watches clipboard mime types, and reads clipboard data.
pub struct ClipboardReader {
    /// None only while a blocking roundtrip task owns the queue (or after one
    /// panicked, in which case every read fails fast).
    inner: Option<ReaderInner>,
}

/// The wayland event queue and its dispatch state. Roundtrips block on the
/// compositor, so reads move these onto a blocking worker thread (see read):
/// they are Send, but not shareable across an await.
struct ReaderInner {
    queue: EventQueue<state::State>,
    state: state::State,
}

impl ClipboardReader {
    pub fn new() -> Result<Self> {
        let conn = Connection::connect_to_env().context("Couldn't reach Wayland compositor socket")?;
        let (globals, mut queue) = registry_queue_init::<state::State>(&conn)
            .context("Failed to init Wayland registry queue")?;
        let qh = queue.handle();

        let clipboard_manager = common::clipboard_manager(&globals, &qh)
            .context("Wayland missing both ext-data-control and wlr-data-control support")?;

        let mut seats = HashMap::new();
        for seat in common::seats(&globals, &qh) {
            let data = state::SeatData::new(clipboard_manager.get_data_device(&seat, &qh, seat.clone()));
            seats.insert(seat, data);
        }
        if seats.is_empty() {
            bail!("No wayland seats found");
        }
        let mut state = state::State::new(seats, None);

        // Initial load of clipboard/mimetype data
        queue.roundtrip(&mut state)?;

        Ok(Self{
            inner: Some(ReaderInner { queue, state }),
        })
    }
}

impl ReaderInner {
    fn get_offer(&mut self, mime_type: String) -> Result<Option<PipeReader>> {
        // Refresh state data to find a matching offer
        self.queue.roundtrip(&mut self.state)?;

        // Just scan the seats for the first match (has the requested mime type).
        // Keep it simple until/unless we know we need multi-seat support.
        let matching_offer = self.state.find_regular_offer(&mime_type);
        let matching_offer = if let Some(found) = matching_offer {
            found
        } else {
            debug!("didn't find clipboard with type {} in wayland", mime_type);
            return Ok(None);
        };
        trace!("fetching clipboard with type {} from wayland", mime_type);
        let (read, write) = pipe().context("Couldn't create a pipe for clipboard content transfer")?;

        matching_offer.receive(mime_type, write.as_fd());
        drop(write);

        // Another roundtrip needed to ensure the read will go through
        self.queue.roundtrip(&mut self.state)?;

        Ok(Some(read))
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
        // The roundtrips inside get_offer block on the compositor; run them on
        // a blocking worker so a wedged compositor can't park an async
        // executor thread for the duration.
        let inner = self
            .inner
            .take()
            .context("Wayland clipboard reader was lost to a failed roundtrip")?;
        let mime_type = requested_type.to_string();
        let (inner, offer) = task::spawn_blocking(move || {
            let mut inner = inner;
            let offer = inner.get_offer(mime_type);
            (inner, offer)
        })
        .await
        .context("Wayland clipboard roundtrip worker failed")?;
        self.inner = Some(inner);
        let mut pipe_reader = if let Some(rdr) = offer? {
            rdr
        } else {
            bail!("No clipboard available");
        };

        // Read on a worker thread with a timeout: a hung local app could otherwise
        // block the pipe read forever, freezing the whole event loop.
        let buf = match time::timeout(
            Duration::from_secs(CLIPBOARD_TIMEOUT_SECS),
            task::spawn_blocking(move || -> Result<Vec<u8>> {
                let mut limited = limited::LimitedCursor::new(max_size_bytes);
                std::io::copy(&mut pipe_reader, &mut limited)?;
                Ok(limited.into_inner())
            }),
        )
        .await
        {
            Ok(Ok(read_result)) => read_result?,
            Ok(Err(e)) => bail!("Wayland clipboard read worker failed: {:?}", e),
            Err(_e) => {
                warn!("Wayland clipboard read timed out after {}s", CLIPBOARD_TIMEOUT_SECS);
                Vec::new()
            }
        };
        debug!(
            "Read {} for {}: {} bytes",
            requested_type,
            request_source,
            buf.len()
        );
        Ok(buf)
    }
}
