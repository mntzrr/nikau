use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use os_pipe::{pipe, PipeReader};
use rustix::fs::{fcntl_setfl, OFlags};
use tokio::task;
use tracing::{debug, trace, warn};
use wayland_client::globals::registry_queue_init;
use wayland_client::{Connection, EventQueue};

use crate::clipboard::{CLIPBOARD_TIMEOUT_SECS, ClipboardReader as ClipboardReaderTrait, limited};
use crate::clipboard::wayland::{common, state};

/// Retrieves clipboard data from other applications on the system.
/// Watches clipboard mime types, and reads clipboard data.
pub struct ClipboardReader {
    /// Shared owner of the queue/state: a read moves them onto a blocking
    /// worker thread, and the worker ALWAYS puts them back when done — even
    /// if the awaiting read was cancelled (e.g. a client-side read timeout) —
    /// so a wedged compositor roundtrip can never strand them on the detached
    /// worker and permanently kill the reader (see run_on_worker).
    slot: Arc<BlockingSlot<ReaderInner>>,
}

/// The wayland event queue and its dispatch state. Roundtrips block on the
/// compositor, so reads move these onto a blocking worker thread (see read):
/// they are Send, but not shareable across an await.
struct ReaderInner {
    queue: EventQueue<state::State>,
    state: state::State,
}

/// Shared slot for state that blocking workers borrow and always return (see
/// run_on_worker). The state is None only while a worker owns it, or forever
/// after a worker panicked (later users then fail fast).
struct BlockingSlot<T> {
    state: Mutex<Option<T>>,
    /// True while a worker owns the state. Lets an empty slot distinguish
    /// "roundtrip still running after its caller gave up" (retry later) from
    /// "roundtrip panicked" (permanent).
    work_active: AtomicBool,
}

impl<T> BlockingSlot<T> {
    fn new(state: T) -> Self {
        Self {
            state: Mutex::new(Some(state)),
            work_active: AtomicBool::new(false),
        }
    }

    /// Hands out the state for one worker, marking it busy. Fails fast when
    /// the state is gone: still owned by an earlier roundtrip (busy), or lost
    /// to a panicked one (permanent).
    fn take(&self) -> Result<T> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let state = guard.take().with_context(|| {
            if self.work_active.load(Ordering::Relaxed) {
                "Wayland clipboard roundtrip still in progress after its caller gave up (wedged compositor?)"
            } else {
                "Wayland clipboard reader was lost to a failed roundtrip"
            }
        })?;
        self.work_active.store(true, Ordering::Relaxed);
        Ok(state)
    }

    /// Puts the state back and marks the slot idle. Called by the worker
    /// itself, so it also runs when the awaiting caller was cancelled.
    fn restore(&self, state: T) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(state);
        self.work_active.store(false, Ordering::Relaxed);
    }

    /// Marks the slot dead-idle after a panicked worker: the state is gone
    /// for good, so later takers get the permanent "lost" error instead of a
    /// misleading "still in progress".
    fn mark_dead(&self) {
        self.work_active.store(false, Ordering::Relaxed);
    }
}

/// Runs `work` with the slot's state on the blocking thread pool, returning
/// its result. The worker ALWAYS restores the state on completion, even when
/// this future is dropped while awaiting (spawn_blocking tasks keep running
/// detached), so a cancelled caller (e.g. a read timeout) can't strand the
/// state. A panicked worker never restores: the slot stays empty and later
/// callers fail fast. Reads stay serialized: at most one worker owns the
/// state at any time.
async fn run_on_worker<T, R, F>(slot: &Arc<BlockingSlot<T>>, work: F) -> Result<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: FnOnce(T) -> (T, R) + Send + 'static,
{
    let state = slot.take()?;
    let worker_slot = slot.clone();
    let join = task::spawn_blocking(move || {
        let (state, result) = work(state);
        // Restore unconditionally: this detached closure is the only place
        // that can put the state back, whatever happened to the caller.
        worker_slot.restore(state);
        result
    });
    match join.await {
        Ok(result) => Ok(result),
        Err(e) => {
            slot.mark_dead();
            Err(e).context("Wayland clipboard roundtrip worker failed")
        }
    }
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
            slot: Arc::new(BlockingSlot::new(ReaderInner { queue, state })),
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
        let mime_type = requested_type.to_string();
        let offer = run_on_worker(&self.slot, move |mut inner| {
            let offer = inner.get_offer(mime_type);
            (inner, offer)
        })
        .await??;
        let pipe_reader = if let Some(rdr) = offer {
            rdr
        } else {
            bail!("No clipboard available");
        };

        // Read on a worker thread: a hung local app could otherwise block the
        // pipe read forever, freezing the whole event loop. The worker
        // enforces the deadline itself (see read_offer_pipe), so a timeout
        // also releases the read end — no detached worker left parked on the
        // pipe with the fd held open.
        let buf = task::spawn_blocking(move || read_offer_pipe(pipe_reader, max_size_bytes))
            .await
            .context("Wayland clipboard read worker failed")??;
        debug!(
            "Read {} for {}: {} bytes",
            requested_type,
            request_source,
            buf.len()
        );
        Ok(buf)
    }
}

/// Reads a clipboard offer pipe to EOF with a deadline, so a hung source app
/// can't block the read forever. The fd is set non-blocking and polled,
/// mirroring write_paste_fd in writer.rs: on timeout the worker returns here
/// and drops the pipe reader, closing the read end — rather than staying
/// parked on the pipe with the fd held open. (Closing the fd from ANOTHER
/// thread is not an alternative: on Linux that doesn't interrupt a blocked
/// read, and the eventual double close could hit a reused fd.)
fn read_offer_pipe(mut pipe_reader: PipeReader, max_size_bytes: u64) -> Result<Vec<u8>> {
    fcntl_setfl(&pipe_reader, OFlags::NONBLOCK).map_err(io::Error::from)?;
    let raw_fd = pipe_reader.as_raw_fd();
    let deadline = std::time::Instant::now() + Duration::from_secs(CLIPBOARD_TIMEOUT_SECS);
    let mut limited = limited::LimitedCursor::new(max_size_bytes);
    let mut chunk = [0u8; 65536];
    loop {
        match pipe_reader.read(&mut chunk) {
            // EOF: the source app closed its end.
            Ok(0) => break,
            Ok(n) => limited.write_all(&chunk[..n])?,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    // Same contract as the timeout this replaces: a timed-out
                    // read serves nothing.
                    warn!(
                        "Wayland clipboard read timed out after {}s",
                        CLIPBOARD_TIMEOUT_SECS
                    );
                    return Ok(Vec::new());
                }
                let mut pfd = libc::pollfd {
                    fd: raw_fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut pfd, 1, remaining.as_millis() as libc::c_int) } < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err).context("Failed to poll clipboard pipe");
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(limited.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time;

    /// Completing work returns its result and hands the state back, so the
    /// slot is immediately usable for the next read.
    #[tokio::test]
    async fn completed_work_restores_state() {
        let slot = Arc::new(BlockingSlot::new(41u32));
        let result = run_on_worker(&slot, |n| (n, n + 1)).await.unwrap();
        assert_eq!(result, 42);
        let result = run_on_worker(&slot, |n| (n, n + 1)).await.unwrap();
        assert_eq!(result, 42);
        assert!(!slot.work_active.load(Ordering::Relaxed));
    }

    /// A caller cancelled mid-roundtrip (e.g. the client-side 4s read
    /// timeout) drops the awaiting future, but the detached worker still
    /// restores the state when it finishes: the reader is never lost to a
    /// slow compositor.
    #[tokio::test]
    async fn cancelled_caller_still_gets_state_restored() {
        let slot = Arc::new(BlockingSlot::new(7u32));
        let slow = run_on_worker(&slot, |n| {
            std::thread::sleep(Duration::from_millis(200));
            (n, n)
        });
        // The caller gives up before the wedged roundtrip returns.
        assert!(time::timeout(Duration::from_millis(20), slow).await.is_err());
        // While the worker still owns the state, reads fail fast as busy...
        let busy = run_on_worker(&slot, |n| (n, n)).await;
        assert!(format!("{:?}", busy.unwrap_err()).contains("still in progress"));
        // ...but once the detached worker finishes, the state is back and
        // reads proceed again.
        time::sleep(Duration::from_millis(400)).await;
        let result = run_on_worker(&slot, |n| (n, n + 1)).await.unwrap();
        assert_eq!(result, 8);
        assert!(!slot.work_active.load(Ordering::Relaxed));
    }

    /// A panicked worker never restores the state: later callers fail fast
    /// with the permanent "lost" error (same as before the shared-slot change).
    #[tokio::test]
    async fn panicked_worker_leaves_the_reader_lost() {
        let slot = Arc::new(BlockingSlot::new(1u32));
        let result: Result<u32> = run_on_worker(&slot, |_| -> (u32, u32) {
            panic!("compositor roundtrip exploded");
        })
        .await;
        assert!(format!("{:?}", result.unwrap_err()).contains("worker failed"));
        // The state is gone: every later read fails fast with the original error.
        let lost = run_on_worker(&slot, |n| (n, n)).await;
        assert!(format!("{:?}", lost.unwrap_err()).contains("lost to a failed roundtrip"));
    }
}
