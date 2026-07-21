use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use rustix::fs::{fcntl_setfl, OFlags};
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};
use wayland_client::globals::{registry_queue_init, GlobalListContents};
use wayland_client::protocol::wl_registry::WlRegistry;
use wayland_client::protocol::wl_seat;
use wayland_client::{event_created_child, Connection, Dispatch, EventQueue};

use crate::clipboard::{ClipboardWriter as ClipboardWriterTrait, data};
use crate::clipboard::wayland::{common, state};
use crate::clipboard::wayland::data_control::{
    self, impl_dispatch_device, impl_dispatch_manager, impl_dispatch_offer, impl_dispatch_source,
};

#[derive(Clone, Debug)]
pub enum ClipboardType {
    /// Write to the regular clipboard (Ctrl+C style)
    Regular,
}

struct State {
    seats: HashMap<wl_seat::WlSeat, data_control::Device>,
    prepared_copy_state: Option<PreparedCopyState>,
}

/// Clipboard contents for the currently advertised clipboard, fetched in the
/// background so that paste (Send) requests never block the dispatch thread
/// waiting on a network/timeout fetch. Maps the requested mime type to its data.
type ClipboardCache = std::sync::Arc<std::sync::Mutex<Option<(String, Vec<u8>)>>>;

struct PreparedCopyState {
    mime_types: Vec<String>,
    fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
    config_dir: PathBuf,
    /// Safety limit to the uncompressed size of a clipboard
    max_uncompressed_size_bytes: u64,
    /// Populated in the background (see spawn_fetch), never by blocking the
    /// dispatch thread.
    clipboard_data: ClipboardCache,
    /// Paste (Send) request count within the current one-second window, for
    /// detecting clipboard-manager storms (see the Send handler).
    send_stats: std::sync::Arc<std::sync::Mutex<(std::time::Instant, u32)>>,
}

/// Runs a clipboard data fetch on the current (background) thread, using a
/// short-lived runtime. Returns None on retryable failure.
fn fetch_sync(
    mime_type: &str,
    fetch_data_tx: &mpsc::Sender<data::ClipboardFetch>,
    max_uncompressed_size_bytes: u64,
    config_dir: &PathBuf,
) -> Option<data::ClipboardData> {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!("Failed to create clipboard fetch runtime: {}", e);
            return None;
        }
    };
    rt.block_on(data::fetch_clipboard_data(
        fetch_data_tx,
        mime_type,
        max_uncompressed_size_bytes,
        config_dir,
    ))
}

/// Fetches clipboard data for a mime type on a background thread and stores it
/// in the shared cache. Doing this off the dispatch thread is what keeps the
/// wayland writer (and, through backpressure, the compositor) responsive while
/// clipboard managers hammer us with paste requests.
fn spawn_fetch(
    mime_type: String,
    fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
    max_uncompressed_size_bytes: u64,
    config_dir: PathBuf,
    clipboard_data: ClipboardCache,
) {
    std::thread::spawn(move || {
        if let Some(d) = fetch_sync(&mime_type, &fetch_data_tx, max_uncompressed_size_bytes, &config_dir) {
            debug!("Background-fetched clipboard type {}: {} bytes", d.requested_type, d.bytes.len());
            *clipboard_data.lock().unwrap() = Some((d.requested_type, d.bytes));
        }
    });
}

impl Dispatch<wl_seat::WlSeat, ()> for State {
    fn event(
        _state: &mut Self,
        _seat: &wl_seat::WlSeat,
        _event: <wl_seat::WlSeat as wayland_client::Proxy>::Event,
        _data: &(),
        _conn: &wayland_client::Connection,
        _qh: &wayland_client::QueueHandle<Self>,
    ) {
    }
}

impl Dispatch<WlRegistry, GlobalListContents> for State {
    fn event(
        _state: &mut Self,
        _proxy: &WlRegistry,
        _event: <WlRegistry as wayland_client::Proxy>::Event,
        _data: &GlobalListContents,
        _conn: &wayland_client::Connection,
        _qhandle: &wayland_client::QueueHandle<Self>,
    ) {
    }
}

impl_dispatch_manager!(State);

impl_dispatch_device!(State, wl_seat::WlSeat, |state: &mut Self, event, seat| {
    match event {
        Event::DataOffer { id } => id.destroy(),
        Event::PrimarySelection { .. } => {}
        Event::Finished => {
            if let Some(device) = state.seats.remove(seat) {
                device.destroy();
            }
        }
        _ => (),
    }
});

impl_dispatch_offer!(State);

impl_dispatch_source!(State, |state: &mut Self, source: data_control::Source, event| {
    match event {
        Event::Send { mime_type, fd } => {
            let prepared_state = if let Some(state) = state.prepared_copy_state.as_ref() {
                state
            } else {
                error!("Missing prepared_copy_state when serving paste request");
                return;
            };

            if !prepared_state.mime_types.contains(&mime_type) {
                error!("Requested type {} is not advertised={:?}", mime_type, prepared_state.mime_types);
                return;
            }

            // Storm detection: clipboard managers (wl-clip-persist, wl-paste
            // --watch) can fire dozens of paste requests per second. Warn once
            // per window so freeze reports can be correlated with storms.
            {
                let mut stats = prepared_state.send_stats.lock().unwrap();
                if stats.0.elapsed() >= std::time::Duration::from_secs(1) {
                    *stats = (std::time::Instant::now(), 0);
                }
                stats.1 += 1;
                if stats.1 == 20 {
                    warn!(
                        "Clipboard paste storm: 20 paste requests within {:.1}s (a clipboard manager is hammering us; correlate with input freezes)",
                        stats.0.elapsed().as_secs_f32()
                    );
                }
            }
            debug!("Serving paste request for type {}", mime_type);

            // Serve the paste on a background thread (fetch + write). The
            // dispatch thread must never block here: a slow fetch (remote
            // clipboard over a slow link) or a slow paste reader would
            // otherwise park it for seconds, backpressuring the compositor's
            // wayland connection to us during clipboard-manager storms and
            // freezing input.
            std::thread::spawn({
                let fetch_data_tx = prepared_state.fetch_data_tx.clone();
                let config_dir = prepared_state.config_dir.clone();
                let max_uncompressed_size_bytes = prepared_state.max_uncompressed_size_bytes;
                let clipboard_data = prepared_state.clipboard_data.clone();
                move || serve_send(mime_type, fd, fetch_data_tx, config_dir, max_uncompressed_size_bytes, clipboard_data)
            });
        }
        Event::Cancelled => source.destroy(),
        _ => (),
    }
});

/// Serves a paste (Send) request on a background thread: fetch the data (or
/// reuse the cache) and write it to the paste fd.
fn serve_send(
    mime_type: String,
    fd: std::os::fd::OwnedFd,
    fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
    config_dir: PathBuf,
    max_uncompressed_size_bytes: u64,
    clipboard_data: ClipboardCache,
) {
    let started = std::time::Instant::now();
    let bytes = {
        let cached = clipboard_data.lock().unwrap().clone();
        match cached {
            Some((cached_type, cached_bytes)) if cached_type == mime_type => {
                debug!("Reusing cached clipboard with type {}: {} bytes", mime_type, cached_bytes.len());
                cached_bytes
            }
            _ => {
                match fetch_sync(&mime_type, &fetch_data_tx, max_uncompressed_size_bytes, &config_dir) {
                    Some(d) => {
                        debug!("Background-fetched clipboard type {}: {} bytes", d.requested_type, d.bytes.len());
                        let bytes = d.bytes;
                        *clipboard_data.lock().unwrap() = Some((d.requested_type, bytes.clone()));
                        bytes
                    }
                    // Retryable fetch failure: serve empty, the next request retries.
                    None => Vec::new(),
                }
            }
        }
    };
    let byte_count = bytes.len();
    if let Err(err) = write_paste_fd(fd, &bytes) {
        if err.kind() == io::ErrorKind::BrokenPipe {
            // The paste requester closed the pipe before we could serve it
            // (e.g. 'wl-paste --watch' with a command that doesn't read stdin).
            debug!("Paste requester closed the pipe before clipboard could be served");
        } else {
            error!("Failed to write clipboard data: {}", err);
        }
    }
    // Slow serves are the freeze suspect: if a paste takes this long, any
    // backpressure on the compositor connection lasted at least as long.
    let elapsed = started.elapsed();
    if elapsed > std::time::Duration::from_secs(1) {
        warn!(
            "Serving paste request for type {} took {:.1}s ({} bytes)",
            mime_type,
            elapsed.as_secs_f32(),
            byte_count
        );
    }
}

/// Writes clipboard data to the paste fd with a deadline, so that a stuck
/// paste reader can't hang the serving thread forever.
fn write_paste_fd(fd: std::os::fd::OwnedFd, bytes: &[u8]) -> io::Result<u64> {
    fcntl_setfl(&fd, OFlags::NONBLOCK).map_err(io::Error::from)?;
    let mut file = File::from(fd);
    let raw_fd = file.as_raw_fd();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut written: usize = 0;
    while written < bytes.len() {
        match file.write(&bytes[written..]) {
            Ok(0) => break,
            Ok(n) => written += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    warn!(
                        "Timed out writing clipboard data to paste request, aborting ({} of {} bytes written)",
                        written,
                        bytes.len()
                    );
                    return Ok(written as u64);
                }
                let mut pfd = libc::pollfd {
                    fd: raw_fd,
                    events: libc::POLLOUT,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut pfd, 1, remaining.as_millis() as libc::c_int) } < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(written as u64)
}

/// Connects to the wayland environment, or returns Ok(None) if wayland isn't available
fn init_state() -> Result<(State, data_control::Manager, EventQueue<State>)> {
    let conn = Connection::connect_to_env()?;
    let (globals, queue) = registry_queue_init::<State>(&conn)?;
    let qh = queue.handle();

    let clipboard_manager = common::clipboard_manager(&globals, &qh)
        .context("Wayland missing both ext-data-control and wlr-data-control support")?;

    let mut seats = HashMap::new();
    for seat in common::seats(&globals, &qh) {
        let device = clipboard_manager.get_data_device(&seat, &qh, seat.clone());
        seats.insert(seat, device);
    }
    if seats.is_empty() {
        bail!("No wayland seats");
    }

    Ok((
        State{
            seats,
            prepared_copy_state: None,
        },
        clipboard_manager,
        queue,
    ))
}

/// Launches a thread to serve the provided clipboard data to wayland,
/// automatically exiting the thread when the clipboard gets overridden
fn write_clipboard(
    clipboard_type: ClipboardType,
    mut mime_types: Vec<String>,
    fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
    config_dir: PathBuf,
    max_uncompressed_size_bytes: u64,
) -> Result<()> {
    let (mut state, clipboard_manager, mut queue) = init_state()
        .context("Failed to init wayland session for clipboard write")?;

    // Sources stay empty when clearing the clipboard.
    let mut sources = vec![];
    if mime_types.is_empty() {
        // Clearing the clipboard: explicitly release the selection, so compositors
        // that require an explicit clear don't keep offering the stale clipboard.
        debug!("Clearing {:?} clipboard in wayland", clipboard_type);
        for device in state.seats.values() {
            device.set_selection(None);
        }
    } else {
        // Ensure the clipboard we're advertising includes the ignored type,
        // which ensures we don't treat this clipboard as if it's from another application source on the system.
        let ignored_type = state::IGNORED_MIME_TYPE.to_string();
        if !mime_types.contains(&ignored_type) {
            mime_types.push(ignored_type);
        }
        debug!("Advertising {:?} clipboard to wayland: {:?}", clipboard_type, mime_types);

        for device in state.seats.values() {
            let data_source = clipboard_manager.create_data_source(&queue.handle());

            for mime_type in &mime_types {
                data_source.offer(mime_type.clone());
            }

            device.set_selection(Some(&data_source));
            sources.push(data_source);
        }

        state.prepared_copy_state = Some(PreparedCopyState{
            mime_types: mime_types.clone(),
            fetch_data_tx: fetch_data_tx.clone(),
            config_dir: config_dir.clone(),
            max_uncompressed_size_bytes,
            clipboard_data: std::sync::Arc::new(std::sync::Mutex::new(None)),
            send_stats: std::sync::Arc::new(std::sync::Mutex::new((std::time::Instant::now(), 0))),
        });
        // Pre-fetch the primary mime type in the background so the cache is
        // warm before the first paste request arrives (skipping our own
        // ignored marker type).
        if let Some(primary) = mime_types.iter().find(|t| **t != state::IGNORED_MIME_TYPE).cloned() {
            let prepared = state.prepared_copy_state.as_ref().expect("just set");
            spawn_fetch(
                primary,
                fetch_data_tx.clone(),
                max_uncompressed_size_bytes,
                config_dir.clone(),
                prepared.clipboard_data.clone(),
            );
        }
    }

    // All queue dispatch (including the initial roundtrip that publishes the
    // sources or applies the clear) must happen on this dedicated plain thread:
    // the Send handler uses block_on, which panics if it ever runs on a tokio
    // worker thread, and a paste request (e.g. from a clipboard manager) can
    // legally arrive as early as the first roundtrip.
    // State also owns the tokio Runtime used for that block_on, so it must be
    // dropped on this plain thread too: dropping it in the caller's async
    // context panics ("Cannot drop a runtime in a context where blocking is
    // not allowed"), e.g. when clearing the clipboard after a connection loss.
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Result<()>>(1);
    let _ = std::thread::spawn(move || {
        if let Err(e) = queue.roundtrip(&mut state) {
            error!("Wayland roundtrip error when serving copy requests: {}", e);
            let _ = ready_tx.send(Err(e.into()));
            return;
        }
        if ready_tx.send(Ok(())).is_err() {
            error!("Failed to send ready_tx");
            return;
        }
        if sources.is_empty() {
            // Clipboard was cleared: the roundtrip above applied it, and
            // there is nothing to serve.
            trace!("Exiting clipboard serving thread after clear");
            return;
        }
        loop {
            if let Err(e) = queue.blocking_dispatch(&mut state) {
                error!("Wayland dispatch error when serving copy requests: {}", e);
                return;
            }
            if sources.iter().all(|x| !x.is_alive()) {
                // Clipboard has updated and the objects we're serving have been dropped
                break;
            }
        }
        trace!("Exiting clipboard serving thread");
    });
    // Wait for the thread to have published the clipboard (or failed to)
    match ready_rx.recv() {
        Ok(result) => result?,
        Err(e) => bail!("Clipboard serving thread died before startup: {}", e),
    }

    Ok(())
}

/// Task that advertises received clipboard types to local programs,
/// and fetches clipboard contents from monux in response to local type requests (pastes).
pub struct ClipboardWriter {
    clipboard_type: ClipboardType,
    config_dir: PathBuf,
    max_uncompressed_size_bytes: u64,
    /// Send available clipboard types, received from Monux server
    clipboard_fetch_tx: mpsc::Sender<data::ClipboardFetch>,
}

impl ClipboardWriter {
    pub fn new(
        clipboard_type: ClipboardType,
        config_dir: PathBuf,
        max_uncompressed_size_bytes: u64,
        clipboard_fetch_tx: mpsc::Sender<data::ClipboardFetch>,
    ) -> Self {
        Self {
            clipboard_type,
            config_dir,
            max_uncompressed_size_bytes,
            clipboard_fetch_tx,
        }
    }
}

impl ClipboardWriterTrait for ClipboardWriter {
    /// Advertises with the local environment that we have a new clipboard entry available
    fn store_types(&self, types: Vec<String>) -> Result<()> {
        write_clipboard(
            self.clipboard_type.clone(),
            types,
            self.clipboard_fetch_tx.clone(),
            self.config_dir.clone(),
            self.max_uncompressed_size_bytes,
        )
    }
}
