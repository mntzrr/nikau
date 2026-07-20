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
    /// Update the primary-selection clipboard (highlighted text style)
    Primary,
    /// Update both clipboards
    Both,
}

struct State {
    seats: HashMap<wl_seat::WlSeat, data_control::Device>,
    prepared_copy_state: Option<PreparedCopyState>,
    async_runtime: tokio::runtime::Runtime,
}

struct PreparedCopyState {
    mime_types: Vec<String>,
    fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
    config_dir: PathBuf,
    /// Safety limit to the uncompressed size of a clipboard
    max_uncompressed_size_bytes: u64,
    /// Populated with clipboard contents on the first retrieval
    clipboard_data: Option<data::ClipboardData>,
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
            let prepared_state = if let Some(state) = state.prepared_copy_state.as_mut() {
                state
            } else {
                error!("Missing prepared_copy_state when serving paste request");
                return;
            };

            if !prepared_state.mime_types.contains(&mime_type) {
                error!("Requested type {} is not advertised={:?}", mime_type, prepared_state.mime_types);
                return;
            }

            let copy_result = || {
                // We need to fetch if the data is missing or the requested type doesn't match what we have
                let needs_fetch = prepared_state.clipboard_data
                    .as_ref()
                    .map(|d| d.requested_type != mime_type)
                    .unwrap_or(true);
                if needs_fetch {
                    // Fetch missing data, setting data to None for retryable failure
                    prepared_state.clipboard_data = state.async_runtime.block_on(data::fetch_clipboard_data(
                        &prepared_state.fetch_data_tx,
                        &mime_type,
                        prepared_state.max_uncompressed_size_bytes,
                        &prepared_state.config_dir,
                    ));
                } else {
                    // Reuse fetched data
                    debug!(
                        "Reusing existing clipboard with type {}: {} bytes",
                        mime_type,
                        prepared_state.clipboard_data
                            .as_ref()
                            .map(|d| d.bytes.len())
                            .unwrap_or(0),
                    );
                }
                let bytes = if let Some(data) = &prepared_state.clipboard_data {
                    // Use cached or newly fetched data
                    &data.bytes
                } else {
                    // Fetch failed in a retryable way, use empty data
                    &vec![]
                };
                // Set O_NONBLOCK and write via a poll() loop with a deadline,
                // so that a stuck paste reader can't hang us forever.
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
            };

            if let Err(err) = copy_result() {
                if err.kind() == io::ErrorKind::BrokenPipe {
                    // The paste requester closed the pipe before we could serve it
                    // (e.g. 'wl-paste --watch' with a command that doesn't read stdin).
                    // Not an error: there is simply nobody left to serve.
                    debug!("Paste requester closed the pipe before clipboard could be served");
                } else {
                    error!("Failed to write clipboard data: {}", err);
                }
            }
        }
        Event::Cancelled => source.destroy(),
        _ => (),
    }
});

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
            async_runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
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
            match clipboard_type {
                ClipboardType::Regular => device.set_selection(None),
                ClipboardType::Primary => device.set_primary_selection(None),
                ClipboardType::Both => {
                    device.set_selection(None);
                    device.set_primary_selection(None);
                }
            }
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

            match clipboard_type {
                ClipboardType::Regular => {
                    device.set_selection(Some(&data_source));
                },
                ClipboardType::Primary => {
                    device.set_primary_selection(Some(&data_source));
                },
                ClipboardType::Both => {
                    device.set_selection(Some(&data_source));
                    device.set_primary_selection(Some(&data_source));
                },
            }
            sources.push(data_source);
        }

        state.prepared_copy_state = Some(PreparedCopyState{
            mime_types,
            fetch_data_tx,
            config_dir,
            max_uncompressed_size_bytes,
            clipboard_data: None,
        });
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
/// and fetches clipboard contents from nikau in response to local type requests (pastes).
pub struct ClipboardWriter {
    clipboard_type: ClipboardType,
    config_dir: PathBuf,
    max_uncompressed_size_bytes: u64,
    /// Send available clipboard types, received from Nikau server
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
