use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::{task, time};
use tracing::{debug, error, info, trace, warn};
use x11rb_async::connection::Connection;
use x11rb_async::protocol::xproto::{
    Atom, AtomEnum, ChangeWindowAttributesAux, ConnectionExt, EventMask, PropMode, Property,
    SelectionNotifyEvent, SelectionRequestEvent, Time, Window, SELECTION_NOTIFY_EVENT,
};
use x11rb_async::protocol::Event;
use x11rb_async::rust_connection::RustConnection;

use crate::x11clipboard::{convert, shared, ClipboardData};

/// Max X11 property size. We ignore the 16M size reported by X11 via conn.maximum_request_bytes(),
/// which is apparently a lie because e.g. a 4M property will cause panics.
/// Meanwhile other applications are observed to use 262144 byte chunks when we fetch clipboards from them.
/// Let's go slightly smaller than that to ensure plenty of headroom and avoid panics.
const CLIPBOARD_MAX_CHUNK_BYTES: usize = 256000;

/// A clipboard fetch request
pub struct ClipboardFetch {
    /// The type that we want. The resulting ClipboardData may have a different type.
    pub requested_type: String,

    /// The channel for sending back the result.
    pub fetch_result_tx: oneshot::Sender<ClipboardData>,
}

pub struct ClipboardWriter {
    /// Send available clipboard types, received from Nikau server
    store_types_tx: watch::Sender<Vec<String>>,
}

/// Launches an X11 background task for advertising received clipboard types,
/// and fetching clipboard contents in response to type requests (pastes).
impl ClipboardWriter {
    /// Launches the async background task and returns a call for sending clipboard type updates.
    /// fetch_data_tx is the call for requesting clipboard contents for a given type from Nikau.
    pub async fn start(
        config_dir: PathBuf,
        max_uncompressed_size_bytes: u64,
        fetch_data_tx: mpsc::Sender<ClipboardFetch>,
    ) -> Result<Self> {
        let context = shared::XContext::new()
            .await
            .context("Failed to set up X11 API context")?;
        let (store_types_tx, store_types_rx) = watch::channel(vec![]);
        task::spawn(async move {
            if let Err(e) = serve(
                config_dir,
                max_uncompressed_size_bytes,
                context,
                store_types_rx,
                fetch_data_tx,
            )
            .await
            {
                warn!("clipboard server died: {}", e);
            }
        });
        Ok(ClipboardWriter { store_types_tx })
    }

    /// Advertises with X11 that we have a new clipboard entry available
    pub fn store_types<K: Into<Vec<String>>>(&self, types: K) -> Result<()> {
        self.store_types_tx.send(types.into())?;
        Ok(())
    }
}

async fn serve(
    config_dir: PathBuf,
    max_uncompressed_size_bytes: u64,
    context: shared::XContext,
    // Receive available clipboard types, advertised by Nikau server.
    // Uses watch rather than an mpsc since we only care about the current/latest clipboard.
    mut store_types_rx: watch::Receiver<Vec<String>>,
    // Ask Nikau to get clipboard content, for one of the types previously advertised
    // via store_types()/store_types_rx. The ClipboardFetch has a oneshot for sending the data.
    fetch_data_tx: mpsc::Sender<ClipboardFetch>,
) -> Result<()> {
    let mut state =
        ClipboardServerState::new(&context.conn, config_dir, max_uncompressed_size_bytes).await?;
    loop {
        tokio::select! {
            types_notify = store_types_rx.changed() => {
                if let Err(e) = types_notify {
                    warn!("store_types_rx has closed: {}", e);
                    return Err(anyhow!(e));
                }

                // New (or cleared) clipboard: Update types, and clear any prior clipboard data
                {
                    let clipboard_types = store_types_rx.borrow().clone();
                    debug!("Received new clipboard types for serving locally: {}", clipboard_types.join(" "));
                    if clipboard_types.is_empty() {
                        // Treat empty types as a clipboard clear
                        state.clipboard = None;
                    } else {
                        let mut type_atoms = Vec::with_capacity(clipboard_types.len());
                        for type_ in clipboard_types {
                            type_atoms.push((state.atoms.get_atom(&context.conn, &type_).await?, type_));
                        }
                        state.clipboard = Some(ClipboardInfo{
                            types: type_atoms,
                            data: None,
                        });
                    }
                }

                // Advertise the new clipboard (or lack thereof) to X11
                context.conn.set_selection_owner(
                    context.window,
                    state.atoms.clipboard,
                    Time::CURRENT_TIME
                ).await?.check().await?;
            },
            event = context.conn.wait_for_event() => {
                if let Ok(event) = event {
                    if let Err(e) = state.handle_event(event, &context, &fetch_data_tx).await {
                        warn!("X11 event handling failed: {:?}", e);
                        // keep going...
                    }
                }
            }
        }
    }
}

struct IncrState {
    requestor: Window,
    property: Atom,
    target: Atom,
    pos: usize,
}

struct ClipboardInfo {
    /// Received via advertisement from a server or client
    types: Vec<(Atom, String)>,
    /// Received in response to retrieval request
    data: Option<ClipboardData>,
}

struct ClipboardServerState {
    config_dir: PathBuf,
    selection_to_property: HashMap<Atom, Atom>,
    property_to_state: HashMap<Atom, IncrState>,
    atoms: shared::Atoms,
    /// Safety limit to the uncompressed size of a clipboard
    max_uncompressed_size_bytes: u64,
    /// Large X11 clipboards are passed in chunks. Max size per chunk.
    max_chunk_size_bytes: usize,
    /// Populated with latest clipboard details
    clipboard: Option<ClipboardInfo>,
}

impl ClipboardServerState {
    async fn new(
        conn: &RustConnection,
        config_dir: PathBuf,
        max_uncompressed_size_bytes: u64,
    ) -> Result<Self> {
        let ret = ClipboardServerState {
            config_dir,
            selection_to_property: HashMap::<Atom, Atom>::new(),
            property_to_state: HashMap::<Atom, IncrState>::new(),
            atoms: shared::Atoms::new(conn).await?,
            max_uncompressed_size_bytes,
            max_chunk_size_bytes: CLIPBOARD_MAX_CHUNK_BYTES,
            clipboard: None,
        };
        Ok(ret)
    }

    async fn handle_event(
        &mut self,
        event: Event,
        context: &shared::XContext,
        fetch_data_tx: &mpsc::Sender<ClipboardFetch>,
    ) -> Result<()> {
        trace!("X11 writer/server event: {:?}", event);
        match event {
            Event::SelectionRequest(event) => {
                if let Some(clipboard) = &mut self.clipboard {
                    // We have a clipboard to advertise
                    if event.target == self.atoms.targets {
                        // request to get the available clipboard targets
                        debug!(
                            "Returning available clipboard types to requestor={}: {:?}",
                            event.requestor, clipboard.types
                        );
                        // TARGETS, NIKAU_REMOTE, and the data types themselves:
                        let target_count = 2 + clipboard.types.len();
                        let mut data_u8 = Vec::with_capacity(4 * target_count);
                        data_u8.extend(self.atoms.targets.to_ne_bytes());
                        data_u8.extend(self.atoms.nikau_remote.to_ne_bytes());
                        for type_ in &clipboard.types {
                            data_u8.extend(type_.0.to_ne_bytes());
                        }
                        context
                            .conn
                            .change_property(
                                PropMode::REPLACE,
                                event.requestor,
                                event.property,
                                Atom::from(AtomEnum::ATOM),
                                32,
                                target_count as u32,
                                &data_u8,
                            )
                            .await?;
                    } else if event.target == self.atoms.timestamp {
                        // Clients may ask for TIMESTAMP even if we don't advertise it.
                        // Let's keep it simple and just return the CURRENT_TIME, rather than tracking real time.
                        debug!("Returning clipboard timestamp to requestor={}", event.requestor);
                        context
                            .conn
                            .change_property(
                                PropMode::REPLACE,
                                event.requestor,
                                event.property,
                                Atom::from(AtomEnum::ATOM),
                                8,
                                1,
                                // CURRENT_TIME
                                &[0 as u8],
                            )
                            .await?;
                    } else {
                        // This is a clipboard retrieval.
                        // If we don't have the correct type data already, fetch it.
                        let target = match clipboard.types.iter().find(|t| t.0 == event.target) {
                            Some(t) => t,
                            None => bail!(
                                "Got request for clipboard type {} ({:?}) from requestor={} when we have {:?}",
                                event.target,
                                self.atoms.get_name(&context.conn, event.target).await,
                                event.requestor,
                                clipboard.types
                            ),
                        };
                        let needs_fetch = clipboard.data
                            .as_ref()
                            .map(|d| d.requested_type != target.1)
                            .unwrap_or(true);
                        if needs_fetch {
                            clipboard.data = Some(
                                fetch_clipboard_data(
                                    fetch_data_tx,
                                    &target.1,
                                    self.max_uncompressed_size_bytes,
                                    &event,
                                    &self.config_dir,
                                )
                                .await?,
                            );
                        } else {
                            info!(
                                "Reusing existing clipboard content to requestor={} with type {}: {} bytes",
                                event.requestor,
                                target.1,
                                clipboard.data
                                    .as_ref()
                                    .map(|d| d.data.len())
                                    .unwrap_or(0),
                            );
                        }

                        if let Some(data) = &clipboard.data {
                            send_clipboard_data(
                                &data.data,
                                &context,
                                &event,
                                self.max_chunk_size_bytes,
                                self.atoms.incr,
                            )
                            .await?;
                            self.selection_to_property
                                .insert(event.selection, event.property);
                            self.property_to_state.insert(
                                event.property,
                                IncrState {
                                    requestor: event.requestor,
                                    property: event.property,
                                    target: event.target,
                                    pos: 0,
                                },
                            );
                        } else {
                            return Ok(());
                        }
                    }
                }

                context
                    .conn
                    .send_event(
                        false,
                        event.requestor,
                        EventMask::default(),
                        SelectionNotifyEvent {
                            response_type: SELECTION_NOTIFY_EVENT,
                            sequence: 0,
                            time: event.time,
                            requestor: event.requestor,
                            selection: event.selection,
                            target: event.target,
                            property: event.property,
                        },
                    )
                    .await?;
                context.conn.flush().await?;
            }
            Event::PropertyNotify(event) => {
                if event.state != Property::DELETE {
                    return Ok(());
                };

                // Requestor has deleted the last chunk of clipboard content, write the next chunk
                let state = match self.property_to_state.get_mut(&event.atom) {
                    Some(val) => val,
                    None => return Ok(()),
                };
                let clipboard_data = match &self.clipboard {
                    Some(clipboard) => match &clipboard.data {
                        Some(data) => data,
                        None => return Ok(()),
                    },
                    None => return Ok(()),
                };

                let mut len = clipboard_data.data.len() - state.pos;
                // Enforce a max size per chunk
                if len > self.max_chunk_size_bytes {
                    len = self.max_chunk_size_bytes;
                }
                let data = &clipboard_data.data[state.pos..][..len];
                context
                    .conn
                    .change_property(
                        PropMode::REPLACE,
                        state.requestor,
                        state.property,
                        state.target,
                        8,
                        data.len() as u32,
                        data,
                    )
                    .await?;
                state.pos += len;
                if len == 0 {
                    self.property_to_state.remove(&event.atom);
                }
                context.conn.flush().await?;
            }
            Event::SelectionClear(event) => {
                if let Some(property) = self.selection_to_property.remove(&event.selection) {
                    self.property_to_state.remove(&property);
                }
            }
            _ => (),
        }
        Ok(())
    }
}

async fn fetch_clipboard_data(
    fetch_data_tx: &mpsc::Sender<ClipboardFetch>,
    requested_type: &str,
    max_uncompressed_size_bytes: u64,
    event: &SelectionRequestEvent,
    config_dir: &PathBuf,
) -> Result<ClipboardData> {
    debug!(
        "Fetching clipboard with type {} for requestor={}",
        requested_type, event.requestor
    );
    let (fetch_result_tx, fetch_result_rx) = oneshot::channel();
    fetch_data_tx
        .send(ClipboardFetch {
            requested_type: requested_type.to_string(),
            fetch_result_tx,
        })
        .await?;

    // Wait for response with clipboard data, or give up
    match time::timeout(
        Duration::from_secs(shared::CLIPBOARD_TIMEOUT_SECS),
        fetch_result_rx,
    )
    .await
    {
        Ok(Ok(mut clipboard_data)) => {
            if clipboard_data.requested_type != requested_type {
                error!("Returned clipboard type {} doesn't match requested type {} for requestor={}, writing empty clipboard", clipboard_data.requested_type, requested_type, event.requestor);
            } else {
                if let Some(data_type) = &clipboard_data.data_type {
                    clipboard_data.data = convert::write(
                        clipboard_data.data,
                        max_uncompressed_size_bytes,
                        requested_type,
                        data_type,
                        config_dir,
                    )
                    .await?;
                }
                info!(
                    "Writing clipboard data to requestor={} with type {}: {} bytes",
                    event.requestor,
                    clipboard_data.requested_type,
                    clipboard_data.data.len()
                );
                return Ok(clipboard_data);
            };
        }
        Ok(Err(e)) => {
            error!(
                "Waiting for clipboard data failed, writing empty clipboard: {}",
                e
            );
        }
        Err(_e) => {
            error!(
                "Waiting for clipboard data timed out after {}s, writing empty clipboard",
                shared::CLIPBOARD_TIMEOUT_SECS
            );
        }
    }

    // For timeout and conversion errors, return an empty clipboard entry to avoid things freezing up.
    Ok(ClipboardData {
        requested_type: requested_type.to_string(),
        data_type: None,
        data: vec![],
        remaining_bytes: 0,
    })
}

async fn send_clipboard_data(
    clipboard_data: &Vec<u8>,
    context: &shared::XContext,
    event: &SelectionRequestEvent,
    max_chunk_size_bytes: usize,
    incr_atom: Atom,
) -> Result<()> {
    if clipboard_data.len() < max_chunk_size_bytes - 24 {
        // Request to get clipboard content, and data fits within max_chunk_size_bytes
        // If the size is too big, then the underlying X11 thread will panic here.
        context
            .conn
            .change_property(
                PropMode::REPLACE,
                event.requestor,
                event.property,
                event.target,
                8,
                clipboard_data.len() as u32,
                clipboard_data,
            )
            .await?;
        return Ok(());
    }

    // Request to get clipboard content, but data doesn't fit within max_chunk_size_bytes
    context
        .conn
        .change_window_attributes(
            event.requestor,
            &ChangeWindowAttributesAux::new().event_mask(EventMask::PROPERTY_CHANGE),
        )
        .await?;
    context
        .conn
        .change_property(
            PropMode::REPLACE,
            event.requestor,
            event.property,
            incr_atom,
            32,
            0,
            &[],
        )
        .await?;
    Ok(())
}
