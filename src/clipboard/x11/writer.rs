use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tracing::{debug, trace, warn};
use x11rb_async::connection::Connection;
use x11rb_async::protocol::xproto::{
    Atom, AtomEnum, ChangeWindowAttributesAux, ConnectionExt, EventMask, PropMode, Property,
    SelectionNotifyEvent, SelectionRequestEvent, Time, Window, SELECTION_NOTIFY_EVENT,
};
use x11rb_async::protocol::Event;
use x11rb_async::rust_connection::RustConnection;

use crate::clipboard::{ClipboardWriter as ClipboardWriterTrait, data, x11::shared};

/// Max X11 property size. We ignore the 16M size reported by X11 via conn.maximum_request_bytes(),
/// which is apparently a lie because e.g. a 4M property will cause panics.
/// Meanwhile other applications are observed to use 262144 byte chunks when we fetch clipboards from them.
/// Let's go slightly smaller than that to ensure plenty of headroom and avoid panics.
const CLIPBOARD_MAX_CHUNK_BYTES: usize = 256000;

/// Task that advertises received clipboard types to local programs,
/// and fetches clipboard contents from monux in response to local type requests (pastes).
pub struct ClipboardWriter {
    /// Send available clipboard types, received from Monux server
    store_types_tx: watch::Sender<Vec<String>>,
}

impl ClipboardWriter {
    /// Launches the async background task and returns a call for sending clipboard type updates.
    /// fetch_data_tx is the call for requesting clipboard contents for a given type from Monux.
    pub async fn start(
        config_dir: PathBuf,
        max_uncompressed_size_bytes: u64,
        fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
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
}

impl ClipboardWriterTrait for ClipboardWriter {
    /// Advertises with the local environment that we have a new clipboard entry available
    fn store_types(&self, types: Vec<String>) -> Result<()> {
        self.store_types_tx.send(types)?;
        Ok(())
    }
}

async fn serve(
    config_dir: PathBuf,
    max_uncompressed_size_bytes: u64,
    context: shared::XContext,
    // Receive available clipboard types, advertised by Monux server.
    // Uses watch rather than an mpsc since we only care about the current/latest clipboard.
    mut store_types_rx: watch::Receiver<Vec<String>>,
    // Ask Monux to get clipboard content, for one of the types previously advertised
    // via store_types()/store_types_rx. The ClipboardFetch has a oneshot for sending the data.
    fetch_data_tx: mpsc::Sender<data::ClipboardFetch>,
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
                        state.clipboard_info = None;
                    } else {
                        let mut type_atoms = Vec::with_capacity(clipboard_types.len());
                        for type_ in clipboard_types {
                            type_atoms.push((state.atoms.get_atom(&context.conn, &type_).await?, type_));
                        }
                        state.clipboard_info = Some(ClipboardInfo{
                            types: type_atoms,
                            data: None,
                        });
                    }
                    // Clear any selections that are in progress.
                    // This avoids potential weirdness where the content changes mid-copy.
                    state.selection_to_property.clear();
                    state.property_to_state.clear();
                }

                // Advertise the new clipboard (or lack thereof) to X11
                let owner = if state.clipboard_info.is_some() {
                    context.window
                } else {
                    // Clipboard was cleared: release ownership rather than holding it with nothing to serve.
                    Window::from(AtomEnum::NONE)
                };
                context.conn.set_selection_owner(
                    owner,
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
    data: Option<data::ClipboardData>,
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
    clipboard_info: Option<ClipboardInfo>,
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
            clipboard_info: None,
        };
        Ok(ret)
    }

    async fn handle_event(
        &mut self,
        event: Event,
        context: &shared::XContext,
        fetch_data_tx: &mpsc::Sender<data::ClipboardFetch>,
    ) -> Result<()> {
        trace!("X11 writer/server event: {:?}", event);
        match event {
            Event::SelectionRequest(event) => {
                if let Some(clipboard_info) = &mut self.clipboard_info {
                    // We have a clipboard to advertise
                    if event.target == self.atoms.targets {
                        // This is a request to get the available clipboard targets
                        debug!(
                            "Returning available clipboard types to requestor={}: {:?}",
                            event.requestor, clipboard_info.types
                        );
                        // TARGETS, MONUX_REMOTE, and the data types themselves:
                        let target_count = 2 + clipboard_info.types.len();
                        let mut data_u8 = Vec::with_capacity(4 * target_count);
                        data_u8.extend(self.atoms.targets.to_ne_bytes());
                        data_u8.extend(self.atoms.monux_remote.to_ne_bytes());
                        for type_ in &clipboard_info.types {
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
                        debug!(
                            "Returning clipboard timestamp to requestor={}",
                            event.requestor
                        );
                        context
                            .conn
                            .change_property(
                                PropMode::REPLACE,
                                event.requestor,
                                event.property,
                                Atom::from(AtomEnum::INTEGER),
                                32,
                                1,
                                // CURRENT_TIME
                                &0u32.to_ne_bytes(),
                            )
                            .await?;
                    } else {
                        // This is a clipboard retrieval.
                        // If we don't have the correct type data already, fetch it.
                        let target = match clipboard_info.types.iter().find(|t| t.0 == event.target) {
                            Some(t) => t,
                            None => bail!(
                                "Got request for clipboard type {} ({:?}) from requestor={} when we have {:?}",
                                event.target,
                                self.atoms.get_name(&context.conn, event.target).await,
                                event.requestor,
                                clipboard_info.types
                            ),
                        };
                        // We need to fetch if the data is missing or the requested type doesn't match what we have
                        let needs_fetch = clipboard_info
                            .data
                            .as_ref()
                            .map(|d| d.requested_type != target.1)
                            .unwrap_or(true);
                        if needs_fetch {
                            // Saves clipboard data, or None if there's a retryable failure
                            clipboard_info.data = data::fetch_clipboard_data(
                                fetch_data_tx,
                                &target.1,
                                self.max_uncompressed_size_bytes,
                                &self.config_dir,
                            ).await;
                        } else {
                            debug!(
                                "Reusing existing clipboard content to requestor={} with type {}: {} bytes",
                                event.requestor,
                                target.1,
                                clipboard_info.data
                                    .as_ref()
                                    .map(|d| d.bytes.len())
                                    .unwrap_or(0),
                            );
                        }

                        let bytes = if let Some(data) = &clipboard_info.data {
                            &data.bytes
                        } else {
                            // Data is missing due to retryable failure, send empty bytes
                            &vec![]
                        };
                        if let Err(e) = send_clipboard_data(
                            bytes,
                            &context,
                            &event,
                            self.max_chunk_size_bytes,
                            self.atoms.incr,
                        ).await {
                            // Sending failed: drop any stale transfer state for this request.
                            self.selection_to_property.remove(&event.selection);
                            self.property_to_state.remove(&event.property);
                            return Err(e);
                        }
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
                    }
                }

                // If we have no clipboard to serve (e.g. it was cleared), refuse the
                // request by reporting a NONE property, rather than claiming success.
                let property = if self.clipboard_info.is_some() {
                    event.property
                } else {
                    Atom::from(AtomEnum::NONE)
                };
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
                            property,
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
                if !self.property_to_state.contains_key(&event.atom) {
                    return Ok(());
                }
                let clipboard_bytes = match &self.clipboard_info {
                    Some(clipboard_info) => match &clipboard_info.data {
                        Some(bytes) => bytes,
                        // Clipboard data is gone mid-transfer: abort and drop the transfer state
                        None => {
                            self.property_to_state.remove(&event.atom);
                            return Ok(());
                        }
                    },
                    None => {
                        self.property_to_state.remove(&event.atom);
                        return Ok(());
                    }
                };
                let state = self
                    .property_to_state
                    .get_mut(&event.atom)
                    .expect("property_to_state entry checked above");

                let mut len = clipboard_bytes.bytes.len().saturating_sub(state.pos);
                // Enforce a max size per chunk
                if len > self.max_chunk_size_bytes {
                    len = self.max_chunk_size_bytes;
                }
                // If pos is past the end (stale state, e.g. the clipboard changed mid-copy),
                // len is 0: an empty final chunk is sent, ending the transfer below.
                let data = &clipboard_bytes.bytes[state.pos.min(clipboard_bytes.bytes.len())..][..len];
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
