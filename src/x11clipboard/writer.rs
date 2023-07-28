use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tracing::{debug, info, trace, warn};
use x11rb_async::connection::Connection;
use x11rb_async::protocol::xproto::{
    Atom, AtomEnum, ChangeWindowAttributesAux, ConnectionExt, EventMask, PropMode, Property,
    SelectionNotifyEvent, Time, Window, SELECTION_NOTIFY_EVENT,
};
use x11rb_async::protocol::Event;
use x11rb_async::rust_connection::RustConnection;

use crate::x11clipboard::{shared, ClipboardData};

/// Max X11 property size. We ignore the 16M size reported by X11 via conn.maximum_request_bytes(),
/// which is apparently a lie because e.g. a 4M property will cause panics.
/// Meanwhile other applications are observed to use 262144 byte chunks when we fetch clipboards from them.
/// Let's go slightly smaller than that to ensure plenty of headroom and avoid panics.
const CLIPBOARD_MAX_CHUNK_BYTES: usize = 256000;

/// A fetch request, and a path for sending the response.
pub struct ClipboardFetch {
    /// The type that we want
    pub type_: String,
}

pub struct ClipboardWriter {
    /// Send available clipboard types, advertised by server
    store_types_tx: watch::Sender<Vec<String>>,
    /// Send clipboard content for one of the previously advertised types
    store_data_tx: mpsc::Sender<ClipboardData>,
}

impl ClipboardWriter {
    pub async fn new(fetch_data_tx: mpsc::Sender<ClipboardFetch>) -> Result<Self> {
        let context = shared::XContext::new().await?;
        let (store_types_tx, store_types_rx) = watch::channel(vec![]);
        let (store_data_tx, store_data_rx) = mpsc::channel(32);
        task::spawn(async move {
            if let Err(e) = serve(context, store_types_rx, fetch_data_tx, store_data_rx).await {
                warn!("clipboard server died: {}", e);
            }
        });
        Ok(ClipboardWriter {
            store_types_tx,
            store_data_tx,
        })
    }

    /// Advertises with X11 that we have a new clipboard entry available
    pub fn store_types<K: Into<Vec<String>>>(&self, types: K) -> Result<()> {
        self.store_types_tx.send(types.into())?;
        Ok(())
    }

    /// Makes the provided clipboard data available to X11 for a paste operation
    pub async fn store_data(&self, data: ClipboardData) -> Result<()> {
        // TODO(later) check if we're expecting a fetch and discard the data if not?
        self.store_data_tx.send(data).await?;
        Ok(())
    }
}

async fn serve(
    context: shared::XContext,
    // Receive available clipboard types, advertised by Nikau server
    // Events are from calls to store_types()
    mut store_types_rx: watch::Receiver<Vec<String>>,
    // Request clipboard content for one of the types received to store_types_rx
    fetch_data_tx: mpsc::Sender<ClipboardFetch>,
    // Receive clipboard content in response to a fetch_data_tx query
    // Events are from calls to store_data()
    mut store_data_rx: mpsc::Receiver<ClipboardData>,
) -> Result<()> {
    let mut state = ClipboardServerState::new(&context.conn).await?;
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
                        state.clipboard_types = None;
                    } else {
                        let mut type_atoms = Vec::with_capacity(clipboard_types.len());
                        for type_ in clipboard_types {
                            type_atoms.push((state.atoms.to_atom(&context.conn, &type_).await?, type_));
                        }
                        state.clipboard_types = Some(type_atoms);
                    }
                }
                state.clipboard_data = None;

                // Advertise the new clipboard (or lack thereof) to X11
                context.conn.set_selection_owner(
                    context.window,
                    state.atoms.clipboard,
                    Time::CURRENT_TIME
                ).await?.check().await?;
            },
            event = context.conn.wait_for_event() => {
                if let Ok(event) = event {
                    if let Err(e) = state.handle_event(event, &context, &fetch_data_tx, &mut store_data_rx).await {
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

struct ClipboardServerState {
    selection_to_property: HashMap<Atom, Atom>,
    property_to_state: HashMap<Atom, IncrState>,
    atoms: shared::Atoms,
    max_length: usize,
    /// Received via server advertisement
    clipboard_types: Option<Vec<(Atom, String)>>,
    /// Received in response to lookup from server
    clipboard_data: Option<ClipboardData>,
}

impl ClipboardServerState {
    async fn new(conn: &RustConnection) -> Result<Self> {
        let ret = ClipboardServerState {
            selection_to_property: HashMap::<Atom, Atom>::new(),
            property_to_state: HashMap::<Atom, IncrState>::new(),
            atoms: shared::Atoms::new(conn).await?,
            max_length: CLIPBOARD_MAX_CHUNK_BYTES,
            clipboard_types: None,
            clipboard_data: None,
        };
        Ok(ret)
    }

    async fn handle_event(
        &mut self,
        event: Event,
        context: &shared::XContext,
        fetch_data_tx: &mpsc::Sender<ClipboardFetch>,
        store_data_rx: &mut mpsc::Receiver<ClipboardData>,
    ) -> Result<()> {
        trace!("X11 writer/server event: {:?}", event);
        match event {
            Event::SelectionRequest(event) => {
                if let Some(clipboard_types) = &self.clipboard_types {
                    // We have a clipboard to advertise
                    if event.target == self.atoms.targets {
                        // request to get the available clipboard targets
                        debug!(
                            "Returning available clipboard types to requestor={}: {:?}",
                            event.requestor, clipboard_types
                        );
                        // TARGETS, NIKAU_REMOTE, and the data types themselves:
                        let target_count = 2 + clipboard_types.len();
                        let mut data_u8 = Vec::with_capacity(4 * target_count);
                        data_u8.extend(self.atoms.targets.to_ne_bytes());
                        data_u8.extend(self.atoms.nikau_remote.to_ne_bytes());
                        for type_ in clipboard_types {
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
                    } else {
                        // This is a clipboard retrieval.
                        // If we don't have the correct type data already, fetch it.
                        let target = match clipboard_types.iter().find(|t| t.0 == event.target) {
                            Some(t) => t,
                            None => bail!(
                                "Got request for clipboard type {} from requestor={} when we have {:?}",
                                event.target,
                                event.requestor,
                                clipboard_types
                            ),
                        };
                        let needs_fetch = self
                            .clipboard_data
                            .as_ref()
                            .map(|d| d.type_ != target.1)
                            .unwrap_or(true);
                        if needs_fetch {
                            debug!(
                                "Fetching clipboard with type {}={} for requestor={}",
                                target.0, target.1, event.requestor
                            );
                            fetch_data_tx
                                .send(ClipboardFetch {
                                    type_: target.1.clone(),
                                })
                                .await?;
                            // TODO(later) timeout on retrieving data, where we give up and return empty data?
                            let clipboard_data = store_data_rx
                                .recv()
                                .await
                                .context("failed to wait for clipboard data")?;
                            if clipboard_data.type_ != target.1 {
                                bail!("Requested clipboard type {} for requestor={}, but fetched clipboard had type {}", target.1, event.requestor, clipboard_data.type_);
                            }
                            info!(
                                "Providing clipboard data to requestor={} with type {}: {} bytes",
                                event.requestor,
                                clipboard_data.type_,
                                clipboard_data.data.len()
                            );
                            self.clipboard_data = Some(clipboard_data);
                        } else {
                            info!(
                                "Reusing existing clipboard content to requestor={} with type {}: {} bytes",
                                event.requestor,
                                target.1,
                                self.clipboard_data
                                    .as_ref()
                                    .map(|d| d.data.len())
                                    .unwrap_or(0),
                            );
                        }

                        let clipboard_data = match &self.clipboard_data {
                            Some(data) => data,
                            None => return Ok(()),
                        };
                        if clipboard_data.data.len() < self.max_length - 24 {
                            // Request to get clipboard content, and data fits within max_length
                            // If the size is too big, then the underlying X11 thread will panic here.
                            context
                                .conn
                                .change_property(
                                    PropMode::REPLACE,
                                    event.requestor,
                                    event.property,
                                    event.target,
                                    8,
                                    clipboard_data.data.len() as u32,
                                    &clipboard_data.data,
                                )
                                .await?;
                        } else {
                            // Request to get clipboard content, but data doesn't fit within max_length
                            context
                                .conn
                                .change_window_attributes(
                                    event.requestor,
                                    &ChangeWindowAttributesAux::new()
                                        .event_mask(EventMask::PROPERTY_CHANGE),
                                )
                                .await?;
                            context
                                .conn
                                .change_property(
                                    PropMode::REPLACE,
                                    event.requestor,
                                    event.property,
                                    self.atoms.incr,
                                    32,
                                    0,
                                    &[],
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
                let clipboard_data = match &self.clipboard_data {
                    Some(data) => data,
                    None => return Ok(()),
                };

                let mut len = clipboard_data.data.len() - state.pos;
                // Enforce a max size per chunk
                if len > self.max_length {
                    len = self.max_length;
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
