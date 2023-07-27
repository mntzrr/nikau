use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use tokio::{sync::watch, task, time};
use tracing::{debug, trace, warn};
use x11rb_async::connection::Connection;
use x11rb_async::protocol::xproto::{Atom, AtomEnum, ConnectionExt, Property, Time};
use x11rb_async::protocol::{xfixes, Event};
use x11rb_async::x11_utils::TryParse;

use crate::x11clipboard::shared;

const CLIPBOARD_TIMEOUT_SECS: u64 = 3;

/// Task that listens for updates to the clipboard types (local cut or copy).
/// Sends out an event when an update occurs, indicating a new clipboard is available.
pub struct ClipboardTypeWatcher {
    context: shared::XContext,
    atoms: shared::Atoms,
}

impl ClipboardTypeWatcher {
    pub async fn start(types_tx: watch::Sender<Vec<String>>) -> Result<()> {
        let context = shared::XContext::new().await?;
        let atoms = shared::Atoms::new(&context.conn).await?;
        task::spawn(async move {
            let mut watcher = Self { context, atoms };
            loop {
                match watcher.types_wait().await {
                    Ok(types) => {
                        // We should only announce clipboard events from other applications.
                        // If we announce our own type updates, then something like this will happen:
                        // - we get advertised types pushed from server/client
                        // - we store the advertised types to X11 for future pastes into other applications
                        // - we see the update and think that another application took over the clipboard
                        if types.is_empty()
                            || types.contains(&shared::NIKAU_REMOTE_TARGET.to_string())
                        {
                            debug!(
                                "Ignoring clipboard update that's empty or from nikau itself: {:?}",
                                types
                            );
                            continue;
                        }
                        debug!(
                            "Received updated clipboard from local system with types: {:?}",
                            types
                        );
                        if let Err(e) = types_tx.send(types) {
                            warn!("Failed to send updated clipboard types: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to wait for new clipboard types: {}", e)
                    }
                }
            }
        });
        Ok(())
    }

    async fn types_wait(&mut self) -> Result<Vec<String>> {
        let buf = self.read_wait(self.atoms.targets).await?;
        let mut atom_names = Vec::new();
        for atom in to_atoms(&buf)? {
            atom_names.push(self.atoms.to_name(&self.context.conn, atom).await?);
        }
        Ok(atom_names)
    }

    async fn read_wait(&self, target: Atom) -> Result<Vec<u8>> {
        let screen = &self
            .context
            .conn
            .setup()
            .roots
            .get(self.context.screen)
            .ok_or(anyhow!("xcb connection error: invalid screen"))?;

        xfixes::query_version(&self.context.conn, 5, 0).await?;
        xfixes::select_selection_input(
            &self.context.conn,
            screen.root,
            self.atoms.clipboard,
            xfixes::SelectionEventMask::default(),
        )
        .await?;
        xfixes::select_selection_input(
            &self.context.conn,
            screen.root,
            self.atoms.clipboard,
            xfixes::SelectionEventMask::SET_SELECTION_OWNER
                | xfixes::SelectionEventMask::SELECTION_CLIENT_CLOSE
                | xfixes::SelectionEventMask::SELECTION_WINDOW_DESTROY,
        )
        .await?
        .check()
        .await?;

        let mut buf = Vec::new();
        process_event(
            &self.context,
            &self.atoms,
            &mut buf,
            0,
            target,
            self.atoms.recv_clipboard,
        )
        .await?;

        self.context
            .conn
            .delete_property(self.context.window, self.atoms.recv_clipboard)
            .await?
            .check()
            .await?;

        Ok(buf)
    }
}

pub struct ClipboardReader {
    context: shared::XContext,
    atoms: shared::Atoms,
}

impl ClipboardReader {
    pub async fn new() -> Result<Self> {
        let context = shared::XContext::new().await?;
        let atoms = shared::Atoms::new(&context.conn).await?;
        Ok(Self { context, atoms })
    }

    /// Reads the clipboard data for the specified type.
    pub async fn read(
        &mut self,
        type_: &str,
        max_size_bytes: u64,
        request_client: &Option<SocketAddr>,
    ) -> Result<Vec<u8>> {
        debug!(
            "Reading local clipboard content as requested by {}: type={} max_size_bytes={}",
            if let Some(c) = request_client {
                format!("client {}", c)
            } else {
                format!("server")
            },
            type_,
            max_size_bytes
        );
        let type_atom = self.atoms.to_atom(&self.context.conn, type_).await?;

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
            process_event(
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
                warn!(
                    "X11 clipboard read timed out after {}s",
                    CLIPBOARD_TIMEOUT_SECS
                );
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

async fn process_event(
    context: &shared::XContext,
    atoms: &shared::Atoms,
    buf: &mut Vec<u8>,
    max_size_bytes: u64,
    target: Atom,
    property: Atom,
) -> Result<()> {
    let mut is_incr = false;
    loop {
        let event = context.conn.wait_for_event().await?;
        trace!("X11 reader event: {:?}", event);

        match event {
            Event::XfixesSelectionNotify(event) => {
                context
                    .conn
                    .convert_selection(
                        context.window,
                        atoms.clipboard,
                        target,
                        property,
                        event.timestamp,
                    )
                    .await?
                    .check()
                    .await?;
            }
            Event::SelectionNotify(event) => {
                if event.selection != atoms.clipboard {
                    continue;
                }
                if event.property == Atom::from(AtomEnum::NONE) {
                    break;
                }

                let reply = context
                    .conn
                    .get_property(
                        false,
                        context.window,
                        event.property,
                        AtomEnum::NONE,
                        // Fetch data as of this offset
                        buf.len() as u32,
                        u32::MAX,
                    )
                    .await?
                    .reply()
                    .await?;

                if reply.type_ == atoms.incr {
                    if let Some(mut value) = reply.value32() {
                        if let Some(size) = value.next() {
                            buf.reserve(size as usize);
                        }
                    }
                    context
                        .conn
                        .delete_property(context.window, property)
                        .await?
                        .check()
                        .await?;
                    is_incr = true;
                    continue;
                }

                buf.extend_from_slice(&reply.value);
                break;
            }
            Event::PropertyNotify(event) if is_incr => {
                if event.state != Property::NEW_VALUE {
                    continue;
                };

                let length = context
                    .conn
                    .get_property(false, context.window, property, AtomEnum::NONE, 0, 0)
                    .await?
                    .reply()
                    .await?
                    .bytes_after;

                let reply = context
                    .conn
                    .get_property(true, context.window, property, AtomEnum::NONE, 0, length)
                    .await?
                    .reply()
                    .await?;
                if reply.type_ != target {
                    continue;
                };

                if reply.value.is_empty() {
                    // End of data
                    break;
                }

                if max_size_bytes > 0 && (buf.len() + reply.value.len()) > max_size_bytes as usize {
                    // When this happens, we still need to send _something_ back,
                    // so that the receiving client (and its WM) can stop waiting.
                    // So let's just send back a zero-byte clipboard, which isn't great but probably won't hurt.
                    warn!(
                        "Sending empty clipboard data: size read so far ({}) exceeds max={}",
                        buf.len() + reply.value.len(),
                        max_size_bytes
                    );
                    buf.clear();
                    break;
                }

                buf.extend_from_slice(&reply.value);
            }
            _ => (),
        }
    }
    Ok(())
}

fn to_atoms(buf: &Vec<u8>) -> Result<Vec<Atom>> {
    if buf.len() % 4 != 0 {
        bail!("Expected u32s, but buf.len={}", buf.len());
    }
    let mut atoms: Vec<Atom> = Vec::new();
    let mut next = buf.as_slice();
    loop {
        if next.len() <= 0 {
            break;
        }
        if let Ok((atom, remaining)) = Atom::try_parse(&next) {
            atoms.push(atom);
            next = remaining;
        } else {
            break;
        }
    }
    Ok(atoms)
}
