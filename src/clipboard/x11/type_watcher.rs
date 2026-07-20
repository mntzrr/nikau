use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::{sync::watch, task, time};
use tracing::{debug, warn};
use x11rb_async::connection::Connection;
use x11rb_async::protocol::xproto::{Atom, ConnectionExt};
use x11rb_async::protocol::xfixes;
use x11rb_async::x11_utils::TryParse;

use crate::clipboard::{CLIPBOARD_TIMEOUT_SECS, x11::{events, shared}};

/// Task that listens for updates to the clipboard types (local cut or copy).
/// Sends out an event when an update occurs, indicating a new clipboard is available.
pub struct ClipboardTypeWatcher {
    context: shared::XContext,
    atoms: shared::Atoms,
}

impl ClipboardTypeWatcher {
    pub async fn start(types_tx: watch::Sender<Vec<String>>) -> Result<()> {
        // Fail up-front if context can't be created at least once
        let mut watcher = new_watcher().await?;
        task::spawn(async move {
            loop {
                match watcher.types_wait().await {
                    Ok(types) => {
                        // We should only announce clipboard events from other applications.
                        // If we announce our own type updates, then something like this will happen:
                        // - we get advertised types pushed from server/client
                        // - we store the advertised types to X11 for future pastes into other applications
                        // - we see the update and think that another application took over the clipboard
                        if types.is_empty()
                            || types.contains(&shared::MONUX_REMOTE_TARGET.to_string())
                        {
                            debug!(
                                "Ignoring clipboard update that's empty or from monux itself: {:?}",
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
                        warn!("Failed to wait for new clipboard types: {}", e);
                        // This can happen if the context is lost (e.g. WM crash?). Try to create a new context.
                        // Wait a bit first so that a persistent failure doesn't hot-loop.
                        time::sleep(Duration::from_secs(CLIPBOARD_TIMEOUT_SECS)).await;
                        match new_watcher().await {
                            Ok(w) => {
                                watcher = w;
                            }
                            Err(e) => {
                                warn!("Failed to init new watcher: {}", e);
                            }
                        }
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
            atom_names.push(self.atoms.get_name(&self.context.conn, atom).await?);
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
        // Mirror the clipboard reader: don't let a stuck X11 exchange block forever.
        match time::timeout(
            Duration::from_secs(CLIPBOARD_TIMEOUT_SECS),
            events::process_event(
                &self.context,
                &self.atoms,
                &mut buf,
                0,
                target,
                self.atoms.recv_clipboard,
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_e) => {
                warn!("X11 clipboard type watch read timed out after {}s", CLIPBOARD_TIMEOUT_SECS);
                // Discard any partially-read data so we don't advertise a bogus type list.
                buf.clear();
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

async fn new_watcher() -> Result<ClipboardTypeWatcher> {
    let context = shared::XContext::new()
        .await
        .context("Failed to set up X11 API context")?;
    let atoms = shared::Atoms::new(&context.conn).await?;
    Ok(ClipboardTypeWatcher { context, atoms })
}

fn to_atoms(buf: &Vec<u8>) -> Result<Vec<Atom>> {
    if buf.len() % 4 != 0 {
        bail!("Expected u32s, but buf.len={}", buf.len());
    }
    let mut atoms: Vec<Atom> = Vec::new();
    let mut next = buf.as_slice();
    loop {
        if next.is_empty() {
            break;
        }
        if let Ok((atom, remaining)) = Atom::try_parse(next) {
            atoms.push(atom);
            next = remaining;
        } else {
            break;
        }
    }
    Ok(atoms)
}
