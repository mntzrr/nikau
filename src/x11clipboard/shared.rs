use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use tokio::task;
use x11rb_async::connection::Connection;
use x11rb_async::protocol::xproto::{
    Atom, ConnectionExt, CreateWindowAux, EventMask, Window, WindowClass,
};
use x11rb_async::rust_connection::RustConnection;

pub(crate) const CLIPBOARD_TIMEOUT_SECS: u64 = 5;

/// A clipboard type that we advertise to tell if we're serving a clipboard already
pub(crate) const NIKAU_REMOTE_TARGET: &str = "__NIKAU_REMOTE__";

pub(crate) struct Atoms {
    // atoms that are needed internally:
    pub(crate) clipboard: Atom,
    pub(crate) targets: Atom,
    pub(crate) nikau_remote: Atom,
    pub(crate) incr: Atom,
    pub(crate) recv_clipboard: Atom,

    atom_to_name: HashMap<Atom, String>,
    name_to_atom: HashMap<String, Atom>,
}

impl Atoms {
    pub(crate) async fn new(conn: &RustConnection) -> Result<Self> {
        let mut atoms = Atoms {
            // start with stub values:
            clipboard: Atom::MIN,
            targets: Atom::MIN,
            nikau_remote: Atom::MIN,
            incr: Atom::MIN,
            recv_clipboard: Atom::MIN,
            atom_to_name: HashMap::new(),
            name_to_atom: HashMap::new(),
        };
        // populate values and fill in cache maps:
        atoms.clipboard = atoms.get_atom(conn, "CLIPBOARD").await?;
        atoms.targets = atoms.get_atom(conn, "TARGETS").await?;
        atoms.nikau_remote = atoms.get_atom(conn, NIKAU_REMOTE_TARGET).await?;
        atoms.incr = atoms.get_atom(conn, "INCR").await?;
        atoms.recv_clipboard = atoms.get_atom(conn, "NIKAU_CLIPBOARD_OUT").await?;
        Ok(atoms)
    }

    pub(crate) async fn get_name(&mut self, conn: &RustConnection, atom: Atom) -> Result<String> {
        if let Some(name) = self.atom_to_name.get(&atom) {
            // cached
            Ok(name.clone())
        } else {
            // fetch
            let resp = x11rb_async::protocol::xproto::get_atom_name(conn, atom)
                .await
                .with_context(|| format!("bad atom={}", atom))?;
            let name = String::from_utf8_lossy(&resp.reply().await?.name).to_string();
            self.atom_to_name.insert(atom, name.clone());
            self.name_to_atom.insert(name.clone(), atom);
            Ok(name)
        }
    }

    pub(crate) async fn get_atom(&mut self, conn: &RustConnection, name: &str) -> Result<Atom> {
        if let Some(atom) = self.name_to_atom.get(name) {
            // cached
            Ok(*atom)
        } else {
            // fetch
            let atom = conn
                .intern_atom(false, name.as_bytes())
                .await
                .with_context(|| format!("bad atom_name={}", name))?
                .reply()
                .await?
                .atom;
            self.atom_to_name.insert(atom, name.to_string());
            self.name_to_atom.insert(name.to_string(), atom);
            Ok(atom)
        }
    }
}

pub(crate) struct XContext {
    pub(crate) conn: RustConnection,
    pub(crate) screen: usize,
    pub(crate) window: Window,
    _driver: task::JoinHandle<()>,
}

impl XContext {
    pub(crate) async fn new() -> Result<Self> {
        let (conn, screen, drive) = RustConnection::connect(None).await?;

        // Get driver running early, in particular before calling create_window() below
        let driver = task::spawn(async move {
            if let Err(e) = drive.await {
                tracing::error!("Error while driving the connection: {}", e);
            }
        });

        let window = conn.generate_id().await?;
        let screen_info = conn
            .setup()
            .roots
            .get(screen)
            .ok_or(anyhow!("xcb connection error: invalid screen"))?;

        conn.create_window(
            0, // COPY_DEPTH_FROM_PARENT
            window,
            screen_info.root,
            0,
            0,
            1,
            1,
            0,
            WindowClass::INPUT_OUTPUT,
            screen_info.root_visual,
            &CreateWindowAux::new()
                .event_mask(EventMask::STRUCTURE_NOTIFY | EventMask::PROPERTY_CHANGE),
        )
        .await?
        .check()
        .await?;

        Ok(XContext {
            conn,
            screen,
            window,
            _driver: driver,
        })
    }
}
