//! Local control IPC: both daemons (server and client) publish their live
//! state and accept a small command set over a per-user unix socket. This is
//! the backend of `monux system status` and of the tray indicator
//! (`monux system indicator`), so the field names below are STABLE — the tray
//! consumes them.
//!
//! # Socket location
//!
//! `$XDG_RUNTIME_DIR/monux/server.sock` and `$XDG_RUNTIME_DIR/monux/client.sock`.
//! When XDG_RUNTIME_DIR is unset, `/tmp/monux-<uid>/` is used instead. The
//! directory is created 0700 and the socket file is removed on shutdown. A
//! socket file left by a crashed instance is reclaimed; a path that answers a
//! connect belongs to another live daemon and is left alone (the daemon logs a
//! warning and runs without control IPC).
//!
//! # Security
//!
//! Same-user only: XDG_RUNTIME_DIR is 0700 per the XDG spec and the /tmp
//! fallback dir is created 0700 as well, so no other user can reach the
//! socket. There is no authentication beyond that — any process running as
//! the same user can drive switches, pause, updates and shutdown, exactly as
//! it already could via signals (SIGUSR1/SIGUSR2/SIGTERM).
//!
//! # Wire protocol (newline-delimited JSON)
//!
//! One JSON object per line in each direction; every request line gets
//! exactly one response line. Requests:
//!
//! - `{"cmd":"status"}` — the daemon's live state (schema below).
//! - `{"cmd":"diagnostics"}` — a troubleshooting bundle for bug reports
//!   (schema below); the tray indicator's "Copy diagnostics" uses it.
//! - `{"cmd":"switch","target":"next"|"prev"|"local"|<fingerprint-prefix>}` —
//!   rotate input to another machine (server socket only).
//! - `{"cmd":"pause"}` / `{"cmd":"resume"}` — suspend/resume input handling
//!   (server socket only). These are IDEMPOTENT, unlike the pause hotkey's
//!   toggle: pausing an already-paused server is a no-op success, so a GUI
//!   can send the command matching the state it wants without reading first.
//! - `{"cmd":"update_now"}` — wake the background auto-update check
//!   immediately instead of waiting for the daily tick.
//! - `{"cmd":"indicator","action":"hide"|"show"}` — hide the auto-spawned
//!   tray indicator (SIGTERM the daemon's spawned indicator child, and keep
//!   it down: no spawns/respawns) or show it again (spawn immediately when
//!   none is running). The hidden state is in-memory only: a daemon restart
//!   always starts the indicator fresh. show is REFUSED when the daemon runs
//!   with --no-indicator — an explicit opt-out the socket may not override.
//!   Manually-started indicators are never managed by this. The tray menu's
//!   "Hide tray icon" and `monux system tray hide|show` drive this command.
//! - `{"cmd":"restart"}` — graceful shutdown, then re-exec into the installed
//!   binary (the auto-updater's restart path).
//! - `{"cmd":"exit"}` — graceful shutdown.
//!
//! Responses: `{"ok":true,"state":{...}}` for status,
//! `{"ok":true,"diagnostics":{...}}` for diagnostics, `{"ok":true}` for
//! accepted commands, `{"ok":false,"error":"..."}` on failure (unknown
//! command, wrong role, missing target, event queue full, ...). A command's
//! `ok` means it was accepted by the daemon's event loop — the effect (e.g.
//! a rotation switch) lands asynchronously; poll status to observe it. The
//! server socket serves the full command set; the client socket serves only
//! status/diagnostics/update_now/indicator/restart/exit (rotation and pause
//! are server concepts).
//!
//! # Diagnostics schema (the `diagnostics` object of a diagnostics response)
//!
//! Served by both roles:
//! - `version`, `protocol_version`, `role`: as in the status state
//! - `state_dump`: the server's SIGHUP rotation-state dump string
//!   (rotation::DiagnosticsMirror::state_dump); for the client role, the
//!   human-readable rendering of the client mirror's state
//! - `recent_logs`: the daemon's last ~50 log lines (the logging.rs ring
//!   buffer), oldest first; empty when nothing was logged yet
//!
//! # State schema (the `state` object of a status response)
//!
//! Server:
//! - `role`: "server"
//! - `version`: crate version string, e.g. "1.4.0"
//! - `protocol_version`: wire protocol version (int)
//! - `listen`: QUIC listen address, "ip:port"
//! - `paused`: bool — input handling suspended (see pause/resume)
//! - `current_target`: "local" or the addr of the client owning input
//! - `clients`: array of `{addr, fingerprint, connected_since_secs, rtt_ms}`
//!   (rtt_ms from QUIC path stats, null when unavailable)
//! - `clipboard`: `{owner: "none"|"local"|client addr, types: [mime strings]}`
//! - `update_available`: sha of a newer commit the auto-updater has seen,
//!   or null
//!
//! Client:
//! - `role`: "client"
//! - `version`, `protocol_version`: as above
//! - `server`: server address the client connects to, "ip:port"
//! - `connected`: bool — a session is currently established
//! - `active`: bool — this client currently owns the server's input
//!   (the ServerEvent::Switch state)
//! - `connected_since_secs`, `rtt_ms`, `lost_packets`: connection age, QUIC
//!   path RTT in ms, and cumulative lost packets; all null while disconnected

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::device::Event;
use crate::msgs::shared::PROTOCOL_VERSION;
use crate::rotation::DiagnosticsMirror;

/// Longest accepted request line, ENFORCED by capping the read itself (see
/// serve_connection: take() presents EOF at the cap, so a same-user peer
/// sending a never-terminated line gets a protocol error instead of growing
/// our read buffer without bound). Requests are tiny; the cap is generous.
const MAX_REQUEST_LINE: usize = 8192;

/// Which daemon a control socket belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

impl Role {
    fn socket_name(&self) -> &'static str {
        match self {
            Role::Server => "server.sock",
            Role::Client => "client.sock",
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Role::Server => "server",
            Role::Client => "client",
        }
    }
}

/// The directory holding the control sockets, honoring XDG_RUNTIME_DIR (also
/// the test override) with a per-user /tmp fallback.
fn socket_dir_from(runtime_dir: Option<&std::ffi::OsStr>) -> PathBuf {
    match runtime_dir {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("monux"),
        _ => PathBuf::from(format!("/tmp/monux-{}", unsafe { libc::geteuid() })),
    }
}

/// The directory holding the control sockets for this process.
pub fn socket_dir() -> PathBuf {
    socket_dir_from(std::env::var_os("XDG_RUNTIME_DIR").as_deref())
}

/// The default socket path for `role`.
pub fn socket_path(role: Role) -> PathBuf {
    socket_dir().join(role.socket_name())
}

/// Live server state, published in status responses (see module docs).
/// The rotation loop refreshes a snapshot of this in the DiagnosticsMirror
/// after every loop iteration. The `role` key on the wire comes from the
/// State enum's tag when this is wrapped for sending.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerState {
    pub version: String,
    pub protocol_version: u64,
    /// QUIC listen address "ip:port". Filled by the mirror (the rotation loop
    /// doesn't know it), so snapshots built by the loop leave it empty.
    pub listen: String,
    pub paused: bool,
    /// "local" or the addr of the client currently owning input.
    pub current_target: String,
    pub clients: Vec<ServerClientState>,
    pub clipboard: ServerClipboardState,
    /// Sha of a newer commit seen by the auto-updater, if any.
    pub update_available: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerClientState {
    pub addr: String,
    pub fingerprint: String,
    pub connected_since_secs: u64,
    pub rtt_ms: Option<u64>,
    /// The edge-map direction this client resolves to on the server, if any
    /// (for verifying --edge-map without testing edges).
    #[serde(default)]
    pub edge: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerClipboardState {
    /// "none", "local", or the addr of the client owning the clipboard.
    pub owner: String,
    pub types: Vec<String>,
}

/// Live client state, published in status responses (see module docs).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientState {
    pub version: String,
    pub protocol_version: u64,
    /// Server address this client connects to.
    pub server: String,
    pub connected: bool,
    /// Whether this client currently owns the server's input.
    pub active: bool,
    pub connected_since_secs: Option<u64>,
    pub rtt_ms: Option<u64>,
    pub lost_packets: Option<u64>,
}

/// Either daemon's state, parsed by the status CLI (`role` discriminates).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum State {
    Server(ServerState),
    Client(ClientState),
}

impl std::fmt::Display for State {
    /// Human-readable rendering for `monux system status`.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            State::Server(s) => {
                writeln!(f, "monux server v{} (protocol {})", s.version, s.protocol_version)?;
                writeln!(f, "  listening:      {}", s.listen)?;
                writeln!(f, "  paused:         {}", yes_no(s.paused))?;
                writeln!(f, "  current target: {}", s.current_target)?;
                match &s.update_available {
                    Some(sha) => writeln!(f, "  update:         available ({})", sha)?,
                    None => writeln!(f, "  update:         up to date")?,
                }
                writeln!(f, "clipboard:")?;
                writeln!(f, "  owner:          {}", s.clipboard.owner)?;
                if s.clipboard.types.is_empty() {
                    writeln!(f, "  types:          -")?;
                } else {
                    writeln!(f, "  types:          {}", s.clipboard.types.join(", "))?;
                }
                writeln!(f, "clients ({}):", s.clients.len())?;
                for c in &s.clients {
                    // Lead with the fingerprint prefix that --edge-map and
                    // --shortcut-goto accept, so it's copy-paste ready.
                    let prefix: String = c.fingerprint.chars().take(8).collect();
                    writeln!(
                        f,
                        "  {} fingerprint {} (prefix: {}) connected {}s ago, rtt {}",
                        c.addr,
                        c.fingerprint,
                        prefix,
                        c.connected_since_secs,
                        c.rtt_ms
                            .map(|rtt| format!("{}ms", rtt))
                            .unwrap_or_else(|| "?".to_string())
                    )?;
                }
                Ok(())
            }
            State::Client(s) => {
                writeln!(f, "monux client v{} (protocol {})", s.version, s.protocol_version)?;
                writeln!(f, "  server:         {}", s.server)?;
                match (s.connected, s.connected_since_secs) {
                    (true, age) => writeln!(
                        f,
                        "  connected:      yes ({}, rtt {}, {} packets lost)",
                        age.map(|secs| format!("for {}s", secs))
                            .unwrap_or_else(|| "just now".to_string()),
                        s.rtt_ms
                            .map(|rtt| format!("{}ms", rtt))
                            .unwrap_or_else(|| "?".to_string()),
                        s.lost_packets.unwrap_or(0)
                    )?,
                    (false, _) => writeln!(f, "  connected:      no")?,
                }
                writeln!(f, "  active:         {}", yes_no(s.active))?;
                Ok(())
            }
        }
    }
}

fn yes_no(v: bool) -> &'static str {
    if v {
        "yes"
    } else {
        "no"
    }
}

/// Mirror of the client daemon's live state for the control socket (the
/// client-side analog of the rotation's DiagnosticsMirror). Written by the
/// connection lifecycle in main.rs and the Switch handler in client.rs; read
/// by the socket task. Stats are sampled live from the QUIC handle at query
/// time, so a status request always sees the current RTT.
pub struct ClientStateMirror {
    inner: Mutex<ClientStateInner>,
}

struct ClientStateInner {
    server: SocketAddr,
    connected: bool,
    active: bool,
    connected_at: Option<Instant>,
    /// Live connection handle for path stats; cleared on disconnect.
    conn: Option<quinn::Connection>,
}

impl ClientStateMirror {
    pub fn new(server: SocketAddr) -> Self {
        Self {
            inner: Mutex::new(ClientStateInner {
                server,
                connected: false,
                active: false,
                connected_at: None,
                conn: None,
            }),
        }
    }

    /// The reconnect loop re-discovered the server elsewhere.
    pub fn set_server(&self, server: SocketAddr) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.server = server;
        }
    }

    /// A session was established (called once per successful connect).
    pub fn set_connected(&self, conn: quinn::Connection) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.connected = true;
            inner.active = false;
            inner.connected_at = Some(Instant::now());
            inner.conn = Some(conn);
        }
    }

    /// The session dropped (or is about to be retried after a failure).
    pub fn set_disconnected(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.connected = false;
            inner.active = false;
            inner.connected_at = None;
            inner.conn = None;
        }
    }

    /// The server switched input to or away from this client.
    pub fn set_active(&self, active: bool) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.active = active;
        }
    }

    /// Builds the status state, sampling QUIC path stats live when connected.
    pub fn snapshot(&self) -> ClientState {
        let (server, connected, active, connected_at, conn) = match self.inner.lock() {
            Ok(inner) => (
                inner.server,
                inner.connected,
                inner.active,
                inner.connected_at,
                inner.conn.clone(),
            ),
            Err(_) => (
                "0.0.0.0:0".parse().expect("valid fallback addr"),
                false,
                false,
                None,
                None,
            ),
        };
        let (connected_since_secs, rtt_ms, lost_packets) = if connected {
            let stats = conn.as_ref().map(|c| c.stats());
            (
                connected_at.map(|at| at.elapsed().as_secs()),
                stats.map(|s| s.path.rtt.as_millis() as u64),
                stats.map(|s| s.path.lost_packets),
            )
        } else {
            (None, None, None)
        };
        ClientState {
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: PROTOCOL_VERSION,
            server: server.to_string(),
            connected,
            active,
            connected_since_secs,
            rtt_ms,
            lost_packets,
        }
    }
}

/// A parsed request line (see module docs for the protocol).
#[derive(Debug, Deserialize)]
pub struct Request {
    pub cmd: String,
    pub target: Option<String>,
    /// Sub-action for commands that need one ("indicator": hide|show).
    pub action: Option<String>,
}

/// Troubleshooting bundle served by `{"cmd":"diagnostics"}` (see module docs
/// for the schema). The tray indicator formats and copies this for bug
/// reports.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Diagnostics {
    pub version: String,
    pub protocol_version: u64,
    pub role: String,
    /// SIGHUP dump string (server) or client state rendering (client).
    pub state_dump: String,
    /// The daemon's last ~50 log lines, oldest first.
    pub recent_logs: Vec<String>,
}

impl Diagnostics {
    fn server(mirror: &DiagnosticsMirror) -> Self {
        Diagnostics {
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: PROTOCOL_VERSION,
            role: Role::Server.as_str().to_string(),
            state_dump: mirror.state_dump(),
            recent_logs: crate::logging::recent_logs(crate::logging::RECENT_LOGS_DEFAULT),
        }
    }

    fn client(mirror: &ClientStateMirror) -> Self {
        Diagnostics {
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: PROTOCOL_VERSION,
            role: Role::Client.as_str().to_string(),
            state_dump: State::Client(mirror.snapshot())
                .to_string()
                .trim_end()
                .to_string(),
            recent_logs: crate::logging::recent_logs(crate::logging::RECENT_LOGS_DEFAULT),
        }
    }
}

/// The single response line sent back for every request.
#[derive(Debug, Serialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<Diagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    fn ok_empty() -> Self {
        Response {
            ok: true,
            state: None,
            diagnostics: None,
            error: None,
        }
    }

    fn ok_state(state: impl Serialize) -> Self {
        Response {
            ok: true,
            state: serde_json::to_value(state).ok(),
            diagnostics: None,
            error: None,
        }
    }

    fn ok_diagnostics(diagnostics: Diagnostics) -> Self {
        Response {
            ok: true,
            state: None,
            diagnostics: Some(diagnostics),
            error: None,
        }
    }

    fn err(error: impl Into<String>) -> Self {
        Response {
            ok: false,
            state: None,
            diagnostics: None,
            error: Some(error.into()),
        }
    }
}

/// A deferred effect of a command, executed only AFTER the response has been
/// written and flushed, so the peer reliably sees the ack first.
#[derive(Debug, PartialEq, Eq)]
enum PostAction {
    /// Graceful shutdown, then re-exec into the installed binary.
    Restart,
    /// Graceful shutdown.
    Exit,
    /// SIGTERM the spawned tray indicator. Deferred because the requester is
    /// usually the indicator itself (its "Hide tray icon" menu action): the
    /// ack must be on the wire before the requester is killed, or it would
    /// report its own hide as a failure.
    IndicatorHide,
}

/// Command/context bundle for the server socket.
pub struct ServerHandler {
    /// Structured live state, refreshed by the rotation loop.
    pub state: Arc<DiagnosticsMirror>,
    /// Commands enter the server events loop through the same channel as the
    /// hotkey/signal paths — the socket task never touches rotation state.
    pub event_tx: mpsc::Sender<Event>,
    /// Whether the background auto-updater is running (update_now otherwise
    /// errors clearly instead of silently doing nothing).
    pub auto_update: bool,
    /// Hide/show control for the auto-spawned tray indicator.
    pub indicator: crate::indicator_spawn::SupervisorHandle,
}

/// Command/context bundle for the client socket.
pub struct ClientHandler {
    pub state: Arc<ClientStateMirror>,
    pub auto_update: bool,
    /// Hide/show control for the auto-spawned tray indicator.
    pub indicator: crate::indicator_spawn::SupervisorHandle,
}

pub enum Handler {
    Server(ServerHandler),
    Client(ClientHandler),
}

impl Handler {
    /// Validates and dispatches one request. Shared commands behave the same
    /// on both roles; rotation/pause are server-only (see module docs).
    async fn dispatch(&self, req: &Request) -> (Response, Option<PostAction>) {
        match req.cmd.as_str() {
            "status" => match self {
                Handler::Server(h) => match h.state.server_state() {
                    Some(state) => (Response::ok_state(State::Server(state)), None),
                    None => (
                        Response::err("state not available yet (rotation loop has not run)"),
                        None,
                    ),
                },
                Handler::Client(h) => (Response::ok_state(State::Client(h.state.snapshot())), None),
            },
            "diagnostics" => match self {
                Handler::Server(h) => (
                    Response::ok_diagnostics(Diagnostics::server(&h.state)),
                    None,
                ),
                Handler::Client(h) => (
                    Response::ok_diagnostics(Diagnostics::client(&h.state)),
                    None,
                ),
            },
            "update_now" => {
                let auto_update = match self {
                    Handler::Server(h) => h.auto_update,
                    Handler::Client(h) => h.auto_update,
                };
                if auto_update {
                    crate::autoupdate::hint_update_available();
                    info!("Control socket: update check requested");
                    (Response::ok_empty(), None)
                } else {
                    (
                        Response::err("auto-update is disabled (--no-auto-update)"),
                        None,
                    )
                }
            }
            "restart" => {
                info!("Control socket: restart requested");
                (Response::ok_empty(), Some(PostAction::Restart))
            }
            "exit" => {
                info!("Control socket: exit requested");
                (Response::ok_empty(), Some(PostAction::Exit))
            }
            "indicator" => {
                let indicator = match self {
                    Handler::Server(h) => &h.indicator,
                    Handler::Client(h) => &h.indicator,
                };
                match req.action.as_deref() {
                    // Deferred (see PostAction::IndicatorHide): the requester
                    // is usually the indicator about to be killed.
                    Some("hide") => {
                        info!("Control socket: tray indicator hide requested");
                        (Response::ok_empty(), Some(PostAction::IndicatorHide))
                    }
                    // Synchronous: spawn errors belong in the response.
                    Some("show") => {
                        info!("Control socket: tray indicator show requested");
                        match indicator.show() {
                            Ok(()) => (Response::ok_empty(), None),
                            Err(e) => (Response::err(format!("{:#}", e)), None),
                        }
                    }
                    _ => (
                        Response::err("indicator needs an action: hide|show"),
                        None,
                    ),
                }
            }
            "switch" | "pause" | "resume" => match self {
                Handler::Client(_) => (
                    Response::err(format!(
                        "'{}' is a server-side command (this is the client socket)",
                        req.cmd
                    )),
                    None,
                ),
                Handler::Server(h) => {
                    let event = match req.cmd.as_str() {
                        "switch" => match req.target.as_deref() {
                            Some("next") => Event::SwitchNext,
                            Some("prev") => Event::SwitchPrev,
                            Some("local") => Event::SwitchTo(String::new()),
                            Some(prefix) if !prefix.is_empty() => {
                                Event::SwitchTo(prefix.to_string())
                            }
                            _ => {
                                return (
                                    Response::err(
                                        "switch needs a target: next|prev|local|<fingerprint-prefix>",
                                    ),
                                    None,
                                )
                            }
                        },
                        "pause" => Event::SetPaused(true),
                        // "resume"
                        _ => Event::SetPaused(false),
                    };
                    // Non-blocking hand-off: a full queue means the events
                    // loop is stalled, which the caller should know about
                    // instead of waiting on it.
                    match h.event_tx.try_send(event) {
                        Ok(()) => (Response::ok_empty(), None),
                        Err(_) => (
                            Response::err("server event queue is full (events loop stalled?)"),
                            None,
                        ),
                    }
                }
            },
            other => (Response::err(format!("unknown command '{}'", other)), None),
        }
    }
}

/// A bound control socket. Owns the socket file: dropping removes it, so a
/// clean shutdown never leaves a stale path behind. (A crash still can —
/// bind() reclaims stale files.)
pub struct Listener {
    listener: tokio::net::UnixListener,
    path: PathBuf,
}

impl Listener {
    /// Binds the default socket path for `role` (see socket_path).
    pub fn bind(role: Role) -> Result<Listener> {
        let dir = socket_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create control socket dir {}", dir.display()))?;
        // 0700 even if the dir pre-existed with looser perms: the socket is
        // same-user only (see module docs).
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        Self::bind_at(&dir.join(role.socket_name()), role.as_str())
    }

    /// Binds an explicit socket path. `role` is only used in error messages.
    fn bind_at(path: &Path, role: &str) -> Result<Listener> {
        if path.exists() {
            // A connect succeeds only when a live daemon owns the path: refuse
            // to hijack it. Otherwise it's a stale file from a crash; reclaim.
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                bail!(
                    "Refusing to take over control socket {}: another monux {} is serving it",
                    path.display(),
                    role
                );
            }
            std::fs::remove_file(path).with_context(|| {
                format!("Failed to remove stale control socket {}", path.display())
            })?;
        }
        let listener = tokio::net::UnixListener::bind(path)
            .with_context(|| format!("Failed to bind control socket {}", path.display()))?;
        info!("Control socket listening: {}", path.display());
        Ok(Listener {
            listener,
            path: path.to_path_buf(),
        })
    }

    /// Accepts connections forever; each is served by its own task so a slow
    /// or stuck peer never blocks the daemon's event loops (or other peers).
    pub async fn run(self, handler: Handler) -> Result<()> {
        let handler = Arc::new(handler);
        loop {
            let (stream, _) = self.listener.accept().await?;
            let handler = handler.clone();
            tokio::task::spawn(async move {
                if let Err(e) = serve_connection(stream, handler).await {
                    debug!("Control socket connection ended: {:?}", e);
                }
            });
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Serves one control connection: newline-delimited requests in, one response
/// line each out, until the peer closes or misbehaves.
async fn serve_connection(stream: tokio::net::UnixStream, handler: Arc<Handler>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = read;
    loop {
        let mut line = String::new();
        // Bound the read itself, not just the result: take() presents EOF at
        // the cap, so read_line stops there instead of buffering a
        // never-terminated line without bound. The capacity-1 buffer reads
        // byte-by-byte, so it never swallows bytes belonging to the NEXT
        // request line (a connection may carry several, pipelined).
        let n = {
            let limited = AsyncReadExt::take(&mut read, MAX_REQUEST_LINE as u64 + 1);
            tokio::io::BufReader::with_capacity(1, limited)
                .read_line(&mut line)
                .await?
        };
        if n == 0 {
            return Ok(()); // peer closed
        }
        if line.len() > MAX_REQUEST_LINE + 1 || !line.ends_with('\n') {
            return Err(anyhow!("request line exceeds {} bytes", MAX_REQUEST_LINE));
        }
        let (response, post_action) = match serde_json::from_str::<Request>(line.trim()) {
            Ok(req) => handler.dispatch(&req).await,
            Err(e) => (Response::err(format!("invalid request: {}", e)), None),
        };
        let mut out = serde_json::to_string(&response)?;
        out.push('\n');
        write.write_all(out.as_bytes()).await?;
        write.flush().await?;
        // Deferred effects run only after the ack is on the wire.
        match post_action {
            Some(PostAction::Restart) => crate::autoupdate::schedule_restart(),
            Some(PostAction::Exit) => {
                // The same graceful shutdown as SIGTERM (main.rs).
                unsafe {
                    libc::kill(std::process::id() as i32, libc::SIGTERM);
                }
            }
            Some(PostAction::IndicatorHide) => match &*handler {
                Handler::Server(h) => h.indicator.hide(),
                Handler::Client(h) => h.indicator.hide(),
            },
            None => {}
        }
    }
}

/// Longest a synchronous socket request may block. The daemon answers in
/// microseconds (dispatch only reads mirrors or hands off to a channel), so
/// hitting this means the daemon is wedged; the tray indicator's poll loop
/// must not hang on that.
const SOCKET_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Sends one request to a control socket and returns the raw response line.
/// Synchronous with a short timeout: used by the short-lived
/// `monux system status` CLI and the tray indicator's poll loop.
pub fn request_line(socket: &Path, request: &str) -> Result<String> {
    use std::io::{BufRead, BufReader, Write};
    let stream = std::os::unix::net::UnixStream::connect(socket)
        .with_context(|| format!("Failed to connect to {}", socket.display()))?;
    stream
        .set_read_timeout(Some(SOCKET_TIMEOUT))
        .context("Failed to set control socket read timeout")?;
    stream
        .set_write_timeout(Some(SOCKET_TIMEOUT))
        .context("Failed to set control socket write timeout")?;
    let mut writer = stream
        .try_clone()
        .context("Failed to clone control socket stream")?;
    writer.write_all(request.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    let mut response = String::new();
    let n = BufReader::new(stream).read_line(&mut response)?;
    if n == 0 {
        bail!("{} closed without a response", socket.display());
    }
    Ok(response.trim_end().to_string())
}

/// Wire view of a response for the CLI (state parsed separately when ok).
#[derive(Deserialize)]
struct RawResponse {
    ok: bool,
    state: Option<serde_json::Value>,
    error: Option<String>,
}

/// Implements `monux system status`: queries a daemon's control socket and
/// returns the text to print — the raw response line with `json`, otherwise a
/// human-readable summary. `server`/`client` restrict the default discovery
/// to that role's socket; `socket` overrides discovery entirely.
pub fn status_cli(
    server: bool,
    client: bool,
    socket: Option<&Path>,
    json: bool,
) -> Result<String> {
    let candidates: Vec<PathBuf> = match (socket, server, client) {
        (Some(path), _, _) => vec![path.to_path_buf()],
        (None, true, false) => vec![socket_path(Role::Server)],
        (None, false, true) => vec![socket_path(Role::Client)],
        (None, false, false) => {
            // Default: a machine usually runs either role — try the server
            // socket first, then the client's.
            vec![socket_path(Role::Server), socket_path(Role::Client)]
        }
        (None, true, true) => bail!("--server and --client are mutually exclusive"),
    };
    let (path, raw) = query_first(&candidates, r#"{"cmd":"status"}"#)?;
    format_status(&path, &raw, json)
}

/// Implements the `monux daemon` management verbs: sends a daemon-management
/// command (switch/pause/resume/restart/exit/update_now) to the control
/// socket (server socket first, then the client's) and returns the text to
/// print. The daemon's error string propagates — e.g. switch/pause from a
/// client socket, an unknown switch target, or --no-auto-update on update.
pub fn daemon_cli(request: &str, ok_message: &str, socket: Option<&Path>) -> Result<String> {
    let candidates: Vec<PathBuf> = match socket {
        Some(path) => vec![path.to_path_buf()],
        None => vec![socket_path(Role::Server), socket_path(Role::Client)],
    };
    let (path, raw) = query_first(&candidates, request)?;
    let response: RawResponse = serde_json::from_str(&raw)
        .with_context(|| format!("Malformed response from {}: {}", path.display(), raw))?;
    if !response.ok {
        bail!(
            "The daemon reported an error: {}",
            response.error.unwrap_or_default()
        );
    }
    Ok(ok_message.to_string())
}

/// Implements `monux system tray hide|show`: sends the indicator hide/show
/// command to the daemon's control socket (server socket first, then the
/// client's, exactly like status discovery; `socket` overrides) and returns
/// the text to print. The daemon's error string propagates — e.g. the
/// refusal to override a --no-indicator daemon on show.
pub fn tray_cli(hide: bool, socket: Option<&Path>) -> Result<String> {
    let candidates: Vec<PathBuf> = match socket {
        Some(path) => vec![path.to_path_buf()],
        None => vec![socket_path(Role::Server), socket_path(Role::Client)],
    };
    let action = if hide { "hide" } else { "show" };
    let request = format!(r#"{{"cmd":"indicator","action":"{}"}}"#, action);
    let (path, raw) = query_first(&candidates, &request)?;
    let response: RawResponse = serde_json::from_str(&raw)
        .with_context(|| format!("Malformed response from {}: {}", path.display(), raw))?;
    if !response.ok {
        bail!(
            "The daemon reported an error: {}",
            response.error.unwrap_or_default()
        );
    }
    Ok(if hide {
        "Tray indicator hidden (no respawns until 'monux system tray show' or a daemon restart)".to_string()
    } else {
        "Tray indicator shown".to_string()
    })
}

/// Implements `monux system clients`: lists the server's connected clients
/// with fingerprint prefixes and resolved edge directions — the reference for
/// configuring and verifying --edge-map.
pub fn clients_cli(socket: Option<&Path>) -> Result<String> {
    let candidates: Vec<PathBuf> = match socket {
        Some(path) => vec![path.to_path_buf()],
        None => vec![socket_path(Role::Server)],
    };
    let (path, raw) = query_first(&candidates, r#"{"cmd":"status"}"#)?;
    let response: RawResponse = serde_json::from_str(&raw)
        .with_context(|| format!("Malformed response from {}: {}", path.display(), raw))?;
    if !response.ok {
        bail!(
            "The daemon reported an error: {}",
            response.error.unwrap_or_default()
        );
    }
    let state: State = serde_json::from_value(
        response.state.context("The daemon returned no state")?,
    )
    .with_context(|| format!("Unrecognized state from {}", path.display()))?;
    let State::Server(server) = state else {
        bail!("This machine is running a monux client, not a server");
    };
    if server.clients.is_empty() {
        return Ok("No clients connected.".to_string());
    }
    let mut out = String::from("prefix   addr                     rtt    edge\n");
    for c in &server.clients {
        let prefix: String = c.fingerprint.chars().take(8).collect();
        let edge = c.edge.as_deref().unwrap_or("-");
        let rtt = c
            .rtt_ms
            .map(|r| format!("{}ms", r))
            .unwrap_or_else(|| "?".to_string());
        out.push_str(&format!("{:<8} {:<24} {:<6} {}\n", prefix, c.addr, rtt, edge));
    }
    Ok(out.trim_end().to_string())
}

/// Sends `request` to the first candidate socket that answers, returning the
/// answering path and the raw response line. The first socket that answers
/// wins; missing files and stale sockets (crash remnants) fall through to
/// the next candidate. Shared by status_cli, clients_cli and tray_cli.
fn query_first(candidates: &[PathBuf], request: &str) -> Result<(PathBuf, String)> {
    let mut last_err = None;
    for path in candidates {
        if !path.exists() {
            continue;
        }
        match request_line(path, request) {
            Ok(raw) => return Ok((path.clone(), raw)),
            Err(e) => {
                debug!("Control socket {} unusable: {:?}", path.display(), e);
                last_err = Some(e);
            }
        }
    }
    match last_err {
        // A socket existed but didn't answer.
        Some(e) => Err(e).with_context(|| {
            format!(
                "Failed to query the monux daemon at {} — is monux running?",
                candidates
                    .iter()
                    .filter(|p| p.exists())
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }),
        None => bail!(
            "No monux control socket found (tried {}) — is monux running?",
            candidates
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Renders one raw status response line for the CLI: verbatim with `json`,
/// parsed and pretty-printed otherwise.
fn format_status(path: &Path, raw: &str, json: bool) -> Result<String> {
    if json {
        return Ok(raw.to_string());
    }
    let response: RawResponse = serde_json::from_str(raw)
        .with_context(|| format!("Malformed response from {}: {}", path.display(), raw))?;
    if !response.ok {
        bail!(
            "The daemon reported an error: {}",
            response.error.unwrap_or_default()
        );
    }
    let state: State = serde_json::from_value(response.state.context("The daemon returned no state")?)
        .with_context(|| format!("Unrecognized state from {}", path.display()))?;
    Ok(state.to_string().trim_end().to_string())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn req(cmd: &str, target: Option<&str>) -> Request {
        Request {
            cmd: cmd.to_string(),
            target: target.map(|t| t.to_string()),
            action: None,
        }
    }

    fn req_action(cmd: &str, action: Option<&str>) -> Request {
        Request {
            cmd: cmd.to_string(),
            target: None,
            action: action.map(|a| a.to_string()),
        }
    }

    /// A supervisor handle whose daemon opted out of the indicator: no
    /// child, no task, and show() refuses — all without touching the
    /// environment or spawning processes.
    fn opted_out_indicator() -> crate::indicator_spawn::SupervisorHandle {
        let supervisor = crate::indicator_spawn::Supervisor::new(true);
        let handle = supervisor.handle();
        // The guard must outlive the handle: its Drop flips the shutdown
        // flag the handle checks (by design, for the daemon-exit window).
        // Test handlers never drop their fields anyway.
        std::mem::forget(supervisor);
        handle
    }

    fn server_handler(
        event_tx: mpsc::Sender<Event>,
        auto_update: bool,
    ) -> Handler {
        Handler::Server(ServerHandler {
            state: Arc::new(DiagnosticsMirror::new("127.0.0.1:1".parse().unwrap())),
            event_tx,
            auto_update,
            indicator: opted_out_indicator(),
        })
    }

    fn client_handler(auto_update: bool) -> (Handler, Arc<ClientStateMirror>) {
        let mirror = Arc::new(ClientStateMirror::new("127.0.0.1:9999".parse().unwrap()));
        (
            Handler::Client(ClientHandler {
                state: mirror.clone(),
                auto_update,
                indicator: opted_out_indicator(),
            }),
            mirror,
        )
    }

    #[test]
    fn socket_dir_resolution() {
        // XDG_RUNTIME_DIR is honored (also the test override)...
        assert_eq!(
            socket_dir_from(Some(std::ffi::OsStr::new("/run/user/1000"))),
            PathBuf::from("/run/user/1000/monux")
        );
        // ...and unset/empty falls back to a per-user /tmp dir.
        let fallback = PathBuf::from(format!("/tmp/monux-{}", unsafe { libc::geteuid() }));
        assert_eq!(socket_dir_from(None), fallback);
        assert_eq!(socket_dir_from(Some(std::ffi::OsStr::new(""))), fallback);
    }

    #[test]
    fn request_parsing() {
        let r: Request = serde_json::from_str(r#"{"cmd":"status"}"#).unwrap();
        assert_eq!(r.cmd, "status");
        assert!(r.target.is_none());
        let r: Request = serde_json::from_str(r#"{"cmd":"switch","target":"d1d88653"}"#).unwrap();
        assert_eq!(r.cmd, "switch");
        assert_eq!(r.target.as_deref(), Some("d1d88653"));
        // The indicator command carries an action.
        let r: Request = serde_json::from_str(r#"{"cmd":"indicator","action":"hide"}"#).unwrap();
        assert_eq!(r.cmd, "indicator");
        assert_eq!(r.action.as_deref(), Some("hide"));
        assert!(r.target.is_none());
        // Unknown fields are ignored, so newer peers keep working.
        let r: Request = serde_json::from_str(r#"{"cmd":"exit","extra":42}"#).unwrap();
        assert_eq!(r.cmd, "exit");
        // Garbage is rejected by the caller as an invalid request.
        assert!(serde_json::from_str::<Request>("not json").is_err());
        assert!(serde_json::from_str::<Request>(r#"{"nope":1}"#).is_err());
    }

    #[test]
    fn response_wire_shapes() {
        // A command ack is exactly {"ok":true} — no state/error keys.
        let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&Response::ok_empty()).unwrap()).unwrap();
        assert_eq!(v, serde_json::json!({"ok": true}));
        // Failures are exactly {"ok":false,"error":"..."}.
        let v: serde_json::Value = serde_json::from_str(
            &serde_json::to_string(&Response::err("boom")).unwrap(),
        )
        .unwrap();
        assert_eq!(v, serde_json::json!({"ok": false, "error": "boom"}));
        // Status carries the state under "state".
        let (handler, _mirror) = client_handler(true);
        let mirror_state = match &handler {
            Handler::Client(h) => h.state.snapshot(),
            _ => unreachable!(),
        };
        let v: serde_json::Value = serde_json::from_str(
            &serde_json::to_string(&Response::ok_state(State::Client(mirror_state))).unwrap(),
        )
        .unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["state"]["role"], "client");
        assert!(v.get("error").is_none());
    }

    #[tokio::test]
    async fn server_commands_become_events() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let handler = server_handler(event_tx, false);

        // switch targets map onto the same events the hotkeys send.
        let (resp, post) = handler.dispatch(&req("switch", Some("next"))).await;
        assert!(resp.ok && post.is_none());
        assert!(matches!(event_rx.recv().await, Some(Event::SwitchNext)));
        let (resp, _) = handler.dispatch(&req("switch", Some("prev"))).await;
        assert!(resp.ok);
        assert!(matches!(event_rx.recv().await, Some(Event::SwitchPrev)));
        // "local" is the empty-fingerprint goto; a prefix goes through as-is.
        let (resp, _) = handler.dispatch(&req("switch", Some("local"))).await;
        assert!(resp.ok);
        match event_rx.recv().await {
            Some(Event::SwitchTo(f)) => assert!(f.is_empty()),
            other => panic!("expected SwitchTo, got {:?}", other),
        }
        let (resp, _) = handler.dispatch(&req("switch", Some("d1d8"))).await;
        assert!(resp.ok);
        match event_rx.recv().await {
            Some(Event::SwitchTo(f)) => assert_eq!(f, "d1d8"),
            other => panic!("expected SwitchTo, got {:?}", other),
        }
        // pause/resume are explicit state sets (idempotent downstream).
        let (resp, _) = handler.dispatch(&req("pause", None)).await;
        assert!(resp.ok);
        assert!(matches!(event_rx.recv().await, Some(Event::SetPaused(true))));
        let (resp, _) = handler.dispatch(&req("resume", None)).await;
        assert!(resp.ok);
        assert!(matches!(event_rx.recv().await, Some(Event::SetPaused(false))));

        // A switch without a target is a validation error, not an event.
        let (resp, _) = handler.dispatch(&req("switch", None)).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("needs a target"));
        let (resp, _) = handler.dispatch(&req("switch", Some(""))).await;
        assert!(!resp.ok);

        // Unknown commands are rejected.
        let (resp, _) = handler.dispatch(&req("explode", None)).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("unknown command"));

        // update_now errors clearly when the auto-updater isn't running.
        let (resp, _) = handler.dispatch(&req("update_now", None)).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("auto-update is disabled"));

        // restart/exit ack first and act after the response is flushed.
        let (resp, post) = handler.dispatch(&req("restart", None)).await;
        assert!(resp.ok && post == Some(PostAction::Restart));
        let (resp, post) = handler.dispatch(&req("exit", None)).await;
        assert!(resp.ok && post == Some(PostAction::Exit));

        // Status before the rotation loop's first refresh is a clear error.
        let (resp, _) = handler.dispatch(&req("status", None)).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("not available yet"));
    }

    #[tokio::test]
    async fn server_dispatch_reports_a_full_event_queue() {
        let (event_tx, _event_rx) = mpsc::channel(1);
        event_tx.try_send(Event::SwitchNext).unwrap(); // now full
        let handler = server_handler(event_tx, true);
        let (resp, _) = handler.dispatch(&req("pause", None)).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("queue is full"));
    }

    #[tokio::test]
    async fn client_socket_serves_only_its_command_set() {
        let (handler, _mirror) = client_handler(true);

        // status works and reports a disconnected client.
        let (resp, _) = handler.dispatch(&req("status", None)).await;
        assert!(resp.ok);
        let state = resp.state.unwrap();
        assert_eq!(state["role"], "client");
        assert_eq!(state["server"], "127.0.0.1:9999");
        assert_eq!(state["connected"], false);
        assert_eq!(state["active"], false);
        assert!(state["connected_since_secs"].is_null());
        assert!(state["rtt_ms"].is_null());
        assert!(state["lost_packets"].is_null());

        // Rotation and pause are server concepts: clear role error.
        for cmd in ["switch", "pause", "resume"] {
            let (resp, _) = handler.dispatch(&req(cmd, Some("next"))).await;
            assert!(!resp.ok, "{} must fail on the client socket", cmd);
            assert!(resp.error.unwrap().contains("server-side"));
        }

        // The lifecycle commands are shared.
        let (resp, _) = handler.dispatch(&req("update_now", None)).await;
        assert!(resp.ok);
        let (resp, post) = handler.dispatch(&req("exit", None)).await;
        assert!(resp.ok && post == Some(PostAction::Exit));
    }

    #[tokio::test]
    async fn indicator_command_maps_to_the_supervisor_handle() {
        // Both roles serve it (the test handles sit on opted-out
        // supervisors: no child, no task, show refused).
        let (event_tx, _event_rx) = mpsc::channel(8);
        let handler = server_handler(event_tx, false);
        // hide acks and defers the SIGTERM until after the response is on
        // the wire (the requester is usually the indicator itself).
        let (resp, post) = handler.dispatch(&req_action("indicator", Some("hide"))).await;
        assert!(resp.ok && post == Some(PostAction::IndicatorHide));
        // show must not override an explicit --no-indicator opt-out.
        let (resp, post) = handler.dispatch(&req_action("indicator", Some("show"))).await;
        assert!(!resp.ok && post.is_none());
        let err = resp.error.unwrap();
        assert!(err.contains("--no-indicator"), "unexpected error: {}", err);
        // Missing or unknown actions are validation errors.
        let (resp, _) = handler.dispatch(&req_action("indicator", None)).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("hide|show"));
        let (resp, _) = handler.dispatch(&req_action("indicator", Some("blink"))).await;
        assert!(!resp.ok);

        let (handler, _mirror) = client_handler(true);
        let (resp, post) = handler.dispatch(&req_action("indicator", Some("hide"))).await;
        assert!(resp.ok && post == Some(PostAction::IndicatorHide));
        let (resp, _) = handler.dispatch(&req_action("indicator", Some("show"))).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("--no-indicator"));
    }

    #[tokio::test]
    async fn indicator_command_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client.sock");
        let listener = Listener::bind_at(&path, "client").unwrap();
        let (handler, _mirror) = client_handler(true);
        let task = tokio::spawn(listener.run(handler));

        // hide: plain ack (the deferred effect runs after the response and
        // is a no-op here — the opted-out supervisor has no child).
        let raw = request(path.clone(), r#"{"cmd":"indicator","action":"hide"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v, serde_json::json!({"ok": true}));
        // show on an opted-out daemon: the refusal comes back as an error.
        let raw = request(path.clone(), r#"{"cmd":"indicator","action":"show"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["ok"], false);
        assert!(v["error"].as_str().unwrap().contains("--no-indicator"));
        // An unknown action is a validation error, not a hang.
        let raw = request(path.clone(), r#"{"cmd":"indicator","action":"blink"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["ok"], false);
        assert!(v["error"].as_str().unwrap().contains("hide|show"));

        task.abort();
        let _ = task.await;
    }

    #[test]
    fn client_mirror_tracks_lifecycle() {
        let mirror = ClientStateMirror::new("127.0.0.1:9999".parse().unwrap());
        let state = mirror.snapshot();
        assert!(!state.connected && !state.active);

        mirror.set_server("127.0.0.1:8888".parse().unwrap());
        assert_eq!(mirror.snapshot().server, "127.0.0.1:8888");
        mirror.set_active(true);
        assert!(mirror.snapshot().active);
        // A drop clears active along with the connection.
        mirror.set_disconnected();
        let state = mirror.snapshot();
        assert!(!state.connected && !state.active);
    }

    /// Runs the synchronous request helper off the runtime: the test's
    /// single-threaded runtime must stay free to drive the accept loop.
    async fn request(path: PathBuf, req: &'static str) -> String {
        tokio::task::spawn_blocking(move || request_line(&path, req).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn socket_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client.sock");
        let listener = Listener::bind_at(&path, "client").unwrap();
        let (handler, mirror) = client_handler(true);
        let task = tokio::spawn(listener.run(handler));

        // A status request gets the live client state as JSON.
        let raw = request(path.clone(), r#"{"cmd":"status"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["state"]["role"], "client");
        assert_eq!(v["state"]["connected"], false);
        // Mirror updates are visible to later requests.
        mirror.set_active(true);
        let raw = request(path.clone(), r#"{"cmd":"status"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["state"]["active"], true);

        // A server-only command on the client socket: clear error.
        let raw = request(path.clone(), r#"{"cmd":"switch","target":"next"}"#).await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["ok"], false);
        assert!(v["error"].as_str().unwrap().contains("server-side"));

        // Invalid JSON gets a protocol-level error response, not a hang.
        let raw = request(path.clone(), "not json").await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["ok"], false);
        assert!(v["error"].as_str().unwrap().contains("invalid request"));

        // Dropping the listener removes the socket file (clean shutdown).
        task.abort();
        let _ = task.await;
        assert!(!path.exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_cli_sends_commands_and_propagates_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sock");
        let listener = Listener::bind_at(&path, "server").unwrap();
        let (event_tx, _event_rx) = mpsc::channel(8);
        let task = tokio::spawn(listener.run(server_handler(event_tx, false)));
        // Let the listener come up before querying (connect would EAGAIN).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // ok:true passes the ok message through.
        let out = daemon_cli(r#"{"cmd":"pause"}"#, "fine", Some(&path)).unwrap();
        assert_eq!(out, "fine");
        // A daemon-side error (update without auto-update) propagates.
        let err = daemon_cli(r#"{"cmd":"update_now"}"#, "started", Some(&path)).unwrap_err();
        assert!(err.to_string().contains("The daemon reported an error"));

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn bind_reclaims_stale_socket_and_refuses_a_live_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.sock");
        // A leftover file that answers no connect (crash remnant): reclaimed.
        std::fs::write(&path, b"stale").unwrap();
        let listener = Listener::bind_at(&path, "server").unwrap();
        assert!(path.exists());
        // A live daemon owning the path: refused politely, not hijacked.
        let err = match Listener::bind_at(&path, "server") {
            Ok(_) => panic!("a second bind on a live socket must fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("Refusing"));
        drop(listener);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn pipelined_requests_work_and_oversized_lines_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client.sock");
        let listener = Listener::bind_at(&path, "client").unwrap();
        let (handler, _mirror) = client_handler(true);
        let task = tokio::spawn(listener.run(handler));

        // Two requests written in one go (pipelined) on a single connection
        // each get their response: the bounded per-line read must not swallow
        // bytes belonging to the next line.
        let path2 = path.clone();
        let lines = tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, BufReader, Write};
            let stream = std::os::unix::net::UnixStream::connect(&path2).unwrap();
            let mut writer = stream.try_clone().unwrap();
            writer
                .write_all(b"{\"cmd\":\"status\"}\n{\"cmd\":\"status\"}\n")
                .unwrap();
            writer.flush().unwrap();
            let mut reader = BufReader::new(stream);
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let mut second = String::new();
            reader.read_line(&mut second).unwrap();
            (first, second)
        })
        .await
        .unwrap();
        assert!(lines.0.contains("\"ok\":true"), "{}", lines.0);
        assert!(lines.1.contains("\"ok\":true"), "{}", lines.1);

        // A never-terminated line longer than MAX_REQUEST_LINE is a protocol
        // error: the daemon closes the connection instead of buffering the
        // line without bound.
        let path3 = path.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, BufReader, Write};
            let stream = std::os::unix::net::UnixStream::connect(&path3).unwrap();
            let mut writer = stream.try_clone().unwrap();
            writer
                .write_all(&vec![b'a'; MAX_REQUEST_LINE + 100])
                .unwrap();
            writer.flush().unwrap();
            let mut line = String::new();
            match BufReader::new(stream).read_line(&mut line) {
                // Orderly close, or a reset from closing with our unread
                // bytes still queued: both mean no response was served.
                Ok(0) | Err(_) => {}
                Ok(n) => panic!("oversized line got {} response bytes: {:?}", n, line),
            }
        })
        .await
        .unwrap();

        task.abort();
        let _ = task.await;
    }

    #[test]
    fn state_parses_and_pretty_prints() {
        // The wire JSON parses into the tagged State enum by "role"...
        let server: State = serde_json::from_str(
            r#"{"role":"server","version":"1.4.0","protocol_version":8,"listen":"127.0.0.1:9999","paused":true,"current_target":"10.0.0.2:1213","clients":[{"addr":"10.0.0.2:1213","fingerprint":"d1d88653","connected_since_secs":42,"rtt_ms":1}],"clipboard":{"owner":"local","types":["text/plain"]},"update_available":"abc123"}"#,
        )
        .unwrap();
        let text = server.to_string();
        assert!(text.contains("monux server v1.4.0 (protocol 8)"));
        assert!(text.contains("current target: 10.0.0.2:1213"));
        assert!(text.contains("paused:         yes"));
        assert!(text.contains("available (abc123)"));
        assert!(text.contains(
            "10.0.0.2:1213 fingerprint d1d88653 (prefix: d1d88653) connected 42s ago, rtt 1ms"
        ));
        assert!(text.contains("owner:          local"));

        let client: State = serde_json::from_str(
            r#"{"role":"client","version":"1.4.0","protocol_version":8,"server":"10.0.0.1:1213","connected":true,"active":false,"connected_since_secs":42,"rtt_ms":1,"lost_packets":0}"#,
        )
        .unwrap();
        let text = client.to_string();
        assert!(text.contains("monux client v1.4.0 (protocol 8)"));
        assert!(text.contains("server:         10.0.0.1:1213"));
        assert!(text.contains("connected:      yes (for 42s, rtt 1ms, 0 packets lost)"));
    }

    #[tokio::test]
    async fn diagnostics_from_the_client_socket() {
        let (handler, mirror) = client_handler(true);
        mirror.set_server("10.0.0.1:1213".parse().unwrap());
        let (resp, post) = handler.dispatch(&req("diagnostics", None)).await;
        assert!(resp.ok && post.is_none());
        assert!(resp.state.is_none());
        let diag = resp.diagnostics.unwrap();
        assert_eq!(diag.role, "client");
        assert_eq!(diag.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(diag.protocol_version, PROTOCOL_VERSION);
        // The client state dump is the client mirror's rendering.
        assert!(diag.state_dump.contains("monux client"), "{}", diag.state_dump);
        assert!(diag.state_dump.contains("10.0.0.1:1213"), "{}", diag.state_dump);
        // No logging layer in tests: the buffer is simply empty.
        assert!(diag.recent_logs.is_empty());

        // The wire shape is {"ok":true,"diagnostics":{...}}.
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&Response::ok_diagnostics(diag)).unwrap())
                .unwrap();
        assert_eq!(v["ok"], true);
        assert!(v.get("state").is_none());
        assert!(v.get("error").is_none());
        for key in ["version", "protocol_version", "role", "state_dump", "recent_logs"] {
            assert!(v["diagnostics"].get(key).is_some(), "missing {}", key);
        }
    }

    #[tokio::test]
    async fn diagnostics_from_the_server_socket() {
        let (event_tx, _event_rx) = mpsc::channel(8);
        let handler = server_handler(event_tx, true);
        let (resp, post) = handler.dispatch(&req("diagnostics", None)).await;
        assert!(resp.ok && post.is_none());
        let diag = resp.diagnostics.unwrap();
        assert_eq!(diag.role, "server");
        // The server state dump is the SIGHUP rotation dump string; it works
        // even before the rotation loop's first iteration.
        assert!(
            diag.state_dump.contains("rotation loop last completed an iteration"),
            "{}",
            diag.state_dump
        );
    }
}
