//! `monux system indicator` — a StatusNotifierItem tray icon for the local
//! monux daemon, built on the ksni crate (pure-Rust SNI client over D-Bus).
//!
//! The indicator is a THIN CLIENT of the control socket (control.rs): every
//! POLL_INTERVAL it queries `{"cmd":"status"}` against server.sock, falling
//! back to client.sock, and renders the result; menu actions send the same
//! commands a CLI would. It never spawns or blocks on the daemon's event
//! loops — the daemon serves each control connection on its own task and
//! dispatch only reads mirrors, and the indicator's own socket reads carry a
//! short timeout, so a wedged daemon degrades the icon instead of hanging
//! the tray. When no daemon answers, the indicator keeps running and shows
//! the "not running" state; when there is no D-Bus session bus or SNI host
//! (headless TTY), run() fails with a clean error and exit code 1.
//!
//! # Icon colors (the dot is a programmatically generated ARGB pixmap — SNI
//! supports pixmaps, so no icon-theme lookup is involved)
//!
//! - GREEN:  input is local — server with current_target "local" and no
//!           degradation; client connected but not owning input
//! - BLUE:   input is on a client — server with current_target set to a
//!           client addr; client that currently owns the server's input
//!           (active)
//! - GREY:   the server is paused
//! - RED:    the link is degraded. Precisely, for v1:
//!           - server role: any connected client with rtt_ms > 50
//!             (DEGRADED_RTT_MS)
//!           - client role: connected == false
//!           RED outranks GREY — a degraded link is a problem worth seeing
//!           even while paused.
//! - Unknown (no daemon answers): a hollow grey "?" instead of the dot.
//!
//! The tooltip carries the details ("monux: input on 192.168.1.102", per-client
//! rtt/uptime, clipboard owner, update availability, the last action's error).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tracing::{debug, info};

use ksni::blocking::TrayMethods;

use crate::control::{self, Diagnostics, Role, ServerState, State};
use crate::notify::{self, Urgency};

/// How often the indicator re-queries the daemon's control socket.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// A client link above this RTT (ms) counts as degraded: RED icon (server
/// role only; see module docs for the precise color rules).
const DEGRADED_RTT_MS: u64 = 50;

/// Tray icon edge length in pixels (square ARGB pixmap).
const ICON_SIZE: i32 = 22;

/// Dot colors, as (R, G, B). The alpha channel is always fully opaque.
const GREEN: (u8, u8, u8) = (0x2e, 0xcc, 0x40);
const BLUE: (u8, u8, u8) = (0x33, 0x7e, 0xf6);
const GREY: (u8, u8, u8) = (0x96, 0x96, 0x96);
const RED: (u8, u8, u8) = (0xe6, 0x28, 0x28);

/// Notification id for indicator messages (replaces, never stacks — see
/// notify.rs).
const NOTIFY_ID: &str = "monux-indicator";

/// The icon's semantic color; mapping rules are in the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IconColor {
    Green,
    Blue,
    Grey,
    Red,
    /// No daemon answered: rendered as a hollow grey "?", not a dot.
    Unknown,
}

/// Maps daemon state to the icon color (see module docs).
fn color_of(state: &State) -> IconColor {
    match state {
        State::Server(s) => {
            if s.clients
                .iter()
                .any(|c| c.rtt_ms.map(|rtt| rtt > DEGRADED_RTT_MS).unwrap_or(false))
            {
                return IconColor::Red;
            }
            if s.paused {
                return IconColor::Grey;
            }
            if s.current_target != "local" {
                return IconColor::Blue;
            }
            IconColor::Green
        }
        State::Client(c) => {
            if !c.connected {
                return IconColor::Red;
            }
            if c.active {
                return IconColor::Blue;
            }
            IconColor::Green
        }
    }
}

/// What the indicator renders right now.
struct View {
    color: IconColor,
    /// Tooltip title ("monux: input on 10.0.0.2:1213", ...).
    title: String,
    /// Tooltip body: role/version plus per-connection details.
    details: String,
    /// The last status, for the menu; None when no daemon answers.
    state: Option<State>,
}

impl View {
    fn from_state(state: State) -> View {
        View {
            color: color_of(&state),
            title: title_of(&state),
            details: details_of(&state),
            state: Some(state),
        }
    }

    fn not_running() -> View {
        View {
            color: IconColor::Unknown,
            title: "monux: not running".to_string(),
            details: "No monux daemon is answering its control socket.".to_string(),
            state: None,
        }
    }
}

fn fmt_rtt(rtt_ms: Option<u64>) -> String {
    rtt_ms
        .map(|rtt| format!("{}ms", rtt))
        .unwrap_or_else(|| "?".to_string())
}

fn clipboard_summary(s: &ServerState) -> String {
    if s.clipboard.owner == "none" {
        "none".to_string()
    } else if s.clipboard.types.is_empty() {
        s.clipboard.owner.clone()
    } else {
        format!("{} ({})", s.clipboard.owner, s.clipboard.types.join(", "))
    }
}

fn title_of(state: &State) -> String {
    match state {
        State::Server(s) => match color_of(state) {
            IconColor::Red => {
                // Name the worst offender.
                match s
                    .clients
                    .iter()
                    .filter(|c| c.rtt_ms.map(|rtt| rtt > DEGRADED_RTT_MS).unwrap_or(false))
                    .max_by_key(|c| c.rtt_ms)
                {
                    Some(c) => format!("monux: degraded — {} rtt {}", c.addr, fmt_rtt(c.rtt_ms)),
                    None => "monux: degraded".to_string(),
                }
            }
            IconColor::Grey => "monux: paused".to_string(),
            IconColor::Blue => format!("monux: input on {}", s.current_target),
            IconColor::Green => "monux: input local".to_string(),
            IconColor::Unknown => unreachable!("a state always maps to a dot color"),
        },
        State::Client(c) => match color_of(state) {
            IconColor::Red => format!("monux: not connected to {}", c.server),
            IconColor::Blue => format!("monux: input here (server {})", c.server),
            IconColor::Green => format!("monux: connected to {}", c.server),
            IconColor::Grey | IconColor::Unknown => {
                unreachable!("client states never map to grey/unknown")
            }
        },
    }
}

fn details_of(state: &State) -> String {
    match state {
        State::Server(s) => {
            let mut lines = vec![format!(
                "server v{} (protocol {}), listening {}",
                s.version, s.protocol_version, s.listen
            )];
            for c in &s.clients {
                lines.push(format!(
                    "{} — rtt {}, up {}s",
                    c.addr,
                    fmt_rtt(c.rtt_ms),
                    c.connected_since_secs
                ));
            }
            lines.push(format!("clipboard: {}", clipboard_summary(s)));
            if let Some(sha) = &s.update_available {
                lines.push(format!("update available: {}", sha));
            }
            lines.join("\n")
        }
        State::Client(c) => {
            let mut lines = vec![format!(
                "client v{} (protocol {})",
                c.version, c.protocol_version
            )];
            if c.connected {
                lines.push(format!(
                    "server {} — rtt {}, up {}s, {} packets lost",
                    c.server,
                    fmt_rtt(c.rtt_ms),
                    c.connected_since_secs.unwrap_or(0),
                    c.lost_packets.unwrap_or(0)
                ));
            } else {
                lines.push(format!("server {} — not connected", c.server));
            }
            lines.join("\n")
        }
    }
}

/// One row of the tray menu, before conversion to ksni types. Unit tests
/// check this model; the ksni conversion (to_ksni_menu) is mechanical.
#[derive(Clone, Debug, PartialEq, Eq)]
enum MenuRow {
    /// A disabled informative row.
    Label(String),
    /// A row triggering a control-socket action; `enabled: false` renders it
    /// greyed out (used for switch rows while the server is paused: rotation
    /// drops switches then, so a clickable row would silently do nothing).
    Action {
        label: String,
        action: MenuAction,
        enabled: bool,
    },
    Separator,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MenuAction {
    SwitchLocal,
    /// Switch to the client with this (full) fingerprint.
    SwitchTo(String),
    Pause,
    Resume,
    UpdateNow,
    CopyDiagnostics,
    Restart,
    Exit,
}

impl MenuAction {
    /// Short name for error messages and notifications.
    fn label(&self) -> &'static str {
        match self {
            MenuAction::SwitchLocal => "Switch to local",
            MenuAction::SwitchTo(_) => "Switch",
            MenuAction::Pause => "Pause",
            MenuAction::Resume => "Resume",
            MenuAction::UpdateNow => "Update check",
            MenuAction::CopyDiagnostics => "Copy diagnostics",
            MenuAction::Restart => "Restart monux",
            MenuAction::Exit => "Exit monux",
        }
    }
}

/// Builds the menu model for the current state (see the module docs and the
/// phase spec: dynamic per state, switch/pause rows on the server socket
/// only).
fn menu_rows(state: Option<&State>) -> Vec<MenuRow> {
    let mut rows = Vec::new();
    match state {
        None => {
            rows.push(MenuRow::Label("monux is not running".to_string()));
        }
        Some(State::Server(s)) => {
            rows.push(MenuRow::Label(format!("Input: {}", s.current_target)));
            rows.push(MenuRow::Separator);
            // While paused, rotation drops switch events (the paused guard),
            // so the switch rows are greyed out: a clickable row would ack
            // the command yet visibly do nothing. Resume stays enabled.
            let switching_enabled = !s.paused;
            if s.current_target != "local" {
                rows.push(MenuRow::Action {
                    label: "Switch to local".to_string(),
                    action: MenuAction::SwitchLocal,
                    enabled: switching_enabled,
                });
            }
            for client in &s.clients {
                // No switch row for the client that already owns input.
                if client.addr == s.current_target {
                    continue;
                }
                rows.push(MenuRow::Action {
                    label: format!("Switch to {}", client.addr),
                    // The full fingerprint is always a unique prefix.
                    action: MenuAction::SwitchTo(client.fingerprint.clone()),
                    enabled: switching_enabled,
                });
            }
            rows.push(MenuRow::Action {
                label: if s.paused { "Resume" } else { "Pause" }.to_string(),
                action: if s.paused {
                    MenuAction::Resume
                } else {
                    MenuAction::Pause
                },
                enabled: true,
            });
            rows.push(MenuRow::Separator);
            for client in &s.clients {
                rows.push(MenuRow::Label(format!(
                    "Connection: {} — rtt {}, up {}s",
                    client.addr,
                    fmt_rtt(client.rtt_ms),
                    client.connected_since_secs
                )));
            }
            rows.push(MenuRow::Label(format!("Clipboard: {}", clipboard_summary(s))));
            rows.push(MenuRow::Separator);
            match &s.update_available {
                Some(sha) => rows.push(MenuRow::Action {
                    label: format!("Update available: {} — update now", sha),
                    action: MenuAction::UpdateNow,
                    enabled: true,
                }),
                None => rows.push(MenuRow::Action {
                    label: "Check for update now".to_string(),
                    action: MenuAction::UpdateNow,
                    enabled: true,
                }),
            }
            rows.push(MenuRow::Action {
                label: "Copy diagnostics".to_string(),
                action: MenuAction::CopyDiagnostics,
                enabled: true,
            });
            rows.push(MenuRow::Separator);
            rows.push(MenuRow::Action {
                label: "Restart monux".to_string(),
                action: MenuAction::Restart,
                enabled: true,
            });
            rows.push(MenuRow::Action {
                label: "Exit monux".to_string(),
                action: MenuAction::Exit,
                enabled: true,
            });
        }
        Some(State::Client(c)) => {
            rows.push(MenuRow::Label(format!("Server: {}", c.server)));
            rows.push(MenuRow::Label(format!(
                "Connection: {}",
                if c.connected {
                    format!(
                        "rtt {}, up {}s",
                        fmt_rtt(c.rtt_ms),
                        c.connected_since_secs.unwrap_or(0)
                    )
                } else {
                    "not connected".to_string()
                }
            )));
            rows.push(MenuRow::Label(format!(
                "Input: {}",
                if c.active { "here" } else { "server" }
            )));
            rows.push(MenuRow::Separator);
            // The client state has no update_available field, so this is
            // always the plain manual check.
            rows.push(MenuRow::Action {
                label: "Check for update now".to_string(),
                action: MenuAction::UpdateNow,
                enabled: true,
            });
            rows.push(MenuRow::Action {
                label: "Copy diagnostics".to_string(),
                action: MenuAction::CopyDiagnostics,
                enabled: true,
            });
            rows.push(MenuRow::Separator);
            rows.push(MenuRow::Action {
                label: "Restart monux".to_string(),
                action: MenuAction::Restart,
                enabled: true,
            });
            rows.push(MenuRow::Action {
                label: "Exit monux".to_string(),
                action: MenuAction::Exit,
                enabled: true,
            });
        }
    }
    rows
}

/// Converts the menu model to ksni items; action closures dispatch through
/// run_action.
fn to_ksni_menu(rows: Vec<MenuRow>) -> Vec<ksni::menu::MenuItem<MonuxTray>> {
    use ksni::menu::{MenuItem, StandardItem};
    rows.into_iter()
        .map(|row| match row {
            MenuRow::Separator => MenuItem::Separator,
            MenuRow::Label(label) => StandardItem {
                label,
                enabled: false,
                ..Default::default()
            }
            .into(),
            MenuRow::Action {
                label,
                action,
                enabled,
            } => StandardItem {
                label,
                enabled,
                activate: Box::new(move |tray: &mut MonuxTray| run_action(tray, &action)),
                ..Default::default()
            }
            .into(),
        })
        .collect()
}

/// Draws a filled circle with a 2px transparent margin. ARGB32 in network
/// byte order (A, R, G, B per pixel), as the SNI pixmap format requires.
fn dot_pixmap(size: i32, rgb: (u8, u8, u8)) -> Vec<u8> {
    let mut data = vec![0u8; (size * size * 4) as usize];
    let center = (size as f32 - 1.0) / 2.0;
    let radius = size as f32 / 2.0 - 2.0;
    for y in 0..size {
        for x in 0..size {
            let dx = x as f32 - center;
            let dy = y as f32 - center;
            if (dx * dx + dy * dy).sqrt() <= radius {
                let i = ((y * size + x) * 4) as usize;
                data[i] = 0xff;
                data[i + 1] = rgb.0;
                data[i + 2] = rgb.1;
                data[i + 3] = rgb.2;
            }
        }
    }
    data
}

/// 5x7 bitmap of '?', one bit per pixel (MSB is the leftmost pixel), scaled
/// up by 2 when rendered — the "hollow" unknown state (no font rendering
/// involved).
const QUESTION_GLYPH: [u8; 7] = [
    0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0b00000, 0b00100,
];

/// Draws the grey "?" glyph (scaled 2x, centered) on a transparent canvas.
fn question_pixmap(size: i32, rgb: (u8, u8, u8)) -> Vec<u8> {
    const SCALE: i32 = 2;
    let mut data = vec![0u8; (size * size * 4) as usize];
    let origin_x = (size - 5 * SCALE) / 2;
    let origin_y = (size - 7 * SCALE) / 2;
    for (row, bits) in QUESTION_GLYPH.iter().enumerate() {
        for col in 0..5 {
            if bits & (0b10000 >> col) == 0 {
                continue;
            }
            for dy in 0..SCALE {
                for dx in 0..SCALE {
                    let x = origin_x + col as i32 * SCALE + dx;
                    let y = origin_y + row as i32 * SCALE + dy;
                    let i = ((y * size + x) * 4) as usize;
                    data[i] = 0xff;
                    data[i + 1] = rgb.0;
                    data[i + 2] = rgb.1;
                    data[i + 3] = rgb.2;
                }
            }
        }
    }
    data
}

fn icon_for(color: IconColor) -> ksni::Icon {
    let data = match color {
        IconColor::Green => dot_pixmap(ICON_SIZE, GREEN),
        IconColor::Blue => dot_pixmap(ICON_SIZE, BLUE),
        IconColor::Grey => dot_pixmap(ICON_SIZE, GREY),
        IconColor::Red => dot_pixmap(ICON_SIZE, RED),
        IconColor::Unknown => question_pixmap(ICON_SIZE, GREY),
    };
    ksni::Icon {
        width: ICON_SIZE,
        height: ICON_SIZE,
        data,
    }
}

/// The ksni tray object. Mutated only on ksni's service thread (menu
/// callbacks and Handle::update closures both run there), so no locking is
/// needed.
struct MonuxTray {
    view: View,
    /// The control socket that last answered a status poll; menu actions go
    /// here. None when no daemon has answered yet.
    socket: Option<PathBuf>,
    /// Note from the last menu action (an error, or a success confirmation
    /// for Copy diagnostics), shown in the tooltip; cleared by the next
    /// successful command action.
    note: Option<String>,
}

impl MonuxTray {
    fn new() -> Self {
        MonuxTray {
            view: View::not_running(),
            socket: None,
            note: None,
        }
    }

    /// Re-polls the control sockets and swaps in the fresh view.
    fn refresh(&mut self) {
        let (socket, view) = poll();
        self.socket = socket;
        self.view = view;
    }
}

impl ksni::Tray for MonuxTray {
    // There is no window to activate: a left click opens the menu.
    const MENU_ON_ACTIVATE: bool = true;

    fn id(&self) -> String {
        "monux".to_string()
    }

    fn title(&self) -> String {
        "monux".to_string()
    }

    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        vec![icon_for(self.view.color)]
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        let description = match &self.note {
            Some(note) => format!("{}\n{}", self.view.details, note),
            None => self.view.details.clone(),
        };
        ksni::ToolTip {
            icon_name: String::new(),
            icon_pixmap: Vec::new(),
            title: self.view.title.clone(),
            description,
        }
    }

    fn menu(&self) -> Vec<ksni::menu::MenuItem<Self>> {
        to_ksni_menu(menu_rows(self.view.state.as_ref()))
    }

    fn menu_about_to_show(&mut self) {
        // Fresh state right before the menu renders, even between poll ticks.
        self.refresh();
    }
}

/// Queries server.sock first, then client.sock; the first socket answering
/// with a parseable status wins (a machine usually runs one role, and the
/// server view is the richer one). (None, not-running view) when no daemon
/// answers.
fn poll() -> (Option<PathBuf>, View) {
    for role in [Role::Server, Role::Client] {
        let path = control::socket_path(role);
        if !path.exists() {
            continue;
        }
        match control::request_line(&path, r#"{"cmd":"status"}"#)
            .and_then(|raw| parse_ok(&raw, &path))
        {
            Ok(v) => match serde_json::from_value::<State>(v["state"].clone()) {
                Ok(state) => return (Some(path), View::from_state(state)),
                Err(e) => debug!(
                    "Indicator: unrecognized state from {}: {:?}",
                    path.display(),
                    e
                ),
            },
            Err(e) => debug!(
                "Indicator: status from {} failed: {:?}",
                path.display(),
                e
            ),
        }
    }
    (None, View::not_running())
}

/// Validates a response line and returns the parsed body. An `ok:false`
/// response becomes an Err carrying the daemon's error string.
fn parse_ok(raw: &str, socket: &Path) -> Result<serde_json::Value> {
    let v: serde_json::Value = serde_json::from_str(raw)
        .with_context(|| format!("Malformed response from {}", socket.display()))?;
    if v.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
        return Ok(v);
    }
    Err(anyhow!(
        "{}",
        v.get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("the daemon reported an error")
    ))
}

/// Sends a command and checks the ack; the daemon's error string propagates.
fn send_command(socket: &Path, request: &str) -> Result<()> {
    let raw = control::request_line(socket, request)?;
    parse_ok(&raw, socket).map(|_| ())
}

fn action_request(action: &MenuAction) -> String {
    match action {
        MenuAction::SwitchLocal => r#"{"cmd":"switch","target":"local"}"#.to_string(),
        MenuAction::SwitchTo(fingerprint) => {
            serde_json::json!({"cmd": "switch", "target": fingerprint}).to_string()
        }
        MenuAction::Pause => r#"{"cmd":"pause"}"#.to_string(),
        MenuAction::Resume => r#"{"cmd":"resume"}"#.to_string(),
        MenuAction::UpdateNow => r#"{"cmd":"update_now"}"#.to_string(),
        MenuAction::Restart => r#"{"cmd":"restart"}"#.to_string(),
        MenuAction::Exit => r#"{"cmd":"exit"}"#.to_string(),
        MenuAction::CopyDiagnostics => {
            unreachable!("copy diagnostics fetches instead of commanding")
        }
    }
}

/// Runs one menu action against the bound socket, then re-polls immediately
/// so the icon and menu reflect the effect — including the daemon vanishing
/// after restart/exit, which simply lands on the not-running view until the
/// daemon (re)appears. Errors surface in the tooltip AND a transient
/// notification.
fn run_action(tray: &mut MonuxTray, action: &MenuAction) {
    let outcome = match tray.socket.clone() {
        Some(socket) => match action {
            MenuAction::CopyDiagnostics => copy_diagnostics(&socket)
                .map(|tool| format!("Diagnostics copied to the clipboard ({})", tool)),
            other => send_command(&socket, &action_request(other)).map(|_| String::new()),
        },
        None => Err(anyhow!("monux is not running")),
    };
    match outcome {
        Ok(note) => {
            if let MenuAction::CopyDiagnostics = action {
                tray.note = Some(note.clone());
                notify::notify(NOTIFY_ID, Urgency::Low, 3000, "monux", &note);
            } else {
                tray.note = None;
            }
        }
        Err(e) => {
            let note = format!("{} failed: {:#}", action.label(), e);
            tray.note = Some(note.clone());
            notify::notify(NOTIFY_ID, Urgency::Normal, 5000, "monux", &note);
        }
    }
    tray.refresh();
}

/// Fetches the diagnostics bundle from the daemon and copies it to the
/// desktop clipboard; returns the clipboard tool that worked.
fn copy_diagnostics(socket: &Path) -> Result<&'static str> {
    let raw = control::request_line(socket, r#"{"cmd":"diagnostics"}"#)?;
    let v = parse_ok(&raw, socket)?;
    let diagnostics: Diagnostics = serde_json::from_value(v["diagnostics"].clone())
        .context("The daemon returned no diagnostics")?;
    let bundle = format_diagnostics_bundle(&diagnostics);
    copy_to_clipboard(&bundle)
}

/// The plain-text bundle "Copy diagnostics" puts on the clipboard.
fn format_diagnostics_bundle(d: &Diagnostics) -> String {
    let mut out = format!(
        "monux {} diagnostics ({} role, protocol {})\n\n== state ==\n{}\n\n== recent logs (oldest first) ==\n",
        d.version,
        d.role,
        d.protocol_version,
        d.state_dump.trim_end()
    );
    if d.recent_logs.is_empty() {
        out.push_str("<no log lines captured>\n");
    } else {
        for line in &d.recent_logs {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// Copies `text` to the desktop clipboard, trying wl-copy (Wayland), then
/// xclip and xsel (X11); returns the tool that worked. All three daemonize
/// after taking the selection, so spawn + write + wait returns promptly.
fn copy_to_clipboard(text: &str) -> Result<&'static str> {
    const TOOLS: [(&str, &[&str]); 3] = [
        ("wl-copy", &[]),
        ("xclip", &["-selection", "clipboard"]),
        ("xsel", &["--clipboard", "--input"]),
    ];
    for (tool, args) in TOOLS {
        match pipe_into(tool, args, text) {
            Ok(()) => return Ok(tool),
            Err(e) => debug!("Indicator: {} failed: {:?}", tool, e),
        }
    }
    bail!("no clipboard tool available (tried wl-copy, xclip, xsel)");
}

/// Longest we wait for a clipboard tool to exit before killing it. All three
/// tools daemonize after taking the selection and exit in milliseconds; a
/// wedged compositor must not freeze the tray's service thread in wait().
const CLIPBOARD_TOOL_TIMEOUT: Duration = Duration::from_secs(5);

fn pipe_into(tool: &str, args: &[&str], text: &str) -> Result<()> {
    use std::io::Write;
    let mut child = Command::new(tool)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to spawn {}", tool))?;
    // A tool that rejects the session (no WAYLAND_DISPLAY/DISPLAY) exits
    // immediately, breaking the pipe; that error moves us to the next tool.
    child
        .stdin
        .as_mut()
        .expect("stdin is piped")
        .write_all(text.as_bytes())
        .with_context(|| format!("failed to write to {}", tool))?;
    match wait_with_timeout(&mut child, CLIPBOARD_TOOL_TIMEOUT)? {
        Some(status) if status.success() => Ok(()),
        Some(status) => bail!("{} exited with {}", tool, status),
        None => {
            // Wedged: kill and reap, then report like any other tool failure.
            let _ = child.kill();
            let _ = child.wait();
            bail!("{} did not exit within {:?}", tool, CLIPBOARD_TOOL_TIMEOUT)
        }
    }
}

/// child.wait() with a deadline: Ok(None) when it expires (the caller then
/// kills and reaps). try_wait polling is plenty for a process that exits in
/// milliseconds in the common case.
fn wait_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Result<Option<std::process::ExitStatus>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().context("failed to poll child")? {
            return Ok(Some(status));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Runs the indicator until the tray service shuts down. With no D-Bus
/// session bus or no StatusNotifierItem host (headless TTY), fails with a
/// clean error — main turns that into exit code 1. A missing monux daemon is
/// NOT an error: the indicator shows the "?" state and keeps polling.
pub fn run() -> Result<()> {
    let handle = match MonuxTray::new().spawn() {
        Ok(handle) => handle,
        Err(e) => bail!(
            "no D-Bus session / no tray host: {} — the indicator needs a desktop session running a StatusNotifierItem host (waybar, KDE Plasma, ...)",
            e
        ),
    };
    info!("Tray indicator running (polling every {:?})", POLL_INTERVAL);
    loop {
        // update() returns None once the tray service has shut down.
        if handle.update(|tray| tray.refresh()).is_none() {
            info!("Tray service shut down, exiting the indicator");
            return Ok(());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{ClientState, ServerClientState, ServerClipboardState};

    fn server_state(paused: bool, target: &str, clients: Vec<(&str, Option<u64>)>) -> State {
        State::Server(ServerState {
            version: "1.5.0".to_string(),
            protocol_version: 8,
            listen: "10.0.0.1:1213".to_string(),
            paused,
            current_target: target.to_string(),
            clients: clients
                .into_iter()
                .map(|(addr, rtt_ms)| ServerClientState {
                    addr: addr.to_string(),
                    fingerprint: format!("fp-{}", addr),
                    connected_since_secs: 42,
                    rtt_ms,
                })
                .collect(),
            clipboard: ServerClipboardState {
                owner: "none".to_string(),
                types: vec![],
            },
            update_available: None,
        })
    }

    fn client_state(connected: bool, active: bool) -> State {
        State::Client(ClientState {
            version: "1.5.0".to_string(),
            protocol_version: 8,
            server: "10.0.0.1:1213".to_string(),
            connected,
            active,
            connected_since_secs: if connected { Some(42) } else { None },
            rtt_ms: if connected { Some(3) } else { None },
            lost_packets: if connected { Some(0) } else { None },
        })
    }

    #[test]
    fn server_color_mapping() {
        // Healthy, input local: green.
        let state = server_state(false, "local", vec![("10.0.0.2:1213", Some(3))]);
        assert_eq!(color_of(&state), IconColor::Green);
        assert_eq!(title_of(&state), "monux: input local");
        // Input on a client: blue.
        let state = server_state(false, "10.0.0.2:1213", vec![("10.0.0.2:1213", Some(3))]);
        assert_eq!(color_of(&state), IconColor::Blue);
        assert_eq!(title_of(&state), "monux: input on 10.0.0.2:1213");
        // Paused: grey.
        let state = server_state(true, "local", vec![]);
        assert_eq!(color_of(&state), IconColor::Grey);
        assert_eq!(title_of(&state), "monux: paused");
        // A client over the degradation threshold: red — even while paused
        // (problems outrank the deliberate paused state).
        let state = server_state(false, "local", vec![("10.0.0.2:1213", Some(120))]);
        assert_eq!(color_of(&state), IconColor::Red);
        assert_eq!(title_of(&state), "monux: degraded — 10.0.0.2:1213 rtt 120ms");
        let state = server_state(true, "local", vec![("10.0.0.2:1213", Some(120))]);
        assert_eq!(color_of(&state), IconColor::Red);
        // Exactly at the threshold is NOT degraded; an unknown rtt is ignored.
        let state = server_state(false, "local", vec![("10.0.0.2:1213", Some(50))]);
        assert_eq!(color_of(&state), IconColor::Green);
        let state = server_state(false, "local", vec![("10.0.0.2:1213", None)]);
        assert_eq!(color_of(&state), IconColor::Green);
    }

    #[test]
    fn client_color_mapping() {
        // Connected but not owning input: green; owning: blue; disconnected
        // (with a known server address): red.
        let state = client_state(true, false);
        assert_eq!(color_of(&state), IconColor::Green);
        assert_eq!(title_of(&state), "monux: connected to 10.0.0.1:1213");
        let state = client_state(true, true);
        assert_eq!(color_of(&state), IconColor::Blue);
        assert_eq!(title_of(&state), "monux: input here (server 10.0.0.1:1213)");
        let state = client_state(false, false);
        assert_eq!(color_of(&state), IconColor::Red);
        assert_eq!(title_of(&state), "monux: not connected to 10.0.0.1:1213");
    }

    #[test]
    fn tooltip_details_carry_connection_facts() {
        let state = server_state(
            false,
            "10.0.0.2:1213",
            vec![("10.0.0.2:1213", Some(3)), ("10.0.0.3:1213", None)],
        );
        let details = details_of(&state);
        assert!(details.contains("server v1.5.0 (protocol 8), listening 10.0.0.1:1213"));
        assert!(details.contains("10.0.0.2:1213 — rtt 3ms, up 42s"));
        assert!(details.contains("10.0.0.3:1213 — rtt ?, up 42s"));
        assert!(details.contains("clipboard: none"));

        let details = details_of(&client_state(true, false));
        assert!(details.contains("client v1.5.0 (protocol 8)"));
        assert!(details.contains("rtt 3ms, up 42s, 0 packets lost"));
    }

    #[test]
    fn menu_for_a_server_with_local_input() {
        let state = server_state(
            false,
            "local",
            vec![("10.0.0.2:1213", Some(3)), ("10.0.0.3:1213", Some(7))],
        );
        let rows = menu_rows(Some(&state));
        // Header row names the current input owner.
        assert!(rows.contains(&MenuRow::Label("Input: local".to_string())));
        // Local input: no "Switch to local", one switch row per client
        // carrying the full fingerprint as the target.
        assert!(!rows
            .iter()
            .any(|r| matches!(r, MenuRow::Action { action: MenuAction::SwitchLocal, .. })));
        assert!(rows.contains(&action_row(
            "Switch to 10.0.0.2:1213",
            MenuAction::SwitchTo("fp-10.0.0.2:1213".to_string()),
            true
        )));
        assert!(rows.contains(&action_row(
            "Switch to 10.0.0.3:1213",
            MenuAction::SwitchTo("fp-10.0.0.3:1213".to_string()),
            true
        )));
        // Not paused: the Pause action is offered.
        assert!(rows.contains(&action_row("Pause", MenuAction::Pause, true)));
        // Per-client connection rows and the clipboard row are disabled labels.
        assert!(rows.contains(&MenuRow::Label(
            "Connection: 10.0.0.2:1213 — rtt 3ms, up 42s".to_string()
        )));
        assert!(rows.contains(&MenuRow::Label("Clipboard: none".to_string())));
        // No update pending: plain manual check.
        assert!(rows.contains(&action_row("Check for update now", MenuAction::UpdateNow, true)));
        assert!(rows.contains(&action_row("Copy diagnostics", MenuAction::CopyDiagnostics, true)));
        assert!(rows.contains(&action_row("Restart monux", MenuAction::Restart, true)));
        assert!(rows.contains(&action_row("Exit monux", MenuAction::Exit, true)));
    }

    #[test]
    fn menu_for_a_server_with_remote_input_pause_and_update() {
        let mut state = server_state(
            true,
            "10.0.0.2:1213",
            vec![("10.0.0.2:1213", Some(3)), ("10.0.0.3:1213", Some(7))],
        );
        if let State::Server(s) = &mut state {
            s.clipboard.owner = "local".to_string();
            s.clipboard.types = vec!["text/plain".to_string()];
            s.update_available = Some("abc123".to_string());
        }
        let rows = menu_rows(Some(&state));
        assert!(rows.contains(&MenuRow::Label("Input: 10.0.0.2:1213".to_string())));
        // Remote input: switching back to local is listed...
        assert!(rows.contains(&action_row("Switch to local", MenuAction::SwitchLocal, false)));
        // ...but the client already owning input has no switch row...
        assert!(!rows
            .iter()
            .any(|r| matches!(r, MenuRow::Action { label, .. } if label == "Switch to 10.0.0.2:1213")));
        // ...and while paused every switch row is DISABLED (rotation drops
        // switches then; a clickable row would silently do nothing)...
        assert!(rows.contains(&action_row(
            "Switch to 10.0.0.3:1213",
            MenuAction::SwitchTo("fp-10.0.0.3:1213".to_string()),
            false
        )));
        // ...while the Resume row stays enabled.
        assert!(rows.contains(&action_row("Resume", MenuAction::Resume, true)));
        assert!(rows
            .iter()
            .all(|r| !matches!(r, MenuRow::Action { action: MenuAction::SwitchLocal | MenuAction::SwitchTo(_), enabled: true, .. })));
        // Update pending: the sha is in the label.
        assert!(rows.contains(&action_row(
            "Update available: abc123 — update now",
            MenuAction::UpdateNow,
            true
        )));
        assert!(rows.contains(&MenuRow::Label(
            "Clipboard: local (text/plain)".to_string()
        )));
        // Unpausing re-enables the switch rows.
        let state = server_state(
            false,
            "10.0.0.2:1213",
            vec![("10.0.0.2:1213", Some(3)), ("10.0.0.3:1213", Some(7))],
        );
        let rows = menu_rows(Some(&state));
        assert!(rows.contains(&action_row("Switch to local", MenuAction::SwitchLocal, true)));
        assert!(rows.contains(&action_row(
            "Switch to 10.0.0.3:1213",
            MenuAction::SwitchTo("fp-10.0.0.3:1213".to_string()),
            true
        )));
    }

    /// Builds an Action menu row concisely for the assertions.
    fn action_row(label: &str, action: MenuAction, enabled: bool) -> MenuRow {
        MenuRow::Action {
            label: label.to_string(),
            action,
            enabled,
        }
    }

    #[test]
    fn menu_for_a_client_has_no_server_only_actions() {
        let rows = menu_rows(Some(&client_state(true, true)));
        assert!(rows.contains(&MenuRow::Label("Server: 10.0.0.1:1213".to_string())));
        assert!(rows.contains(&MenuRow::Label("Connection: rtt 3ms, up 42s".to_string())));
        assert!(rows.contains(&MenuRow::Label("Input: here".to_string())));
        // Rotation and pause are server concepts: absent on the client menu.
        for row in &rows {
            if let MenuRow::Action { action, enabled, .. } = row {
                assert!(matches!(
                    action,
                    MenuAction::UpdateNow
                        | MenuAction::CopyDiagnostics
                        | MenuAction::Restart
                        | MenuAction::Exit
                ));
                assert!(enabled);
            }
        }
        // A disconnected client still gets the lifecycle actions.
        let rows = menu_rows(Some(&client_state(false, false)));
        assert!(rows.contains(&MenuRow::Label("Connection: not connected".to_string())));
        assert!(rows.contains(&MenuRow::Label("Input: server".to_string())));
    }

    #[test]
    fn menu_without_a_daemon_is_a_single_disabled_label() {
        let rows = menu_rows(None);
        assert_eq!(rows, vec![MenuRow::Label("monux is not running".to_string())]);
    }

    #[test]
    fn dot_pixmap_draws_a_filled_circle() {
        let data = dot_pixmap(ICON_SIZE, GREEN);
        assert_eq!(data.len(), (ICON_SIZE * ICON_SIZE * 4) as usize);
        // The center pixel is opaque green (A, R, G, B order).
        let center = ((ICON_SIZE / 2) * ICON_SIZE + ICON_SIZE / 2) as usize * 4;
        assert_eq!(&data[center..center + 4], &[0xff, 0x2e, 0xcc, 0x40]);
        // The corners are fully transparent (2px margin around the dot).
        for px in corner_pixels(&data) {
            assert_eq!(px, &[0, 0, 0, 0]);
        }
    }

    /// The four corner pixels of a square ARGB pixmap.
    fn corner_pixels(data: &[u8]) -> [&[u8]; 4] {
        let size = ICON_SIZE as usize;
        [
            &data[0..4],
            &data[(size - 1) * 4..size * 4],
            &data[(size * (size - 1)) * 4..(size * (size - 1) + 1) * 4],
            &data[(size * size - 1) * 4..size * size * 4],
        ]
    }

    #[test]
    fn question_pixmap_is_a_sparse_grey_glyph() {
        let data = question_pixmap(ICON_SIZE, GREY);
        assert_eq!(data.len(), (ICON_SIZE * ICON_SIZE * 4) as usize);
        let mut opaque = 0usize;
        for px in data.chunks(4) {
            if px[3] != 0 {
                opaque += 1;
                // Every drawn pixel is opaque grey.
                assert_eq!(px, &[0xff, 0x96, 0x96, 0x96]);
            } else {
                assert_eq!(px, &[0, 0, 0, 0]);
            }
        }
        // The glyph is sparse (a "?", not a filled dot) but visible.
        assert!(opaque > 20, "glyph too small: {}", opaque);
        assert!(opaque < (ICON_SIZE * ICON_SIZE) as usize / 4, "glyph too dense");
        // Corners stay transparent.
        assert_eq!(data[0], 0);
    }

    #[test]
    fn diagnostics_bundle_contains_everything() {
        let d = Diagnostics {
            version: "1.5.0".to_string(),
            protocol_version: 8,
            role: "server".to_string(),
            state_dump: "rotation loop last completed an iteration 5ms ago; ...".to_string(),
            recent_logs: vec!["INFO monux: first".to_string(), "INFO monux: last".to_string()],
        };
        let bundle = format_diagnostics_bundle(&d);
        assert!(bundle.contains("monux 1.5.0 diagnostics (server role, protocol 8)"));
        assert!(bundle.contains("== state =="));
        assert!(bundle.contains("rotation loop last completed an iteration 5ms ago"));
        assert!(bundle.contains("== recent logs (oldest first) =="));
        assert!(bundle.contains("INFO monux: first\nINFO monux: last\n"));

        // An empty log buffer still formats.
        let mut d2 = d.clone();
        d2.recent_logs = vec![];
        assert!(format_diagnostics_bundle(&d2).contains("<no log lines captured>"));
    }

    #[test]
    fn wait_with_timeout_returns_status_or_none_on_expiry() {
        // A child that exits promptly: status comes back well within the
        // timeout.
        let mut fast = Command::new("true").spawn().unwrap();
        let status = wait_with_timeout(&mut fast, Duration::from_secs(5)).unwrap();
        assert!(status.expect("fast child must have exited").success());

        // A child that never exits on its own: Ok(None) after the timeout,
        // and the caller can kill + reap it.
        let mut slow = Command::new("sleep").arg("30").spawn().unwrap();
        let start = std::time::Instant::now();
        let status = wait_with_timeout(&mut slow, Duration::from_millis(150)).unwrap();
        assert!(status.is_none(), "wedged child must hit the timeout");
        assert!(start.elapsed() < Duration::from_secs(5));
        slow.kill().unwrap();
        slow.wait().unwrap();
    }
}
