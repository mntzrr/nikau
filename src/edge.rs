//! Screen-edge switching (opt-in via --edge-map), in two modes sharing one
//! detection machinery:
//!
//! SERVER: when the local cursor is pushed against a configured screen edge
//! and dwells there, input switches to the client mapped to that edge — the
//! classic "screen-edge KVM" behavior. The switch itself reuses the existing
//! rotation path (Event::SwitchTo → Rotation::set_client), so
//! debounce/pause/no-op cleanup all apply for free.
//!
//! CLIENT (run_client): when the cursor is pushed against a configured edge
//! of the CLIENT machine and dwells there, the client asks the server to
//! take input back (ClientEvent::SwitchRequest, carrying the fraction along
//! the edge where the cursor crossed — reserved for future cursor warping).
//! The only valid target on a client is `auto` (its one peer, the server);
//! the server honors the request only from the current client.
//!
//! Detection polls the cursor position from Hyprland's IPC every
//! POLL_INTERVAL (Hyprland delivers no usable pointer enter/leave at screen
//! edges — verified empirically with layer-shell probes — so an event-driven
//! design is not viable there). The cursor is "on" a mapped edge when its
//! coordinate crosses that edge's line within an EXPOSED segment of it (see
//! exposed_segments), minus a corner dead zone at each segment end. Edge
//! contact is debounced (a state must hold for two consecutive polls), then
//! the dwell timer (--edge-dwell-ms) runs; a leave before the deadline
//! cancels it; a completed dwell fires the switch once and a short re-arm
//! cooldown prevents machine-gunning. The poller runs on its own thread
//! (blocking socket IO), forwarding positions to the edge manager task.
//!
//! The monitor layout comes from Hyprland's IPC (the only compositor
//! supported in this phase); if it's unavailable the feature disables itself
//! with a warning. The layout is re-queried periodically so monitor
//! (un)plugs and resolution changes recompute the trigger zones.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::time;
use tracing::{debug, info, warn};

use crate::device::Event;
use crate::msgs::event::Direction;

/// Parses a --edge-map direction ("left"/"right"/"top"/"bottom"). The type
/// itself lives on the wire side (msgs::event) for ServerEvent::EdgeInfo;
/// the parsing helper stays here, next to the --edge-map handling.
impl Direction {
    fn parse(s: &str) -> Result<Direction> {
        match s.to_ascii_lowercase().as_str() {
            "left" => Ok(Direction::Left),
            "right" => Ok(Direction::Right),
            "top" => Ok(Direction::Top),
            "bottom" => Ok(Direction::Bottom),
            other => bail!(
                "invalid edge direction '{}': expected left|right|top|bottom",
                other
            ),
        }
    }
}

/// The --edge-map target of one direction: who sits beyond that edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeTarget {
    /// The literal `auto`: exactly one connected client. An error while zero
    /// or more than one client is connected.
    Auto,
    /// A fingerprint prefix (like set_client's goto matching), or — when no
    /// connected client's fingerprint starts with it — a hostname resolved
    /// via the system resolver and matched to a connected client by IP.
    Named(String),
}

impl std::fmt::Display for EdgeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            EdgeTarget::Auto => f.write_str("auto"),
            EdgeTarget::Named(name) => f.write_str(name),
        }
    }
}

/// Parsed --edge-map: which client sits beyond which screen edge.
#[derive(Clone, Debug, Default)]
pub struct EdgeMap {
    pub targets: BTreeMap<Direction, EdgeTarget>,
}

/// Parses the repeatable, comma-separated --edge-map values
/// ("right=auto", "left=aa11bb,top=laptop") into an EdgeMap.
pub fn parse_edge_map(specs: &[String]) -> Result<EdgeMap> {
    let mut map = EdgeMap::default();
    for spec in specs {
        for part in spec.split(',') {
            let part = part.trim();
            let (dir, target) = part.split_once('=').with_context(|| {
                format!(
                    "invalid --edge-map entry '{}': expected <direction>=<target>",
                    part
                )
            })?;
            let dir = Direction::parse(dir.trim())?;
            let target = target.trim();
            if target.is_empty() {
                bail!("invalid --edge-map entry '{}': empty target", part);
            }
            let target = if target == "auto" {
                EdgeTarget::Auto
            } else {
                EdgeTarget::Named(target.to_string())
            };
            if map.targets.insert(dir, target).is_some() {
                bail!("duplicate direction '{}' in --edge-map", dir.as_str());
            }
        }
    }
    if map.targets.is_empty() {
        bail!("--edge-map requires at least one direction=target entry");
    }
    Ok(map)
}

/// Parses --edge-map on the CLIENT: same syntax as the server's, but the
/// only valid target is `auto` (meaning "the server" — a client has exactly
/// one peer), so a fingerprint/hostname target is a config error at startup
/// here rather than a runtime resolution failure.
pub fn parse_client_edge_map(specs: &[String]) -> Result<EdgeMap> {
    let map = parse_edge_map(specs)?;
    for (dir, target) in &map.targets {
        if *target != EdgeTarget::Auto {
            bail!(
                "invalid --edge-map target '{}' for the {} edge: on a client the only valid target is 'auto' (the server)",
                target,
                dir.as_str()
            );
        }
    }
    Ok(map)
}

/// One output's logical rectangle in the compositor's layout coordinate
/// space (scale already applied). Injectable so the geometry is testable
/// without a running compositor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutputRect {
    pub name: String,
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
}

/// A contiguous exposed piece of one output's boundary: no other output
/// abuts it, so the cursor jams against it (and a trigger zone there sees it).
/// `start`/`len` run along the edge axis in global layout coordinates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EdgeSegment {
    pub direction: Direction,
    pub output: String,
    pub start: i32,
    pub len: i32,
}

/// Computes the exposed edge segments of a monitor layout: for each output
/// boundary, the intervals not abutted by another output. Two 1920x1080
/// monitors side by side at (0,0) and (1920,0) yield the right edge only on
/// the rightmost monitor, the left edge only on the leftmost, and full
/// top/bottom edges on both. Pure over the layout so tests need no Hyprland.
pub(crate) fn exposed_segments(outputs: &[OutputRect]) -> Vec<EdgeSegment> {
    let mut segments = Vec::new();
    for (i, r) in outputs.iter().enumerate() {
        if r.width <= 0 || r.height <= 0 {
            continue;
        }
        for direction in [
            Direction::Left,
            Direction::Right,
            Direction::Top,
            Direction::Bottom,
        ] {
            // This edge's interval along its axis.
            let (edge_lo, edge_hi) = match direction {
                Direction::Left | Direction::Right => (r.y, r.y + r.height),
                Direction::Top | Direction::Bottom => (r.x, r.x + r.width),
            };
            // Intervals of other outputs abutting this edge's line, clamped
            // to the edge interval.
            let mut abutting: Vec<(i32, i32)> = Vec::new();
            for (j, q) in outputs.iter().enumerate() {
                if i == j || q.width <= 0 || q.height <= 0 {
                    continue;
                }
                // Tolerate ±1px: fractional scales round each output's
                // dimensions independently, so abutting monitors can be off
                // by a pixel — an exact equality would manufacture a
                // mid-desktop trigger zone in the gap.
                let shares_boundary = match direction {
                    Direction::Right => (q.x - (r.x + r.width)).abs() <= 1,
                    Direction::Left => ((q.x + q.width) - r.x).abs() <= 1,
                    Direction::Bottom => (q.y - (r.y + r.height)).abs() <= 1,
                    Direction::Top => ((q.y + q.height) - r.y).abs() <= 1,
                };
                if !shares_boundary {
                    continue;
                }
                let (lo, hi) = match direction {
                    Direction::Left | Direction::Right => (q.y, q.y + q.height),
                    Direction::Top | Direction::Bottom => (q.x, q.x + q.width),
                };
                let (lo, hi) = (lo.max(edge_lo), hi.min(edge_hi));
                if lo < hi {
                    abutting.push((lo, hi));
                }
            }
            // Subtract the abutting intervals; what remains is exposed.
            abutting.sort_unstable();
            let mut cursor = edge_lo;
            let mut push = |start: i32, end: i32| {
                if start < end {
                    segments.push(EdgeSegment {
                        direction,
                        output: r.name.clone(),
                        start,
                        len: end - start,
                    });
                }
            };
            for (lo, hi) in abutting {
                push(cursor, lo);
                cursor = cursor.max(hi);
            }
            push(cursor, edge_hi);
        }
    }
    segments
}

/// Fraction of an exposed segment trimmed at each end as a corner dead zone.
/// Every segment end is a desktop-outline corner or an abutment step — both
/// are points the cursor jams into when flung diagonally (or aimed at corner
/// UI), so both get the dead zone: corners never trigger a switch.
pub(crate) const CORNER_TRIM_PERCENT: i32 = 8;

/// Trims CORNER_TRIM_PERCENT off both ends of an exposed segment (see
/// CORNER_TRIM_PERCENT). Returns None if nothing usable remains.
fn trim_corner_dead_zones(segment: EdgeSegment) -> Option<EdgeSegment> {
    let trim = segment.len * CORNER_TRIM_PERCENT / 100;
    let len = segment.len - 2 * trim;
    if len <= 0 {
        return None;
    }
    Some(EdgeSegment {
        start: segment.start + trim,
        len,
        ..segment
    })
}

/// The Hyprland IPC socket
/// ($XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket.sock), the
/// same channel hyprctl uses. Errors when not running under Hyprland.
fn hyprland_socket_path() -> Result<PathBuf> {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .context("XDG_RUNTIME_DIR is not set (no wayland session?)")?;
    let signature = std::env::var("HYPRLAND_INSTANCE_SIGNATURE")
        .context("HYPRLAND_INSTANCE_SIGNATURE is not set (not running under Hyprland)")?;
    Ok(PathBuf::from(runtime_dir)
        .join("hypr")
        .join(signature)
        .join(".socket.sock"))
}

/// Runs one command against Hyprland's IPC socket: connect, send the
/// command, half-close the write side, read the reply to EOF. One-shot per
/// query: the compositor closes the connection after each reply (verified
/// empirically — hyprctl --batch's single connection works only because all
/// its commands go out in ONE write), so each query reconnects.
fn hyprland_query(socket: &Path, cmd: &[u8]) -> Result<String> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("Failed to connect to Hyprland IPC at {}", socket.display()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("Failed to configure Hyprland IPC socket")?;
    let query = String::from_utf8_lossy(cmd);
    stream
        .write_all(cmd)
        .with_context(|| format!("Failed to query Hyprland '{}'", query))?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .with_context(|| format!("Failed to finish the Hyprland '{}' query", query))?;
    let mut reply = String::new();
    stream
        .read_to_string(&mut reply)
        .with_context(|| format!("Failed to read the Hyprland '{}' reply", query))?;
    Ok(reply)
}

/// Queries the monitor layout from Hyprland's IPC socket (see
/// hyprland_socket_path). Errors when not running under Hyprland.
pub(crate) fn hyprland_layout(socket: &Path) -> Result<Vec<OutputRect>> {
    // "j/monitors" is the JSON variant of the monitors request.
    parse_monitors_json(&hyprland_query(socket, b"j/monitors")?)
}

/// Parses Hyprland's JSON monitors reply into logical output rectangles
/// (mode size divided by scale). Disabled outputs are skipped.
fn parse_monitors_json(json: &str) -> Result<Vec<OutputRect>> {
    let value: serde_json::Value =
        serde_json::from_str(json).context("Failed to parse Hyprland monitors reply")?;
    let monitors = value
        .as_array()
        .context("Hyprland monitors reply is not a JSON array")?;
    let mut outputs = Vec::new();
    for monitor in monitors {
        if monitor["disabled"].as_bool() == Some(true) {
            continue;
        }
        let name = monitor["name"]
            .as_str()
            .context("Hyprland monitor entry lacks a name")?
            .to_string();
        let get_i64 = |key: &str| -> Result<i64> {
            monitor[key]
                .as_i64()
                .with_context(|| format!("Hyprland monitor '{}' lacks '{}'", name, key))
        };
        let (x, y, mut width, mut height) = (
            get_i64("x")?,
            get_i64("y")?,
            get_i64("width")?,
            get_i64("height")?,
        );
        // Hyprland reports the native (pre-rotation) mode size. Odd transforms
        // (90°/270° and their flipped variants) rotate the output, so the
        // logical width and height are swapped relative to the mode.
        let transform = monitor["transform"].as_i64().unwrap_or(0);
        if transform % 2 == 1 {
            std::mem::swap(&mut width, &mut height);
        }
        let scale = monitor["scale"]
            .as_f64()
            .filter(|s| *s > 0.0)
            .unwrap_or(1.0);
        outputs.push(OutputRect {
            name,
            x: x as i32,
            y: y as i32,
            width: (width as f64 / scale).round() as i32,
            height: (height as f64 / scale).round() as i32,
        });
    }
    Ok(outputs)
}

/// Minimum spacing between two fires of the same edge: after a completed
/// dwell fires the switch, enters inside the cooldown are ignored so parking
/// on (or bouncing against) the edge can't machine-gun switches.
const REARM_COOLDOWN: Duration = Duration::from_secs(1);

/// Edge-resistance state machine for one direction: an enter starts a dwell
/// timer, a leave before the deadline cancels it, a completed dwell fires
/// once and the re-arm cooldown blocks immediate refires. Pure over `now`
/// instants so the state machine is testable without sleeping.
pub(crate) struct DwellTimer {
    dwell: Duration,
    cooldown: Duration,
    /// When the cursor entered (dwell in progress), None while disarmed.
    entered_at: Option<Instant>,
    /// When the last fire happened (for the re-arm cooldown).
    last_fired: Option<Instant>,
}

impl DwellTimer {
    pub fn new(dwell: Duration, cooldown: Duration) -> Self {
        Self {
            dwell,
            cooldown,
            entered_at: None,
            last_fired: None,
        }
    }

    /// The cursor entered the edge. Returns the fire deadline, or None when
    /// the enter is ignored because the re-arm cooldown is still running.
    pub fn enter(&mut self, now: Instant) -> Option<Instant> {
        if let Some(fired) = self.last_fired {
            if now.duration_since(fired) < self.cooldown {
                return None;
            }
        }
        self.entered_at = Some(now);
        Some(now + self.dwell)
    }

    /// The cursor left the edge: cancel any pending dwell.
    pub fn leave(&mut self) {
        self.entered_at = None;
    }

    /// Whether the dwell completed (fires once: the state resets and the
    /// re-arm cooldown starts).
    pub fn poll(&mut self, now: Instant) -> bool {
        match self.entered_at {
            Some(entered) if now.duration_since(entered) >= self.dwell => {
                self.entered_at = None;
                self.last_fired = Some(now);
                true
            }
            _ => false,
        }
    }
}

/// Why an edge target couldn't be resolved against the live client list.
#[derive(Debug, PartialEq)]
pub enum ResolveError {
    /// No clients are connected at all.
    NoClients,
    /// `auto` with more than one connected client.
    AutoAmbiguous(usize),
    /// The fingerprint prefix matched more than one connected client.
    AmbiguousFingerprint(String, usize),
    /// The hostname didn't resolve via the system resolver.
    UnresolvedHostname(String),
    /// The hostname resolved, but no connected client has any of its IPs.
    HostnameMatchesNothing(String),
    /// The hostname's IPs matched more than one connected client.
    AmbiguousHostname(String, usize),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResolveError::NoClients => write!(f, "no clients connected"),
            ResolveError::AutoAmbiguous(n) => write!(
                f,
                "'auto' requires exactly one connected client, but {} are connected",
                n
            ),
            ResolveError::AmbiguousFingerprint(prefix, n) => write!(
                f,
                "fingerprint prefix '{}' matches {} connected clients",
                prefix, n
            ),
            ResolveError::UnresolvedHostname(name) => write!(
                f,
                "couldn't resolve '{}'; use the fingerprint prefix from the 'Added client ...' log line",
                name
            ),
            ResolveError::HostnameMatchesNothing(name) => write!(
                f,
                "'{}' resolved, but no connected client has its IP; use the fingerprint prefix from the 'Added client ...' log line",
                name
            ),
            ResolveError::AmbiguousHostname(name, n) => write!(
                f,
                "hostname '{}' matches {} connected clients by IP",
                name, n
            ),
        }
    }
}

/// Resolves an edge target to the fingerprint of a connected client, against
/// the LIVE client list (tolerates reconnects and IP changes: nothing is
/// resolved to an IP at startup). `clients` are (endpoint, fingerprint)
/// pairs; `resolve_host` is injected for tests. Fingerprint prefix matching
/// mirrors the rotation's goto resolution; `auto` requires exactly one
/// client; anything else falls through to hostname→IP matching.
pub fn resolve_edge_target(
    target: &EdgeTarget,
    clients: &[(SocketAddr, String)],
    resolve_host: &dyn Fn(&str) -> Vec<IpAddr>,
) -> std::result::Result<String, ResolveError> {
    match target {
        EdgeTarget::Auto => match clients.len() {
            0 => Err(ResolveError::NoClients),
            1 => Ok(clients[0].1.clone()),
            n => Err(ResolveError::AutoAmbiguous(n)),
        },
        EdgeTarget::Named(name) => {
            // A fingerprint prefix first (like goto): a client whose
            // certificate fingerprint starts with the target string.
            let matching: Vec<&(SocketAddr, String)> = clients
                .iter()
                .filter(|(_, fp)| fp.starts_with(name.as_str()))
                .collect();
            match matching.len() {
                1 => return Ok(matching[0].1.clone()),
                n if n > 1 => {
                    return Err(ResolveError::AmbiguousFingerprint(name.clone(), n));
                }
                _ => {}
            }
            // Then a hostname: resolve it (and its .local mDNS variant) and
            // match a connected client by IP.
            let ips = resolve_host(name);
            if ips.is_empty() {
                return Err(ResolveError::UnresolvedHostname(name.clone()));
            }
            let matching: Vec<&(SocketAddr, String)> = clients
                .iter()
                .filter(|(endpoint, _)| ips.contains(&endpoint.ip()))
                .collect();
            match matching.len() {
                0 => Err(ResolveError::HostnameMatchesNothing(name.clone())),
                1 => Ok(matching[0].1.clone()),
                n => Err(ResolveError::AmbiguousHostname(name.clone(), n)),
            }
        }
    }
}

/// System-resolves a hostname to IPs: the bare name first, then the `.local`
/// mDNS variant (avahi host records resolve through NSS on LANs set up for
/// it). Best-effort: an empty result just means "unresolvable here".
pub fn resolve_hostname(name: &str) -> Vec<IpAddr> {
    let mut ips = Vec::new();
    for candidate in [name.to_string(), format!("{}.local", name)] {
        if let Ok(addrs) = (candidate.as_str(), 0).to_socket_addrs() {
            ips.extend(addrs.map(|addr| addr.ip()));
        }
    }
    ips.sort();
    ips.dedup();
    ips
}

/// How often the cursor position is polled from Hyprland's IPC.
const POLL_INTERVAL: Duration = Duration::from_millis(40);

/// How long the poller waits after a failed query before retrying.
const POLL_FAILURE_BACKOFF: Duration = Duration::from_millis(500);

/// Queries the cursor position from Hyprland's IPC (see hyprland_query for
/// why each poll reconnects).
fn cursor_position(socket: &Path) -> Result<(i32, i32)> {
    parse_cursorpos(&hyprland_query(socket, b"cursorpos")?)
}

/// Parses Hyprland's cursorpos reply ("x, y"; coordinates can be negative
/// when outputs sit left of/above the layout origin).
fn parse_cursorpos(reply: &str) -> Result<(i32, i32)> {
    let reply = reply.trim();
    let (x, y) = reply
        .split_once(',')
        .with_context(|| format!("unexpected cursorpos reply '{}'", reply))?;
    let x = x
        .trim()
        .parse::<i32>()
        .with_context(|| format!("unexpected cursorpos reply '{}'", reply))?;
    let y = y
        .trim()
        .parse::<i32>()
        .with_context(|| format!("unexpected cursorpos reply '{}'", reply))?;
    Ok((x, y))
}

/// Polls the cursor position every POLL_INTERVAL and forwards it to the edge
/// manager; a failed query is logged at debug and retried after
/// POLL_FAILURE_BACKOFF. Runs on its own thread (blocking socket IO) and
/// ends when the edge manager is gone (server shutting down).
fn run_cursor_poller(socket: PathBuf, pos_tx: mpsc::UnboundedSender<(i32, i32)>) {
    loop {
        match cursor_position(&socket) {
            Ok(pos) => {
                if pos_tx.send(pos).is_err() {
                    return;
                }
                std::thread::sleep(POLL_INTERVAL);
            }
            Err(e) => {
                debug!(
                    "Screen-edge cursor poll failed ({:#}), retrying in {:?}",
                    e, POLL_FAILURE_BACKOFF
                );
                std::thread::sleep(POLL_FAILURE_BACKOFF);
            }
        }
    }
}

/// How far beyond an edge line the cursor still counts as "on the edge", in
/// logical pixels. 0 = the cursor must reach the very edge: left x <= 0,
/// right x >= the output's last column (and likewise for top/bottom rows).
pub(crate) const EDGE_TRIGGER_PX: i32 = 0;

/// A trigger zone: one exposed, corner-trimmed segment of one output's edge,
/// in global layout coordinates. The cursor is on the zone's edge when its
/// coordinate crosses the edge line while its along-axis coordinate lies
/// within [start, start + len).
#[derive(Clone, Debug, PartialEq, Eq)]
struct EdgeZone {
    direction: Direction,
    output: String,
    /// The edge line: the output's first (left/top) or last (right/bottom)
    /// pixel column/row on that side.
    edge: i32,
    /// Range start along the edge axis (y for left/right, x for top/bottom).
    start: i32,
    len: i32,
}

/// Whether the cursor at (x, y) is on this zone's edge.
fn zone_contains(zone: &EdgeZone, x: i32, y: i32) -> bool {
    let along = match zone.direction {
        Direction::Left | Direction::Right => y,
        Direction::Top | Direction::Bottom => x,
    };
    if along < zone.start || along >= zone.start + zone.len {
        return false;
    }
    match zone.direction {
        Direction::Left => x <= zone.edge + EDGE_TRIGGER_PX,
        Direction::Right => x >= zone.edge - EDGE_TRIGGER_PX,
        Direction::Top => y <= zone.edge + EDGE_TRIGGER_PX,
        Direction::Bottom => y >= zone.edge - EDGE_TRIGGER_PX,
    }
}

/// Where along a zone's range the cursor at (x, y) sits, as a fraction
/// (0.0..=1.0): the y fraction for left/right edges, the x fraction for
/// top/bottom. Sent with the client's return request (see
/// ClientEvent::SwitchRequest; reserved for future cursor warping — the
/// server ignores it for now).
fn edge_fraction(zone: &EdgeZone, x: i32, y: i32) -> f64 {
    let along = match zone.direction {
        Direction::Left | Direction::Right => y,
        Direction::Top | Direction::Bottom => x,
    };
    ((along - zone.start) as f64 / zone.len as f64).clamp(0.0, 1.0)
}

/// Turns a layout into trigger zones for the mapped directions: exposed
/// segments only, corner dead zones applied.
fn edge_zones(map: &EdgeMap, layout: &[OutputRect]) -> Vec<EdgeZone> {
    let mut zones = Vec::new();
    for segment in exposed_segments(layout) {
        if !map.targets.contains_key(&segment.direction) {
            continue;
        }
        let Some(segment) = trim_corner_dead_zones(segment) else {
            continue;
        };
        let Some(output) = layout.iter().find(|o| o.name == segment.output) else {
            continue;
        };
        let edge = match segment.direction {
            Direction::Left => output.x,
            Direction::Right => output.x + output.width - 1,
            Direction::Top => output.y,
            Direction::Bottom => output.y + output.height - 1,
        };
        zones.push(EdgeZone {
            direction: segment.direction,
            output: segment.output,
            edge,
            start: segment.start,
            len: segment.len,
        });
    }
    zones
}

/// Logs the trigger zones, one per line — or the warning that --edge-map
/// matches no exposed segment on the current layout.
fn log_zones(zones: &[EdgeZone]) {
    if zones.is_empty() {
        warn!("Screen-edge switching: no exposed screen-edge segments match --edge-map on the current monitor layout");
        return;
    }
    for zone in zones {
        info!(
            "Screen-edge switching: watching the {} edge of {} ({} {}..{})",
            zone.direction.as_str(),
            zone.output,
            match zone.direction {
                Direction::Left | Direction::Right => "y",
                Direction::Top | Direction::Bottom => "x",
            },
            zone.start,
            zone.start + zone.len
        );
    }
}

/// Consecutive equal poll outcomes required before a direction's on/off
/// state transitions: single-poll jitter (the cursor grazing a zone
/// boundary) never reaches the dwell timer.
const STABLE_POLLS: u32 = 2;

/// Per-direction edge-contact debouncer (see STABLE_POLLS): being on the
/// edge is the Enter equivalent, leaving it the Leave equivalent. Pure over
/// successive poll outcomes so the transition logic is testable.
struct EdgeDebounce {
    /// The committed state (true = cursor on the edge).
    on: bool,
    /// The candidate state and how many consecutive polls reported it.
    candidate: Option<bool>,
    streak: u32,
}

impl EdgeDebounce {
    fn new() -> Self {
        Self {
            on: false,
            candidate: None,
            streak: 0,
        }
    }

    /// Feeds one poll outcome; returns Some(state) when the committed state
    /// transitioned.
    fn poll(&mut self, on: bool) -> Option<bool> {
        if on == self.on {
            self.candidate = None;
            self.streak = 0;
            return None;
        }
        if self.candidate == Some(on) {
            self.streak += 1;
        } else {
            self.candidate = Some(on);
            self.streak = 1;
        }
        if self.streak >= STABLE_POLLS {
            self.on = on;
            self.candidate = None;
            self.streak = 0;
            Some(on)
        } else {
            None
        }
    }
}

/// How often the monitor layout is re-queried; a change recomputes the
/// trigger zones.
const LAYOUT_REQUERY_INTERVAL: Duration = Duration::from_secs(30);

/// What a completed dwell fires. Server mode resolves the edge's target and
/// fires Event::SwitchTo into the rotation; client mode asks the server to
/// take input back, carrying the fraction along the edge where the cursor
/// crossed (see ClientEvent::SwitchRequest).
enum Fire {
    /// Server mode: the rotation's event queue.
    Event(mpsc::Sender<Event>),
    /// Client mode: the queue the client connection's event loop drains onto
    /// its events stream; closed means the connection (and with it the
    /// receiver) is gone — detection goes quiet while disconnected.
    Request(mpsc::UnboundedSender<f64>),
}

/// The edge manager task, server mode: spawns the cursor poller, owns the
/// trigger zones, the dwell state machines, target resolution, and the
/// periodic layout re-query. Exits (disabling the feature) when Hyprland's
/// IPC is unavailable; otherwise runs until the server shuts down.
pub async fn run(
    map: EdgeMap,
    dwell: Duration,
    event_tx: mpsc::Sender<Event>,
    clients_rx: watch::Receiver<Vec<(SocketAddr, String)>>,
) {
    run_inner(map, dwell, Fire::Event(event_tx), Some(clients_rx)).await
}

/// The edge manager task, client mode: same detection as the server, but a
/// completed dwell sends the server a return request (the fraction along the
/// edge) instead of switching to a client. Spawned per connection: it exits
/// when the connection's request receiver is dropped, so detection is quiet
/// while disconnected.
pub async fn run_client(map: EdgeMap, dwell: Duration, request_tx: mpsc::UnboundedSender<f64>) {
    run_inner(map, dwell, Fire::Request(request_tx), None).await
}

/// The shared edge manager loop (see run / run_client). `clients_rx` is the
/// live client list the server resolves targets against; None in client
/// mode, where the one peer (the server) needs no resolution.
async fn run_inner(
    map: EdgeMap,
    dwell: Duration,
    fire: Fire,
    mut clients_rx: Option<watch::Receiver<Vec<(SocketAddr, String)>>>,
) {
    // The socket path is resolved once here, at manager start, instead of on
    // every 40ms cursor poll (the env vars it derives from are fixed for the
    // lifetime of the session).
    let socket = match hyprland_socket_path() {
        Ok(socket) => socket,
        Err(e) => {
            warn!("Screen-edge switching disabled: {:#}", e);
            return;
        }
    };
    let socket_for_layout = socket.clone();
    let layout = match tokio::task::spawn_blocking(move || hyprland_layout(&socket_for_layout)).await {
        Ok(Ok(layout)) if !layout.is_empty() => layout,
        Ok(Ok(_)) => {
            warn!("Screen-edge switching disabled: Hyprland reports no outputs");
            return;
        }
        Ok(Err(e)) => {
            warn!("Screen-edge switching disabled: {:#}", e);
            return;
        }
        Err(e) => {
            warn!("Screen-edge switching disabled: layout query panicked: {:#}", e);
            return;
        }
    };
    info!(
        "Screen-edge switching enabled (dwell {:?}, cooldown {:?}): {}",
        dwell,
        REARM_COOLDOWN,
        map.targets
            .iter()
            .map(|(dir, target)| format!("{}={}", dir.as_str(), target))
            .collect::<Vec<String>>()
            .join(", ")
    );
    log_layout(&layout);
    let (pos_tx, mut pos_rx) = mpsc::unbounded_channel::<(i32, i32)>();
    let poller_socket = socket.clone();
    std::thread::spawn(move || run_cursor_poller(poller_socket, pos_tx));
    let mut zones = edge_zones(&map, &layout);
    log_zones(&zones);
    let mut current_layout = layout;

    // Per-direction state: the debouncer turns polled edge contact into
    // enter/leave equivalents that drive the dwell timer.
    struct DirState {
        timer: DwellTimer,
        debounce: EdgeDebounce,
        deadline: Option<Instant>,
    }
    let mut dirs: HashMap<Direction, DirState> = map
        .targets
        .keys()
        .map(|dir| {
            (
                *dir,
                DirState {
                    timer: DwellTimer::new(dwell, REARM_COOLDOWN),
                    debounce: EdgeDebounce::new(),
                    deadline: None,
                },
            )
        })
        .collect();
    if let Some(rx) = &clients_rx {
        log_edge_resolutions(&map, &rx.borrow());
    }
    // The last polled cursor position: client mode reads the crossing
    // fraction off it at fire time.
    let mut last_pos: Option<(i32, i32)> = None;

    let mut requery = time::interval(LAYOUT_REQUERY_INTERVAL);
    // Skip the immediate first tick; the startup query just ran.
    requery.tick().await;
    loop {
        let next_deadline = dirs.values().filter_map(|state| state.deadline).min();
        tokio::select! {
            pos = pos_rx.recv() => {
                let Some((x, y)) = pos else {
                    warn!("Screen-edge switching disabled: the cursor poller is gone");
                    return;
                };
                last_pos = Some((x, y));
                let now = Instant::now();
                for (dir, state) in dirs.iter_mut() {
                    let on = zones
                        .iter()
                        .any(|zone| zone.direction == *dir && zone_contains(zone, x, y));
                    match state.debounce.poll(on) {
                        Some(true) => state.deadline = state.timer.enter(now),
                        Some(false) => {
                            state.timer.leave();
                            state.deadline = None;
                        }
                        None => {}
                    }
                }
            }
            // Server mode only: re-log target resolutions on client
            // (dis)connect. Pending forever in client mode (no client list).
            changed = async {
                match &mut clients_rx {
                    Some(rx) => rx.changed().await,
                    None => std::future::pending().await,
                }
            } => {
                if changed.is_err() {
                    // The rotation loop is gone: the server is shutting down.
                    return;
                }
                if let Some(rx) = &clients_rx {
                    log_edge_resolutions(&map, &rx.borrow());
                }
            }
            // Client mode only: the request receiver was dropped with the
            // connection — go quiet until a new connection respawns us.
            _ = async {
                match &fire {
                    Fire::Request(request_tx) => request_tx.closed().await,
                    Fire::Event(_) => std::future::pending().await,
                }
            } => {
                debug!("Screen-edge switching: connection gone, edge detection off");
                return;
            }
            _ = requery.tick() => {
                let socket_for_requery = socket.clone();
                match tokio::task::spawn_blocking(move || hyprland_layout(&socket_for_requery)).await {
                    Ok(Ok(new_layout)) if !new_layout.is_empty() => {
                        if new_layout != current_layout {
                            info!("Screen-edge switching: monitor layout changed, recomputing edge zones");
                            log_layout(&new_layout);
                            zones = edge_zones(&map, &new_layout);
                            log_zones(&zones);
                            current_layout = new_layout;
                            // Contact states measured against the old layout's
                            // zones are meaningless under the new one.
                            for state in dirs.values_mut() {
                                state.debounce = EdgeDebounce::new();
                                state.deadline = None;
                                state.timer.leave();
                            }
                        }
                    }
                    Ok(Ok(_)) => {
                        warn!("Screen-edge switching: Hyprland layout re-query returned no outputs, keeping existing zones");
                    }
                    Ok(Err(e)) => {
                        warn!("Screen-edge switching: Hyprland layout re-query failed ({:#}), keeping existing zones", e);
                    }
                    Err(e) => {
                        warn!("Screen-edge switching: Hyprland layout re-query panicked ({:#}), keeping existing zones", e);
                    }
                }
            }
            _ = async {
                match next_deadline {
                    Some(deadline) => time::sleep_until(time::Instant::from_std(deadline)).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                let now = Instant::now();
                for (dir, state) in dirs.iter_mut() {
                    if !state.deadline.is_some_and(|deadline| deadline <= now)
                        || !state.timer.poll(now)
                    {
                        continue;
                    }
                    // Fired: the timer reset and started its re-arm cooldown.
                    state.deadline = None;
                    // Require fresh contact at fire time: the debounce needs
                    // STABLE_POLLS consecutive off-polls to commit a leave,
                    // so the dwell deadline can arrive before leave() is
                    // called. If the cursor already left the zone, skip.
                    if let Some((x, y)) = last_pos {
                        if !zones.iter().any(|zone| zone.direction == *dir && zone_contains(zone, x, y)) {
                            debug!("Edge switch via {} edge skipped: cursor left before the dwell completed", dir.as_str());
                            continue;
                        }
                    }
                    match &fire {
                        Fire::Event(event_tx) => {
                            let clients = clients_rx
                                .as_ref()
                                .expect("server mode always carries the client list")
                                .borrow()
                                .clone();
                            let target = map.targets[dir].clone();
                            // resolve_hostname does blocking getaddrinfo — run
                            // it off the async executor (only 2 worker threads;
                            // one may already be blockable by cert prompts).
                            match tokio::task::spawn_blocking(move || {
                                resolve_edge_target(&target, &clients, &resolve_hostname)
                            }).await {
                                Ok(Ok(fingerprint)) => {
                                    info!("Edge switch to client {} via {} edge", fingerprint, dir.as_str());
                                    if let Err(e) = event_tx.send(Event::SwitchTo(fingerprint)).await {
                                        warn!("Failed to submit edge switch event: {:?}", e);
                                    }
                                }
                                Ok(Err(e)) => {
                                    warn!("Edge switch via {} edge did not fire: {}", dir.as_str(), e);
                                }
                                Err(e) => {
                                    warn!("Edge switch via {} edge resolution panicked: {:#}", dir.as_str(), e);
                                }
                            }
                        }
                        Fire::Request(request_tx) => {
                            // Where along the edge the cursor crossed, off the
                            // last polled position inside this direction's
                            // zone (0.5 if it slipped out between polls).
                            let y_fraction = last_pos
                                .and_then(|(x, y)| {
                                    zones
                                        .iter()
                                        .find(|zone| zone.direction == *dir && zone_contains(zone, x, y))
                                        .map(|zone| edge_fraction(zone, x, y))
                                })
                                .unwrap_or(0.5);
                            info!("Edge switch request to server via {} edge", dir.as_str());
                            if request_tx.send(y_fraction).is_err() {
                                // The connection (and its receiver) is gone.
                                debug!("Screen-edge switching: connection gone, edge detection off");
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Logs the monitor layout in one line.
fn log_layout(layout: &[OutputRect]) {
    info!(
        "Screen-edge switching: monitor layout: {}",
        layout
            .iter()
            .map(|o| format!("{} {}x{}@({},{})", o.name, o.width, o.height, o.x, o.y))
            .collect::<Vec<String>>()
            .join(", ")
    );
}

/// Resolves every mapped target against the current client list and logs the
/// outcome — at startup and on every client (dis)connect, so the mapping is
/// visible before anyone pushes the cursor into an edge.
fn log_edge_resolutions(map: &EdgeMap, clients: &[(SocketAddr, String)]) {
    for (dir, target) in &map.targets {
        match resolve_edge_target(target, clients, &resolve_hostname) {
            Ok(fingerprint) => info!(
                "Screen-edge switching: {} edge → client {} (target '{}')",
                dir.as_str(),
                fingerprint,
                target
            ),
            Err(e) => warn!(
                "Screen-edge switching: {} edge target '{}' is not resolvable right now: {}",
                dir.as_str(),
                target,
                e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rect(name: &str, x: i32, y: i32, width: i32, height: i32) -> OutputRect {
        OutputRect {
            name: name.to_string(),
            x,
            y,
            width,
            height,
        }
    }

    /// Segments of one direction, as (output, start, len), sorted for
    /// stable comparison.
    fn segments_of(
        segments: &[EdgeSegment],
        direction: Direction,
    ) -> Vec<(String, i32, i32)> {
        let mut found: Vec<(String, i32, i32)> = segments
            .iter()
            .filter(|s| s.direction == direction)
            .map(|s| (s.output.clone(), s.start, s.len))
            .collect();
        found.sort();
        found
    }

    #[test]
    fn exposed_side_by_side() {
        // The user's setup: two 1920x1080 monitors, the right edge exposed
        // only on the rightmost one.
        let layout = vec![
            rect("DP-1", 0, 0, 1920, 1080),
            rect("HDMI-A-1", 1920, 0, 1920, 1080),
        ];
        let segments = exposed_segments(&layout);
        assert_eq!(
            segments_of(&segments, Direction::Right),
            vec![("HDMI-A-1".to_string(), 0, 1080)]
        );
        assert_eq!(
            segments_of(&segments, Direction::Left),
            vec![("DP-1".to_string(), 0, 1080)]
        );
        assert_eq!(
            segments_of(&segments, Direction::Top),
            vec![
                ("DP-1".to_string(), 0, 1920),
                ("HDMI-A-1".to_string(), 1920, 1920)
            ]
        );
        assert_eq!(
            segments_of(&segments, Direction::Bottom),
            vec![
                ("DP-1".to_string(), 0, 1920),
                ("HDMI-A-1".to_string(), 1920, 1920)
            ]
        );
    }

    #[test]
    fn exposed_stacked() {
        let layout = vec![
            rect("DP-1", 0, 0, 1920, 1080),
            rect("HDMI-A-1", 0, 1080, 1920, 1080),
        ];
        let segments = exposed_segments(&layout);
        // DP-1's bottom and HDMI-A-1's top are fully abutted.
        assert_eq!(
            segments_of(&segments, Direction::Bottom),
            vec![("HDMI-A-1".to_string(), 0, 1920)]
        );
        assert_eq!(
            segments_of(&segments, Direction::Top),
            vec![("DP-1".to_string(), 0, 1920)]
        );
        assert_eq!(
            segments_of(&segments, Direction::Left),
            vec![
                ("DP-1".to_string(), 0, 1080),
                ("HDMI-A-1".to_string(), 1080, 1080)
            ]
        );
    }

    #[test]
    fn exposed_l_shape_with_offset() {
        // Step segments: B is shifted down by half a height, splitting both
        // facing edges into an abutted and an exposed interval.
        let layout = vec![
            rect("A", 0, 0, 1920, 1080),
            rect("B", 1920, 540, 1920, 1080),
        ];
        let segments = exposed_segments(&layout);
        assert_eq!(
            segments_of(&segments, Direction::Right),
            vec![
                ("A".to_string(), 0, 540),
                ("B".to_string(), 540, 1080)
            ]
        );
        assert_eq!(
            segments_of(&segments, Direction::Left),
            vec![
                ("A".to_string(), 0, 1080),
                ("B".to_string(), 1080, 540)
            ]
        );
        // A's bottom and B's top are fully exposed (nothing below/above).
        assert_eq!(
            segments_of(&segments, Direction::Bottom),
            vec![
                ("A".to_string(), 0, 1920),
                ("B".to_string(), 1920, 1920)
            ]
        );
        assert_eq!(
            segments_of(&segments, Direction::Top),
            vec![
                ("A".to_string(), 0, 1920),
                ("B".to_string(), 1920, 1920)
            ]
        );
    }

    #[test]
    fn exposed_three_monitors() {
        let layout = vec![
            rect("L", 0, 0, 1920, 1080),
            rect("M", 1920, 0, 1920, 1080),
            rect("R", 3840, 0, 1920, 1080),
        ];
        let segments = exposed_segments(&layout);
        assert_eq!(
            segments_of(&segments, Direction::Left),
            vec![("L".to_string(), 0, 1080)]
        );
        assert_eq!(
            segments_of(&segments, Direction::Right),
            vec![("R".to_string(), 0, 1080)]
        );
        assert_eq!(segments_of(&segments, Direction::Top).len(), 3);
        assert_eq!(segments_of(&segments, Direction::Bottom).len(), 3);
    }

    #[test]
    fn exposed_differing_heights() {
        // B is taller: A's right edge is fully covered, and B's left edge
        // keeps an exposed step segment below A.
        let layout = vec![
            rect("A", 0, 0, 1920, 1080),
            rect("B", 1920, 0, 1920, 1440),
        ];
        let segments = exposed_segments(&layout);
        assert_eq!(
            segments_of(&segments, Direction::Right),
            vec![("B".to_string(), 0, 1440)]
        );
        assert_eq!(
            segments_of(&segments, Direction::Left),
            vec![
                ("A".to_string(), 0, 1080),
                ("B".to_string(), 1080, 360)
            ]
        );
    }

    #[test]
    fn monitors_json_applies_scale_and_skips_disabled() {
        let json = r#"[
            {"name": "DP-1", "x": 0, "y": 0, "width": 3840, "height": 2160, "scale": 2.0, "disabled": false},
            {"name": "HDMI-A-1", "x": 1920, "y": 0, "width": 1920, "height": 1080, "scale": 1.0, "disabled": true}
        ]"#;
        let outputs = parse_monitors_json(json).unwrap();
        assert_eq!(outputs, vec![rect("DP-1", 0, 0, 1920, 1080)]);
    }

    #[test]
    fn corner_dead_zones_trim_both_ends() {
        let segment = EdgeSegment {
            direction: Direction::Right,
            output: "A".to_string(),
            start: 0,
            len: 1080,
        };
        let trimmed = trim_corner_dead_zones(segment).unwrap();
        // 8% of 1080 = 86 px off each end.
        assert_eq!(trimmed.start, 86);
        assert_eq!(trimmed.len, 1080 - 2 * 86);
    }

    #[test]
    fn corner_dead_zones_keep_short_segments_usable() {
        // A 60px step segment loses only 4px per end, not the whole segment.
        let segment = EdgeSegment {
            direction: Direction::Right,
            output: "A".to_string(),
            start: 0,
            len: 60,
        };
        let trimmed = trim_corner_dead_zones(segment).unwrap();
        assert_eq!(trimmed.start, 4);
        assert_eq!(trimmed.len, 52);
    }

    fn client_list(entries: &[(&str, &str)]) -> Vec<(SocketAddr, String)> {
        entries
            .iter()
            .map(|(endpoint, fp)| {
                (endpoint.parse::<SocketAddr>().unwrap(), fp.to_string())
            })
            .collect()
    }

    fn no_ips(_: &str) -> Vec<IpAddr> {
        vec![]
    }

    #[test]
    fn resolve_fingerprint_prefix() {
        let clients = client_list(&[
            ("10.0.0.1:9000", "aaaa1111ffff"),
            ("10.0.0.2:9000", "bbbb2222ffff"),
        ]);
        let target = EdgeTarget::Named("aaaa".to_string());
        assert_eq!(
            resolve_edge_target(&target, &clients, &no_ips),
            Ok("aaaa1111ffff".to_string())
        );
        // No match: falls through to hostname resolution, which fails here.
        let target = EdgeTarget::Named("cccc".to_string());
        assert_eq!(
            resolve_edge_target(&target, &clients, &no_ips),
            Err(ResolveError::UnresolvedHostname("cccc".to_string()))
        );
        // Ambiguous prefix.
        let dupes = client_list(&[
            ("10.0.0.1:9000", "aaaa1111ffff"),
            ("10.0.0.2:9000", "aaaa2222ffff"),
        ]);
        assert_eq!(
            resolve_edge_target(&target_named("aaaa"), &dupes, &no_ips),
            Err(ResolveError::AmbiguousFingerprint("aaaa".to_string(), 2))
        );
    }

    fn target_named(name: &str) -> EdgeTarget {
        EdgeTarget::Named(name.to_string())
    }

    #[test]
    fn resolve_auto_requires_exactly_one_client() {
        let one = client_list(&[("10.0.0.1:9000", "aaaa1111ffff")]);
        assert_eq!(
            resolve_edge_target(&EdgeTarget::Auto, &one, &no_ips),
            Ok("aaaa1111ffff".to_string())
        );
        let none = client_list(&[]);
        assert_eq!(
            resolve_edge_target(&EdgeTarget::Auto, &none, &no_ips),
            Err(ResolveError::NoClients)
        );
        let two = client_list(&[
            ("10.0.0.1:9000", "aaaa1111ffff"),
            ("10.0.0.2:9000", "bbbb2222ffff"),
        ]);
        assert_eq!(
            resolve_edge_target(&EdgeTarget::Auto, &two, &no_ips),
            Err(ResolveError::AutoAmbiguous(2))
        );
    }

    #[test]
    fn resolve_hostname_matches_client_by_ip() {
        let clients = client_list(&[
            ("10.0.0.1:9000", "aaaa1111ffff"),
            ("10.0.0.2:9000", "bbbb2222ffff"),
        ]);
        let resolver = |name: &str| -> Vec<IpAddr> {
            match name {
                "laptop" => vec!["10.0.0.2".parse().unwrap()],
                _ => vec![],
            }
        };
        assert_eq!(
            resolve_edge_target(&target_named("laptop"), &clients, &resolver),
            Ok("bbbb2222ffff".to_string())
        );
        // Resolves, but to an IP no connected client has.
        let resolver = |_: &str| -> Vec<IpAddr> { vec!["10.0.0.99".parse().unwrap()] };
        assert_eq!(
            resolve_edge_target(&target_named("laptop"), &clients, &resolver),
            Err(ResolveError::HostnameMatchesNothing("laptop".to_string()))
        );
    }

    #[test]
    fn dwell_fires_at_deadline_once() {
        let now = Instant::now();
        let mut timer = DwellTimer::new(Duration::from_millis(250), Duration::from_secs(1));
        let deadline = timer.enter(now).expect("enter should arm the dwell");
        assert_eq!(deadline, now + Duration::from_millis(250));
        assert!(!timer.poll(now + Duration::from_millis(249)));
        assert!(timer.poll(now + Duration::from_millis(250)));
        // Fires once: a second poll without a new enter does not refire.
        assert!(!timer.poll(now + Duration::from_millis(500)));
    }

    #[test]
    fn dwell_leave_cancels() {
        let now = Instant::now();
        let mut timer = DwellTimer::new(Duration::from_millis(250), Duration::from_secs(1));
        timer.enter(now);
        timer.leave();
        assert!(!timer.poll(now + Duration::from_secs(5)));
    }

    #[test]
    fn dwell_cooldown_then_rearm() {
        let now = Instant::now();
        let mut timer = DwellTimer::new(Duration::from_millis(250), Duration::from_secs(1));
        timer.enter(now);
        assert!(timer.poll(now + Duration::from_millis(250)));
        // Re-entering inside the 1s re-arm cooldown is ignored.
        let during = now + Duration::from_millis(500);
        assert!(timer.enter(during).is_none());
        assert!(!timer.poll(during + Duration::from_secs(5)));
        // After the cooldown the edge re-arms and a fresh dwell fires.
        let after = now + Duration::from_millis(1500);
        let deadline = timer.enter(after).expect("cooldown over, should re-arm");
        assert_eq!(deadline, after + Duration::from_millis(250));
        assert!(timer.poll(deadline));
    }

    #[test]
    fn edge_map_parses_forms() {
        let map = parse_edge_map(&["right=auto".to_string()]).unwrap();
        assert_eq!(map.targets.len(), 1);
        assert_eq!(map.targets[&Direction::Right], EdgeTarget::Auto);

        // Repeatable flags and comma-separated values mix.
        let map = parse_edge_map(&[
            "left=aa11bb".to_string(),
            "right=auto,top=laptop".to_string(),
        ])
        .unwrap();
        assert_eq!(map.targets.len(), 3);
        assert_eq!(
            map.targets[&Direction::Left],
            EdgeTarget::Named("aa11bb".to_string())
        );
        assert_eq!(map.targets[&Direction::Right], EdgeTarget::Auto);
        assert_eq!(
            map.targets[&Direction::Top],
            EdgeTarget::Named("laptop".to_string())
        );
    }

    #[test]
    fn edge_map_rejects_bad_entries() {
        // Unknown direction.
        assert!(parse_edge_map(&["diagonal=auto".to_string()]).is_err());
        // Missing '='.
        assert!(parse_edge_map(&["right".to_string()]).is_err());
        // Empty target.
        assert!(parse_edge_map(&["right=".to_string()]).is_err());
        // Duplicate direction.
        assert!(parse_edge_map(&["right=auto,right=laptop".to_string()]).is_err());
        // Nothing usable at all.
        assert!(parse_edge_map(&["".to_string()]).is_err());
    }

    #[test]
    fn client_edge_map_accepts_only_auto() {
        // 'auto' on one or several edges is fine, in both syntax forms.
        let map = parse_client_edge_map(&["left=auto".to_string()]).unwrap();
        assert_eq!(map.targets[&Direction::Left], EdgeTarget::Auto);
        let map = parse_client_edge_map(&["left=auto".to_string(), "top=auto,bottom=auto".to_string()])
            .unwrap();
        assert_eq!(map.targets.len(), 3);
        // A fingerprint prefix or a hostname is a config error on the client:
        // its only peer is the server.
        assert!(parse_client_edge_map(&["left=aa11bb".to_string()]).is_err());
        assert!(parse_client_edge_map(&["left=laptop".to_string()]).is_err());
        // ... even mixed with a valid entry.
        assert!(parse_client_edge_map(&["left=auto,right=laptop".to_string()]).is_err());
        // The base syntax errors still apply.
        assert!(parse_client_edge_map(&["left".to_string()]).is_err());
    }

    #[test]
    fn edge_fraction_tracks_the_along_axis() {
        // A left/right zone reads the y fraction of its range.
        let zone = EdgeZone {
            direction: Direction::Left,
            output: "A".to_string(),
            edge: 0,
            start: 100,
            len: 200,
        };
        assert_eq!(edge_fraction(&zone, 0, 100), 0.0);
        assert_eq!(edge_fraction(&zone, 0, 200), 0.5);
        assert_eq!(edge_fraction(&zone, 0, 299), 0.995);
        // Out-of-range positions clamp into 0.0..=1.0.
        assert_eq!(edge_fraction(&zone, 0, 50), 0.0);
        assert_eq!(edge_fraction(&zone, 0, 400), 1.0);
        // A top/bottom zone reads the x fraction instead.
        let zone = EdgeZone {
            direction: Direction::Top,
            output: "A".to_string(),
            edge: 0,
            start: 1000,
            len: 500,
        };
        assert_eq!(edge_fraction(&zone, 1250, 0), 0.5);
    }

    #[test]
    fn zones_only_mapped_directions_with_corner_trim() {
        let mut map = EdgeMap::default();
        map.targets.insert(Direction::Right, EdgeTarget::Auto);
        let layout = vec![
            rect("DP-1", 0, 0, 1920, 1080),
            rect("HDMI-A-1", 1920, 0, 1920, 1080),
        ];
        let zones = edge_zones(&map, &layout);
        assert_eq!(zones.len(), 1);
        assert_eq!(zones[0].output, "HDMI-A-1");
        assert_eq!(zones[0].direction, Direction::Right);
        // The edge line is the output's last pixel column.
        assert_eq!(zones[0].edge, 1920 + 1920 - 1);
        // Global [86, 994): 8% of 1080 = 86 px trimmed off each end.
        assert_eq!(zones[0].start, 86);
        assert_eq!(zones[0].len, 908);
        assert!(zone_contains(&zones[0], 3839, 500));
        assert!(!zone_contains(&zones[0], 3838, 500));
    }

    /// The offset multi-monitor layout from the user's setup: HDMI-A-1
    /// 3440x1440@(0,0) with eDP-1 2048x1280@(3440,160) to its lower right.
    fn offset_layout() -> Vec<OutputRect> {
        vec![
            rect("HDMI-A-1", 0, 0, 3440, 1440),
            rect("eDP-1", 3440, 160, 2048, 1280),
        ]
    }

    fn all_mapped() -> EdgeMap {
        let mut map = EdgeMap::default();
        for dir in [
            Direction::Left,
            Direction::Right,
            Direction::Top,
            Direction::Bottom,
        ] {
            map.targets.insert(dir, EdgeTarget::Auto);
        }
        map
    }

    fn zone_of(zones: &[EdgeZone], direction: Direction, output: &str) -> EdgeZone {
        zones
            .iter()
            .find(|zone| zone.direction == direction && zone.output == output)
            .unwrap_or_else(|| panic!("no {:?} zone on {}", direction, output))
            .clone()
    }

    #[test]
    fn zones_offset_layout_left() {
        let zones = edge_zones(&all_mapped(), &offset_layout());
        // HDMI-A-1's left edge is fully exposed; 8% of 1440 = 115 trimmed
        // per end → y in [115, 1325).
        let zone = zone_of(&zones, Direction::Left, "HDMI-A-1");
        assert_eq!((zone.edge, zone.start, zone.len), (0, 115, 1210));
        assert!(zone_contains(&zone, 0, 115));
        assert!(zone_contains(&zone, 0, 1324));
        assert!(!zone_contains(&zone, 1, 500));
        assert!(!zone_contains(&zone, 0, 114));
        assert!(!zone_contains(&zone, 0, 1325));
        // eDP-1's left edge is fully abutted by HDMI-A-1: no zone there.
        assert!(zones
            .iter()
            .all(|zone| !(zone.direction == Direction::Left && zone.output == "eDP-1")));
    }

    #[test]
    fn zones_offset_layout_right() {
        let zones = edge_zones(&all_mapped(), &offset_layout());
        // eDP-1's right edge is fully exposed; 8% of 1280 = 102 per end →
        // y in [262, 1338), edge line at the output's last column.
        let zone = zone_of(&zones, Direction::Right, "eDP-1");
        assert_eq!((zone.edge, zone.start, zone.len), (5487, 262, 1076));
        assert!(zone_contains(&zone, 5487, 262));
        assert!(zone_contains(&zone, 5487, 1337));
        assert!(!zone_contains(&zone, 5486, 500));
        assert!(!zone_contains(&zone, 5487, 261));
        assert!(!zone_contains(&zone, 5487, 1338));
        // HDMI-A-1's right edge keeps only the exposed step above eDP-1:
        // [0, 160) trimmed by 12 per end → y in [12, 148).
        let step = zone_of(&zones, Direction::Right, "HDMI-A-1");
        assert_eq!((step.edge, step.start, step.len), (3439, 12, 136));
        assert!(zone_contains(&step, 3439, 12));
        assert!(!zone_contains(&step, 3439, 148));
    }

    #[test]
    fn zones_offset_layout_top_bottom() {
        let zones = edge_zones(&all_mapped(), &offset_layout());
        // Both tops are fully exposed (nothing above either output).
        let top_hdmi = zone_of(&zones, Direction::Top, "HDMI-A-1");
        assert_eq!((top_hdmi.edge, top_hdmi.start, top_hdmi.len), (0, 275, 2890));
        assert!(zone_contains(&top_hdmi, 275, 0));
        assert!(!zone_contains(&top_hdmi, 274, 0));
        assert!(!zone_contains(&top_hdmi, 500, 1));
        let top_edp = zone_of(&zones, Direction::Top, "eDP-1");
        assert_eq!((top_edp.edge, top_edp.start, top_edp.len), (160, 3603, 1722));
        assert!(zone_contains(&top_edp, 4000, 160));
        assert!(!zone_contains(&top_edp, 4000, 161));
        // Bottoms: both outputs end at y = 1439, trimmed the same way.
        let bottom_hdmi = zone_of(&zones, Direction::Bottom, "HDMI-A-1");
        assert_eq!((bottom_hdmi.edge, bottom_hdmi.start, bottom_hdmi.len), (1439, 275, 2890));
        assert!(zone_contains(&bottom_hdmi, 500, 1439));
        assert!(!zone_contains(&bottom_hdmi, 500, 1438));
        let bottom_edp = zone_of(&zones, Direction::Bottom, "eDP-1");
        assert_eq!((bottom_edp.edge, bottom_edp.start, bottom_edp.len), (1439, 3603, 1722));
        assert!(zone_contains(&bottom_edp, 4000, 1439));
    }

    #[test]
    fn debounce_ignores_single_poll_jitter() {
        let mut debounce = EdgeDebounce::new();
        // Alternating outcomes never hold for two consecutive polls.
        for _ in 0..10 {
            assert_eq!(debounce.poll(true), None);
            assert_eq!(debounce.poll(false), None);
        }
    }

    #[test]
    fn debounce_transitions_after_two_stable_polls() {
        let mut debounce = EdgeDebounce::new();
        // Already off: offs are no-ops.
        assert_eq!(debounce.poll(false), None);
        // One on is only a candidate; the second consecutive one commits.
        assert_eq!(debounce.poll(true), None);
        assert_eq!(debounce.poll(true), Some(true));
        // Back off again, same pattern.
        assert_eq!(debounce.poll(false), None);
        assert_eq!(debounce.poll(false), Some(false));
        // A single stray poll between stable ones delays the transition
        // instead of firing early.
        assert_eq!(debounce.poll(true), None);
        assert_eq!(debounce.poll(false), None);
        assert_eq!(debounce.poll(true), None);
        assert_eq!(debounce.poll(true), Some(true));
    }

    #[test]
    fn cursorpos_parses_replies() {
        assert_eq!(parse_cursorpos("3440, 160").unwrap(), (3440, 160));
        assert_eq!(parse_cursorpos("0, 0").unwrap(), (0, 0));
        // Outputs left of/above the layout origin report negatives.
        assert_eq!(parse_cursorpos("-100, -200").unwrap(), (-100, -200));
        assert_eq!(parse_cursorpos("3440,160\n").unwrap(), (3440, 160));
    }

    #[test]
    fn cursorpos_rejects_garbage() {
        assert!(parse_cursorpos("").is_err());
        assert!(parse_cursorpos("3440").is_err());
        assert!(parse_cursorpos("a, b").is_err());
        assert!(parse_cursorpos("1, 2, 3").is_err());
    }
}
