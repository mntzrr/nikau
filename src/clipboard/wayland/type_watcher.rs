use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tracing::{info, warn};
use wayland_client::globals::registry_queue_init;
use wayland_client::Connection;

use crate::clipboard::wayland::{common, state};

/// Maximum backoff between reconnect attempts.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(10);

/// How long start() waits for the watcher's first readiness signal. The
/// handshake must never deadlock a server start: a missed signal would
/// otherwise leave the process parked before the rotation loop exists, with
/// the keyboards already grabbed.
const READY_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Task that listens for updates to the clipboard types (local cut or copy).
/// Sends out an event when an update occurs, indicating a new clipboard is available.
/// If wayland is unavailable, this returns Ok(None).
pub fn start(
    regular_types_tx: Option<watch::Sender<Vec<String>>>,
) -> Result<Option<()>> {
    // Initial availability probe: if wayland isn't reachable at all, there's
    // nothing to reconnect to. A compositor crash later is handled by the
    // reconnect loop in the spawned thread.
    if Connection::connect_to_env().is_err() {
        warn!("Disabling wayland clipboard support: Failed to connect to wayland");
        return Ok(None);
    }

    let (thread_ready_tx, thread_ready_rx) = std::sync::mpsc::sync_channel(1);
    let _ = std::thread::spawn(move || {
        let mut backoff = Duration::from_secs(1);
        let mut signalled_ready = false;
        loop {
            match connect_and_watch(&regular_types_tx, &thread_ready_tx, &mut signalled_ready) {
                WatchOutcome::Unavailable => {
                    if !signalled_ready {
                        let _ = thread_ready_tx.send(());
                    }
                    return;
                }
                WatchOutcome::Error(e) => {
                    warn!(
                        "Wayland clipboard type watcher error, reconnecting in {:?}: {}",
                        backoff, e
                    );
                    if !signalled_ready {
                        signalled_ready = true;
                        let _ = thread_ready_tx.send(());
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                }
            }
        }
    });
    // Bounded handshake: the watcher signals on its first outcome — connected,
    // unavailable, or error — but a startup must never hinge on that signal
    // arriving. On timeout, start without clipboard sharing rather than
    // deadlocking the server with the keyboards already grabbed.
    match thread_ready_rx.recv_timeout(READY_HANDSHAKE_TIMEOUT) {
        Ok(()) => Ok(Some(())),
        Err(e) => {
            warn!(
                "Wayland clipboard type watcher did not report readiness ({}); clipboard sharing disabled",
                e
            );
            Ok(None)
        }
    }
}

/// Result of one connect + watch cycle.
enum WatchOutcome {
    /// Wayland or its clipboard protocols aren't available — don't reconnect.
    Unavailable,
    /// A dispatch error occurred (e.g. compositor crash) — reconnect.
    Error(anyhow::Error),
}

/// Connects to wayland, sets up the clipboard registry, and dispatches events
/// until the connection is lost. Returns Unavailable if wayland or the
/// clipboard protocols aren't present, Error on a dispatch failure. Signals
/// readiness on the FIRST successful connect too: previously only the
/// Unavailable/Error arms signalled, so a first-try success (a healthy,
/// fast compositor — e.g. right after a reboot) left start()'s readiness
/// recv blocked forever, deadlocking the server with the keyboards grabbed.
fn connect_and_watch(
    regular_types_tx: &Option<watch::Sender<Vec<String>>>,
    thread_ready_tx: &std::sync::mpsc::SyncSender<()>,
    signalled_ready: &mut bool,
) -> WatchOutcome {
    let conn = match Connection::connect_to_env() {
        Ok(conn) => conn,
        Err(e) => {
            return WatchOutcome::Error(
                anyhow::anyhow!("Failed to connect to wayland: {}", e),
            );
        }
    };
    let (globals, mut queue) = match registry_queue_init::<state::State>(&conn) {
        Ok(vals) => vals,
        Err(e) => {
            return WatchOutcome::Error(
                anyhow::anyhow!("Failed to init Wayland registry queue: {}", e),
            );
        }
    };
    let qh = queue.handle();

    let clipboard_manager = if let Some(clipboard_manager) = common::clipboard_manager(&globals, &qh) {
        clipboard_manager
    } else {
        return WatchOutcome::Unavailable;
    };

    let mut seats = HashMap::new();
    for seat in common::seats(&globals, &qh) {
        let data = state::SeatData::new(clipboard_manager.get_data_device(&seat, &qh, seat.clone()));
        seats.insert(seat, data);
    }
    if seats.is_empty() {
        return WatchOutcome::Unavailable;
    }
    // State handles advertising the regular clipboard types to upstream listeners
    let mut state = state::State::new(seats, regular_types_tx.clone());

    if let Err(e) = queue.roundtrip(&mut state).context("Failed to initialize Wayland state") {
        return WatchOutcome::Error(e);
    }
    info!("Wayland clipboard type watcher connected");
    // Report readiness on the first successful connect as well (see the fn
    // docs): start()'s recv unblocks here, on Unavailable, or on first Error.
    if !*signalled_ready {
        *signalled_ready = true;
        let _ = thread_ready_tx.send(());
    }
    loop {
        if let Err(e) = queue.blocking_dispatch(&mut state) {
            return WatchOutcome::Error(anyhow::anyhow!(
                "Wayland clipboard type watcher queue dispatch error: {}",
                e
            ));
        }
    }
}
