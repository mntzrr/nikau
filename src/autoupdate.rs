//! Optional background auto-update (`--auto-update`): periodically checks the
//! update repo for a newer commit and, when one appears, rebuilds and installs
//! it at low CPU priority, then restarts the process to apply it. The restart
//! is the ordinary graceful shutdown (SIGTERM to ourselves) followed by main
//! re-exec'ing the new binary, so the session drops for a few seconds and then
//! heals itself: clients reconnect automatically and the server re-activates
//! whichever machine was active (session resumption in rotation.rs).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::update;

/// How often to check for updates.
const CHECK_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Delay before the first check after startup (let the session settle).
const INITIAL_DELAY: Duration = Duration::from_secs(60);
/// Grace period between a successful background update and the automatic
/// restart: lets the notification be seen and in-flight input settle.
const RESTART_DELAY: Duration = Duration::from_secs(20);

/// Set when an automatic restart is due after a background update; main
/// re-execs the new binary once the graceful shutdown completes.
static RESTART_AFTER_EXIT: AtomicBool = AtomicBool::new(false);

/// Test hook: override the startup delay (seconds).
fn initial_delay() -> Duration {
    std::env::var("MONUX_AUTO_UPDATE_INITIAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(INITIAL_DELAY)
}

/// Test hook: override the check interval (seconds).
fn check_interval() -> Duration {
    std::env::var("MONUX_AUTO_UPDATE_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(CHECK_INTERVAL)
}

/// Test hook: override the restart grace period (seconds).
fn restart_delay() -> Duration {
    std::env::var("MONUX_AUTO_UPDATE_RESTART_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(RESTART_DELAY)
}

fn short(sha: &str) -> &str {
    &sha[..12.min(sha.len())]
}

/// Whether a post-update restart was scheduled; checked by main after the
/// server/client loop has shut down gracefully.
pub fn restart_scheduled() -> bool {
    RESTART_AFTER_EXIT.load(Ordering::SeqCst)
}

/// Triggers the restart: flag it for main, then send ourselves SIGTERM so the
/// process shuts down via the exact same graceful path as a manual stop
/// (releasing grabs, held keys and clipboard state) before main re-execs.
fn schedule_restart() {
    RESTART_AFTER_EXIT.store(true, Ordering::SeqCst);
    unsafe {
        libc::kill(std::process::id() as i32, libc::SIGTERM);
    }
}

/// Runs the auto-update loop; spawn it on the tokio runtime.
pub async fn run() {
    tokio::time::sleep(initial_delay()).await;
    // Test hook: pretend an update was installed, exercising the automatic
    // restart without a rebuild. Fires once per boot lineage (the re-exec'd
    // image has MONUX_RESTARTED set) and skips the real update loop entirely.
    if std::env::var_os("MONUX_AUTO_UPDATE_FAKE").is_some() {
        if std::env::var_os("MONUX_RESTARTED").is_none() {
            info!("Pretending a background update succeeded (MONUX_AUTO_UPDATE_FAKE)");
            restart_after_grace("fake-update", false).await;
        }
        return;
    }
    // The sha of the last update attempt, so a persistent failure (or a
    // successful install whose restart is still pending) doesn't rebuild
    // every interval.
    let mut last_attempted: Option<String> = None;
    loop {
        let repo = update::repo_url();
        match update::latest_remote_sha(&repo) {
            Ok(remote_sha) => {
                let newer = update::is_newer_remote(&remote_sha, update::CURRENT_REVISION);
                let attempted = last_attempted.as_deref() == Some(remote_sha.as_str());
                if newer && !attempted {
                    info!(
                        "monux update available ({}), rebuilding in the background...",
                        short(&remote_sha)
                    );
                    last_attempted = Some(remote_sha.clone());
                    let result = tokio::task::spawn_blocking(|| update::run(false, true)).await;
                    match result {
                        Ok(Ok(())) => restart_after_grace(&remote_sha, true).await,
                        Ok(Err(e)) => warn!("Background monux update failed: {:?}", e),
                        Err(e) => error!("Background monux update task failed: {:?}", e),
                    }
                } else {
                    debug!("monux is up to date ({})", short(&remote_sha));
                }
            }
            Err(e) => {
                debug!("monux update check failed (offline?): {:?}", e);
            }
        }
        tokio::time::sleep(check_interval()).await;
    }
}

/// Announces the update, then gives the session a short grace period before
/// scheduling the automatic restart.
async fn restart_after_grace(remote_sha: &str, notify: bool) {
    let delay = restart_delay();
    info!(
        "monux was updated to {}; restarting in {}s to apply (the session resumes automatically)",
        short(remote_sha),
        delay.as_secs()
    );
    if notify {
        notify_update(remote_sha, delay);
    }
    tokio::time::sleep(delay).await;
    info!("Restarting to apply monux {}...", short(remote_sha));
    schedule_restart();
}

/// Shows a best-effort desktop notification that an update was installed and
/// the process is about to restart (same pattern as notify_switch in
/// rotation.rs). Any failure (missing binary, no session bus) is ignored.
fn notify_update(remote_sha: &str, delay: Duration) {
    let _ = std::process::Command::new("notify-send")
        .args([
            "-a",
            "monux",
            "-u",
            "normal",
            "-t",
            "10000",
            // Replace a previous update notification instead of stacking.
            "-h",
            "string:x-canonical-private-synchronous:monux-update",
            "monux update installed",
            &format!(
                "monux {} was installed in the background; restarting in {}s to apply it (your session will resume automatically)",
                short(remote_sha),
                delay.as_secs()
            ),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}
