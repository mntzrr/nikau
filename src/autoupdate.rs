//! Optional background auto-update (`--auto-update`): periodically checks the
//! update repo for a newer commit and, when one appears, rebuilds and installs
//! it at low CPU priority. The running session is NEVER restarted
//! automatically: a desktop notification reports that an update is ready, and
//! restarting is seamless (the active session resumes on reconnect).

use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::update;

/// How often to check for updates.
const CHECK_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Delay before the first check after startup (let the session settle).
const INITIAL_DELAY: Duration = Duration::from_secs(60);

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

fn short(sha: &str) -> &str {
    &sha[..12.min(sha.len())]
}

/// Runs the auto-update loop; spawn it on the tokio runtime.
pub async fn run() {
    tokio::time::sleep(initial_delay()).await;
    // The sha of the last update attempt, so a persistent failure (or a
    // successful install awaiting restart) doesn't rebuild every interval.
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
                        Ok(Ok(())) => {
                            info!(
                                "monux was updated to {}; restart to apply (the session will resume automatically)",
                                short(&remote_sha)
                            );
                            notify_update(&remote_sha);
                        }
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

/// Shows a best-effort desktop notification that an update was installed and
/// is waiting for a restart (same pattern as notify_switch in rotation.rs).
/// Any failure (missing binary, no session bus, root without -E) is ignored.
fn notify_update(remote_sha: &str) {
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
                "monux {} was installed in the background; restart to apply (your session will resume automatically)",
                short(remote_sha)
            ),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}
