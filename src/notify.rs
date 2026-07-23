//! Best-effort desktop notifications via notify-send (libnotify), shared by
//! the switch, update, connection-lifecycle, and link-quality call sites.
//!
//! Every notification KIND carries a distinct
//! `x-canonical-private-synchronous` id, so repeats of the same kind replace
//! the previous one instead of stacking, while different kinds never replace
//! each other. Current ids: `monux-switch`, `monux-update`, `monux-client`
//! (server-side roster changes), `monux-connection` (client-side
//! connect/lost), `monux-link` (degradation/recovery), `monux-indicator`
//! (tray indicator action feedback).

use std::process::{Command, Stdio};

/// notify-send urgency (-u).
#[derive(Clone, Copy)]
pub enum Urgency {
    Low,
    Normal,
}

impl Urgency {
    fn as_str(self) -> &'static str {
        match self {
            Urgency::Low => "low",
            Urgency::Normal => "normal",
        }
    }
}

/// Shows a desktop notification, fire-and-forget: spawning notify-send never
/// blocks the caller, and any failure (missing binary, no session bus, root
/// without -E) is silently ignored — notifications are strictly best-effort.
/// `id` is the x-canonical-private-synchronous hint (see module docs).
/// Safe to call from any thread: std::process needs no tokio runtime, unlike
/// tokio::process, whose spawn panics ("there is no reactor running") on
/// plain threads such as the tray indicator's menu-action callbacks.
pub fn notify(id: &str, urgency: Urgency, timeout_ms: u32, summary: &str, body: &str) {
    let timeout = timeout_ms.to_string();
    let hint = format!("string:x-canonical-private-synchronous:{}", id);
    let _ = Command::new("notify-send")
        .args([
            "-a",
            "monux",
            "-u",
            urgency.as_str(),
            "-t",
            &timeout,
            "-h",
            &hint,
            summary,
            body,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for the tray-indicator panic: notify() must not need a
    /// tokio runtime (the indicator calls it from ksni's plain service
    /// thread, where tokio::process spawning panicked with "there is no
    /// reactor running"). Also passes without notify-send installed — a
    /// failed spawn is best-effort and swallowed, not a panic.
    #[test]
    fn notify_without_a_tokio_runtime_does_not_panic() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        notify("monux-test", Urgency::Low, 1, "monux", "test notification");
    }
}
