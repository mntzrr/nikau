//! Best-effort desktop notifications via notify-send (libnotify), shared by
//! the switch, update, connection-lifecycle, and link-quality call sites.
//!
//! Every notification KIND carries a distinct
//! `x-canonical-private-synchronous` id, so repeats of the same kind replace
//! the previous one instead of stacking, while different kinds never replace
//! each other. Current ids: `monux-switch`, `monux-update`, `monux-client`
//! (server-side roster changes), `monux-connection` (client-side
//! connect/lost), `monux-link` (degradation/recovery).

use std::process::Stdio;

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
/// Must be called from within the tokio runtime; the spawned child is reaped
/// by the runtime.
pub fn notify(id: &str, urgency: Urgency, timeout_ms: u32, summary: &str, body: &str) {
    let timeout = timeout_ms.to_string();
    let hint = format!("string:x-canonical-private-synchronous:{}", id);
    let _ = tokio::process::Command::new("notify-send")
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
