use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;

pub mod client;
pub mod convert;
pub mod data;
pub mod serve;
pub mod server;
pub mod wayland;

mod limited;

pub const CLIPBOARD_TIMEOUT_SECS: u64 = 5;

/// Mime type prefixes that must never enter the sharing layer. These are
/// machine-internal, application-specific markers (e.g. Chromium's internal
/// drag/source token) that are meaningless on any other machine: advertising
/// them only invites cross-machine fetches that stall the serving side and
/// time out the requester. Extend as new offenders show up.
pub const UNSHAREABLE_MIME_PREFIXES: &[&str] = &["chromium/x-internal-"];

/// Whether a mime type may be shared with a peer (advertised or served).
pub fn is_shareable_mime_type(mime_type: &str) -> bool {
    !UNSHAREABLE_MIME_PREFIXES
        .iter()
        .any(|prefix| mime_type.starts_with(prefix))
}

/// Drops unshareable mime types from a types list entering the sharing layer
/// (see UNSHAREABLE_MIME_PREFIXES). Filtering changes nothing about the local
/// clipboard itself — only what gets announced to or advertised from a peer.
pub fn filter_shareable_mime_types(types: Vec<String>) -> Vec<String> {
    types
        .into_iter()
        .filter(|t| {
            let shareable = is_shareable_mime_type(t);
            if !shareable {
                debug!(
                    "Filtering machine-internal clipboard type {} out of sharing",
                    t
                );
            }
            shareable
        })
        .collect()
}

/// Overall timeout for serving one clipboard fetch (read + convert), applied
/// on both the client and the server serve paths. Deliberately below
/// CLIPBOARD_TIMEOUT_SECS so the requester always gets an answer — even an
/// empty one — before its own fetch timeout expires. Convert/zip of a large
/// copy can run arbitrarily long under the serve mutex, so the inner wayland
/// read timeout alone isn't enough.
pub const CLIPBOARD_SERVE_TIMEOUT_SECS: u64 = 4;

/// How long the writer dispatcher waits for a single store_types call before
/// giving up. store_types does wayland roundtrips that can hang on a wedged
/// compositor; without a bound, one hung advertisement blocks every
/// subsequent one forever (and grows the channel unboundedly while waiting).
const WRITER_DISPATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Clipboard writes (advertising types to the local environment) can block for
/// a long time: each call opens a fresh wayland connection, does roundtrips,
/// and spawns a serving thread. Running them on the rotation or client event
/// loop stalls input forwarding — fatal under clipboard-manager churn (e.g.
/// wl-clip-persist re-owning every clipboard, wl-paste --watch pollers), where
/// dozens of advertisements arrive in bursts. This dispatcher serializes them
/// on a dedicated thread instead.
///
/// Only the latest advertisement is served: while a store_types call is in
/// flight, newer advertisements drain and replace older queued ones (a stale
/// clipboard type list is pointless once a fresher one exists). A hung
/// store_types call (wedged compositor) is abandoned after WRITER_DISPATCH_TIMEOUT
/// so the dispatcher doesn't deadlock.
pub(crate) fn spawn_writer_dispatcher(
    writer: Box<dyn ClipboardWriter>,
) -> std::sync::mpsc::Sender<Vec<String>> {
    let writer = std::sync::Arc::<dyn ClipboardWriter>::from(writer);
    let (tx, rx) = std::sync::mpsc::channel::<Vec<String>>();
    std::thread::spawn(move || {
        while let Ok(mut types) = rx.recv() {
            // Drain stale advertisements: only the latest matters. This
            // bounds the queue under burst churn and skips pointless serves.
            while let Ok(newer) = rx.try_recv() {
                types = newer;
            }
            // Run store_types with a timeout so a wedged compositor can't
            // deadlock the dispatcher forever. store_types does blocking
            // wayland roundtrips; the timeout thread is abandoned (the
            // serving thread it spawned will exit on its own when the
            // connection drops).
            let writer = writer.clone();
            let (result_tx, result_rx) =
                std::sync::mpsc::channel::<anyhow::Result<()>>();
            let handle = std::thread::spawn(move || {
                let _ = result_tx.send(writer.store_types(types));
            });
            match result_rx.recv_timeout(WRITER_DISPATCH_TIMEOUT) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Failed to advertise clipboard types: {}", e);
                }
                Err(_) => {
                    tracing::warn!(
                        "Clipboard advertisement timed out after {:?} (wedged compositor?); abandoning",
                        WRITER_DISPATCH_TIMEOUT
                    );
                    // The store_types thread is leaked but will exit when its
                    // wayland connection drops; detach its handle.
                    let _ = handle;
                }
            }
        }
    });
    tx
}

/// Trait for watching the addition and removal of devices from the machine
#[async_trait]
pub trait ClipboardReader: Send {
    /// Reads the clipboard data for the specified type.
    /// The result may be converted/compressed to a different type for network transfer.
    async fn read(
        &mut self,
        requested_type: &str,
        max_size_bytes: u64,
        request_source: &str,
    ) -> Result<Vec<u8>>;
}

/// Trait for advertising clipboard data to the local environment
pub trait ClipboardWriter: Send + Sync {
    /// Advertises with the local environment that we have a new clipboard entry available
    fn store_types(&self, types: Vec<String>) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chromium_internal_token_is_not_shareable() {
        assert!(!is_shareable_mime_type(
            "chromium/x-internal-source-rfh-token"
        ));
    }

    #[test]
    fn regular_types_are_shareable() {
        assert!(is_shareable_mime_type("text/plain"));
        assert!(is_shareable_mime_type("text/html"));
        assert!(is_shareable_mime_type("image/png"));
    }

    #[test]
    fn matching_is_a_prefix_not_a_substring() {
        // The marker appearing later in the type is not a prefix match.
        assert!(is_shareable_mime_type(
            "application/x-chromium/x-internal-source-rfh-token"
        ));
        // A lookalike without the trailing dash doesn't match either.
        assert!(is_shareable_mime_type("chromium/x-internal"));
    }

    #[test]
    fn filter_drops_only_unshareable_types() {
        let filtered = filter_shareable_mime_types(vec![
            "chromium/x-internal-source-rfh-token".to_string(),
            "text/plain".to_string(),
            "text/html".to_string(),
        ]);
        assert_eq!(filtered, vec!["text/plain", "text/html"]);
    }

    #[test]
    fn filter_empty_list_stays_empty() {
        assert!(filter_shareable_mime_types(vec![]).is_empty());
    }
}
