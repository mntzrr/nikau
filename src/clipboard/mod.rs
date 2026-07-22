use anyhow::Result;
use async_trait::async_trait;

pub mod client;
pub mod convert;
pub mod data;
pub mod serve;
pub mod server;
pub mod wayland;
pub mod x11;

mod limited;

pub const CLIPBOARD_TIMEOUT_SECS: u64 = 5;

/// Overall timeout for serving one clipboard fetch (read + convert), applied
/// on both the client and the server serve paths. Deliberately below
/// CLIPBOARD_TIMEOUT_SECS so the requester always gets an answer — even an
/// empty one — before its own fetch timeout expires. Convert/zip of a large
/// copy can run arbitrarily long under the serve mutex, so the inner wayland
/// read timeout alone isn't enough.
pub const CLIPBOARD_SERVE_TIMEOUT_SECS: u64 = 4;

/// Clipboard writes (advertising types to the local environment) can block for
/// a long time: each call opens a fresh wayland connection, does roundtrips,
/// and spawns a serving thread. Running them on the rotation or client event
/// loop stalls input forwarding — fatal under clipboard-manager churn (e.g.
/// wl-clip-persist re-owning every clipboard, wl-paste --watch pollers), where
/// dozens of advertisements arrive in bursts. This dispatcher serializes them
/// on a dedicated thread instead.
pub(crate) fn spawn_writer_dispatcher(
    writer: Box<dyn ClipboardWriter>,
) -> std::sync::mpsc::Sender<Vec<String>> {
    let (tx, rx) = std::sync::mpsc::channel::<Vec<String>>();
    std::thread::spawn(move || {
        while let Ok(types) = rx.recv() {
            if let Err(e) = writer.store_types(types) {
                tracing::warn!("Failed to advertise clipboard types: {}", e);
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
pub trait ClipboardWriter: Send {
    /// Advertises with the local environment that we have a new clipboard entry available
    fn store_types(&self, types: Vec<String>) -> Result<()>;
}
