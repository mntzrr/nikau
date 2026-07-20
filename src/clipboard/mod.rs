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
