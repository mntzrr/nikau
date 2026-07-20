pub mod uinput;

use crate::msgs::event;
use anyhow::Result;
use async_trait::async_trait;

/// Name prefix to use on nikau-created devices that should not be consumed by nikau
pub const VIRTUAL_DEVICE_NAME_PREFIX: &str = "nikau virtual";

/// Trait for watching the addition and removal of devices from the machine
#[async_trait]
pub trait OutputHandler {
    async fn write(&mut self, event: Vec<event::InputEvent>) -> Result<()>;

    /// Releases all keys/buttons currently held on the output devices.
    /// Used to avoid stuck keys when the input stream ends or moves to another machine.
    async fn release_all(&mut self) -> Result<()>;
}
