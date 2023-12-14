pub mod gadget;
pub mod uinput;

use crate::msgs::event;
use anyhow::Result;
use async_trait::async_trait;

/// Name prefix to use on nikau-created devices that should not be consumed by nikau
pub const VIRTUAL_DEVICE_NAME_PREFIX: &str = "nikau virtual";

/// Trait for watching the addition and removal of devices from the machine
#[async_trait]
pub trait OutputHandler {
    async fn add_event(&mut self, event: event::InputEvent) -> Result<()>;
    async fn flush_events(&mut self) -> Result<()>;
}
