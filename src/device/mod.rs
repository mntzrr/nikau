pub mod input;
pub mod output;
pub mod shortcut;
pub mod util;
pub mod watch;

use crate::msgs::event;

#[derive(Clone, Debug)]
pub enum Event {
    /// A group of input events to send to the active client, if any
    Input(Vec<event::InputEvent>),
    /// Activate the next client (or the server) in the rotation
    SwitchNext,
    /// Activate the previous client (or the server) in the rotation
    SwitchPrev,
    /// Activate the client with matching cert fingerprint, or the server if the string is empty
    SwitchTo(String),
}
