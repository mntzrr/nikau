pub mod handles;
pub mod input;
pub mod output;
pub mod shortcut;
pub mod util;
pub mod watch;

use crate::msgs::event;

use std::sync::OnceLock;

#[derive(Clone, Copy, Debug)]
pub enum GrabEvent {
    Grab,
    Ungrab,
}

/// Key codes to trace through the input pipeline, parsed once from
/// MONUX_TRACE_KEYS (e.g. "28,42,56"). For catching where a specific key dies
/// in the wild: every stage logs traced keys at INFO with a KEYTRACE prefix.
static TRACE_KEY_CODES: OnceLock<Vec<u16>> = OnceLock::new();

fn trace_key_codes() -> &'static [u16] {
    TRACE_KEY_CODES.get_or_init(|| {
        std::env::var("MONUX_TRACE_KEYS")
            .unwrap_or_default()
            .split([',', ' '])
            .filter_map(|s| s.trim().parse::<u16>().ok())
            .collect()
    })
}

/// Whether events for this evdev code should be KEYTRACE-logged (see
/// TRACE_KEY_CODES). Cheap: the list is empty unless MONUX_TRACE_KEYS is set.
pub fn key_traced(code: u16) -> bool {
    let codes = trace_key_codes();
    !codes.is_empty() && codes.contains(&code)
}

#[derive(Clone, Debug)]
pub struct InputBatch {
    pub events: Vec<event::InputEvent>,
    pub is_grabbed: bool,
}

#[derive(Clone, Debug)]
pub enum Event {
    /// A group of input events to send to the active client, if any
    Input(InputBatch),
    /// Activate the next client (or the server) in the rotation
    SwitchNext,
    /// Activate the previous client (or the server) in the rotation
    SwitchPrev,
    /// Activate the client with matching cert fingerprint, or the server if the string is empty
    SwitchTo(String),
}
