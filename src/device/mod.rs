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

/// Broadcast grab state for ALL input devices, single-sourced by the rotation
/// loop. Every device task subscribes and derives its own grab target from its
/// DeviceClass, so a state that must reach keyboards and mice alike (pause)
/// can't leave one class behind.
#[derive(Clone, Copy, Debug)]
pub struct GrabState {
    /// A remote client currently owns the input. Toggled devices (mice) stay
    /// grabbed exactly while this is true.
    pub client_active: bool,
    /// Input is paused (see --pause-shortcut): EVERYTHING is ungrabbed,
    /// keyboards included, so the local machine gets raw evdev input with
    /// monux's re-emit fully out of the way (games, raw-input apps). monux
    /// keeps listening ungrabbed, so the pause chord itself still works.
    pub paused: bool,
}

/// How a device's grab is managed (see DeviceHandles).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DeviceClass {
    /// Keyboard-class: the device supports one or more switch/pause combo keys,
    /// so it stays grabbed whenever input isn't paused — combos must be
    /// swallowed consistently when the local machine is the active target.
    Keyboard,
    /// Toggled (e.g. mice): grabbed only while a client is active (and input
    /// isn't paused); otherwise its input passes through to the local system.
    Toggled,
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
    /// Toggle pause mode: ungrab ALL input devices (keyboards included) so the
    /// local machine gets raw input, or re-grab per the rotation state.
    PauseToggle,
}
