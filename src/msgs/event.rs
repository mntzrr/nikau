use serde::{Deserialize, Serialize};

/// A serialized event message sent from the server to a client.
/// Changes to this signature likely require changing PROTOCOL_VERSION.
#[derive(Debug, Deserialize, Serialize)]
pub enum ServerEvent<'a> {
    /// Notification to client that the input stream has started or ended.
    /// This allows the client to init or clear any local state, or to indicate being selected to the user.
    Switch(SwitchEvent),

    /// One or more input events to be written as a group to virtual devices on the client.
    Input(Vec<InputEvent>),

    /// Broadcasts the types of a clipboard that can be retrieved from the server.
    #[serde(borrow)]
    ClipboardTypes(ClipboardTypes<'a>),
}

impl<'a> std::fmt::Display for ServerEvent<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ServerEvent::Switch(e) => e.fmt(f),
            ServerEvent::Input(e) => f.write_str(format!("{:?}", e).as_str()),
            ServerEvent::ClipboardTypes(e) => e.fmt(f),
        }
    }
}

/// A serialized event message sent from a client to the server.
#[derive(Debug, Deserialize, Serialize)]
pub enum ClientEvent<'a> {
    /// Broadcasts the types of a clipboard that can be retrieved from the client.
    #[serde(borrow)]
    ClipboardTypes(ClipboardTypes<'a>),
}

impl<'a> std::fmt::Display for ClientEvent<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClientEvent::ClipboardTypes(e) => e.fmt(f),
        }
    }
}

// SwitchEvent

/// Notifies the client that it should enable or disable its virtual devices for input.
#[derive(Debug, Deserialize, Serialize)]
pub struct SwitchEvent {
    pub enabled: bool,
}

impl std::fmt::Display for SwitchEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(format!("SwitchEvent(enabled={})", self.enabled).as_str())
    }
}

// InputEvent

/// A pointer-motion batch sent from server to client as a QUIC datagram
/// (unreliable, unordered). Motion updates are stale the moment a newer one
/// exists, so skipping a lost datagram beats stalling all later input behind
/// a stream retransmission. Only pure REL_X/REL_Y batches use this path;
/// everything else (keys, buttons, wheel, absolute axes) stays on the ordered
/// events stream.
#[derive(Debug, Deserialize, Serialize)]
pub struct MotionDatagram {
    /// Per-connection sequence number; the client drops datagrams that arrive
    /// older than the newest one it has already applied.
    pub seq: u64,
    pub events: Vec<InputEvent>,
}

/// An input event to be written to a virtual device indicated by the target.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InputEvent {
    /// For discrete unscaled values
    pub inputi32: Option<InputI32>,
    /// For continuous values, this is scaled from 0.0 to 1.0
    pub inputf64: Option<InputF64>,
}

impl std::fmt::Display for InputEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(evt) = &self.inputi32 {
            f.write_str(format!("InputEvent(inputi32={})", evt).as_str())
        } else if let Some(evt) = &self.inputf64 {
            f.write_str(format!("InputEvent(inputf64={})", evt).as_str())
        } else {
            f.write_str("InputEvent(?)")
        }
    }
}

// InputI32

/// Equivalent to a uinput event for the client to emit locally.
/// Omits the timestamp since it isn't required.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InputI32 {
    pub type_: u16,
    pub code: u16,
    pub value: i32,
}

impl std::fmt::Display for InputI32 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "InputI32(type={}, code={}, value={})",
                self.type_, self.code, self.value
            )
            .as_str(),
        )
    }
}

impl InputI32 {
    pub fn from_evdev(e: evdev::InputEvent) -> InputI32 {
        InputI32 {
            type_: e.event_type().0,
            code: e.code(),
            value: e.value(),
        }
    }

    pub fn to_evdev(&self) -> evdev::InputEvent {
        evdev::InputEvent::new(self.type_, self.code, self.value)
    }
}

// InputF64

/// Equivalent to a uinput event for the client to emit locally.
/// Omits the timestamp since it isn't required.
/// Used for absolute coordinates, with a scale of [0.0, 1.0] to be resized by the client.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InputF64 {
    pub type_: u16,
    pub code: u16,
    pub value: f64,
}

impl std::fmt::Display for InputF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "InputF64(type={}, code={}, value={})",
                self.type_, self.code, self.value
            )
            .as_str(),
        )
    }
}

impl InputF64 {
    pub fn from_evdev(e: evdev::InputEvent, min: i32, max: i32) -> InputF64 {
        InputF64 {
            type_: e.event_type().0,
            code: e.code(),
            // For example: min=-10, max=10, vali=5 -> valf=0.75
            value: ((e.value() - min) as f64) / ((max - min) as f64),
        }
    }

    pub fn to_evdev(&self, min: i32, max: i32) -> evdev::InputEvent {
        evdev::InputEvent::new(
            self.type_,
            self.code,
            // Inverse of from_evdev math:
            (self.value * ((max - min) as f64)) as i32 + min,
        )
    }
}

// ClipboardTypes

#[derive(Debug, Deserialize, Serialize)]
pub struct ClipboardTypes<'a> {
    /// Space-separated list of types that are supported by the current clipboard owner
    /// (Couldn't figure out how to have a vec or slice)
    pub types: &'a str,

    /// Maximum size supported by the sender.
    pub max_size_bytes: u64,
}

impl<'a> std::fmt::Display for ClipboardTypes<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(
            format!(
                "ClipboardTypes(types=[{}], max_size_bytes={})",
                self.types, self.max_size_bytes
            )
            .as_str(),
        )
    }
}
