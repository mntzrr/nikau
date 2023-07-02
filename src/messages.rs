/// The protocol version sent from the client to the server.
/// If the message definitions change, then this must change.
pub const PROTOCOL_VERSION: &[u8] = b"v1";

/// Sent from the server to a client, equivalent to a uinput event.
/// Omits the timestamp because client should just handle events ASAP.
pub struct InputEvent {
    type_: u16,
    code: u16,
    value: i32,
}

impl std::fmt::Display for InputEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(format!("InputEvent(type={}, code={}, value={})", self.type_, self.code, self.value).as_str())
    }
}

impl InputEvent {
    pub fn from_evdev(e: evdev::InputEvent) -> InputEvent {
        InputEvent {
            type_: e.event_type().0,
            code: e.code(),
            value: e.value(),
        }
    }

    pub fn to_evdev(&self) -> evdev::InputEvent {
        evdev::InputEvent::new(
            evdev::EventType{0:self.type_},
            self.code,
            self.value,
        )
    }
}
