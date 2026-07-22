use serde::{Deserialize, Serialize};

/// A serialized event message sent from the server to a client.
/// Changes to this signature likely require changing PROTOCOL_VERSION.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum ServerEvent<'a> {
    /// Notification to client that the input stream has started or ended.
    /// This allows the client to init or clear any local state, or to indicate being selected to the user.
    Switch(SwitchEvent),

    /// One or more input events to be written as a group to virtual devices on the client.
    Input(Vec<InputEvent>),

    /// Broadcasts the types of a clipboard that can be retrieved from the server.
    #[serde(borrow)]
    ClipboardTypes(ClipboardTypes<'a>),

    /// App-level liveness check, sent by the server to the current (and any
    /// silenced) client every PING_INTERVAL. The client must answer with a
    /// Pong immediately; a black-holed link (WiFi) otherwise keeps devices
    /// grabbed and keystrokes buffer into the void until the QUIC idle
    /// timeout fires. New variants must be appended (wire format).
    Ping,
}

impl<'a> std::fmt::Display for ServerEvent<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ServerEvent::Switch(e) => e.fmt(f),
            ServerEvent::Input(e) => f.write_str(format!("{:?}", e).as_str()),
            ServerEvent::ClipboardTypes(e) => e.fmt(f),
            ServerEvent::Ping => f.write_str("Ping"),
        }
    }
}

/// A serialized event message sent from a client to the server.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum ClientEvent<'a> {
    /// Broadcasts the types of a clipboard that can be retrieved from the client.
    #[serde(borrow)]
    ClipboardTypes(ClipboardTypes<'a>),

    /// Answer to the server's Ping liveness check (see ServerEvent::Ping),
    /// sent immediately on the same ordered events stream. Any received
    /// message counts as liveness on the server; Pong exists so that an
    /// otherwise idle client has something to say. Appended variant.
    Pong,
}

impl<'a> std::fmt::Display for ClientEvent<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClientEvent::ClipboardTypes(e) => e.fmt(f),
            ClientEvent::Pong => f.write_str("Pong"),
        }
    }
}

// SwitchEvent

/// Notifies the client that it should enable or disable its virtual devices for input.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct SwitchEvent {
    pub enabled: bool,
}

impl std::fmt::Display for SwitchEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(format!("SwitchEvent(enabled={})", self.enabled).as_str())
    }
}

// InputEvent

/// Pointer motion sent from server to client as a QUIC datagram (unreliable,
/// unordered). Motion is stale the moment a newer update exists, so dropping a
/// lost datagram beats stalling later input behind a stream retransmission;
/// and since deltas sum commutatively, carrying recent deltas redundantly lets
/// the receiver heal losses without any retransmission backlog. Only pure
/// REL_X/REL_Y deltas use this path; everything else (keys, buttons, wheel,
/// absolute axes) stays on the ordered events stream.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct MotionDatagram {
    /// Per-connection sequence number of the newest frame in `history`.
    pub seq: u64,
    /// Newest-first per-frame motion deltas: `history[0]` is frame `seq`,
    /// `history[1]` is frame `seq - 1`, and so on. Coalesced mode
    /// (--motion-hz) repeats up to a few recent frames so the client can heal
    /// lost ones; full-rate mode sends a single entry (lost = skipped).
    pub history: Vec<(i32, i32)>,
}

/// How far back (in frames) the receiver tracks application: frames older
/// than `last_seq - MOTION_APPLY_WINDOW` can no longer be healed.
pub const MOTION_APPLY_WINDOW: u64 = 64;

/// Outcome of merging a MotionDatagram into the receiver's applied state.
pub struct MotionApply {
    /// Sequence of the newest frame known so far.
    pub last_seq: u64,
    /// Bit i = frame `last_seq - i` has been applied (bit 0 = newest).
    pub applied_mask: u64,
    /// Sum of deltas this datagram newly contributed (already-applied frames
    /// are skipped, missing ones in `history` are healed).
    pub delta: (i32, i32),
}

impl MotionDatagram {
    /// Merges this datagram into the receiver state (`last_seq`, `applied_mask`),
    /// returning the updated state plus the deltas to emit. Deltas are
    /// commutative, so frames may be applied in any order; applying each frame
    /// exactly once always yields the correct cursor position.
    pub fn apply(&self, last_seq: u64, applied_mask: u64) -> MotionApply {
        let mut last = last_seq;
        let mut mask = applied_mask;
        if self.seq > last {
            let shift = self.seq - last;
            mask = if shift >= MOTION_APPLY_WINDOW {
                0
            } else {
                mask << shift
            };
            last = self.seq;
        }
        let mut dx = 0i32;
        let mut dy = 0i32;
        for (i, &(fx, fy)) in self.history.iter().enumerate() {
            // history[i] carries frame seq - i. Frame numbers start at 1.
            let Some(frame_seq) = self.seq.checked_sub(i as u64) else {
                break;
            };
            if frame_seq == 0 {
                break;
            }
            // last >= self.seq >= frame_seq, so the age can't underflow.
            let age = last - frame_seq;
            if age >= MOTION_APPLY_WINDOW {
                continue;
            }
            let bit = 1u64 << age;
            if mask & bit == 0 {
                mask |= bit;
                dx = dx.saturating_add(fx);
                dy = dy.saturating_add(fy);
            }
        }
        MotionApply {
            last_seq: last,
            applied_mask: mask,
            delta: (dx, dy),
        }
    }
}

/// Builds a single relative-axis input event (for coalesced motion flushes).
pub fn motion_event(code: u16, value: i32) -> InputEvent {
    InputEvent {
        inputi32: Some(InputI32 {
            type_: evdev::EventType::RELATIVE.0,
            code,
            value,
        }),
        inputf64: None,
    }
}

/// An input event to be written to a virtual device indicated by the target.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InputEvent {
    /// For discrete unscaled values: keys, relative axes, and discrete
    /// absolute axes (ABS_MT_SLOT, ABS_MT_TRACKING_ID, ... — the axes
    /// device::util::axis_scale_type classifies as Discrete), which travel
    /// raw so slot indexes and the -1 liftoff marker survive the round trip.
    pub inputi32: Option<InputI32>,
    /// For continuous values (e.g. touchpad positions), this is scaled from
    /// 0.0 to 1.0
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
/// Used for continuous absolute coordinates, with a scale of [0.0, 1.0] to
/// be resized by the client. Discrete absolute axes travel raw via InputI32
/// instead.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
            // A broken device advertising min==max would divide by zero;
            // clamp to 0.0 instead of emitting NaN/inf onto the wire.
            value: if max > min {
                ((e.value() - min) as f64) / ((max - min) as f64)
            } else {
                0.0
            },
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

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct ClipboardTypes<'a> {
    /// Space-separated list of types that are supported by the current clipboard owner
    /// (Couldn't figure out how to have a vec or slice)
    pub types: &'a str,

    /// Maximum size supported by the sender.
    pub max_size_bytes: u64,
}

impl<'a> ClipboardTypes<'a> {
    /// Splits the space-separated types list into individual mime types.
    /// Empty entries are dropped: an empty types string (a clipboard clear)
    /// must yield NO types, but `"".split(' ')` yields `[""]` — advertising
    /// that would offer a phantom "" mime type (plus the ignore marker)
    /// instead of taking the writer's clear branch.
    pub fn types_vec(&self) -> Vec<String> {
        self.types
            .split(' ')
            .filter(|t| !t.is_empty())
            .map(|t| t.to_string())
            .collect()
    }
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


#[cfg(test)]
mod tests {
    use super::*;

    fn datagram(seq: u64, history: &[(i32, i32)]) -> MotionDatagram {
        MotionDatagram {
            seq,
            history: history.to_vec(),
        }
    }

    /// Serializes then deserializes a stream message exactly as the transport
    /// does (postcard + COBS framing) and asserts it survives intact.
    macro_rules! assert_cobs_roundtrip {
        ($ty:ty, $msg:expr) => {{
            let msg = $msg;
            let mut bytes = postcard::to_stdvec_cobs(&msg).unwrap();
            let (decoded, _) = postcard::take_from_bytes_cobs::<$ty>(&mut bytes).unwrap();
            assert_eq!(decoded, msg);
        }};
    }

    #[test]
    fn switch_event_roundtrip() {
        assert_cobs_roundtrip!(ServerEvent, ServerEvent::Switch(SwitchEvent { enabled: true }));
        assert_cobs_roundtrip!(ServerEvent, ServerEvent::Switch(SwitchEvent { enabled: false }));
    }

    #[test]
    fn ping_pong_roundtrip() {
        // The liveness pair (see rotation.rs Ping/Pong): unit variants must
        // survive the postcard + COBS round trip like any payload message.
        assert_cobs_roundtrip!(ServerEvent, ServerEvent::Ping);
        assert_cobs_roundtrip!(ClientEvent, ClientEvent::Pong);
    }

    #[test]
    fn server_event_input_roundtrip() {
        // An empty batch is degenerate but must still round-trip.
        assert_cobs_roundtrip!(ServerEvent, ServerEvent::Input(vec![]));
        // Every InputEvent shape: discrete i32 (incl. negative), scaled f64 at
        // both range extremes, and a both-fields-None event.
        let events = vec![
            InputEvent {
                inputi32: Some(InputI32 { type_: 1, code: 30, value: 1 }),
                inputf64: None,
            },
            InputEvent {
                inputi32: Some(InputI32 { type_: 2, code: 0, value: -127 }),
                inputf64: None,
            },
            InputEvent {
                inputi32: None,
                inputf64: Some(InputF64 { type_: 3, code: 53, value: 0.0 }),
            },
            InputEvent {
                inputi32: None,
                inputf64: Some(InputF64 { type_: 3, code: 53, value: 1.0 }),
            },
            InputEvent {
                inputi32: None,
                inputf64: None,
            },
        ];
        assert_cobs_roundtrip!(ServerEvent, ServerEvent::Input(events));
    }

    #[test]
    fn clipboard_types_roundtrip() {
        // Empty types: a clipboard clear.
        assert_cobs_roundtrip!(
            ServerEvent,
            ServerEvent::ClipboardTypes(ClipboardTypes { types: "", max_size_bytes: 0 })
        );
        // A max-size advertisement.
        assert_cobs_roundtrip!(
            ServerEvent,
            ServerEvent::ClipboardTypes(ClipboardTypes {
                types: "text/plain image/png application/zstd",
                max_size_bytes: u64::MAX,
            })
        );
        // Unicode in the types string, on the client-to-server direction.
        assert_cobs_roundtrip!(
            ClientEvent,
            ClientEvent::ClipboardTypes(ClipboardTypes {
                types: "text/plain;charset=utf-8 ✓ ünïcödé",
                max_size_bytes: 1024,
            })
        );
    }

    #[test]
    fn bare_input_structs_roundtrip() {
        assert_cobs_roundtrip!(
            InputI32,
            InputI32 { type_: u16::MAX, code: u16::MAX, value: i32::MIN }
        );
        assert_cobs_roundtrip!(InputI32, InputI32 { type_: 0, code: 0, value: 0 });
        assert_cobs_roundtrip!(InputF64, InputF64 { type_: 0, code: 0, value: 0.5 });
    }

    #[test]
    fn motion_datagram_roundtrip() {
        // Motion datagrams use plain postcard without COBS framing: QUIC
        // datagrams are already message-framed (see rotation.rs/client.rs).
        for msg in [
            MotionDatagram { seq: 0, history: vec![] },
            MotionDatagram { seq: 1, history: vec![(0, 0)] },
            MotionDatagram { seq: 42, history: vec![(-5, 7), (0, -1), (300, -400)] },
            MotionDatagram { seq: u64::MAX, history: vec![(i32::MIN, i32::MAX); 32] },
        ] {
            let bytes = postcard::to_stdvec(&msg).unwrap();
            let decoded: MotionDatagram = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(decoded, msg);
        }
    }

    #[test]
    fn apply_sequential() {
        let d = datagram(1, &[(1, 2)]);
        let r = d.apply(0, 0);
        assert_eq!(r.last_seq, 1);
        assert_eq!(r.applied_mask, 0b1);
        assert_eq!(r.delta, (1, 2));
        // Next frame in order.
        let d = datagram(2, &[(3, 4)]);
        let r = d.apply(r.last_seq, r.applied_mask);
        assert_eq!(r.last_seq, 2);
        assert_eq!(r.applied_mask, 0b11);
        assert_eq!(r.delta, (3, 4));
    }

    #[test]
    fn apply_heals_gap_from_history() {
        // Applied frame 1, then a datagram for frame 4 arrives carrying 4,3,2.
        let first = datagram(1, &[(1, 0)]).apply(0, 0);
        let d = datagram(4, &[(4, 0), (3, 0), (2, 0)]);
        let r = d.apply(first.last_seq, first.applied_mask);
        assert_eq!(r.last_seq, 4);
        assert_eq!(r.applied_mask, 0b1111);
        // Frames 2, 3 and 4 are newly applied; frame 1 is not repeated.
        assert_eq!(r.delta, (4 + 3 + 2, 0));
    }

    #[test]
    fn apply_skips_already_applied() {
        // Duplicate delivery of the same datagram applies nothing twice.
        let d = datagram(2, &[(3, 4), (1, 2)]);
        let r1 = d.apply(0, 0);
        assert_eq!(r1.delta, (4, 6));
        let r2 = d.apply(r1.last_seq, r1.applied_mask);
        assert_eq!(r2.delta, (0, 0));
        assert_eq!(r2.applied_mask, r1.applied_mask);
    }

    #[test]
    fn apply_out_of_order_datagram_heals_older_frame() {
        // Frames 1 and 3 applied (2 was lost); a late datagram for frame 2
        // still heals it, because deltas commute.
        let d1 = datagram(1, &[(1, 0)]);
        let r1 = d1.apply(0, 0);
        let d3 = datagram(3, &[(3, 0)]);
        let r3 = d3.apply(r1.last_seq, r1.applied_mask);
        assert_eq!(r3.applied_mask, 0b101);
        let d2 = datagram(2, &[(2, 5)]);
        let r2 = d2.apply(r3.last_seq, r3.applied_mask);
        assert_eq!(r2.last_seq, 3);
        assert_eq!(r2.applied_mask, 0b111);
        assert_eq!(r2.delta, (2, 5));
    }

    #[test]
    fn apply_forgets_far_history_on_large_jump() {
        // A jump beyond the tracking window forgets the old state; only the
        // carried history is applied (older losses are unhealable).
        let r = datagram(1, &[(1, 0)]).apply(0, 0);
        let d = datagram(100, &[(100, 0), (99, 0)]);
        let r = d.apply(r.last_seq, r.applied_mask);
        assert_eq!(r.last_seq, 100);
        assert_eq!(r.applied_mask, 0b11);
        assert_eq!(r.delta, (199, 0));
    }

    #[test]
    fn apply_ignores_frames_beyond_the_window() {
        // A very old carrier whose frames all fall outside the window heals
        // nothing.
        let r = datagram(100, &[(100, 0)]).apply(0, 0);
        let d = datagram(36, &[(36, 1)]);
        let r = d.apply(r.last_seq, r.applied_mask);
        assert_eq!(r.last_seq, 100);
        assert_eq!(r.delta, (0, 0));
    }

    #[test]
    fn apply_history_never_underflows_frame_numbers() {
        // More history entries than frames exist: stop at frame 1.
        let d = datagram(2, &[(2, 0), (1, 0), (0, 0)]);
        let r = d.apply(0, 0);
        assert_eq!(r.last_seq, 2);
        assert_eq!(r.applied_mask, 0b11);
        assert_eq!(r.delta, (3, 0));
    }

    /// A clipboard clear arrives as an empty types string. It must split into
    /// ZERO mime types — `"".split(' ')` otherwise yields `[""]`, a phantom
    /// empty type that the wayland writer would advertise (plus the ignore
    /// marker) instead of taking its `mime_types.is_empty()` clear branch.
    #[test]
    fn empty_types_string_splits_to_no_types() {
        let clear = ClipboardTypes {
            types: "",
            max_size_bytes: 0,
        };
        assert!(clear.types_vec().is_empty());
    }

    #[test]
    fn types_vec_drops_empty_entries() {
        let single = ClipboardTypes {
            types: "text/plain",
            max_size_bytes: 0,
        };
        assert_eq!(single.types_vec(), vec!["text/plain".to_string()]);
        // Repeated separators (not produced by join, but tolerated) must not
        // leak empty entries into the advertised types either.
        let padded = ClipboardTypes {
            types: "text/plain  image/png",
            max_size_bytes: 0,
        };
        assert_eq!(
            padded.types_vec(),
            vec!["text/plain".to_string(), "image/png".to_string()]
        );
    }

    #[test]
    fn inputf64_degenerate_range_does_not_divide_by_zero() {
        // A broken device advertising min==max must not emit NaN/inf.
        let e = evdev::InputEvent::new(evdev::EventType::ABSOLUTE.0, 0, 5);
        assert_eq!(InputF64::from_evdev(e, 3, 3).value, 0.0);
        // And the normal case keeps its math.
        assert_eq!(InputF64::from_evdev(e, -10, 10).value, 0.75);
    }
}
