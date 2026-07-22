use anyhow::{Context, Result};
use async_trait::async_trait;
use evdev::{
    uinput, AbsInfo, AbsoluteAxisCode, AttributeSet, EvdevEnum, EventSummary, KeyCode, MiscCode,
    RelativeAxisCode,
};
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

use crate::device::output::{OutputHandler, VIRTUAL_DEVICE_NAME_PREFIX};
use crate::device::util;
use crate::msgs::event;

pub const SCALED_DIM_MIN: i32 = 0;
pub const SCALED_DIM_MAX: i32 = 65535;
pub const SCALED_DIM_RES_X: i32 = 640; // 65536 / 640 = 102.4mm
pub const SCALED_DIM_RES_Y: i32 = 960; // for a 3/2 ratio vs X: 65536 / 960 = 68.3mm

/// Creates virtual uinput devices on the client machine and emits input events locally.
pub struct VirtualUInputDevices {
    keyboard_keys: AttributeSet<KeyCode>,
    mouse_keys: AttributeSet<KeyCode>,
    touchpad_keys: AttributeSet<KeyCode>,

    mouse_axes: AttributeSet<RelativeAxisCode>,
    touchpad_axes: AttributeSet<AbsoluteAxisCode>,

    keyboard_misc: AttributeSet<MiscCode>,
    mouse_misc: AttributeSet<MiscCode>,
    touchpad_misc: AttributeSet<MiscCode>,

    keyboard_device: uinput::VirtualDevice,
    mouse_device: uinput::VirtualDevice,
    touchpad_device: uinput::VirtualDevice,

    /// Currently held keys/buttons with when each press was emitted, so they
    /// can be released on deactivation/disconnect and so delivery anomalies
    /// (duplicated presses, catch-up bursts) can be logged.
    pressed_keys: HashMap<u16, Instant>,
    /// When the last key event was emitted; used to detect catch-up bursts
    /// (a large batch of key events arriving right after a gap = stall flush,
    /// which presents to the user as repeated characters).
    last_key_event_at: Option<Instant>,
}

impl VirtualUInputDevices {
    pub fn new() -> Result<VirtualUInputDevices> {
        let pid = std::process::id();
        let (keyboard_device, keyboard_keys, keyboard_misc) =
            keyboard(pid).context("Failed to create virtual keyboard for simulated output")?;
        let (mouse_device, mouse_keys, mouse_misc, mouse_axes) =
            mouse(pid).context("Failed to create virtual mouse for simulated output")?;
        let (touchpad_device, touchpad_keys, touchpad_misc, touchpad_axes) =
            touchpad(pid).context("Failed to create virtual touchpad for simulated output")?;
        debug!(
            "Event->device routing:

  keyboard_keys: {:?}

  mouse_keys: {:?}

  touchpad_keys: {:?}

  mouse_axes: {:?}

  touchpad_axes: {:?}

  keyboard_misc: {:?}

  mouse_misc: {:?}

  touchpad_misc: {:?}",
            keyboard_keys,
            mouse_keys,
            touchpad_keys,
            mouse_axes,
            touchpad_axes,
            keyboard_misc,
            mouse_misc,
            touchpad_misc
        );
        let ret = VirtualUInputDevices {
            keyboard_keys,
            mouse_keys,
            touchpad_keys,

            mouse_axes,
            touchpad_axes,

            keyboard_misc,
            mouse_misc,
            touchpad_misc,

            keyboard_device,
            mouse_device,
            touchpad_device,
            pressed_keys: HashMap::new(),
            last_key_event_at: None,
        };
        info!("Created virtual uinput devices: keyboard, mouse, touchpad");
        Ok(ret)
    }

    /// Paths of the /dev/input event nodes backing our virtual devices.
    /// Logged at startup and given to the device watcher, which raises an
    /// error if one of them ever disappears mid-session (a broken virtual
    /// keyboard is one way input goes dead while devices are grabbed).
    pub fn device_nodes(&mut self) -> Vec<PathBuf> {
        [
            &mut self.keyboard_device,
            &mut self.mouse_device,
            &mut self.touchpad_device,
        ]
        .into_iter()
        .flat_map(|dev| {
            dev.enumerate_dev_nodes_blocking()
                .map(|nodes| {
                    nodes
                        .filter_map(|res| res.ok())
                        .filter(|p| {
                            p.file_name()
                                .is_some_and(|n| n.to_string_lossy().starts_with("event"))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .collect()
    }

    fn route_event(&self, event: evdev::InputEvent) -> Option<EventDest> {
        match event.destructure() {
            EventSummary::Key(_evt, code, _val) => {
                if self.keyboard_keys.contains(code) {
                    Some(EventDest::Keyboard)
                } else if self.mouse_keys.contains(code) {
                    // mouse_keys and touchpad_keys have a lot of BTN_* key overlap
                    if self.touchpad_keys.contains(code) {
                        Some(EventDest::MouseOrTouchpad)
                    } else {
                        Some(EventDest::Mouse)
                    }
                } else if self.touchpad_keys.contains(code) {
                    Some(EventDest::Touchpad)
                } else {
                    debug!("Dropping key event with unsupported code: {:?}", code);
                    None
                }
            }
            EventSummary::RelativeAxis(_evt, code, _val) => {
                if self.mouse_axes.contains(code) {
                    Some(EventDest::Mouse)
                } else {
                    debug!("Dropping relaxis event with unsupported code: {:?}", code);
                    None
                }
            }
            EventSummary::AbsoluteAxis(_evt, code, _val) => {
                if self.touchpad_axes.contains(code) {
                    Some(EventDest::Touchpad)
                } else {
                    debug!("Dropping absaxis event with unsupported code: {:?}", code);
                    None
                }
            }
            EventSummary::Misc(_evt, code, _val) => {
                if self.keyboard_misc.contains(code) {
                    // keyboard_misc and mouse_misc have MSC_SCAN overlap
                    if self.mouse_misc.contains(code) {
                        Some(EventDest::KeyboardOrMouse)
                    } else {
                        Some(EventDest::Keyboard)
                    }
                } else if self.mouse_misc.contains(code) {
                    Some(EventDest::Mouse)
                } else if self.touchpad_misc.contains(code) {
                    Some(EventDest::Touchpad)
                } else {
                    debug!("Dropping misc event with unsupported code: {:?}", code);
                    None
                }
            }
            _ => {
                debug!("Dropping event with unsupported type: {:?}", event);
                None
            }
        }
    }
}

/// Emits a batch of events plus the terminating SYN_REPORT with a single
/// writev() syscall. evdev's VirtualDevice::emit issues two separate write()
/// calls (events, then SYN_REPORT); at high event rates (e.g. an 8000 Hz
/// gaming mouse) halving the syscall count keeps up more comfortably.
fn emit_events(device: &mut uinput::VirtualDevice, events: &[evdev::InputEvent]) -> std::io::Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let syn = evdev::InputEvent::new(evdev::EventType::SYNCHRONIZATION.0, 0, 0);
    // SAFETY: evdev::InputEvent is a newtype over the kernel's struct input_event
    // and the evdev crate itself byte-casts event slices to write them
    // (evdev::write_events), so viewing the slices as byte iovecs is sound.
    let event_bytes = unsafe {
        std::slice::from_raw_parts(events.as_ptr() as *const u8, std::mem::size_of_val(events))
    };
    let syn_bytes = unsafe {
        std::slice::from_raw_parts(&syn as *const _ as *const u8, std::mem::size_of_val(&syn))
    };
    let iov = [
        libc::iovec {
            iov_base: event_bytes.as_ptr() as *mut libc::c_void,
            iov_len: event_bytes.len(),
        },
        libc::iovec {
            iov_base: syn_bytes.as_ptr() as *mut libc::c_void,
            iov_len: syn_bytes.len(),
        },
    ];
    // SAFETY: the fd is valid (owned by device) and the iovecs point to live data.
    let written = unsafe { libc::writev(device.as_raw_fd(), iov.as_ptr(), iov.len() as libc::c_int) };
    if written < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let expected = (event_bytes.len() + syn_bytes.len()) as isize;
    if written != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "partial write to uinput device",
        ));
    }
    Ok(())
}

#[derive(PartialEq)]
enum EventDest {
    Keyboard,
    Mouse,
    Touchpad,
    KeyboardOrMouse,
    MouseOrTouchpad,
}

#[async_trait]
impl OutputHandler for VirtualUInputDevices {
    async fn release_all(&mut self) -> Result<()> {
        if self.pressed_keys.is_empty() {
            return Ok(());
        }
        debug!(
            "Releasing {} held keys/buttons on virtual devices",
            self.pressed_keys.len()
        );
        let mut releases: Vec<event::InputEvent> = self
            .pressed_keys
            .keys()
            .map(|code| event::InputEvent {
                inputi32: Some(event::InputI32 {
                    type_: evdev::EventType::KEY.0,
                    code: *code,
                    value: 0,
                }),
                inputf64: None,
            })
            .collect();
        releases.push(event::InputEvent {
            inputi32: Some(event::InputI32 {
                type_: evdev::EventType::SYNCHRONIZATION.0,
                code: 0,
                value: 0,
            }),
            inputf64: None,
        });
        // write() routes the releases to the right devices and clears them from
        // the tracking set (on failure it keeps them tracked, so a later
        // release_all can retry).
        self.write(releases).await
    }

    async fn write(&mut self, events: Vec<event::InputEvent>) -> Result<()> {
        let events = events
            .iter()
            .filter_map(|event| {
                if let Some(e) = &event.inputf64 {
                    let evdev_event = e.to_evdev(SCALED_DIM_MIN, SCALED_DIM_MAX);
                    if let Some(dest) = self.route_event(evdev_event) {
                        Some((evdev_event, dest))
                    } else {
                        None
                    }
                } else if let Some(e) = &event.inputi32 {
                    let evdev_event = e.to_evdev();
                    check_discrete_axis_range(&evdev_event);
                    if let Some(dest) = self.route_event(evdev_event) {
                        Some((evdev_event, dest))
                    } else {
                        None
                    }
                } else {
                    warn!("Event missing either an i32 or an f64 value: {}", event);
                    None
                }
            })
            .collect::<Vec<(evdev::InputEvent, EventDest)>>();
        if events.is_empty() {
            return Ok(());
        }

        // Track held keys/buttons so release_all() can unstick them later, and
        // log delivery anomalies that present as spurious repeated characters:
        // duplicated presses (event delivered twice) and catch-up bursts (a
        // backlog flushed after a stall).
        let mut key_events_in_batch = 0u32;
        // Releases untracked by this batch, with their original press time. If
        // the emit below fails, these are re-tracked so the kernel's still-held
        // keys stay visible to a later release_all retry.
        let mut removed_releases: Vec<(u16, Instant)> = Vec::new();
        let mut filtered_events: Vec<(evdev::InputEvent, EventDest)> =
            Vec::with_capacity(events.len());
        for (e, dest) in events {
            if e.event_type() == evdev::EventType::KEY {
                key_events_in_batch += 1;
                match e.value() {
                    0 => {
                        if let Some(since) = self.pressed_keys.remove(&e.code()) {
                            removed_releases.push((e.code(), since));
                            let held = since.elapsed();
                            if held > Duration::from_millis(600) {
                                debug!(
                                    "Key {} was held {:.1}s before its release arrived (delivery delay?)",
                                    e.code(),
                                    held.as_secs_f32()
                                );
                            }
                        }
                    }
                    1 => {
                        if self.pressed_keys.insert(e.code(), Instant::now()).is_some() {
                            warn!(
                                "Duplicate press for key {} with no release in between (event duplicated?)",
                                e.code()
                            );
                        }
                    }
                    // value == 2: auto-repeat, keep the original press timestamp
                    _ => {
                        if !self.pressed_keys.contains_key(&e.code()) {
                            // Repeat for a key we never saw pressed (e.g. held
                            // across a switch): this target never got the press,
                            // so drop the repeat instead of injecting it.
                            if crate::device::key_traced(e.code()) {
                                info!(
                                    "KEYTRACE uinput: dropping auto-repeat for key {} with no matching press",
                                    e.code()
                                );
                            } else {
                                trace!(
                                    "Dropping auto-repeat for key {} with no matching press",
                                    e.code()
                                );
                            }
                            continue;
                        }
                    }
                }
            }
            if e.event_type() == evdev::EventType::KEY && crate::device::key_traced(e.code()) {
                info!("KEYTRACE uinput: emit key {} value {}", e.code(), e.value());
            }
            filtered_events.push((e, dest));
        }
        let events = filtered_events;
        if key_events_in_batch >= 12 {
            if let Some(last) = self.last_key_event_at {
                let gap = last.elapsed();
                if gap > Duration::from_millis(1500) {
                    info!(
                        "Input burst: {} key events delivered after a {:.1}s gap (catch-up after a stall? presents as repeated characters)",
                        key_events_in_batch,
                        gap.as_secs_f32()
                    );
                }
            }
        }
        if key_events_in_batch > 0 {
            self.last_key_event_at = Some(Instant::now());
        }

        // Hand the batch to the kernel. If that fails, the releases untracked
        // above may never have taken effect on the device: re-track them (with
        // their original press times) so the keys don't stay pressed in the
        // kernel while invisible to a later release_all retry.
        if let Err(e) = self.emit_routed_events(&events) {
            for (code, since) in removed_releases {
                self.pressed_keys.insert(code, since);
            }
            return Err(e.into());
        }

        Ok(())
    }
}

impl VirtualUInputDevices {
    /// Routes a prepared batch to the right virtual device(s) and emits it.
    /// Split from write() so the caller can roll back its pressed-key tracking
    /// when the kernel never saw the events.
    fn emit_routed_events(&mut self, events: &[(evdev::InputEvent, EventDest)]) -> std::io::Result<()> {
        // Collect stats on how many events apply to each device
        // We specifically avoid grouping the events themselves so that ordering is preserved
        let mut keyboard_count = 0;
        let mut mouse_count = 0;
        let mut touchpad_count = 0;
        for e in events {
            match e.1 {
                EventDest::Keyboard => {
                    keyboard_count += 1;
                }
                EventDest::Mouse => {
                    mouse_count += 1;
                }
                EventDest::Touchpad => {
                    touchpad_count += 1;
                }
                EventDest::KeyboardOrMouse => {
                    keyboard_count += 1;
                    mouse_count += 1;
                }
                EventDest::MouseOrTouchpad => {
                    mouse_count += 1;
                    touchpad_count += 1;
                }
            }
        }
        // Route the events according to the count stats.
        // The events should be single-device in most cases, but we support mixed events too, just in case.
        if keyboard_count == events.len() {
            // All of the events can be classified as keyboard
            let events = events
                .iter()
                .map(|e| e.0)
                .collect::<Vec<evdev::InputEvent>>();
            trace!(
                "Emitting {} keyboard events: {:?}",
                events.len(),
                events
                    .iter()
                    .map(|e| util::log_event(e))
                    .collect::<Vec<String>>()
            );
            emit_events(&mut self.keyboard_device, &events)?;
        } else if mouse_count == events.len() {
            // All of the events can be classified as mouse
            let events = events
                .iter()
                .map(|e| e.0)
                .collect::<Vec<evdev::InputEvent>>();
            trace!(
                "Emitting {} mouse events: {:?}",
                events.len(),
                events
                    .iter()
                    .map(|e| util::log_event(e))
                    .collect::<Vec<String>>()
            );
            emit_events(&mut self.mouse_device, &events)?;
        } else if touchpad_count == events.len() {
            // All of the events can be classified as touchpad
            let events = events
                .iter()
                .map(|e| e.0)
                .collect::<Vec<evdev::InputEvent>>();
            trace!(
                "Emitting {} touchpad events: {:?}",
                events.len(),
                events
                    .iter()
                    .map(|e| util::log_event(e))
                    .collect::<Vec<String>>()
            );
            emit_events(&mut self.touchpad_device, &events)?;
        } else {
            // Events don't all 'fit' in one device, group by device
            let mut keyboard_events = vec![];
            let mut mouse_events = vec![];
            let mut touchpad_events = vec![];
            for event in events {
                match event.1 {
                    EventDest::Keyboard => {
                        keyboard_events.push(event.0);
                    }
                    EventDest::Mouse => {
                        mouse_events.push(event.0);
                    }
                    EventDest::Touchpad => {
                        touchpad_events.push(event.0);
                    }
                    EventDest::KeyboardOrMouse => {
                        // Arbitrarily pick whichever device has the most events
                        // For example, if the batch is a mix of keyboard and touchpad events,
                        // then this lets us keep the keyboard-or-mouse events with the keyboard.
                        if keyboard_count >= mouse_count {
                            keyboard_events.push(event.0);
                        } else {
                            mouse_events.push(event.0);
                        }
                    }
                    EventDest::MouseOrTouchpad => {
                        // Arbitrarily pick whichever device has the most events
                        // For example, if the batch is a mix of keyboard and touchpad events,
                        // then this lets us keep the mouse-or-touchpad events with the touchpad.
                        if mouse_count >= touchpad_count {
                            mouse_events.push(event.0);
                        } else {
                            touchpad_events.push(event.0);
                        }
                    }
                }
            }
            trace!(
                "Emitting events: keyboard({})={:?} mouse({})={:?} touchpad({})={:?}",
                keyboard_events.len(),
                keyboard_events
                    .iter()
                    .map(|e| util::log_event(e))
                    .collect::<Vec<String>>(),
                mouse_events.len(),
                mouse_events
                    .iter()
                    .map(|e| util::log_event(e))
                    .collect::<Vec<String>>(),
                touchpad_events.len(),
                touchpad_events
                    .iter()
                    .map(|e| util::log_event(e))
                    .collect::<Vec<String>>(),
            );
            if !keyboard_events.is_empty() {
                emit_events(&mut self.keyboard_device, &keyboard_events)?;
            }
            if !mouse_events.is_empty() {
                emit_events(&mut self.mouse_device, &mouse_events)?;
            }
            if !touchpad_events.is_empty() {
                emit_events(&mut self.touchpad_device, &touchpad_events)?;
            }
        }

        Ok(())
    }
}

pub fn keyboard(
    pid: u32,
) -> Result<(
    uinput::VirtualDevice,
    AttributeSet<KeyCode>,
    AttributeSet<MiscCode>,
)> {
    let mut keys = AttributeSet::<KeyCode>::new();
    // Report as many keys as possible to emit by the virtual device.
    for code in 1..libc::KEY_MAX {
        let key = KeyCode::new(code);
        // HACK: Include only known KEY_* keys, or else the keyboard will be ignored.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("KEY_") {
            keys.insert(key);
        }
    }
    let device = uinput::VirtualDevice::builder()?
        .name(format!("{} keyboard for pid {}", VIRTUAL_DEVICE_NAME_PREFIX, pid).as_str())
        .with_keys(&keys)?
        .build()?;

    // We don't seem to need to advertise this, but mark it as a possible event so that we aren't dropping it and logging infos about it.
    let mut misc = AttributeSet::<MiscCode>::new();
    misc.insert(MiscCode::MSC_SCAN);

    Ok((device, keys, misc))
}

pub fn mouse(
    pid: u32,
) -> Result<(
    uinput::VirtualDevice,
    AttributeSet<KeyCode>,
    AttributeSet<MiscCode>,
    AttributeSet<RelativeAxisCode>,
)> {
    let mut keys = AttributeSet::<KeyCode>::new();
    for code in 1..libc::KEY_MAX {
        let key = KeyCode::new(code);
        // HACK: Include only BTN_* keys, and exclude BTN_TOOL_* or else the mouse is ignored.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("BTN_") && !key_name.starts_with("BTN_TOOL_") {
            keys.insert(key);
        }
    }

    // Claim ALL axes. The mouse will be ignored if it claims keys that aren't relevant to claimed axes.
    let mut axes = AttributeSet::<RelativeAxisCode>::new();
    for code in 0..(libc::REL_CNT as u16) {
        axes.insert(RelativeAxisCode(code));
    }

    let device = uinput::VirtualDevice::builder()?
        .name(format!("{} mouse for pid {}", VIRTUAL_DEVICE_NAME_PREFIX, pid).as_str())
        .with_keys(&keys)?
        .with_relative_axes(&axes)?
        .build()?;

    // We don't seem to need to advertise this, but mark it as a possible event so that we aren't dropping it and logging infos about it.
    let mut misc = AttributeSet::<MiscCode>::new();
    misc.insert(MiscCode::MSC_SCAN);

    Ok((device, keys, misc, axes))
}

/// The discrete absolute axes (those util::axis_scale_type returns
/// AxisScale::Discrete for) advertised by the virtual touchpad, with their
/// (min, max) ranges. Discrete events are forwarded raw from the capture
/// side, so these ranges must cover whatever values real devices emit.
const DISCRETE_AXES: &[(AbsoluteAxisCode, i32, i32)] = &[
    // max: arbitrarily big in case some real device uses big values?
    (AbsoluteAxisCode::ABS_MISC, -1, 1048576),
    // max: if this is too big then something panics
    (AbsoluteAxisCode::ABS_MT_SLOT, 0, 32),
    (AbsoluteAxisCode::ABS_MT_TOOL_TYPE, 0, 4095),
    // max: arbitrarily big in case some real device uses big IDs
    (AbsoluteAxisCode::ABS_MT_BLOB_ID, -1, 1048576),
    // max: arbitrarily big in case some real device uses big IDs
    (AbsoluteAxisCode::ABS_MT_TRACKING_ID, -1, 1048576),
];

/// The advertised (min, max) range for a discrete axis on the virtual
/// touchpad, if the axis is one of the DISCRETE_AXES.
fn discrete_axis_range(code: AbsoluteAxisCode) -> Option<(i32, i32)> {
    DISCRETE_AXES
        .iter()
        .find(|(axis, _, _)| *axis == code)
        .map(|(_, min, max)| (*min, *max))
}

/// Loudly logs when a raw discrete-axis event falls outside the range the
/// virtual touchpad advertises for it. Shouldn't happen — the capture side
/// forwards these raw and real devices stay within the advertised ranges —
/// but if a future device exceeds them, libinput would silently drop the
/// event (dead multitouch), so make the mismatch visible.
fn check_discrete_axis_range(event: &evdev::InputEvent) {
    if let EventSummary::AbsoluteAxis(_, code, value) = event.destructure() {
        if let Some((min, max)) = discrete_axis_range(code) {
            if value < min || value > max {
                debug!(
                    "Discrete axis {:?} value {} is outside the virtual touchpad's advertised range {}..={} and will likely be dropped by libinput",
                    code, value, min, max
                );
            }
        }
    }
}

pub fn touchpad(
    pid: u32,
) -> Result<(
    uinput::VirtualDevice,
    AttributeSet<KeyCode>,
    AttributeSet<MiscCode>,
    AttributeSet<AbsoluteAxisCode>,
)> {
    let mut props = AttributeSet::<evdev::PropType>::new();
    // Doesn't seem to be required, but real touchpads have it:
    props.insert(evdev::PropType::BUTTONPAD);
    // Required for movement events to be recognized:
    props.insert(evdev::PropType::POINTER);

    let mut keys = AttributeSet::<KeyCode>::new();
    for code in 1..libc::KEY_MAX {
        let key = KeyCode::new(code);
        // HACK: Limit to only (most) BTN_* keys or else the device won't work.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("BTN_")
        // If one of these keys is present, libinput will classify the device as an ID_INPUT_TABLET,
        // rather than as an ID_INPUT_TOUCHPAD. See also: "sudo libinput record /dev/input/eventNN"
            && key_name != "BTN_TOOL_PEN"
            && key_name != "BTN_STYLUS"
            && key_name != "BTN_STYLUS2"
        {
            keys.insert(key);
        }
    }

    let mut misc = AttributeSet::<MiscCode>::new();
    misc.insert(MiscCode::MSC_TIMESTAMP);

    let name = format!(
        "{} multi touchpad for pid {}",
        VIRTUAL_DEVICE_NAME_PREFIX, pid
    );
    // These are the axes that util::axis_scale_type returns Discrete for,
    // declared from the shared DISCRETE_AXES table so that raw forwarded
    // values always land within an advertised range.
    let mut axis_codes = AttributeSet::<AbsoluteAxisCode>::new();
    let mut axes: Vec<evdev::UinputAbsSetup> = DISCRETE_AXES
        .iter()
        .map(|(axis, min, max)| abs_axis(*axis, *min, *max, 0, &mut axis_codes))
        .collect();
    for i in 0..libc::ABS_MAX + 1 {
        let axis = AbsoluteAxisCode::from_index(i as usize);
        match util::axis_scale_type(axis) {
            util::AxisScale::X => {
                // X axis values: use MAX_X
                axes.push(abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    SCALED_DIM_RES_X,
                    &mut axis_codes,
                ));
                axis_codes.insert(axis);
            }
            util::AxisScale::Y => {
                // Y axis values: use MAX_Y
                axes.push(abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    SCALED_DIM_RES_Y,
                    &mut axis_codes,
                ));
                axis_codes.insert(axis);
            }
            util::AxisScale::Other => {
                axes.push(abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    1,
                    &mut axis_codes,
                ));
                axis_codes.insert(axis);
            }
            _ => {}
        }
    }

    let mut device_builder = uinput::VirtualDevice::builder()?
        .name(name.as_str())
        .with_properties(&props)?
        .with_keys(&keys)?
        .with_msc(&misc)?;
    for axis in &axes {
        device_builder = device_builder.with_absolute_axis(axis)?;
    }
    let device = device_builder.build()?;
    Ok((device, keys, misc, axis_codes))
}

fn abs_axis(
    axis: AbsoluteAxisCode,
    min: i32,
    max: i32,
    res: i32,
    codes: &mut AttributeSet<AbsoluteAxisCode>,
) -> evdev::UinputAbsSetup {
    codes.insert(axis);
    evdev::UinputAbsSetup::new(
        axis,
        AbsInfo::new(
            0,   // value
            min, // min
            max, // max
            0,   // fuzz
            0,   // flat
            res, // res
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every axis classified Discrete must be advertised (with a raw range)
    /// on the virtual touchpad, and nothing else may be in the table: the
    /// capture side forwards exactly the classified-discrete axes raw.
    #[test]
    fn discrete_axes_match_classification() {
        for i in 0..libc::ABS_MAX + 1 {
            let axis = AbsoluteAxisCode::from_index(i as usize);
            assert_eq!(
                util::axis_scale_type(axis) == util::AxisScale::Discrete,
                discrete_axis_range(axis).is_some(),
                "classification/advertisement mismatch for {:?}",
                axis
            );
        }
    }

    /// Raw values a capture device legitimately emits must fit the advertised
    /// ranges — the -1 tracking-id liftoff marker and small slot indexes in
    /// particular.
    #[test]
    fn advertised_ranges_cover_raw_values() {
        let (min, max) = discrete_axis_range(AbsoluteAxisCode::ABS_MT_TRACKING_ID).unwrap();
        assert!(min <= -1 && -1 <= max);
        let (min, max) = discrete_axis_range(AbsoluteAxisCode::ABS_MT_SLOT).unwrap();
        assert!(min <= 3 && 3 <= max);
    }

    /// The injection path emits inputi32 events untouched: no clamping or
    /// remapping of raw discrete values on their way to the virtual device.
    #[test]
    fn raw_discrete_values_are_emitted_untouched() {
        for (code, value) in [
            (AbsoluteAxisCode::ABS_MT_TRACKING_ID.0, -1),
            (AbsoluteAxisCode::ABS_MT_SLOT.0, 3),
        ] {
            let raw = event::InputI32 {
                type_: evdev::EventType::ABSOLUTE.0,
                code,
                value,
            };
            let ev = raw.to_evdev();
            assert_eq!(ev.code(), code);
            assert_eq!(ev.value(), value);
        }
    }

    /// The out-of-range guard accepts in-range values for every discrete
    /// axis without logging (it only fires outside the advertised range).
    #[test]
    fn range_check_accepts_in_range_values() {
        for (axis, min, max) in DISCRETE_AXES {
            for value in [*min, *max] {
                // Must not panic; the interesting property (no log) can't be
                // captured without a tracing subscriber, so this pins the
                // boundary values as accepted by construction.
                check_discrete_axis_range(&evdev::InputEvent::new(
                    evdev::EventType::ABSOLUTE.0,
                    axis.0,
                    value,
                ));
            }
        }
    }
}
