use anyhow::{Context, Result};
use async_trait::async_trait;
use evdev::{
    uinput, AbsInfo, AbsoluteAxisType, AttributeSet, EvdevEnum, InputEvent, InputEventKind, Key,
    MiscType, RelativeAxisType,
};
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
    keyboard_events: Vec<InputEvent>,
    mouse_events: Vec<InputEvent>,
    touchpad_events: Vec<InputEvent>,
    keyboard_or_mouse_events: Vec<InputEvent>,
    mouse_or_touchpad_events: Vec<InputEvent>,

    keyboard_keys: AttributeSet<Key>,
    mouse_keys: AttributeSet<Key>,
    touchpad_keys: AttributeSet<Key>,

    mouse_axes: AttributeSet<RelativeAxisType>,
    touchpad_axes: AttributeSet<AbsoluteAxisType>,

    keyboard_misc: AttributeSet<MiscType>,
    mouse_misc: AttributeSet<MiscType>,
    touchpad_misc: AttributeSet<MiscType>,

    keyboard_device: uinput::VirtualDevice,
    mouse_device: uinput::VirtualDevice,
    touchpad_device: uinput::VirtualDevice,
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
            keyboard_events: vec![],
            mouse_events: vec![],
            touchpad_events: vec![],
            keyboard_or_mouse_events: vec![],
            mouse_or_touchpad_events: vec![],

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
        };
        info!("Created virtual uinput devices: keyboard, mouse, touchpad");
        Ok(ret)
    }

    fn route_event(&mut self, event: evdev::InputEvent) {
        match event.kind() {
            InputEventKind::Key(e) => {
                // TODO route mouse vs touchpad BTN_* events based on any axes in the BATCH
                if self.keyboard_keys.contains(e) {
                    self.keyboard_events.push(event);
                } else if self.mouse_keys.contains(e) {
                    // mouse_keys and touchpad_keys have a lot of BTN_* key overlap
                    if self.touchpad_keys.contains(e) {
                        self.mouse_or_touchpad_events.push(event);
                    } else {
                        self.mouse_events.push(event);
                    }
                } else if self.touchpad_keys.contains(e) {
                    self.touchpad_events.push(event);
                } else {
                    info!("Dropping key event with unsupported code: {:?}", e);
                }
            }
            InputEventKind::RelAxis(e) => {
                if self.mouse_axes.contains(e) {
                    self.mouse_events.push(event);
                } else {
                    info!("Dropping relaxis event with unsupported code: {:?}", e);
                }
            }
            InputEventKind::AbsAxis(e) => {
                if self.touchpad_axes.contains(e) {
                    self.touchpad_events.push(event);
                } else {
                    info!("Dropping absaxis event with unsupported code: {:?}", e);
                }
            }
            InputEventKind::Misc(e) => {
                if self.keyboard_misc.contains(e) {
                    // keyboard_misc and mouse_misc have MSC_SCAN overlap
                    if self.mouse_misc.contains(e) {
                        self.keyboard_or_mouse_events.push(event);
                    } else {
                        self.keyboard_events.push(event);
                    }
                } else if self.mouse_misc.contains(e) {
                    self.mouse_events.push(event);
                } else if self.touchpad_misc.contains(e) {
                    self.touchpad_events.push(event);
                } else {
                    info!("Dropping misc event with unsupported code: {:?}", e);
                }
            }
            _ => {
                info!("Dropping event with unsupported type: {:?}", event);
            }
        }
    }
}

#[async_trait]
impl OutputHandler for VirtualUInputDevices {
    async fn write(&mut self, events: Vec<event::InputEvent>) -> Result<()> {
        let events = events
            .iter()
            .filter_map(|event| {
                if let Some(e) = &event.inputf64 {
                    Some(e.to_evdev(SCALED_DIM_MIN, SCALED_DIM_MAX))
                } else if let Some(e) = &event.inputi32 {
                    Some(e.to_evdev())
                } else {
                    warn!("Event missing either an i32 or an f64 value: {}", event);
                    None
                }
            })
            .collect::<Vec<evdev::InputEvent>>();

        for event in events {
            self.route_event(event);
        }

        // Guess where to route non-definitive events based on any device-only events in the batch.
        // This likely reorders events vs how we received them, but it shouldn't matter within a batch.
        if !self.keyboard_or_mouse_events.is_empty() {
            if !self.mouse_events.is_empty() {
                self.mouse_events
                    .extend(self.keyboard_or_mouse_events.iter());
            } else {
                // Default
                self.keyboard_events
                    .extend(self.keyboard_or_mouse_events.iter());
            }
            self.keyboard_or_mouse_events.clear();
        }
        if !self.mouse_or_touchpad_events.is_empty() {
            if !self.touchpad_events.is_empty() {
                self.touchpad_events
                    .extend(self.mouse_or_touchpad_events.iter());
            } else {
                // Default
                self.mouse_events
                    .extend(self.mouse_or_touchpad_events.iter());
            }
            self.mouse_or_touchpad_events.clear();
        }

        // Send the events to the respective devices.
        // If our mapping is working well, we should only be sending to one device per batch
        trace!(
            "Emitting events: keyboard({})={:?} mouse({})={:?} touchpad({})={:?}",
            self.keyboard_events.len(),
            self.keyboard_events,
            self.mouse_events.len(),
            self.mouse_events,
            self.touchpad_events.len(),
            self.touchpad_events,
        );
        if !self.keyboard_events.is_empty() {
            self.keyboard_device.emit(&self.keyboard_events)?;
            self.keyboard_events.clear();
        }
        if !self.mouse_events.is_empty() {
            self.mouse_device.emit(&self.mouse_events)?;
            self.mouse_events.clear();
        }
        if !self.touchpad_events.is_empty() {
            self.touchpad_device.emit(&self.touchpad_events)?;
            self.touchpad_events.clear();
        }

        Ok(())
    }
}

pub fn keyboard(
    pid: u32,
) -> Result<(
    uinput::VirtualDevice,
    AttributeSet<Key>,
    AttributeSet<MiscType>,
)> {
    let mut keys = AttributeSet::<Key>::new();
    // Report as many keys as possible to emit by the virtual device.
    for code in 1..libc::KEY_MAX {
        let key = Key::new(code);
        // HACK: Include only known KEY_* keys, or else the keyboard will be ignored.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("KEY_") {
            keys.insert(key);
        }
    }
    let device = uinput::VirtualDeviceBuilder::new()?
        .name(format!("{} keyboard for pid {}", VIRTUAL_DEVICE_NAME_PREFIX, pid).as_str())
        .with_keys(&keys)?
        .build()?;

    // We don't seem to need to advertise this, but mark it as a possible event so that we aren't dropping it and logging infos about it.
    let mut misc = AttributeSet::<MiscType>::new();
    misc.insert(MiscType::MSC_SCAN);

    Ok((device, keys, misc))
}

pub fn mouse(
    pid: u32,
) -> Result<(
    uinput::VirtualDevice,
    AttributeSet<Key>,
    AttributeSet<MiscType>,
    AttributeSet<RelativeAxisType>,
)> {
    let mut keys = AttributeSet::<Key>::new();
    for code in 1..libc::KEY_MAX {
        let key = Key::new(code);
        // HACK: Include only BTN_* keys, and exclude BTN_TOOL_* or else the mouse is ignored.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("BTN_") && !key_name.starts_with("BTN_TOOL_") {
            keys.insert(key);
        }
    }

    // Claim ALL axes. The mouse will be ignored if it claims keys that aren't relevant to claimed axes.
    let mut axes = AttributeSet::<RelativeAxisType>::new();
    for code in 0..(libc::REL_CNT as u16) {
        axes.insert(RelativeAxisType(code));
    }

    let device = uinput::VirtualDeviceBuilder::new()?
        .name(format!("{} mouse for pid {}", VIRTUAL_DEVICE_NAME_PREFIX, pid).as_str())
        .with_keys(&keys)?
        .with_relative_axes(&axes)?
        .build()?;

    // We don't seem to need to advertise this, but mark it as a possible event so that we aren't dropping it and logging infos about it.
    let mut misc = AttributeSet::<MiscType>::new();
    misc.insert(MiscType::MSC_SCAN);

    Ok((device, keys, misc, axes))
}

pub fn touchpad(
    pid: u32,
) -> Result<(
    uinput::VirtualDevice,
    AttributeSet<Key>,
    AttributeSet<MiscType>,
    AttributeSet<AbsoluteAxisType>,
)> {
    let mut props = AttributeSet::<evdev::PropType>::new();
    // Doesn't seem to be required, but real touchpads have it:
    props.insert(evdev::PropType::BUTTONPAD);
    // Required for movement events to be recognized:
    props.insert(evdev::PropType::POINTER);

    let mut keys = AttributeSet::<Key>::new();
    for code in 1..libc::KEY_MAX {
        let key = Key::new(code);
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

    let mut misc = AttributeSet::<MiscType>::new();
    misc.insert(MiscType::MSC_TIMESTAMP);

    let name = format!(
        "{} multi touchpad for pid {}",
        VIRTUAL_DEVICE_NAME_PREFIX, pid
    );
    // These are the valid axes that util::axis_scale_type returns DISCRETE
    let mut axis_codes = AttributeSet::<AbsoluteAxisType>::new();
    let mut axes = vec![
        abs_axis(
            AbsoluteAxisType::ABS_MISC,
            -1,      // min
            1048576, // max (arbitrarily big in case some real device uses big values?)
            0,       // res
            &mut axis_codes,
        ),
        abs_axis(
            AbsoluteAxisType::ABS_MT_SLOT,
            0,  // min
            32, // max (if this is too big then something panics)
            0,  // res
            &mut axis_codes,
        ),
        abs_axis(
            AbsoluteAxisType::ABS_MT_TOOL_TYPE,
            0,    // min
            4095, // max
            0,    // res
            &mut axis_codes,
        ),
        abs_axis(
            AbsoluteAxisType::ABS_MT_BLOB_ID,
            -1,      // min
            1048576, // max (arbitrarily big in case some real device uses big IDs)
            0,       // res
            &mut axis_codes,
        ),
        abs_axis(
            AbsoluteAxisType::ABS_MT_TRACKING_ID,
            -1,      // min
            1048576, // max (arbitrarily big in case some real device uses big IDs)
            0,       // res
            &mut axis_codes,
        ),
    ];
    for i in 0..libc::ABS_MAX + 1 {
        let axis = AbsoluteAxisType::from_index(i as usize);
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

    let mut device_builder = uinput::VirtualDeviceBuilder::new()?
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
    axis: AbsoluteAxisType,
    min: i32,
    max: i32,
    res: i32,
    codes: &mut AttributeSet<AbsoluteAxisType>,
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
