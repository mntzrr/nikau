use anyhow::Result;
use evdev::{uinput, AbsInfo, AbsoluteAxisType, AttributeSet, EvdevEnum, InputEvent, Key};
use tracing::{info, trace, warn};

use crate::deviceutil;
use crate::messages;

pub const VIRTUAL_DEVICE_NAME_PREFIX: &str = "nikau virtual";
pub const SCALED_DIM_MIN: i32 = 0;
pub const SCALED_DIM_MAX: i32 = 65535;
pub const SCALED_DIM_RES_X: i32 = 640; // 65536 / 640 = 102.4mm
pub const SCALED_DIM_RES_Y: i32 = 960; // for a 3/2 ratio vs X: 65536 / 960 = 68.3mm

pub struct VirtualDevices {
    keyboard_events: Vec<InputEvent>,
    mouse_events: Vec<InputEvent>,
    touchpad_events: Vec<InputEvent>,

    keyboard_device: uinput::VirtualDevice,
    mouse_device: uinput::VirtualDevice,
    touchpad_device: uinput::VirtualDevice,
}

impl VirtualDevices {
    pub fn new() -> Result<VirtualDevices> {
        info!("Creating virtual devices: keyboard, mouse, touchpad");
        let pid = std::process::id();
        Ok(VirtualDevices {
            keyboard_events: vec![],
            mouse_events: vec![],
            touchpad_events: vec![],
            keyboard_device: keyboard(pid)?,
            mouse_device: mouse(pid)?,
            touchpad_device: touchpad(pid)?,
        })
    }

    pub fn add_event(&mut self, net_event: messages::InputEvent) -> Result<()> {
        let (events, device) = match net_event.target {
            messages::EventTarget::Keyboard => {
                (&mut self.keyboard_events, &mut self.keyboard_device)
            }
            messages::EventTarget::Mouse => (&mut self.mouse_events, &mut self.mouse_device),
            messages::EventTarget::Touchpad => {
                (&mut self.touchpad_events, &mut self.touchpad_device)
            }
        };

        if let Some(e) = net_event.inputf64 {
            events.push(e.to_evdev(SCALED_DIM_MIN, SCALED_DIM_MAX));
        } else if let Some(e) = net_event.inputi32 {
            if e.type_ == evdev::EventType::SYNCHRONIZATION.0 {
                // If it's a sync event, then flush the queued events if any.
                // We only do this queueing because VirtualDevice::emit() internally
                // writes its own sync event that we can't skip.
                if !events.is_empty() {
                    trace!(
                        "Sending {} events to {} device: {:?}",
                        events.len(),
                        net_event.target,
                        events
                            .iter()
                            .map(|e| deviceutil::log_event(e))
                            .collect::<Vec<String>>(),
                    );
                    device.emit(&events)?;
                    events.clear();
                }
            } else {
                events.push(e.to_evdev());
            }
        } else {
            warn!("Event missing either an i32 or an f64 value: {}", net_event);
        }

        if events.len() >= 100 {
            // Just in case, avoid the risk of collecting queued events forever
            warn!("Forcing event flush due to lack of sync events");
            device.emit(&events)?;
            events.clear();
        }

        Ok(())
    }

    pub fn switch(&mut self, enabled: bool) -> Result<()> {
        info!(
            "This client is {}",
            if enabled { "active" } else { "inactive" }
        );
        // TODO(feature): clear device state for client switches, to avoid e.g. leaving a device with a repeating key. but this requires subscribing and keeping track of device state.
        // TODO(feature): flash LEDs on the OTHER, NON-virtual edvices when this client becomes enabled
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

pub fn keyboard(pid: u32) -> Result<uinput::VirtualDevice> {
    let mut keys = AttributeSet::<Key>::new();
    // Report as many keys as possible to emit by the virtual device.
    for code in 1..(libc::KEY_MAX as u16) {
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
    Ok(device)
}

pub fn mouse(pid: u32) -> Result<uinput::VirtualDevice> {
    let mut keys = AttributeSet::<Key>::new();
    for code in 1..(libc::KEY_MAX as u16) {
        let key = Key::new(code);
        // HACK: Include only BTN_* keys, and exclude BTN_TOOL_* or else the mouse is ignored.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("BTN_") && !key_name.starts_with("BTN_TOOL_") {
            keys.insert(key);
        }
    }

    // Claim ALL axes. The mouse will be ignored if it claims keys that aren't relevant to claimed axes.
    let mut axes = AttributeSet::<evdev::RelativeAxisType>::new();
    for code in 0..(libc::REL_CNT as u16) {
        axes.insert(evdev::RelativeAxisType(code));
    }

    let device = uinput::VirtualDeviceBuilder::new()?
        .name(format!("{} mouse for pid {}", VIRTUAL_DEVICE_NAME_PREFIX, pid).as_str())
        .with_keys(&keys)?
        .with_relative_axes(&axes)?
        .build()?;
    Ok(device)
}

pub fn touchpad(pid: u32) -> Result<uinput::VirtualDevice> {
    let mut props = AttributeSet::<evdev::PropType>::new();
    // Doesn't seem to be required, but real touchpads have it:
    props.insert(evdev::PropType::BUTTONPAD);
    // Required for movement events to be recognized:
    props.insert(evdev::PropType::POINTER);

    let mut keys = AttributeSet::<Key>::new();
    for code in 1..(libc::KEY_MAX as u16) {
        let key = Key::new(code);
        // HACK: Limit to only (most) BTN_* keys or else the device won't work,
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

    let mut misc = AttributeSet::<evdev::MiscType>::new();
    misc.insert(evdev::MiscType::MSC_TIMESTAMP);

    let name = format!(
        "{} multi touchpad for pid {}",
        VIRTUAL_DEVICE_NAME_PREFIX, pid
    );
    let mut device_builder = uinput::VirtualDeviceBuilder::new()?
        .name(name.as_str())
        .with_properties(&props)?
        .with_keys(&keys)?
        // These are the valid axes that deviceutil::axis_scale_type returns DISCRETE
        .with_absolute_axis(&abs_axis(
            AbsoluteAxisType::ABS_MISC,
            -1,      // min
            1048576, // max (arbitrarily big in case some real device uses big values?)
            0,       // res
        ))?
        .with_absolute_axis(&abs_axis(
            AbsoluteAxisType::ABS_MT_SLOT,
            0,  // min
            32, // max (if this is too big then something panics)
            0,  // res
        ))?
        .with_absolute_axis(&abs_axis(
            AbsoluteAxisType::ABS_MT_TOOL_TYPE,
            0,    // min
            4095, // max
            0,    // res
        ))?
        .with_absolute_axis(&abs_axis(
            AbsoluteAxisType::ABS_MT_BLOB_ID,
            -1,      // min
            1048576, // max (arbitrarily big in case some real device uses big IDs)
            0,       // res
        ))?
        .with_absolute_axis(&abs_axis(
            AbsoluteAxisType::ABS_MT_TRACKING_ID,
            -1,      // min
            1048576, // max (arbitrarily big in case some real device uses big IDs)
            0,       // res
        ))?
        .with_msc(&misc)?;

    for i in 0..libc::ABS_MAX + 1 {
        let axis = AbsoluteAxisType::from_index(i as usize);
        match deviceutil::axis_scale_type(axis) {
            deviceutil::AxisScale::X => {
                // X axis values: use MAX_X
                device_builder = device_builder.with_absolute_axis(&abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    SCALED_DIM_RES_X,
                ))?;
            }
            deviceutil::AxisScale::Y => {
                // Y axis values: use MAX_Y
                device_builder = device_builder.with_absolute_axis(&abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    SCALED_DIM_RES_Y,
                ))?;
            }
            deviceutil::AxisScale::OTHER => {
                device_builder = device_builder.with_absolute_axis(&abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    1,
                ))?;
            }
            _ => {}
        }
    }

    let device = device_builder.build()?;
    Ok(device)
}

fn abs_axis(axis: AbsoluteAxisType, min: i32, max: i32, res: i32) -> evdev::UinputAbsSetup {
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
