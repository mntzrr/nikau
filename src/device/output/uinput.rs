use anyhow::{Context, Result};
use async_trait::async_trait;
use evdev::{uinput, AbsInfo, AbsoluteAxisType, AttributeSet, EvdevEnum, InputEvent, Key};
use tracing::{info, trace, warn};

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

    keyboard_device: uinput::VirtualDevice,
    mouse_device: uinput::VirtualDevice,
    touchpad_device: uinput::VirtualDevice,
}

impl VirtualUInputDevices {
    pub fn new() -> Result<VirtualUInputDevices> {
        let pid = std::process::id();
        let ret = VirtualUInputDevices {
            keyboard_events: vec![],
            mouse_events: vec![],
            touchpad_events: vec![],
            keyboard_device: keyboard(pid)
                .context("Failed to create virtual keyboard for simulated output")?,
            mouse_device: mouse(pid)
                .context("Failed to create virtual mouse for simulated output")?,
            touchpad_device: touchpad(pid)
                .context("Failed to create virtual touchpad for simulated output")?,
        };
        info!("Created virtual uinput devices: keyboard, mouse, touchpad");
        Ok(ret)
    }
}

#[async_trait]
impl OutputHandler for VirtualUInputDevices {
    async fn add_event(&mut self, event: event::InputEvent) -> Result<()> {
        let (events, device) = match event.target {
            event::EventTarget::Keyboard => (&mut self.keyboard_events, &mut self.keyboard_device),
            event::EventTarget::Mouse => (&mut self.mouse_events, &mut self.mouse_device),
            event::EventTarget::Touchpad => (&mut self.touchpad_events, &mut self.touchpad_device),
        };

        if let Some(e) = event.inputf64 {
            let event = e.to_evdev(SCALED_DIM_MIN, SCALED_DIM_MAX);
            trace!("Queued event {:?} -> {}", e, util::log_event(&event));
            events.push(event);
        } else if let Some(e) = event.inputi32 {
            if e.type_ == evdev::EventType::SYNCHRONIZATION.0 {
                // If it's a sync event, then flush the queued events if any.
                // We only do this queueing because VirtualDevice::emit() internally
                // writes its own sync event that we can't skip.
                if !events.is_empty() {
                    trace!(
                        "Sending {} queued events to {} device: {:?}",
                        events.len(),
                        event.target,
                        events.iter().map(util::log_event).collect::<Vec<String>>(),
                    );
                    device.emit(events)?;
                    events.clear();
                }
            } else {
                let event = e.to_evdev();
                trace!("Queued event {:?} -> {}", e, util::log_event(&event));
                events.push(event);
            }
        } else {
            warn!("Event missing either an i32 or an f64 value: {}", event);
        }

        if events.len() >= 100 {
            // Just in case, avoid the risk of collecting queued events forever
            warn!("Forcing event flush due to lack of sync events");
            device.emit(events)?;
            events.clear();
        }

        Ok(())
    }

    async fn flush_events(&mut self) -> Result<()> {
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
    Ok(device)
}

pub fn mouse(pid: u32) -> Result<uinput::VirtualDevice> {
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
    for code in 1..libc::KEY_MAX {
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
        match util::axis_scale_type(axis) {
            util::AxisScale::X => {
                // X axis values: use MAX_X
                device_builder = device_builder.with_absolute_axis(&abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    SCALED_DIM_RES_X,
                ))?;
            }
            util::AxisScale::Y => {
                // Y axis values: use MAX_Y
                device_builder = device_builder.with_absolute_axis(&abs_axis(
                    axis,
                    SCALED_DIM_MIN,
                    SCALED_DIM_MAX,
                    SCALED_DIM_RES_Y,
                ))?;
            }
            util::AxisScale::Other => {
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
