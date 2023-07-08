use anyhow::Result;
use evdev::{uinput, AbsInfo, AbsoluteAxisType, AttributeSet, EvdevEnum, InputEvent, Key};
use tracing::{debug, info, warn};

use crate::deviceutil;
use crate::messages;

pub const VIRTUAL_DEVICE_NAME_PREFIX: &str = "nikau virtual";
pub const SCALED_DIM_MIN: i32 = 0;
pub const SCALED_DIM_MAX: i32 = 65535;

pub struct VirtualDevices {
    key_events: Vec<InputEvent>,
    rel_events: Vec<InputEvent>,
    abs_events: Vec<InputEvent>,

    key_device: uinput::VirtualDevice,
    rel_device: uinput::VirtualDevice,
    abs_device: uinput::VirtualDevice,
}

impl VirtualDevices {
    pub fn new() -> Result<VirtualDevices> {
        info!("Creating virtual devices: keyboard, mouse, touchpad");
        let pid = std::process::id();
        Ok(VirtualDevices {
            key_events: vec![],
            rel_events: vec![],
            abs_events: vec![],
            key_device: keyboard(pid)?,
            rel_device: mouse(pid)?,
            abs_device: touchpad(pid)?,
        })
    }

    pub fn add_event(&mut self, net_event: messages::InputEventV1) -> Result<()> {
        let (events, device) = match net_event.target {
            messages::EventTargetV1::Key => (&mut self.key_events, &mut self.key_device),
            messages::EventTargetV1::Rel => (&mut self.rel_events, &mut self.rel_device),
            messages::EventTargetV1::Abs => (&mut self.abs_events, &mut self.abs_device),
        };

        if let Some(e) = net_event.f64event {
            events.push(e.to_evdev(SCALED_DIM_MIN, SCALED_DIM_MAX));
        } else if let Some(e) = net_event.i32event {
            if e.type_ == evdev::EventType::SYNCHRONIZATION.0 {
                // If it's a sync event, then flush the queued events if any.
                // We only do this queueing because VirtualDevice::emit() internally
                // writes its own sync event that we can't skip.
                if !events.is_empty() {
                    debug!(
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
        if !self.key_events.is_empty() {
            self.key_device.emit(&self.key_events)?;
            self.key_events.clear();
        }
        if !self.rel_events.is_empty() {
            self.rel_device.emit(&self.rel_events)?;
            self.rel_events.clear();
        }
        if !self.abs_events.is_empty() {
            self.abs_device.emit(&self.abs_events)?;
            self.abs_events.clear();
        }
        Ok(())
    }
}

pub fn keyboard(pid: u32) -> Result<uinput::VirtualDevice> {
    let mut keys = AttributeSet::<Key>::new();
    // Report as many keys as possible to emit by the virtual device.
    for code in 1..195 {
        //(libc::KEY_MAX as u16) {
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

// TODO touchpad still needs work: removing and reapplying finger causes cursor to hop to new absolute coords, and two finger scrolling doesnt work
pub fn touchpad(pid: u32) -> Result<uinput::VirtualDevice> {
    let mut props = AttributeSet::<evdev::PropType>::new();
    // Doesn't seem to be required, but real touchpads have it:
    props.insert(evdev::PropType::BUTTONPAD);
    // Required for movement events to be recognized:
    props.insert(evdev::PropType::POINTER);

    let mut keys = AttributeSet::<Key>::new();
    for code in 1..(libc::KEY_MAX as u16) {
        let key = Key::new(code);
        // HACK: Limit to only BTN_* keys or else the device won't work.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("BTN_") {
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
        // These are the axes that deviceutil::is_scaled_axis returns false
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_SLOT,
            AbsInfo::new(
                0,  // value
                0,  // min
                32, // max (if this is too big then something panics)
                0,  // fuzz
                0,  // flat
                0,  // res
            ),
        ))?
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_TOOL_TYPE,
            AbsInfo::new(
                0,    // value
                0,    // min
                4095, // max
                0,    // fuzz
                0,    // flat
                0,    // res
            ),
        ))?
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_BLOB_ID,
            AbsInfo::new(
                0,       // value
                -1,      // min
                1048576, // max (arbitrarily big in case some real device uses big IDs)
                0,       // fuzz
                0,       // flat
                0,       // res
            ),
        ))?
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_TRACKING_ID,
            AbsInfo::new(
                0,       // value
                -1,      // min
                1048576, // max (arbitrarily big in case some real device uses big IDs)
                0,       // fuzz
                0,       // flat
                0,       // res
            ),
        ))?
        .with_msc(&misc)?;

    for i in 0..libc::ABS_MAX + 1 {
        let axis = AbsoluteAxisType::from_index(i as usize);
        if deviceutil::is_scaled_axis(&axis) {
            device_builder = device_builder.with_absolute_axis(&evdev::UinputAbsSetup::new(
                axis,
                AbsInfo::new(
                    0,              // value
                    SCALED_DIM_MIN, // min
                    SCALED_DIM_MAX, // max
                    0,              // fuzz
                    0,              // flat
                    1,              // res
                ),
            ))?;
        }
    }

    let device = device_builder.build()?;
    Ok(device)
}
