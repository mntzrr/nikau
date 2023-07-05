use anyhow::Result;
use evdev::{uinput, AbsInfo, AbsoluteAxisType, AttributeSet, EvdevEnum, EventType, InputEvent, Key};

use std::thread;
use std::time::Duration;

use crate::deviceutil;

pub const VIRTUAL_DEVICE_NAME_PREFIX: &str = "nikau virtual";
pub const SCALED_DIM_SIZE: i32 = 65536;

pub fn print_virtual_devices() {
    for (_path, device) in evdev::enumerate() {
        if let Some(name) = device.name() {
            if name.starts_with(VIRTUAL_DEVICE_NAME_PREFIX) {
                let _ = deviceutil::device_info(&device);
            }
        }
    }
}

pub fn keyboard(demo: bool) -> Result<uinput::VirtualDevice> {
    let mut keys = AttributeSet::<Key>::new();
    // Report as many keys as possible to emit by the virtual device.
    for code in 1..195 {//(libc::KEY_MAX as u16) {
        let key = Key::new(code);
        // HACK: Include only known KEY_* keys, or else the keyboard will be ignored.
        let key_name = format!("{:?}", key);
        if key_name.starts_with("KEY_") {
            keys.insert(key);
        }
    }

    let mut device = uinput::VirtualDeviceBuilder::new()?
        .name(format!("{} keyboard", VIRTUAL_DEVICE_NAME_PREFIX).as_str())
        .with_keys(&keys)?
        .build()
        .unwrap();

    if demo {
        // NOTE: many of the keypresses are missed without a sleep here... but this is just a demo
        //thread::sleep(Duration::from_secs(1));

        for _ in 0..50 {
            // Each emit() call injects a sync event
            device.emit(&[InputEvent::new(EventType::KEY, Key::KEY_R.code(), 1)]).unwrap();
            device.emit(&[InputEvent::new(EventType::KEY, Key::KEY_R.code(), 0)]).unwrap();
            thread::sleep(Duration::from_micros(5_000));
        }
    }

    Ok(device)
}

pub fn mouse(demo: bool) -> Result<uinput::VirtualDevice> {
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

    let mut device = uinput::VirtualDeviceBuilder::new()?
        .name(format!("{} mouse", VIRTUAL_DEVICE_NAME_PREFIX).as_str())
        .with_keys(&keys)?
        .with_relative_axes(&axes)?
        .build()
        .unwrap();

    if demo {
        for _ in 0..50 {
            // Each emit() call injects a sync event
            device.emit(&[
                InputEvent::new(EventType::RELATIVE, evdev::RelativeAxisType::REL_X.0, 5),
                InputEvent::new(EventType::RELATIVE, evdev::RelativeAxisType::REL_Y.0, 5),
            ]).unwrap();
            thread::sleep(Duration::from_micros(5_000));
        }
    }

    Ok(device)
}

pub fn touchpad(demo: bool) -> Result<uinput::VirtualDevice> {
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

    let name = format!("{} multi touchpad", VIRTUAL_DEVICE_NAME_PREFIX);
    let mut device_builder = uinput::VirtualDeviceBuilder::new()?
        .name(name.as_str())
        .with_properties(&props)?
        .with_keys(&keys)?
        // These are the axes that deviceutil::is_scaled_axis returns false
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_SLOT,
            AbsInfo::new(
                0, // value
                0, // min
                32, // max
                0, // fuzz
                0, // flat
                0, // res
            )
        ))?
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_TOOL_TYPE,
            AbsInfo::new(
                0, // value
                0, // min
                4095, // max
                0, // fuzz
                0, // flat
                0, // res
            )
        ))?
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_BLOB_ID,
            AbsInfo::new(
                0, // value
                0, // min
                65535, // max
                0, // fuzz
                0, // flat
                0, // res
            )
        ))?
        .with_absolute_axis(&evdev::UinputAbsSetup::new(
            AbsoluteAxisType::ABS_MT_TRACKING_ID,
            AbsInfo::new(
                0, // value
                0, // min
                65535, // max
                0, // fuzz
                0, // flat
                0, // res
            )
        ))?
        .with_msc(&misc)?;
    for i in 0..libc::ABS_MAX+1 {
        let axis = AbsoluteAxisType::from_index(i as usize);
        if deviceutil::is_scaled_axis(&axis) {
            device_builder = device_builder.with_absolute_axis(&evdev::UinputAbsSetup::new(
                axis,
                AbsInfo::new(
                    0, // value
                    0, // min
                    SCALED_DIM_SIZE - 1, // max
                    0, // fuzz
                    0, // flat
                    1, // res
                )
            ))?;
        }
    }
    let mut device = device_builder.build()?;

    if demo {
        for i in 100..200 {
            // Each emit() call injects a sync event
            device.emit(&[
                InputEvent::new(EventType::ABSOLUTE, AbsoluteAxisType::ABS_X.0, i * 100),
                InputEvent::new(EventType::ABSOLUTE, AbsoluteAxisType::ABS_Y.0, i * 100),
            ])?;
            thread::sleep(Duration::from_micros(5_000));
        }
    }

    Ok(device)
}
