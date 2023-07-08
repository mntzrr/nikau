use std::collections::BTreeMap;

use evdev::{AbsoluteAxisType, Device, EvdevEnum, EventType, InputEvent, InputEventKind, Key};
use tracing::{debug, trace};

use crate::messages;

pub fn is_scaled_axis(axis: &AbsoluteAxisType) -> bool {
    // HACK: Check for valid enum by looking at the name
    // Unknown values look like "unknown key: N"
    if !format!("{:?}", axis).starts_with("ABS_") {
        return false;
    }

    // In practice it looks like MOST of the AbsoluteAxisTypes are supposed to be big continuous values.
    // So lets just return the ones that we know AREN'T continuous.
    match axis {
        &AbsoluteAxisType::ABS_MT_SLOT => false,
        &AbsoluteAxisType::ABS_MT_TOOL_TYPE => false,
        &AbsoluteAxisType::ABS_MT_BLOB_ID => false,
        &AbsoluteAxisType::ABS_MT_TRACKING_ID => false,
        _ => true,
    }
}

pub struct DeviceInfo {
    pub target: messages::EventTargetV1,
    pub dims: BTreeMap<u16, (i32, i32)>,
}

pub fn device_info(device: &Device) -> DeviceInfo {
    let supported_events = device.supported_events();
    let mut dims = BTreeMap::new();
    let target = if supported_events.contains(EventType::ABSOLUTE) {
        // For each abs axis supported by the device, record its max and min
        // Result will be something like ABS_X(0,100), ABS_Y(0,70), ABS_MT_POSITION_X(0,100) ...
        if let Some(abs_axes) = device.supported_absolute_axes() {
            if let Ok(state) = device.get_abs_state() {
                let mut i: u16 = 0;
                for s in state {
                    let type_ = AbsoluteAxisType::from_index(i as usize);
                    if abs_axes.contains(type_) && is_scaled_axis(&type_) {
                        dims.insert(i, (s.minimum, s.maximum));
                    }
                    i += 1;
                }
            }
        }
        messages::EventTargetV1::Abs
    } else if supported_events.contains(EventType::RELATIVE) {
        messages::EventTargetV1::Rel
    } else {
        messages::EventTargetV1::Key
    };
    log_device(device, &target, &dims);
    DeviceInfo { target, dims }
}

pub fn log_event(event: &InputEvent) -> String {
    let kind = match event.kind() {
        InputEventKind::Key(_key) => {
            // Replace the key with an X to avoid logging passwords etc
            InputEventKind::Key(Key::KEY_X)
        }
        k => k,
    };
    format!("{:?}={}", kind, event.value())
}

fn log_device(device: &Device, target: &messages::EventTargetV1, dims: &BTreeMap<u16, (i32, i32)>) {
    let device_name = device.name().unwrap_or("(Unnamed device)").to_string();
    let mut abs_entries = vec![];
    if let Some(abs_axes) = device.supported_absolute_axes() {
        if let Ok(state) = device.get_abs_state() {
            let mut i = 0;
            for s in state {
                let type_ = AbsoluteAxisType::from_index(i);
                if abs_axes.contains(type_) {
                    abs_entries.push(format!("{:?}:{:?}", type_, s));
                }
                i += 1;
            }
        }
    }
    debug!(
        "Input {} device {} details:
  props: {:?}
  misc: {:?}
  events: {:?}
  keys: {:?}
  leds: {:?}
  rel: {:?}
  abs: {:?}
  dims: {:?}",
        target,
        device_name,
        device.properties(),
        device.misc_properties(),
        device.supported_events(),
        device.supported_keys(),
        device.supported_leds(),
        device.supported_relative_axes(),
        abs_entries,
        dims,
    );
    // under trace, show evdev version of things too, but note that the abs values are missing:
    trace!("{}", device);
}
