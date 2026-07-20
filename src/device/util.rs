use std::collections::BTreeMap;
use std::path::Path;

use evdev::{AbsoluteAxisCode, Device, EvdevEnum, InputEvent, EventSummary, KeyCode};
use tracing::{debug, info, trace};

#[derive(Debug, PartialEq)]
pub enum AxisScale {
    /// Values against the X axis
    X,
    /// Values against the Y axis
    Y,
    /// Continous values against other axes/scales
    Other,
    /// Values that aren't continuous along an axis
    Discrete,
    /// Not known axis values
    Invalid,
}

pub fn axis_scale_type(axis: AbsoluteAxisCode) -> AxisScale {
    match axis {
        AbsoluteAxisCode::ABS_X => AxisScale::X,
        AbsoluteAxisCode::ABS_Y => AxisScale::Y,
        AbsoluteAxisCode::ABS_Z => AxisScale::Other,
        AbsoluteAxisCode::ABS_RX => AxisScale::X,
        AbsoluteAxisCode::ABS_RY => AxisScale::Y,
        AbsoluteAxisCode::ABS_RZ => AxisScale::Other,
        AbsoluteAxisCode::ABS_THROTTLE => AxisScale::Other,
        AbsoluteAxisCode::ABS_RUDDER => AxisScale::Other,
        AbsoluteAxisCode::ABS_WHEEL => AxisScale::Other,
        AbsoluteAxisCode::ABS_GAS => AxisScale::Other,
        AbsoluteAxisCode::ABS_BRAKE => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT0X => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT0Y => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT1X => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT1Y => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT2X => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT2Y => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT3X => AxisScale::Other,
        AbsoluteAxisCode::ABS_HAT3Y => AxisScale::Other,
        AbsoluteAxisCode::ABS_PRESSURE => AxisScale::Other,
        AbsoluteAxisCode::ABS_DISTANCE => AxisScale::Other,
        AbsoluteAxisCode::ABS_TILT_X => AxisScale::Other,
        AbsoluteAxisCode::ABS_TILT_Y => AxisScale::Other,
        AbsoluteAxisCode::ABS_TOOL_WIDTH => AxisScale::Other,
        AbsoluteAxisCode::ABS_VOLUME => AxisScale::Other,
        AbsoluteAxisCode::ABS_MISC => AxisScale::Discrete,
        AbsoluteAxisCode::ABS_MT_SLOT => AxisScale::Discrete,
        AbsoluteAxisCode::ABS_MT_TOUCH_MAJOR => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_TOUCH_MINOR => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_WIDTH_MAJOR => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_WIDTH_MINOR => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_ORIENTATION => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_POSITION_X => AxisScale::X,
        AbsoluteAxisCode::ABS_MT_POSITION_Y => AxisScale::Y,
        AbsoluteAxisCode::ABS_MT_TOOL_TYPE => AxisScale::Discrete,
        AbsoluteAxisCode::ABS_MT_BLOB_ID => AxisScale::Discrete,
        AbsoluteAxisCode::ABS_MT_TRACKING_ID => AxisScale::Discrete,
        AbsoluteAxisCode::ABS_MT_PRESSURE => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_DISTANCE => AxisScale::Other,
        AbsoluteAxisCode::ABS_MT_TOOL_X => AxisScale::X,
        AbsoluteAxisCode::ABS_MT_TOOL_Y => AxisScale::Y,
        _ => AxisScale::Invalid,
    }
}

pub struct DeviceInfo {
    pub dims: BTreeMap<u16, (i32, i32)>,
    pub is_grabbed: bool,
}

impl DeviceInfo {
    pub fn new(device: &Device, is_grabbed: bool) -> DeviceInfo {
        let mut dims = BTreeMap::new();
        // For each abs axis supported by the device, record its max and min
        // Result will be something like ABS_X(0,100), ABS_Y(0,70), ABS_MT_POSITION_X(0,100) ...
        if let Some(abs_axes) = device.supported_absolute_axes() {
            if let Ok(state) = device.get_abs_state() {
                // clippy recommends this ugly way to get a loop counter
                for (i, s) in (0_u16..).zip(state.into_iter()) {
                    let type_ = AbsoluteAxisCode::from_index(i as usize);
                    if abs_axes.contains(type_) && axis_scale_type(type_) != AxisScale::Invalid {
                        dims.insert(i, (s.minimum, s.maximum));
                    }
                }
            }
        }
        DeviceInfo { dims, is_grabbed }
    }
}

pub fn log_device_info(
    device: &Device,
    path: &Path,
    device_info: &DeviceInfo,
    log_prefix: &str,
    info: bool,
) {
    // under info, show device name/path only
    let msg = format!(
        "{}: {} @ {}",
        log_prefix,
        device.name().unwrap_or("(Unnamed device)"),
        path.display(),
    );
    if info {
        info!("{}", msg);
    } else {
        debug!("{}", msg);
    }
    // under debug, show monux version of device details
    debug!(
        "Monux device details:{}",
        device_info_string(device, &device_info.dims)
    );
    // under trace, show evdev version of things too, but note that the abs values are missing:
    trace!("Evdev device details:\n{}", device);
}

/// Summarizes an evdev InputEvent, hiding the key being pressed in the case of a key event.
pub fn log_event(event: &InputEvent) -> String {
    let kind = match event.destructure() {
        EventSummary::Key(evt, _code, value) => {
            // Replace the key with an X to avoid logging passwords etc
            EventSummary::Key(evt, KeyCode::KEY_X, value)
        }
        k => k,
    };
    format!("{:?}={}", kind, event.value())
}

fn device_info_string(device: &Device, dims: &BTreeMap<u16, (i32, i32)>) -> String {
    let mut abs_entries = vec![];
    if let Some(abs_axes) = device.supported_absolute_axes() {
        if let Ok(state) = device.get_abs_state() {
            for (i, s) in state.into_iter().enumerate() {
                let type_ = AbsoluteAxisCode::from_index(i);
                if abs_axes.contains(type_) {
                    abs_entries.push(format!("{:?}:{:?}", type_, s));
                }
            }
        }
    }
    format!(
        "
  name: {}
  props: {:?}
  misc: {:?}
  events: {:?}
  keys: {:?}
  leds: {:?}
  rel: {:?}
  abs: {:?}
  dims: {:?}",
        device.name().unwrap_or("(Unnamed device)"),
        device.properties(),
        device.misc_properties(),
        device.supported_events(),
        device.supported_keys(),
        device.supported_leds(),
        device.supported_relative_axes(),
        abs_entries,
        dims,
    )
}
