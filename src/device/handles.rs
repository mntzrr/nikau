use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use evdev::{Device, EventStream, KeyCode};
use tokio::sync::watch;
use tokio::task;
use tracing::debug;

use crate::device;
use crate::device::util;

pub struct DeviceHandle {
    pub handle: task::JoinHandle<()>,
}

/// Trait for watching the addition and removal of devices from the machine
pub trait DeviceHandler: Send + 'static {
    fn handle_device_stream(
        &mut self,
        events: EventStream,
        grab_rx: Option<watch::Receiver<device::GrabEvent>>,
        device_info: util::DeviceInfo,
    ) -> Result<DeviceHandle>;
}

pub struct DeviceHandles<H: DeviceHandler> {
    /// Devices which support one or more keys specified in client switch key combos.
    /// These devices are always grabbed at the server so that we can consistently
    /// grab/"swallow" the key combo input when the local server is the active target.
    always_grabbed_devices: HashMap<PathBuf, DeviceHandle>,

    /// Devices which don't support one or more key combo keys, such as mice.
    /// When the local server is the active target, nikau ungrabs the device and allows
    /// its input to pass through directly.
    toggled_devices: HashMap<PathBuf, DeviceHandle>,

    handler: H,

    /// Method for subscribing devices to grab events
    grab_tx: watch::Sender<device::GrabEvent>,

    /// All distinct keys used in client switch key combos, for internal accounting.
    all_combo_keys: HashSet<KeyCode>,
}

impl<H: DeviceHandler> DeviceHandles<H> {
    pub fn new(
        handler: H,
        grab_tx: watch::Sender<device::GrabEvent>,
        all_combo_keys: HashSet<KeyCode>,
    ) -> DeviceHandles<H> {
        DeviceHandles {
            always_grabbed_devices: HashMap::<PathBuf, DeviceHandle>::new(),
            toggled_devices: HashMap::<PathBuf, DeviceHandle>::new(),
            handler,
            grab_tx,
            all_combo_keys,
        }
    }

    pub(crate) fn add(&mut self, path: &PathBuf, device: Device) -> Result<()> {
        let device_info = util::DeviceInfo::new(&device, false);
        util::log_device_info(&device, &path, &device_info, "Listening to device", true);
        let supports_any_keys = supports_any_keys(&device, &self.all_combo_keys);
        if supports_any_keys {
            debug!(
                "Device supports one or more configured combo keys: {}",
                device.name().unwrap_or("(Unnamed device)")
            );
        }
        if supports_any_keys {
            // This device supports one or more keys configured for client switch key combinations.
            // We should grab/route its input via nikau so that we can omit keypresses from the combos.
            let join_handle = self.handler.handle_device_stream(
                start_device_stream(device, path)?,
                None,
                device_info,
            )?;
            self.always_grabbed_devices
                .insert(path.clone(), join_handle);
        } else {
            // This device doesn't support keys used in key combinations (e.g. a mouse).
            // When the server is the active input, we can ungrab the device,
            // letting its input pass through directly.
            let join_handle = self.handler.handle_device_stream(
                start_device_stream(device, path)?,
                Some(self.grab_tx.subscribe()),
                device_info,
            )?;
            self.toggled_devices.insert(path.clone(), join_handle);
        }
        Ok(())
    }

    pub(crate) fn remove(&mut self, path: &PathBuf) -> Option<DeviceHandle> {
        if let Some(handle) = self.always_grabbed_devices.remove(path) {
            return Some(handle);
        }
        self.toggled_devices.remove(path)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.always_grabbed_devices.is_empty() && self.toggled_devices.is_empty()
    }
}

fn supports_any_keys(d: &Device, all_combo_keys: &HashSet<KeyCode>) -> bool {
    if let Some(device_keys) = d.supported_keys() {
        for key in all_combo_keys.iter() {
            if device_keys.contains(*key) {
                return true;
            }
        }
    }
    false
}

fn start_device_stream(device: Device, path: &Path) -> Result<EventStream> {
    device.into_event_stream().with_context(|| {
        format!(
            "Failed to initialize async fd for device: {}",
            path.to_string_lossy()
        )
    })
}
