use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use evdev::{Device, EventType, KeyCode};
use notify::Watcher;
use regex::Regex;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use crate::device::{handles, output, util};

#[derive(Debug)]
enum DeviceEventKind {
    Created,
    Deleted,
}

#[derive(Debug)]
struct DeviceEvent {
    pub kind: DeviceEventKind,
    pub path: PathBuf,
}

pub async fn watch_loop<H: handles::DeviceHandler>(
    mut device_handles: handles::DeviceHandles<H>,
    device_filters: Vec<Regex>,
    virtual_nodes: Vec<PathBuf>,
) -> Result<()> {
    // Start watch for new and removed devices BEFORE scanning current devices.
    let (device_event_tx, mut device_event_rx): (
        mpsc::Sender<DeviceEvent>,
        mpsc::Receiver<DeviceEvent>,
    ) = mpsc::channel(32);
    let mut watcher = notify::RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| match res {
            Ok(event) => send_device_events(event, &device_event_tx),
            Err(e) => warn!("filesystem watch error: {:?}", e),
        },
        notify::Config::default(),
    )
    .context("failed to init watcher")?;
    watcher.watch(
        std::path::Path::new("/dev/input"),
        notify::RecursiveMode::NonRecursive,
    )?;

    // Scan current devices
    for (path, device) in evdev::enumerate() {
        // enumerate() already filters for 'event*' filenames
        let device_info = util::DeviceInfo::new(&device, false);
        if !compatible_device(&device, &path, &device_info) {
            continue;
        }
        if !matches_filters(&device_filters, &device, &path, &device_info) {
            continue;
        }
        device_handles.add(&path, device)?;
    }
    if device_handles.is_empty() {
        bail!("Didn't find any compatible input devices to listen to.");
    }

    // Start handler to consume new/removed device events
    loop {
        if let Some(event) = device_event_rx.recv().await {
            handle_device_event(&mut device_handles, &device_filters, &virtual_nodes, event).await;
        } else {
            // Channel lost, exit
            return Ok(());
        }
    }
}

async fn handle_device_event<H: handles::DeviceHandler>(
    device_handles: &mut handles::DeviceHandles<H>,
    device_filters: &Vec<Regex>,
    virtual_nodes: &[PathBuf],
    event: DeviceEvent,
) {
    trace!("Device file event: {:?}", event);
    match event.kind {
        DeviceEventKind::Created => {
            if !compatible_path(&event.path) {
                return;
            }
            match open_device_with_retry(&event.path).await {
                Ok(device) => {
                    let device_info = util::DeviceInfo::new(&device, false);
                    if !compatible_device(&device, &event.path, &device_info) {
                        return;
                    }
                    if !matches_filters(device_filters, &device, &event.path, &device_info) {
                        return;
                    }
                    // Avoid exiting loop and aborting program if a newly added device fails
                    if let Err(e) = device_handles.add(&event.path, device) {
                        warn!(
                            "Failed to set up new device {}: {}",
                            event.path.to_string_lossy(),
                            e
                        );
                    }
                }
                Err(e) => {
                    // Avoid exiting loop and aborting program if a new device fails
                    warn!(
                        "Failed to init device {}: {}",
                        event.path.to_string_lossy(),
                        e
                    );
                }
            };
        }
        DeviceEventKind::Deleted => {
            if virtual_nodes.contains(&event.path) {
                // One of OUR virtual devices disappeared mid-session: any input
                // we emit now goes nowhere, which presents as dead keyboard/
                // mouse while devices are grabbed. There is no recovery path
                // short of recreating the devices, so make this loud.
                error!(
                    "Our own virtual device node {} vanished! Emitted input has nowhere to go until monux restarts",
                    event.path.to_string_lossy()
                );
            }
            if let Some(device_handle) = device_handles.remove(&event.path) {
                info!("Removing device: {}", event.path.to_string_lossy());
                device_handle.handle.abort();
            }
        }
    }
}

/// Opens a newly-appeared device node, tolerating the window between the
/// kernel creating the node (root:root 0600) and udev applying group/mode
/// permissions (root:input 0660). Without this, devices appearing while we
/// run — hot-plugged keyboards, but also the virtual devices of any nikau
/// instance (including our own) — are skipped with a spurious Permission
/// denied and, in the hot-plug case, never picked up at all.
async fn open_device_with_retry(path: &Path) -> std::io::Result<Device> {
    const MAX_ATTEMPTS: u32 = 20;
    const RETRY_DELAY: Duration = Duration::from_millis(50);
    let mut attempt = 0;
    loop {
        match Device::open(path) {
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied && attempt < MAX_ATTEMPTS => {
                attempt += 1;
                tokio::time::sleep(RETRY_DELAY).await;
            }
            result => return result,
        }
    }
}

fn compatible_path(path: &Path) -> bool {
    // Filename should be 'event<N>', like 'event3' or 'event14'
    let is_match = path
        .file_name()
        .filter(|f| f.to_string_lossy().starts_with("event"))
        .is_some();
    if !is_match {
        debug!("Ignoring new device path: {}", path.display());
    }
    is_match
}

fn compatible_device(d: &Device, path: &Path, device_info: &util::DeviceInfo) -> bool {
    // Avoid a situation where we're consuming our own virtual output device, risking an infinite loop.
    // This could happen if client and server are running on the same machine (e.g. for testing)
    if let Some(name) = d.name() {
        if name.contains(output::VIRTUAL_DEVICE_NAME_PREFIX) {
            trace!(
                "Ignoring monux virtual device to avoid loopback problem: {} @ {}",
                name,
                path.display()
            );
            return false;
        }
    }
    // We care about these kinds of devices: keyboard, mouse, and touchpad
    let evts = d.supported_events();
    if evts.contains(EventType::ABSOLUTE) || evts.contains(EventType::RELATIVE) {
        // absolute: probably a touchpad or joystick
        // relative: probably a mouse
        true
    } else if evts.contains(EventType::KEY) {
        // probably a keyboard or utility keys
        if let Some(keys) = d.supported_keys() {
            // Some machines have special devices for the power/suspend button, we can ignore those.
            // If the device only supports one or more of these keys, then ignore the device.
            // If this button is pressed on the server, we shouldn't send the power event to clients.
            !keys
                .iter()
                .all(|key| key == KeyCode::KEY_POWER || key == KeyCode::KEY_SLEEP || key == KeyCode::KEY_WAKEUP)
        } else {
            // Key device without any keys? Skip it
            util::log_device_info(
                d,
                path,
                device_info,
                "Ignoring KEY device lacking supported keys",
                false,
            );
            false
        }
    } else {
        // For example this might be an audio device
        util::log_device_info(
            d,
            path,
            device_info,
            "Ignoring device that isn't ABSOLUTE or RELATIVE or KEY",
            false,
        );
        false
    }
}

fn matches_filters(
    name_filters: &Vec<Regex>,
    device: &Device,
    path: &Path,
    device_info: &util::DeviceInfo,
) -> bool {
    let device_name = device.name().unwrap_or("(Unnamed device)");
    if name_filters.is_empty() {
        return true;
    }
    let matches: Vec<&Regex> = name_filters
        .iter()
        .filter(|p| p.is_match(device_name))
        .collect();
    let is_match = !matches.is_empty();
    if !is_match {
        util::log_device_info(
            &device,
            &path,
            device_info,
            "Ignoring device that doesn't match --device name filters",
            true,
        );
    }
    is_match
}

fn send_device_events(event: notify::Event, device_event_tx: &mpsc::Sender<DeviceEvent>) {
    match event.kind {
        notify::EventKind::Create(notify::event::CreateKind::File) => {
            debug!("File created: {:?}", event);
            for path in event.paths {
                if let Err(e) = device_event_tx.try_send(DeviceEvent {
                    kind: DeviceEventKind::Created,
                    path,
                }) {
                    warn!("Failed to queue device create event: {:?}", e);
                }
            }
        }
        notify::EventKind::Remove(notify::event::RemoveKind::File) => {
            debug!("File deleted: {:?}", event);
            for path in event.paths {
                if let Err(e) = device_event_tx.try_send(DeviceEvent {
                    kind: DeviceEventKind::Deleted,
                    path,
                }) {
                    warn!("Failed to queue device delete event: {:?}", e);
                }
            }
        }
        _ => trace!("Other filesystem event: {:?}", event),
    }
}
