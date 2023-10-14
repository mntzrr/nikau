use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use evdev::{Device, EventStream, EventType, Key};
use notify::Watcher;
use regex::Regex;
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{debug, info, trace, warn};

use crate::device::{output, util};

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

#[derive(Clone, Debug)]
pub enum GrabEvent {
    Grab,
    Ungrab,
}

pub struct DeviceHandle {
    pub handle: task::JoinHandle<()>,
}

/// Trait for watching the addition and removal of devices from the machine
pub trait DeviceHandler: Send + 'static {
    fn handle_device_stream(
        &mut self,
        events: EventStream,
        grab_rx: broadcast::Receiver<GrabEvent>,
        device_info: util::DeviceInfo,
    ) -> Result<DeviceHandle>;
}

pub async fn watch_loop<F: DeviceHandler>(
    mut handler: F,
    mut grab_tx: broadcast::Sender<GrabEvent>,
    device_filters: Vec<Regex>,
) -> Result<()> {
    // Start watch for new and removed devices BEFORE scanning current devices.
    let (device_event_tx, mut device_event_rx): (
        mpsc::Sender<DeviceEvent>,
        mpsc::Receiver<DeviceEvent>,
    ) = mpsc::channel(32);
    let mut watcher = notify::RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| match res {
            Ok(event) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(send_device_events(event, &device_event_tx)),
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
    let mut devices = HashMap::new();
    for (path, device) in evdev::enumerate() {
        // enumerate() already filters for 'event*' filenames
        if !compatible_device(&device, &path) {
            continue;
        }
        if !matches_filters(&device_filters, &device, &path) {
            continue;
        }
        let device_info = util::log_device_info(&device, &path, "Listening to device", true);
        let events = start_device_stream(device, &path)?;
        devices.insert(
            path,
            handler.handle_device_stream(events, grab_tx.subscribe(), device_info)?,
        );
    }
    if devices.is_empty() {
        bail!("Didn't find any compatible input devices to listen to.");
    }

    // Start handler to consume new/removed device events
    loop {
        if let Some(event) = device_event_rx.recv().await {
            handle_device_event(
                &mut handler,
                &mut devices,
                &mut grab_tx,
                &device_filters,
                event,
            )
            .await;
        } else {
            // Channel lost, exit
            return Ok(());
        }
    }
}

async fn handle_device_event<F: DeviceHandler>(
    handler: &mut F,
    devices: &mut HashMap<PathBuf, DeviceHandle>,
    grab_tx: &mut broadcast::Sender<GrabEvent>,
    device_filters: &Vec<Regex>,
    event: DeviceEvent,
) {
    trace!("Device file event: {:?}", event);
    match event.kind {
        DeviceEventKind::Created => {
            if !compatible_path(&event.path) {
                return;
            }
            match Device::open(&event.path) {
                Ok(device) => {
                    if !compatible_device(&device, &event.path) {
                        return;
                    }
                    if !matches_filters(device_filters, &device, &event.path) {
                        return;
                    }
                    let device_info = util::log_device_info(
                        &device,
                        &event.path,
                        "Listening to new device",
                        true,
                    );
                    match start_device_stream(device, &event.path) {
                        Ok(stream) => {
                            match handler.handle_device_stream(
                                stream,
                                grab_tx.subscribe(),
                                device_info,
                            ) {
                                Ok(join_handle) => {
                                    devices.insert(event.path, join_handle);
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to start event handler for device {}: {}",
                                        event.path.to_string_lossy(),
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            // Avoid exiting loop and aborting program if a new device fails
                            warn!(
                                "Failed to read device {}: {}",
                                event.path.to_string_lossy(),
                                e
                            );
                        }
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
            if let Some(device_handle) = devices.remove(&event.path) {
                info!("Removing device: {}", event.path.to_string_lossy());
                device_handle.handle.abort();
            }
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

fn compatible_device(d: &Device, path: &Path) -> bool {
    // Avoid a situation where we're consuming our own virtual output device, risking an infinite loop.
    // This could happen if client and server are running on the same machine (e.g. for testing)
    if let Some(name) = d.name() {
        if name.contains(output::VIRTUAL_DEVICE_NAME_PREFIX) {
            trace!(
                "Ignoring nikau virtual device to avoid loopback problem: {} @ {}",
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
                .all(|key| key == Key::KEY_POWER || key == Key::KEY_SLEEP || key == Key::KEY_WAKEUP)
        } else {
            // Key device without any keys? Skip it
            util::log_device_info(d, path, "Ignoring KEY device lacking supported keys", false);
            false
        }
    } else {
        // For example this might be an audio device
        util::log_device_info(
            d,
            path,
            "Ignoring device that isn't ABSOLUTE or RELATIVE or KEY",
            false,
        );
        false
    }
}

fn matches_filters(name_filters: &Vec<Regex>, d: &Device, path: &Path) -> bool {
    let device_name = d.name().unwrap_or("(Unnamed device)");
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
            d,
            path,
            "Ignoring device that doesn't match --device name filters",
            true,
        );
    }
    is_match
}

fn start_device_stream(device: Device, path: &Path) -> Result<EventStream> {
    device.into_event_stream().with_context(|| {
        format!(
            "Failed to initialize async fd for device: {}",
            path.to_string_lossy()
        )
    })
}

async fn send_device_events(event: notify::Event, device_event_tx: &mpsc::Sender<DeviceEvent>) {
    match event.kind {
        notify::EventKind::Create(notify::event::CreateKind::File) => {
            debug!("File created: {:?}", event);
            for path in event.paths {
                if let Err(e) = device_event_tx
                    .send(DeviceEvent {
                        kind: DeviceEventKind::Created,
                        path,
                    })
                    .await
                {
                    warn!("Failed to queue device create event: {:?}", e);
                }
            }
        }
        notify::EventKind::Remove(notify::event::RemoveKind::File) => {
            debug!("File deleted: {:?}", event);
            for path in event.paths {
                if let Err(e) = device_event_tx
                    .send(DeviceEvent {
                        kind: DeviceEventKind::Deleted,
                        path,
                    })
                    .await
                {
                    warn!("Failed to queue device delete event: {:?}", e);
                }
            }
        }
        _ => trace!("Other filesystem event: {:?}", event),
    }
}
