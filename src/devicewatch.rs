use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use async_std::task;
use evdev::{Device, EventStream, EventType};
use futures::StreamExt;
use notify::Watcher;
use tracing::{debug, info, trace, warn};

use crate::deviceoutput;

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

/// Trait for watching the addition and removal of devices from the machine
pub trait DeviceHandler: Send + 'static {
    fn handle_device_stream(&mut self, stream: EventStream) -> Result<task::JoinHandle<()>>;
}

pub async fn watch_loop<F: DeviceHandler>(mut handler: F) -> Result<()> {
    // Start watch for new and removed devices BEFORE scanning current devices.
    let (device_event_tx, mut device_event_rx): (
        async_channel::Sender<DeviceEvent>,
        async_channel::Receiver<DeviceEvent>,
    ) = async_channel::bounded(32);
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
    let mut devices = HashMap::new();
    for (path, device) in evdev::enumerate() {
        // enumerate() already filters for 'event*' filenames
        if !compatible_device(&device) {
            trace!(
                "Ignoring device: {} @ {}",
                device.name().unwrap_or("(Unnamed device)"),
                path.to_string_lossy()
            );
            continue;
        }
        let stream = start_device_stream(device, &path)?;
        devices.insert(path, handler.handle_device_stream(stream)?);
    }
    info!("Handling {} initial devices", devices.len());
    if devices.len() <= 0 {
        bail!("Didn't find any compatible devices, are you root?");
    }

    // Start handler to consume new/removed device events
    while let Some(event) = device_event_rx.next().await {
        trace!("Device file event: {:?}", event);
        match event.kind {
            DeviceEventKind::Created => {
                if !compatible_path(&event.path) {
                    continue;
                }
                match Device::open(&event.path) {
                    Ok(device) => {
                        if !compatible_device(&device) {
                            debug!(
                                "Ignoring device: {} @ {}",
                                device.name().unwrap_or("(Unnamed device)"),
                                event.path.to_string_lossy()
                            );
                            continue;
                        }
                        info!(
                            "Listening to new device: {} @ {}",
                            device.name().unwrap_or("(Unnamed device)"),
                            event.path.to_string_lossy()
                        );
                        match start_device_stream(device, &event.path) {
                            Ok(stream) => match handler.handle_device_stream(stream) {
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
                            },
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
                if let Some(join_handle) = devices.remove(&event.path) {
                    info!("Removing device: {}", event.path.to_string_lossy());
                    join_handle.cancel().await;
                }
            }
        }
    }
    Ok(())
}

fn compatible_path(path: &PathBuf) -> bool {
    // Filename should be 'event<N>', like 'event3' or 'event14'
    path.file_name()
        .filter(|f| f.to_string_lossy().starts_with("event"))
        .is_some()
}

fn compatible_device(d: &Device) -> bool {
    // Avoid a situation where we're consuming our own virtual output device, risking an infinite loop.
    // This could happen if client and server are running on the same machine (e.g. for testing)
    if let Some(name) = d.name() {
        if name.contains(deviceoutput::VIRTUAL_DEVICE_NAME_PREFIX) {
            return false;
        }
    }
    // We care about these kinds of devices: keyboard, mouse, and touchpad, respectively
    let evts = d.supported_events();
    evts.contains(EventType::KEY)
        || evts.contains(EventType::RELATIVE)
        || evts.contains(EventType::ABSOLUTE)
}

fn start_device_stream(device: Device, path: &PathBuf) -> Result<EventStream> {
    device.into_event_stream().with_context(|| {
        format!(
            "Failed to initialize async fd for device: {}",
            path.to_string_lossy()
        )
    })
}

fn send_device_events(event: notify::Event, device_event_tx: &async_channel::Sender<DeviceEvent>) {
    match event.kind {
        notify::EventKind::Create(notify::event::CreateKind::File) => {
            debug!("File created: {:?}", event);
            task::block_on(async {
                for path in event.paths {
                    if let Err(e) = device_event_tx
                        .send(DeviceEvent {
                            kind: DeviceEventKind::Created,
                            path,
                        })
                        .await
                    {
                        warn!("Failed to queue device create event: {}", e);
                    }
                }
            });
        }
        notify::EventKind::Remove(notify::event::RemoveKind::File) => {
            debug!("File deleted: {:?}", event);
            task::block_on(async {
                for path in event.paths {
                    if let Err(e) = device_event_tx
                        .send(DeviceEvent {
                            kind: DeviceEventKind::Deleted,
                            path,
                        })
                        .await
                    {
                        warn!("Failed to queue device delete event: {}", e);
                    }
                }
            })
        }
        _ => trace!("Other filesystem event: {:?}", event),
    }
}
