mod logging;

use anyhow::{anyhow, bail, Result};
use async_std::task;
use evdev::{Device, EventType, InputEvent, Key};
use futures::StreamExt;
use futures_lite::future::FutureExt;
use tracing::{error, info, warn};

use std::str::FromStr;

fn compatible_device(d: &Device) -> bool {
    let evts = d.supported_events();
    evts.contains(EventType::KEY) || evts.contains(EventType::ABSOLUTE) || evts.contains(EventType::RELATIVE)
}

fn pick_devices() -> Result<Vec<Device>> {
    let devices = evdev::enumerate()
        .map(|t| t.1)
        .filter(|d| compatible_device(d))
        .collect::<Vec<_>>();
    if devices.len() <= 0 {
        bail!("Didn't find any compatible devices");
    }
    for d in &devices {
        info!("- {}, {:?}, {:?}, {:?}", d.name().unwrap_or("(Unnamed device)"), d.properties(), d.supported_events(), d.supported_keys());
    }
    Ok(devices)
}

fn check_combo(event: &InputEvent, combo_keys: &Vec<Key>, keys_on: &mut bit_vec::BitVec) -> bool {
    if event.event_type() == EventType::KEY {
        // Check if this key is one of our assigned combo keys
        for (idx, combo_key) in combo_keys.iter().enumerate() {
            if event.code() == combo_key.code() {
                if event.value() == 2 && keys_on.all() {
                    // The key is being repeated (value=2) and we already have a combo.
                    // Avoid repeating the combo event if the keys are being held down for too long.
                    return false;
                }
                // We allow the combo to be reached in any order (e.g. alt+esc or esc+alt).
                // If we wanted strict ordering then we could set keys_on to false for idx+1..combo_keys.len()
                keys_on.set(idx, event.value() >= 1);
                if keys_on.all() {
                    // Combo has been reached.
                    return true;
                }
            }
        }
    }
    return false;
}

fn main() -> Result<()> {
    logging::init_logging();

    // TODO args for user-specified key combos. require at least one key
    let combo_keys = vec![
        Key::from_str("KEY_LEFTCTRL").map_err(|e| anyhow!("Unsupported key: {:?}", e))?,
        Key::from_str("KEY_LEFTSHIFT").map_err(|e| anyhow!("Unsupported key: {:?}", e))?,
        Key::from_str("KEY_F").map_err(|e| anyhow!("Unsupported key: {:?}", e))?,
    ];

    // TODO update device tasks as devices are added or removed
    let devices = pick_devices()?;
    let devices = task::block_on(async move {
        devices
            .into_iter()
            .filter_map(|d| {
                match d.into_event_stream() {
                    Ok(s) => {
                        Some(s)
                    },
                    Err(e) => {
                        // Skip this device? Something wrong with it, or race where it was just removed?
                        warn!("Failed to initialize async fd for device: {}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
    });
    let mut tasks: Vec<async_std::task::JoinHandle<()>> = Vec::new();
    for mut d in devices {
        let combo_keys = combo_keys.clone();
        tasks.push(task::Builder::new().name(format!("device: {:?}", d.device().name())).spawn(async move {
            // TODO when told by an upstream signal, grab or ungrab the d.device().
            // this should be controlled via the system switcher shortcut

            let mut keys_on = bit_vec::BitVec::from_elem(combo_keys.len(), false);
            while let Some(event) = d.next().await {
                match event {
                    Ok(event) => {
                        if check_combo(&event, &combo_keys, &mut keys_on) {
                            // Combo has been reached on this device
                            // TODO emit signal to switch output target (and grab or ungrab local devices)
                            info!("COMBO!!!!!");
                        } else {
                            info!("got event for {:?}: {:?}", d.device().name(), event);
                        }
                    },
                    Err(e) => warn!("Got error event for {:?}: {:?}", d.device().name(), e),
                }
            }
        })?);
    }

    let poller = TaskPoller { tasks };
    task::block_on(async move {
        error!("{}", poller.await);
    });
    bail!("a task exited, bailing")
}

struct TaskPoller {
    tasks: Vec<task::JoinHandle<()>>,
}

impl std::future::Future for TaskPoller {
    type Output = String;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        for task in &mut self.tasks {
            if let std::task::Poll::Ready(_) = task.poll(cx) {
                let msg = format!("task {:?} has exited, shutting down process", task.task().name());
                return std::task::Poll::Ready(msg);
            }
        }
        std::task::Poll::Pending
    }
}
