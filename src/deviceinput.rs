use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use async_std::task;
use evdev::{EventStream, EventType, InputEvent, Key};
use futures::StreamExt;
use tracing::{debug, warn};

use crate::devicewatch::DeviceHandler;
use crate::{deviceutil, messages};

pub enum Event {
    Input(messages::InputEventV1),
    SwitchNext,
    SwitchPrev,
}

pub struct InputHandler {
    config: HandlerConfig,
}

#[derive(Clone)]
struct HandlerConfig {
    combo_keys_next: Vec<Key>,
    combo_keys_prev: Vec<Key>,
    event_tx: async_channel::Sender<Event>,
}

impl InputHandler {
    pub fn new(
        keys_next: &str,
        keys_prev: Option<&str>,
        event_tx: async_channel::Sender<Event>,
    ) -> Result<InputHandler> {
        let mut combo_keys_next = vec![];
        let mut combo_keys_prev = vec![];
        for key in keys_next.split(",") {
            combo_keys_next.push(
                Key::from_str(format!("KEY_{}", key.trim().to_uppercase()).as_str())
                    .map_err(|e| anyhow!("Unsupported key '{}': {:?}", key, e))?,
            );
        }
        if let Some(keys_prev) = keys_prev {
            for key in keys_prev.split(",") {
                combo_keys_prev.push(
                    Key::from_str(format!("KEY_{}", key.trim().to_uppercase()).as_str())
                        .map_err(|e| anyhow!("Unsupported key '{}': {:?}", key, e))?,
                );
            }
        }
        if combo_keys_next.is_empty() && combo_keys_prev.is_empty() {
            bail!("At least one key must be provided for switching between devices");
        }
        Ok(InputHandler {
            config: HandlerConfig {
                combo_keys_next,
                combo_keys_prev,
                event_tx,
            },
        })
    }
}

impl DeviceHandler for InputHandler {
    fn handle_device_stream(&mut self, stream: EventStream) -> Result<task::JoinHandle<()>> {
        let config = self.config.clone();
        task::Builder::new()
            .name(format!("device: {:?}", stream.device().name()))
            .spawn(async move { read_device_events(stream, config).await })
            .map_err(|e| anyhow!(e))
    }
}

async fn read_device_events(mut stream: EventStream, c: HandlerConfig) {
    let mut pressed_keys_next = bit_vec::BitVec::from_elem(c.combo_keys_next.len(), false);
    let mut pressed_keys_prev = bit_vec::BitVec::from_elem(c.combo_keys_prev.len(), false);
    let (device_target, device_dims) = deviceutil::device_info(&stream.device());
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                // No short-circuit: Ensure that all pressed_keys_* state has a chance to be updated
                let combo_next = check_combo(&event, &c.combo_keys_next, &mut pressed_keys_next);
                let combo_prev = check_combo(&event, &c.combo_keys_prev, &mut pressed_keys_prev);
                let event = if combo_next {
                    Event::SwitchNext
                } else if combo_prev {
                    Event::SwitchPrev
                } else {
                    debug!(
                        "{} event {:?}: {:?}",
                        device_target,
                        stream.device().name(),
                        event
                    );
                    match event.kind() {
                        evdev::InputEventKind::AbsAxis(axis) => {
                            if let Some(axis_dims) = device_dims.get(&axis.0) {
                                // Apply scaling to [0.0, 1.0]
                                Event::Input(messages::InputEventV1 {
                                    target: device_target.clone(),
                                    i32event: None,
                                    f64event: Some(messages::F64EventV1::from_evdev(
                                        event,
                                        axis_dims.0,
                                        axis_dims.1,
                                    )),
                                })
                            } else {
                                // No scaling for this axis
                                Event::Input(messages::InputEventV1 {
                                    target: device_target.clone(),
                                    i32event: Some(messages::I32EventV1::from_evdev(event)),
                                    f64event: None,
                                })
                            }
                        }
                        _ => Event::Input(messages::InputEventV1 {
                            target: device_target.clone(),
                            i32event: Some(messages::I32EventV1::from_evdev(event)),
                            f64event: None,
                        }),
                    }
                };
                if let Err(e) = c.event_tx.send(event).await {
                    warn!("Error trying to send event to server for routing: {}", e);
                }
            }
            Err(e) => {
                // Common when the device has been unplugged.
                // We'll frequently get this error just as inotify is telling us the file is deleted.
                // Exit to avoid an infinite loop on trying to read the missing file.
                warn!(
                    "Error event for {:?}, removing device: {}",
                    stream.device().name(),
                    e
                );
                return;
            }
        }
    }
}

fn check_combo(event: &InputEvent, combo_keys: &Vec<Key>, keys_on: &mut bit_vec::BitVec) -> bool {
    if combo_keys.is_empty() {
        return false;
    }
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
    false
}
