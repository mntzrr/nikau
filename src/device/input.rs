use std::collections::HashMap;

use anyhow::{bail, Result};
use evdev::{EventStream, EventType, Key};
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{debug, info, trace, warn};

use crate::device::{Event, shortcut, util};
use crate::device::watch::{DeviceHandle, DeviceHandler, GrabEvent};
use crate::msgs::event;

pub struct InputHandler {
    config: HandlerConfig,
}

#[derive(Clone)]
struct HandlerConfig {
    combo_states: Vec<shortcut::ComboState>,
    event_tx: mpsc::Sender<Event>,
}

impl InputHandler {
    pub fn new(
        keys_next: shortcut::KeysAction,
        keys_prev: Option<shortcut::KeysAction>,
        keys_goto: Vec<shortcut::KeysAction>,
        event_tx: mpsc::Sender<Event>,
    ) -> Result<InputHandler> {
        let mut keymap = HashMap::new();
        add_key_combo(&mut keymap, keys_next)?;
        if let Some(keys_prev) = keys_prev {
            add_key_combo(&mut keymap, keys_prev)?;
        }
        for entry in keys_goto {
            add_key_combo(&mut keymap, entry)?;
        }
        if keymap.is_empty() {
            bail!(
                "At least one keyboard shortcut must be configured for switching between devices"
            );
        }
        Ok(InputHandler {
            config: HandlerConfig {
                combo_states: keymap
                    .into_iter()
                    .filter_map(|(keys, action)| {
                        if keys.is_empty() {
                            None
                        } else {
                            Some(shortcut::ComboState::new(keys, action))
                        }
                    })
                    .collect(),
                event_tx,
            },
        })
    }
}

fn add_key_combo(
    keymap: &mut HashMap<Vec<Key>, Event>,
    keysaction: shortcut::KeysAction,
) -> Result<()> {
    // Add combo to keymap, complain if an identical combo already exists
    if !keysaction.keys.is_empty() {
        if let Some(existing_action) = keymap.insert(keysaction.keys.clone(), keysaction.action.clone()) {
            bail!(
                "Key combination '{:?}' for {:?} collides with existing combination for {:?}",
                keysaction.keys,
                keysaction.action,
                existing_action
            )
        }
    }
    Ok(())
}

impl DeviceHandler for InputHandler {
    /// Spawns a task for listening to a device's events and for controlling its grab state.
    fn handle_device_stream(
        &mut self,
        mut events: EventStream,
        grab_rx: broadcast::Receiver<GrabEvent>,
        device_info: util::DeviceInfo,
    ) -> Result<DeviceHandle> {
        let config = self.config.clone();
        let handle = task::spawn(async move {
            read_device_events(&mut events, config, grab_rx, device_info).await
        });
        Ok(DeviceHandle { handle })
    }
}

async fn read_device_events(
    stream: &mut EventStream,
    mut c: HandlerConfig,
    mut grab_rx: broadcast::Receiver<GrabEvent>,
    device_info: util::DeviceInfo,
) {
    let mut input_events_batch = Vec::new();
    let mut combo_events_batch = Vec::new();
    loop {
        tokio::select! {
            event = stream.next_event() => {
                match event {
                    Ok(event) => {
                        // 100 limit: Just in case, avoid the risk of collecting queued events forever.
                        //            In practice we should only be collecting 2-3 events between syncs.
                        if event.event_type() == EventType::SYNCHRONIZATION
                            || (input_events_batch.len() + combo_events_batch.len()) >= 100 {
                            // Flush events to be handled by the client as a group
                            if !input_events_batch.is_empty() {
                                if let Err(e) = c.event_tx.send(Event::Input(input_events_batch)).await {
                                    warn!("Error sending input events for routing: {:?}", e);
                                }
                                input_events_batch = Vec::new();
                            }
                            // Follow original events with event(s) from combo completion(s)
                            if !combo_events_batch.is_empty() {
                                for combo_event in combo_events_batch {
                                    if let Err(e) = c.event_tx.send(combo_event).await {
                                        warn!("Error sending combo events for routing: {:?}", e);
                                    }
                                }
                                combo_events_batch = Vec::new();
                            }
                        } else {
                            // Check whether this event completes a key combo, which creates an additional event.
                            // No short-circuit: Ensure that all combo_states have a chance to be updated
                            let mut any_consume = false;
                            for cs in c.combo_states.iter_mut() {
                                match cs.check_combo(&event) {
                                    shortcut::ComboAction::ConsumeEvent => {
                                        any_consume = true;
                                    }
                                    shortcut::ComboAction::PassEvent => {
                                    }
                                    shortcut::ComboAction::ConsumeEventAndEmitAction(action) => {
                                        any_consume = true;
                                        combo_events_batch.push(action);
                                    }
                                    shortcut::ComboAction::PassEventAndEmitAction(action) => {
                                        combo_events_batch.push(action);
                                    }
                                }
                            }
                            if any_consume {
                                debug!("Dropping key event as it's the last key completing one or more combos: {:?}", event);
                            } else {
                                input_events_batch.push(convert_device_event(event, stream.device(), &device_info))
                            }
                        }
                    }
                    Err(e) => {
                        // Common when the device has been unplugged.
                        // We'll frequently get this error just as inotify is telling us the file is deleted.
                        // Exit to avoid an infinite loop on trying to read the missing file.
                        info!(
                            "Got an error event for {:?}, removing device (might be unplugged?): {}",
                            stream.device().name().unwrap_or("(Unnamed device)"),
                            e
                        );
                        return;
                    }
                }
            },
            grab = grab_rx.recv() => {
                match grab {
                    Ok(GrabEvent::Grab) => {
                        debug!("Grabbing device: {:?}", stream.device().name().unwrap_or("(Unnamed device)"));
                        if let Err(e) = stream.device_mut().grab() {
                            panic!("Failed to grab device {:?}: {:?}", stream.device().name(), e);
                        }
                    }
                    Ok(GrabEvent::Ungrab) => {
                        debug!("Ungrabbing device: {:?}", stream.device().name().unwrap_or("(Unnamed device)"));
                        if let Err(e) = stream.device_mut().ungrab() {
                            panic!("Failed to ungrab device {:?}: {:?}", stream.device().name(), e);
                        }
                    }
                    Err(e) => {
                        // Shouldn't happen, but don't want to loop forever if it does
                        warn!(
                            "Error on grab broadcast for {:?}, removing device: {}",
                            stream.device().name(),
                            e
                        );
                        return
                    }
                }
            }
        }
    }
}

fn convert_device_event(
    event: evdev::InputEvent,
    device: &evdev::Device,
    device_info: &util::DeviceInfo,
) -> event::InputEvent {
    // Convert the original event before any combo-generated events.
    let net_event = if let evdev::InputEventKind::AbsAxis(axis) = event.kind() {
        // Special handling for evdev absolute axis (e.g. touchpad) events
        if let Some((axis_min, axis_max)) = device_info.dims.get(&axis.0) {
            // Apply scaling from hardware width to [0.0, 1.0]
            event::InputEvent {
                inputi32: None,
                inputf64: Some(event::InputF64::from_evdev(event, *axis_min, *axis_max)),
            }
        } else {
            event::InputEvent {
                inputi32: Some(event::InputI32::from_evdev(event)),
                inputf64: None,
            }
        }
    } else {
        event::InputEvent {
            inputi32: Some(event::InputI32::from_evdev(event)),
            inputf64: None,
        }
    };
    trace!(
        "Input event @ {}: {} -> {:?}",
        device.name().unwrap_or("(Unnamed device)"),
        util::log_event(&event),
        net_event
    );
    net_event
}
