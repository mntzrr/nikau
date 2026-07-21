use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Result};
use evdev::{EventStream, EventType, KeyCode};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio::time;
use tracing::{debug, info, trace, warn};

use crate::device::handles::{DeviceHandle, DeviceHandler};
use crate::device::{shortcut, util, Event, GrabEvent, InputBatch};
use crate::msgs::event;

/// How long to wait before retrying a failed device grab/ungrab.
const GRAB_RETRY_INTERVAL: Duration = Duration::from_secs(5);

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
        key_combos: &shortcut::KeyCombos,
        event_tx: mpsc::Sender<Event>,
    ) -> Result<InputHandler> {
        let mut keymap = HashMap::new();
        for entry in key_combos.combos.iter() {
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
    keymap: &mut HashMap<Vec<KeyCode>, Event>,
    keysaction: &shortcut::KeyCombo,
) -> Result<()> {
    // Add combo to keymap, complain if an identical combo already exists
    if !keysaction.keys.is_empty() {
        if let Some(existing_action) =
            keymap.insert(keysaction.keys.clone(), keysaction.action.clone())
        {
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
        mut stream: EventStream,
        grab_rx: Option<watch::Receiver<GrabEvent>>,
        mut device_info: util::DeviceInfo,
    ) -> Result<DeviceHandle> {
        let config = self.config.clone();
        let handle = if let Some(grab_rx) = grab_rx {
            // Device has grab toggling enabled
            task::spawn(async move {
                read_device_or_grab_events(&mut stream, config, grab_rx, device_info).await
            })
        } else {
            // Device is to be permanently grabbed
            task::spawn(async move {
                // Don't read events until the grab succeeds. Without the grab,
                // events would leak through to the local system while also being
                // routed onwards by monux. Another process (e.g. a stale monux
                // server) may hold the grab temporarily, so keep retrying.
                while !handle_grab_event(&mut stream, &mut device_info, GrabEvent::Grab) {
                    warn!(
                        "Failed to grab {:?}, retrying in {:?}",
                        stream.device().name().unwrap_or("(Unnamed device)"),
                        GRAB_RETRY_INTERVAL
                    );
                    time::sleep(GRAB_RETRY_INTERVAL).await;
                }
                read_device_events(&mut stream, config, device_info).await
            })
        };
        Ok(DeviceHandle { handle })
    }
}

async fn read_device_events(
    stream: &mut EventStream,
    mut handler_config: HandlerConfig,
    device_info: util::DeviceInfo,
) {
    let mut input_events_batch = Vec::new();
    let mut combo_events_batch = Vec::new();
    loop {
        match stream.next_event().await {
            Ok(event) => {
                handle_input_event(
                    stream,
                    &mut handler_config,
                    event,
                    &device_info,
                    &mut input_events_batch,
                    &mut combo_events_batch,
                )
                .await
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
            }
        }
    }
}

async fn read_device_or_grab_events(
    stream: &mut EventStream,
    mut handler_config: HandlerConfig,
    mut grab_rx: watch::Receiver<GrabEvent>,
    mut device_info: util::DeviceInfo,
) {
    let mut input_events_batch = Vec::new();
    let mut combo_events_batch = Vec::new();
    // Apply the current grab state right away: this device may have been (re)added
    // while a client is already active, so we can't wait for the next switch.
    // If the (un)grab fails, keep retrying in the background instead of giving up
    // on the device: without the grab, input leaks to the local system while also
    // being routed onwards by monux.
    let mut retry_interval = time::interval(GRAB_RETRY_INTERVAL);
    let mut pending_grab = {
        let current = *grab_rx.borrow_and_update();
        if handle_grab_event(stream, &mut device_info, current) {
            None
        } else {
            Some(current)
        }
    };
    loop {
        tokio::select! {
            event = stream.next_event() => {
                match event {
                    Ok(event) => {
                        handle_input_event(stream, &mut handler_config, event, &device_info, &mut input_events_batch, &mut combo_events_batch).await
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
                    }
                }
            }
            grab = grab_rx.changed() => {
                if let Err(e) = grab {
                    // Sender was dropped, shouldn't happen: exit to avoid looping forever.
                    warn!(
                        "Error on grab watch for {:?}, removing device: {}",
                        stream.device().name(),
                        e
                    );
                    return
                }
                let current = *grab_rx.borrow_and_update();
                pending_grab = if handle_grab_event(stream, &mut device_info, current) {
                    None
                } else {
                    Some(current)
                };
            }
            _ = retry_interval.tick(), if pending_grab.is_some() => {
                let current = pending_grab.unwrap();
                if handle_grab_event(stream, &mut device_info, current) {
                    pending_grab = None;
                } else {
                    warn!(
                        "Retrying {:?} for {:?} in {:?}",
                        current,
                        stream.device().name(),
                        GRAB_RETRY_INTERVAL
                    );
                }
            }
        }
    }
}

async fn handle_input_event(
    stream: &mut EventStream,
    c: &mut HandlerConfig,
    event: evdev::InputEvent,
    device_info: &util::DeviceInfo,
    input_events_batch: &mut Vec<event::InputEvent>,
    combo_events_batch: &mut Vec<Event>,
) {
    // 32 limit: Just in case, avoid the risk of collecting queued events forever.
    //            In practice we should only be collecting 2-3 events between syncs.
    if event.event_type() == EventType::SYNCHRONIZATION
        || (input_events_batch.len() + combo_events_batch.len()) >= 32
    {
        // Flush events to be handled by the client as a group
        if !input_events_batch.is_empty() {
            let event = Event::Input(InputBatch {
                // Preserve the allocation: batches reach a steady-state size
                // within a few frames, so regrowing from Vec::new() per flush
                // is wasted churn at high report rates.
                events: std::mem::replace(
                    input_events_batch,
                    Vec::with_capacity(input_events_batch.capacity()),
                ),
                is_grabbed: device_info.is_grabbed,
            });
            if let Err(e) = c.event_tx.send(event).await {
                warn!("Error sending input events for routing: {:?}", e);
            }
        }
        // Follow original events with event(s) from combo completion(s)
        if !combo_events_batch.is_empty() {
            let batch = std::mem::replace(
                combo_events_batch,
                Vec::with_capacity(combo_events_batch.capacity()),
            );
            for combo_event in batch {
                if let Err(e) = c.event_tx.send(combo_event).await {
                    warn!("Error sending combo events for routing: {:?}", e);
                }
            }
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
                shortcut::ComboAction::PassEvent => {}
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
            debug!(
                "Dropping key event as it's the last key completing one or more combos: {:?}",
                event
            );
        } else {
            input_events_batch.push(convert_device_event(event, stream.device(), device_info))
        }
    }
}

fn handle_grab_event(
    stream: &mut EventStream,
    device_info: &mut util::DeviceInfo,
    grab: GrabEvent,
) -> bool {
    match grab {
        GrabEvent::Grab => {
            debug!(
                "Grabbing device: {:?}",
                stream.device().name().unwrap_or("(Unnamed device)")
            );
            if let Err(e) = stream.device_mut().grab() {
                warn!(
                    "Failed to grab device {:?}: {}",
                    stream.device().name(),
                    e
                );
                return false;
            }
            device_info.is_grabbed = true;
            return true;
        }
        GrabEvent::Ungrab => {
            debug!(
                "Ungrabbing device: {:?}",
                stream.device().name().unwrap_or("(Unnamed device)")
            );
            if let Err(e) = stream.device_mut().ungrab() {
                warn!(
                    "Failed to ungrab device {:?}, : {}",
                    stream.device().name(),
                    e
                );
                return false;
            }
            device_info.is_grabbed = false;
            return true;
        }
    }
}

fn convert_device_event(
    event: evdev::InputEvent,
    device: &evdev::Device,
    device_info: &util::DeviceInfo,
) -> event::InputEvent {
    // Convert the original event before any combo-generated events.
    let net_event = if let evdev::EventSummary::AbsoluteAxis(_evt, axis, _val) = event.destructure() {
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
