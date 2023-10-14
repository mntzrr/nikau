use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use evdev::{EventStream, EventType, InputEvent, Key};
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{debug, info, trace, warn};

use crate::device::util;
use crate::device::watch::{DeviceHandle, DeviceHandler, GrabEvent};
use crate::msgs::event;

#[derive(Clone, Debug)]
pub enum Event {
    /// A keypress event to send to the active client, if any
    Input(event::InputEvent),
    /// Activate the next client (or the server) in the rotation
    SwitchNext,
    /// Activate the previous client (or the server) in the rotation
    SwitchPrev,
    /// Activate the client with matching cert fingerprint, or the server if the string is empty
    SwitchTo(String),
}

pub struct InputHandler {
    config: HandlerConfig,
}

#[derive(Clone)]
struct HandlerConfig {
    combo_states: Vec<ComboState>,
    event_tx: mpsc::Sender<Event>,
}

impl InputHandler {
    pub fn new(
        keys_next: &str,
        keys_prev: Option<&str>,
        keys_goto: Vec<String>,
        event_tx: mpsc::Sender<Event>,
    ) -> Result<InputHandler> {
        let mut keymap = HashMap::new();
        add_key_combo(&mut keymap, keys_next, Event::SwitchNext)?;
        if let Some(keys_prev) = keys_prev {
            add_key_combo(&mut keymap, keys_prev, Event::SwitchPrev)?;
        }
        for entry in keys_goto {
            let entry_split: Vec<&str> = entry.split('=').collect();
            if entry_split.len() != 2 {
                bail!("Invalid --shortcut-goto: Expected 'key1,key2,key3=[fingerprint-prefix]', but was '{}'", entry);
            }
            let keystr = entry_split.get(0).expect("entry_split has len=2");
            let fingerprint = entry_split.get(1).expect("entry_split has len=2");
            add_key_combo(
                &mut keymap,
                keystr,
                Event::SwitchTo(fingerprint.to_string()),
            )?;
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
                    .map(|(keys, action)| ComboState::new(keys, action))
                    .collect(),
                event_tx,
            },
        })
    }
}

fn add_key_combo(
    keymap: &mut HashMap<Vec<Key>, Event>,
    keys_raw: &str,
    action: Event,
) -> Result<()> {
    // Allow keys to be either 'x,y,z' or 'x+y+z' but not a mix of both
    let keys_iter = match keys_raw.contains(",") {
        true => keys_raw.split(','),
        false => keys_raw.split('+'),
    };
    let mut keys = vec![];
    for key in keys_iter {
        keys.push(
            Key::from_str(format!("KEY_{}", key.trim().to_uppercase()).as_str())
                .map_err(|e| anyhow!("Unsupported key '{}': {:?}", key, e))?,
        );
    }
    // Sort the keys to detect duplicates across e.g. "shift+alt+n" and "alt+shift+n".
    // The key combo handling waits for all keys to be held simultaneously in any order and
    // so doesn't check for keypress ordering. So sorting the keys here shouldn't affect that.
    keys.sort();
    // Add combo to keymap, complain if an identical combo already exists
    if !keys.is_empty() {
        if let Some(existing_action) = keymap.insert(keys.clone(), action.clone()) {
            bail!(
                "Key combination '{:?}' for {:?} collides with existing combination for {:?}",
                keys,
                action,
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
        let handle =
            task::spawn(async move { read_device_events(&mut events, config, grab_rx, device_info).await });
        Ok(DeviceHandle { handle })
    }
}

/// Checks input events for a specified key combination.
///
/// For now we allow the keys to be pressed in any order, as long as there's a point where they're all being held down at the same time.
///
/// The key combination is only considered "complete" after the combo keys have all been released.
/// This avoids issues around the server machine thinking device keys are still held down when we grab the device.
#[derive(Clone)]
struct ComboState {
    /// The action to take
    action: Event,
    /// The combo keys that we're looking for. Indexes are mapped to pressed_keys.
    combo_keys: Vec<Key>,
    pressed_keys: bit_vec::BitVec,
    waiting_for_released_keys: bool,
}

impl ComboState {
    fn new(combo_keys: Vec<Key>, action: Event) -> ComboState {
        let len = combo_keys.len();
        ComboState {
            action,
            combo_keys,
            pressed_keys: bit_vec::BitVec::from_elem(len, false),
            waiting_for_released_keys: false,
        }
    }

    /// Checks if the provided event completes a combo according to internal state.
    /// If so, then the action to be taken is returned.
    fn check_combo(&mut self, event: &InputEvent) -> Option<Event> {
        if self.combo_keys.is_empty() {
            return None;
        }
        if event.event_type() == EventType::KEY {
            // Check if this key is one of our assigned combo keys.
            // This search should be cheap as it's limited to the size of the key combo (2-4 keys?)
            for (idx, combo_key) in self.combo_keys.iter().enumerate() {
                if event.code() == combo_key.code() {
                    // This event is for a combo key.
                    self.pressed_keys.set(idx, event.value() >= 1);
                    if self.waiting_for_released_keys {
                        if self.pressed_keys.none() {
                            // All of the keys are inactive, after previously all being active. The combo is complete.
                            self.waiting_for_released_keys = false;
                            return Some(self.action.clone());
                        }
                    } else if self.pressed_keys.all() {
                        // All of the combo keys are active. Now we start waiting for them to be inactive.
                        self.waiting_for_released_keys = true;
                        return None;
                    }
                }
            }
        }
        None
    }
}

async fn read_device_events(
    stream: &mut EventStream,
    mut c: HandlerConfig,
    mut grab_rx: broadcast::Receiver<GrabEvent>,
    device_info: util::DeviceInfo,
) {
    loop {
        tokio::select! {
            event = stream.next_event() => {
                match event {
                    Ok(event) => {
                        read_device_event(event, &mut c.event_tx, stream.device(), &device_info, &mut c.combo_states).await;
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

async fn read_device_event(
    event: evdev::InputEvent,
    event_tx: &mut mpsc::Sender<Event>,
    device: &evdev::Device,
    device_info: &util::DeviceInfo,
    combo_states: &mut Vec<ComboState>,
) {
    trace!(
        "{} event {:?}: {}",
        device_info.target,
        device.name().unwrap_or("(Unnamed device)"),
        util::log_event(&event),
    );

    // Check whether this event completes a key combo, which creates an additional event.
    // No short-circuit: Ensure that all combo_states have a chance to be updated
    let combo_events: Vec<Event> = combo_states
        .iter_mut()
        .filter_map(|c| c.check_combo(&event))
        .collect();

    // Convert and send the original event before any combo-generated events.
    let event = if let evdev::InputEventKind::AbsAxis(axis) = event.kind() {
        // Special handling for evdev touchpad axis events
        if let Some((axis_min, axis_max)) = device_info.dims.get(&axis.0) {
            // Apply scaling from hardware width to [0.0, 1.0]
            Event::Input(event::InputEvent {
                target: device_info.target.clone(),
                inputi32: None,
                inputf64: Some(event::InputF64::from_evdev(event, *axis_min, *axis_max)),
            })
        } else {
            Event::Input(event::InputEvent {
                target: device_info.target.clone(),
                inputi32: Some(event::InputI32::from_evdev(event)),
                inputf64: None,
            })
        }
    } else {
        Event::Input(event::InputEvent {
            target: device_info.target.clone(),
            inputi32: Some(event::InputI32::from_evdev(event)),
            inputf64: None,
        })
    };
    if let Err(e) = event_tx.send(event).await {
        warn!("Error trying to send event to server for routing: {:?}", e);
    }

    // Follow original event with event(s) from combo completion(s)
    for combo_event in combo_events {
        if let Err(e) = event_tx.send(combo_event).await {
            warn!("Error trying to send event to server for routing: {:?}", e);
        }
    }
}
