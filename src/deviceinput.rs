use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use async_std::task;
use evdev::{EventStream, EventType, InputEvent, Key};
use futures::{select, FutureExt, StreamExt};
use tracing::{debug, warn};

use crate::devicewatch::{DeviceHandle, DeviceHandler, GrabEvent};
use crate::{deviceutil, messages};

#[derive(Debug)]
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
    /// Spawns a task for listening to a device's events and for controlling its grab state.
    fn handle_device_stream(&mut self, mut stream: EventStream) -> Result<DeviceHandle> {
        let (grab_tx, grab_rx): (
            async_channel::Sender<GrabEvent>,
            async_channel::Receiver<GrabEvent>,
        ) = async_channel::bounded(32);
        let config = self.config.clone();
        let handle = task::Builder::new()
            .name(format!("device: {:?}", stream.device().name()))
            .spawn(async move { read_device_events(&mut stream, config, grab_rx).await })
            .map_err(|e| anyhow!(e))?;
        Ok(DeviceHandle { handle, grab_tx })
    }
}

/// Checks input events for a specified key combination.
///
/// For now we allow the keys to be pressed in any order, as long as there's a point where they're all being held down at the same time.
///
/// The key combination is only considered "complete" after the combo keys have all been released.
/// This avoids issues around the server machine thinking device keys are still held down when we grab the device.
struct ComboState {
    /// The combo keys that we're looking for. Indexes are mapped to pressed_keys.
    combo_keys: Vec<Key>,
    pressed_keys: bit_vec::BitVec,
    waiting_for_released_keys: bool,
}

impl ComboState {
    fn new(combo_keys: Vec<Key>) -> ComboState {
        let len = combo_keys.len();
        ComboState {
            combo_keys,
            pressed_keys: bit_vec::BitVec::from_elem(len, false),
            waiting_for_released_keys: false,
        }
    }

    fn check_combo(&mut self, event: &InputEvent) -> bool {
        if self.combo_keys.is_empty() {
            return false;
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
                            return true;
                        }
                    } else if self.pressed_keys.all() {
                        // All of the combo keys are active. Now we start waiting for them to be inactive.
                        self.waiting_for_released_keys = true;
                        return false;
                    }
                }
            }
        }
        false
    }
}

async fn read_device_events(
    stream: &mut EventStream,
    mut c: HandlerConfig,
    mut grab_rx: async_channel::Receiver<GrabEvent>,
) {
    let mut combo_state_next = ComboState::new(c.combo_keys_next.clone());
    let mut combo_state_prev = ComboState::new(c.combo_keys_prev.clone());
    let device_info = deviceutil::device_info(&stream.device());
    loop {
        select! {
            event = stream.next_event().fuse() => {
                match event {
                    Ok(event) => {
                        read_device_event(event, &mut c.event_tx, &stream.device(), &device_info, &mut combo_state_next, &mut combo_state_prev).await;
                    },
                    Err(e) => {
                        // Common when the device has been unplugged.
                        // We'll frequently get this error just as inotify is telling us the file is deleted.
                        // Exit to avoid an infinite loop on trying to read the missing file.
                        warn!(
                            "Error event for {:?}, removing device: {}",
                            stream.device().name(),
                            e
                        );
                    }
                }
            },
            grab = grab_rx.next() => {
                if let Some(grab) = grab {
                    match grab {
                        GrabEvent::Grab => {
                            debug!("Grabbing device: {:?}", stream.device().name().unwrap_or("(Unnamed device)"));
                            if let Err(e) = stream.device_mut().grab() {
                                panic!("Failed to grab device {:?}: {:?}", stream.device().name(), e);
                            }
                        },
                        GrabEvent::Ungrab => {
                            debug!("Ungrabbing device: {:?}", stream.device().name().unwrap_or("(Unnamed device)"));
                            if let Err(e) = stream.device_mut().ungrab() {
                                panic!("Failed to ungrab device {:?}: {:?}", stream.device().name(), e);
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn read_device_event(
    event: evdev::InputEvent,
    event_tx: &mut async_channel::Sender<Event>,
    device: &evdev::Device,
    device_info: &deviceutil::DeviceInfo,
    combo_state_next: &mut ComboState,
    combo_state_prev: &mut ComboState,
) {
    // No short-circuit: Ensure that all pressed_keys_* state has a chance to be updated
    let combo_next = combo_state_next.check_combo(&event);
    let combo_prev = combo_state_prev.check_combo(&event);
    let event = if combo_next || combo_prev {
        // Combo has completed: User has pressed and released all of the combo keys (in any order)
        // Pass through the released event so that the key doesn't appear held down indefinitely for the switch
        let orig_event = Event::Input(messages::InputEventV1 {
            target: device_info.target.clone(),
            i32event: Some(messages::I32EventV1::from_evdev(event)),
            f64event: None,
        });
        if let Err(e) = event_tx.send(orig_event).await {
            warn!("Error trying to send event to server for routing: {:?}", e);
        }
        // Follow up with our injected switch event reflecting the combo completion
        if combo_next {
            Event::SwitchNext
        } else {
            Event::SwitchPrev
        }
    } else {
        debug!(
            "{} event {:?}: {}",
            device_info.target,
            device.name().unwrap_or("(Unnamed device)"),
            deviceutil::log_event(&event),
        );
        match event.kind() {
            evdev::InputEventKind::AbsAxis(axis) => {
                if let Some(axis_dims) = device_info.dims.get(&axis.0) {
                    // Apply scaling to [0.0, 1.0]
                    Event::Input(messages::InputEventV1 {
                        target: device_info.target.clone(),
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
                        target: device_info.target.clone(),
                        i32event: Some(messages::I32EventV1::from_evdev(event)),
                        f64event: None,
                    })
                }
            }
            _ => Event::Input(messages::InputEventV1 {
                target: device_info.target.clone(),
                i32event: Some(messages::I32EventV1::from_evdev(event)),
                f64event: None,
            }),
        }
    };
    if let Err(e) = event_tx.send(event).await {
        warn!("Error trying to send event to server for routing: {:?}", e);
    }
}
