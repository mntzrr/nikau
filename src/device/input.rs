use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Result};
use evdev::{EventStream, EventType, KeyCode};
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio::time;
use tracing::{debug, info, trace, warn};

use crate::device::handles::{DeviceHandle, DeviceHandler};
use crate::device::{shortcut, util, DeviceClass, Event, GrabEvent, GrabState, InputBatch};
use crate::msgs::event;

/// How long to wait before retrying a failed device grab/ungrab.
const GRAB_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// How long to wait for a keyboard to become quiescent (no keys held) before
/// grabbing it anyway.
const GRAB_QUIESCENT_TIMEOUT: Duration = Duration::from_secs(3);
/// Poll interval while waiting for held keys to be released before grabbing.
const GRAB_QUIESCENT_POLL: Duration = Duration::from_millis(50);

/// Grabs a permanently-grabbed (keyboard-class) device, first waiting until no
/// keys are held on it. Grabbing while a key is held leaves the compositor
/// believing the key is still down — it saw the press before the grab but
/// never sees the release (it goes to monux instead) — and its own key-repeat
/// then injects phantom keypresses (seen in the wild as an Enter flood right
/// after launching `monux server<Enter>`, until the next real keypress).
/// Waiting for quiescence guarantees the compositor always sees complete
/// press+release pairs. Falls back to grabbing anyway (with a loud log) after
/// GRAB_QUIESCENT_TIMEOUT, e.g. when a key is stuck held in the kernel because
/// a wireless dongle lost a release packet.
async fn grab_keyboard_when_quiescent(stream: &mut EventStream, device_info: &mut util::DeviceInfo) {
    let start = std::time::Instant::now();
    loop {
        let held: Vec<u16> = match stream.device().get_key_state() {
            Ok(state) => state.iter().map(|k| k.0).collect(),
            Err(e) => {
                debug!(
                    "Failed to query key state for {:?} ({}), grabbing without a quiescence check",
                    stream.device().name().unwrap_or("(Unnamed device)"),
                    e
                );
                break;
            }
        };
        if held.is_empty() {
            break;
        }
        if start.elapsed() >= GRAB_QUIESCENT_TIMEOUT {
            warn!(
                "Grabbing {:?} with keys still held ({:?}): if the compositor starts repeating a key, press and release it once",
                stream.device().name().unwrap_or("(Unnamed device)"),
                held
            );
            break;
        }
        debug!(
            "Waiting to grab {:?}: keys currently held ({:?})",
            stream.device().name().unwrap_or("(Unnamed device)"),
            held
        );
        time::sleep(GRAB_QUIESCENT_POLL).await;
    }
    // Don't read events until the grab succeeds. Without the grab, events
    // would leak through to the local system while also being routed onwards
    // by monux. Another process (e.g. a stale monux server) may hold the grab
    // temporarily, so keep retrying.
    while !handle_grab_event(stream, device_info, GrabEvent::Grab) {
        warn!(
            "Failed to grab {:?}, retrying in {:?}",
            stream.device().name().unwrap_or("(Unnamed device)"),
            GRAB_RETRY_INTERVAL
        );
        time::sleep(GRAB_RETRY_INTERVAL).await;
    }
}

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
        state_rx: watch::Receiver<GrabState>,
        device_info: util::DeviceInfo,
        class: DeviceClass,
    ) -> Result<DeviceHandle> {
        let config = self.config.clone();
        let handle = task::spawn(async move {
            read_device_or_grab_events(&mut stream, config, state_rx, device_info, class).await
        });
        Ok(DeviceHandle { handle })
    }
}

/// The grab target of a device class for a broadcast grab state: keyboards
/// stay grabbed whenever input isn't paused (their combos must be swallowed),
/// toggled devices (mice) only while a client is also active.
pub(crate) fn class_grabbed(class: DeviceClass, state: &GrabState) -> bool {
    if state.paused {
        return false;
    }
    match class {
        DeviceClass::Keyboard => true,
        DeviceClass::Toggled => state.client_active,
    }
}

/// Drives the device toward the target grab state, returning Some(target) when
/// the transition failed and must be retried (see GRAB_RETRY_INTERVAL).
/// Grabbing a KEYBOARD waits for the device to become quiescent first (see
/// grab_keyboard_when_quiescent) — that helper retries the grab itself until
/// it succeeds, so a keyboard grab never lands on the retry path here.
async fn apply_grab_transition(
    stream: &mut EventStream,
    device_info: &mut util::DeviceInfo,
    class: DeviceClass,
    target: bool,
) -> Option<bool> {
    let ok = if target && class == DeviceClass::Keyboard {
        grab_keyboard_when_quiescent(stream, device_info).await;
        true
    } else {
        handle_grab_event(
            stream,
            device_info,
            if target {
                GrabEvent::Grab
            } else {
                GrabEvent::Ungrab
            },
        )
    };
    if ok {
        None
    } else {
        Some(target)
    }
}

async fn read_device_or_grab_events(
    stream: &mut EventStream,
    mut handler_config: HandlerConfig,
    mut state_rx: watch::Receiver<GrabState>,
    mut device_info: util::DeviceInfo,
    class: DeviceClass,
) {
    let mut input_events_batch = Vec::new();
    let mut combo_events_batch = Vec::new();
    // Apply the current grab state right away: this device may have been (re)added
    // while a client is already active or while input is paused, so we can't wait
    // for the next state change. If the (un)grab fails, keep retrying in the
    // background instead of giving up on the device: without the grab, input
    // leaks to the local system while also being routed onwards by monux.
    let mut retry_interval = time::interval(GRAB_RETRY_INTERVAL);
    let mut pending_grab = {
        let target = class_grabbed(class, &state_rx.borrow_and_update());
        if target == device_info.is_grabbed {
            None
        } else {
            apply_grab_transition(stream, &mut device_info, class, target).await
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
                        // Exit the reader: retrying a dead fd spins at 100% CPU and
                        // floods the log until the inotify handler aborts this task.
                        // The watch's Deleted handler cleans up the device handle.
                        info!(
                            "Got an error event for {:?}, removing device (might be unplugged?): {}",
                            stream.device().name().unwrap_or("(Unnamed device)"),
                            e
                        );
                        return;
                    }
                }
            }
            state = state_rx.changed() => {
                if let Err(e) = state {
                    // Sender was dropped, shouldn't happen: exit to avoid looping forever.
                    warn!(
                        "Error on grab watch for {:?}, removing device: {}",
                        stream.device().name(),
                        e
                    );
                    return
                }
                let target = class_grabbed(class, &state_rx.borrow_and_update());
                // Skip states that don't change this class's target (e.g. a
                // client switch is irrelevant to an already-grabbed keyboard).
                if target != device_info.is_grabbed || pending_grab.is_some() {
                    pending_grab =
                        apply_grab_transition(stream, &mut device_info, class, target).await;
                }
            }
            _ = retry_interval.tick(), if pending_grab.is_some() => {
                let target = pending_grab.unwrap();
                pending_grab = apply_grab_transition(stream, &mut device_info, class, target).await;
                if pending_grab.is_some() {
                    warn!(
                        "Retrying {} for {:?} in {:?}",
                        if target { "grab" } else { "ungrab" },
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
        let mut fired: Vec<(usize, Event)> = Vec::new();
        for cs in c.combo_states.iter_mut() {
            match cs.check_combo(&event) {
                shortcut::ComboAction::ConsumeEvent => {
                    any_consume = true;
                }
                shortcut::ComboAction::PassEvent => {}
                shortcut::ComboAction::ConsumeEventAndEmitAction(action) => {
                    any_consume = true;
                    fired.push((cs.num_keys(), action));
                }
            }
        }
        if !fired.is_empty() {
            // Several combos can complete on the same event when one chord's
            // keys are a subset of another's (e.g. the default pause chord
            // LeftShift+LeftAlt+P and the default prev-switch chord LeftAlt+P):
            // only the most specific (longest) chord(s) fire their action, so
            // pausing doesn't also switch clients. Shorter chords still fired
            // internally above, so their key releases stay consumed as usual.
            let max_keys = fired.iter().map(|(n, _)| *n).max().expect("fired is non-empty");
            combo_events_batch.extend(
                fired
                    .into_iter()
                    .filter(|(n, _)| *n == max_keys)
                    .map(|(_, action)| action),
            );
        }
        if any_consume {
            debug!(
                "Dropping key event consumed by one or more key combos: {:?}",
                event
            );
        } else {
            input_events_batch.push(convert_device_event(event, stream.device(), device_info))
        }
        if event.event_type() == EventType::KEY {
            // A zero timestamp marks a synthetic event injected by the evdev
            // crate's SYN_DROPPED resync. Rare, and always suspicious for key
            // events: a synthetic press whose release is lost is a stuck key
            // (and compositor-visible phantom input) in the making.
            let synthetic = event
                .timestamp()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.is_zero())
                .unwrap_or(false);
            if synthetic {
                info!(
                    "Synthetic (resync-injected) key event: code={} value={} device={:?}",
                    event.code(),
                    event.value(),
                    stream.device().name().unwrap_or("(Unnamed device)")
                );
            }
            if crate::device::key_traced(event.code()) {
                info!(
                    "KEYTRACE capture: code={} value={} synthetic={} consumed={} device={:?}",
                    event.code(),
                    event.value(),
                    synthetic,
                    any_consume,
                    stream.device().name().unwrap_or("(Unnamed device)")
                );
            }
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
            info!(
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
            info!(
                "Ungrabbing device: {:?}",
                stream.device().name().unwrap_or("(Unnamed device)")
            );
            if let Err(e) = stream.device_mut().ungrab() {
                if e.raw_os_error() == Some(libc::ENODEV) {
                    // The device is already gone (unplugged): nothing to ungrab,
                    // and no reason to warn or retry.
                    debug!(
                        "Not ungrabbing {:?}: device already gone",
                        stream.device().name()
                    );
                    device_info.is_grabbed = false;
                    return true;
                }
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
