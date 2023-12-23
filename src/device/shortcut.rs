use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use evdev::{EventType, Key};

use crate::device::Event;

pub struct KeysAction {
    pub keys: Vec<Key>,
    pub action: Event,
}

/// Parses a user-provided goto shortcut of the form "key1,key2,key3=fingerprint-prefix"
pub fn parse_goto(keys_goto: &str) -> Result<KeysAction> {
    let split: Vec<&str> = keys_goto.split('=').collect();
    if split.len() != 2 {
        bail!("Invalid --shortcut-goto: Expected 'key1,key2,key3=[fingerprint-prefix]', but was '{}'", keys_goto);
    }
    let keys = split.get(0).expect("entry_split has len=2");
    let fingerprint_prefix = split.get(1).expect("entry_split has len=2").to_string();
    parse_action(keys, Event::SwitchTo(fingerprint_prefix))
}

/// Parses a user-provided key combination of the form "key1,key2,..." or "key1+key2+...",
/// paired with some action to be performed
pub fn parse_action(keys: &str, action: Event) -> Result<KeysAction> {
    // Allow key string to be either 'x,y,z' or 'x+y+z' but not a mix of both
    let keys_iter = if keys.contains(",") {
        keys.split(',')
    } else {
        keys.split('+')
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

    Ok(KeysAction {
        keys,
        action,
    })
}


/// Result of checking an input event for matching key combination shortcuts
pub(crate) enum ComboAction {
    /// The caller should not send the input event.
    ConsumeEvent,
    /// The caller should emit the input event as-is.
    PassEvent,
    /// The caller should emit the provided switch event, without sending the original input event.
    ConsumeEventAndEmitAction(Event),
    /// The caller should emit the provided switch event after sending the original input event as-is.
    PassEventAndEmitAction(Event),
}

/// Checks input events for a specified key combination.
///
/// For now we allow the keys to be pressed in any order, as long as there's a point where they're all being held down at the same time.
///
/// The key combination is only considered "complete" after the combo keys have all been released.
/// This avoids issues around the server machine thinking device keys are still held down when we grab the device.
#[derive(Clone)]
pub(crate) struct ComboState {
    /// The action to take
    action: Event,
    /// The combo keys that we're looking for. Indexes are mapped to pressed_keys.
    combo_key_codes: Vec<u16>,
    pressed_keys: bit_vec::BitVec,
    last_keypress: Option<evdev::InputEvent>,
}

impl ComboState {
    pub(crate) fn new(combo_keys: Vec<Key>, action: Event) -> ComboState {
        let len = combo_keys.len();
        ComboState {
            action,
            combo_key_codes: combo_keys.into_iter().map(|k| k.code()).collect(),
            pressed_keys: bit_vec::BitVec::from_elem(len, false),
            last_keypress: None,
        }
    }

    /// Checks if the provided event completes a combo according to internal state.
    /// If so, then the action to be taken is returned.
    pub(crate) fn check_combo(&mut self, event: &evdev::InputEvent) -> ComboAction {
        if event.event_type() != EventType::KEY {
            // Not a keypress, pass through
            return ComboAction::PassEvent;
        }
        // Check if this key is one of our assigned combo keys.
        // This search should be cheap as it's limited to the size of the key combo (2-4 keys?)
        if let Some(idx) = self.key_idx(event.code()) {
            // The key event is related to our combo. Update our state to reflect the keypress or release.
            self.pressed_keys.set(idx, event.value() >= 1);
            if let Some(last_keypress) = &self.last_keypress {
                // We're in the stage of waiting for keys to be released so that we can activate the switch.
                // We specifically wait for keys to be released so that a target machine doesn't see a "hanging" keypress.
                // We want the target machine to see the full press+release cycle before we switch targets.
                // However, we do consume/block the last keypress event in the combo, when the combo is "activated".
                // We need to check for the matching release event so that we can block that too.
                // We only block the last event because before that point we don't know if the user is intending to activate a combo,
                // and if we consume keys then the user will notice that e.g. their N key isn't working in the ALT+N combo case.
                let matching = last_keypress.event_type() == event.event_type()
                    && last_keypress.code() == event.code();
                if self.pressed_keys.none() {
                    // Waiting for keys to be released, and all the keys are released.
                    // The combo is complete.
                    self.last_keypress = None;
                    if matching {
                        // This key being released is the one that we consumed the press on earlier.
                        // For example, user pressed ALT, N: We consumed the N press.
                        // Now the user has released ALT and is now releasing N, and we should consume the N release.
                        ComboAction::ConsumeEventAndEmitAction(self.action.clone())
                    } else {
                        // This key being released isn't the one that we consumed earlier.
                        // In the above example, the user released N first and is now releasing ALT, and ALT's keypress wasn't consumed.
                        ComboAction::PassEventAndEmitAction(self.action.clone())
                    }
                } else {
                    // Not all keys have been released yet.
                    // Pass or consume the release event depending on what matching keypress had been consumed.
                    if matching {
                        // This key being released is the one that we consumed the press on earlier.
                        // For example, user pressed ALT, N: We consumed the N press.
                        // Now the user is releasing N first before releasing ALT, and we should consume the N release.
                        ComboAction::ConsumeEvent
                    } else {
                        // This key being released isn't the one that we consumed earlier.
                        // In the above example, the user is releasing ALT before releasing N, and ALT's keypress wasn't consumed.
                        ComboAction::PassEvent
                    }
                }
            } else {
                // Waiting for keys to be pressed before considering the combo to be complete.
                // We consume the last keypress, for example with ALT,N in that order, the destination machine only sees the ALT press/release, we consume the N press/release.
                // If the user pressed N before ALT then we consume the ALT press/release instead. It's just whatever key was pressed last.
                // We only consume the last keypress because we can't speculatively consume keypresses.
                // For example the user might be pressing N,P: If we consume the N keypress before ALT is pressed, it'll look like the N button isn't working.
                if self.pressed_keys.all() {
                    // All the keys are now pressed. Now we start waiting for them to be released.
                    // We also consume this last keypress event. For example if someone presses Alt+N, we drop the N keypress.
                    self.last_keypress = Some(event.clone());
                    ComboAction::ConsumeEvent
                } else {
                    // Not all of the keys are pressed. Keep waiting and let this event through since it might not intended as a combo activation.
                    ComboAction::PassEvent
                }
            }
        } else {
            // The key isn't relevant to the combo at all. Pass it through.
            ComboAction::PassEvent
        }
    }

    fn key_idx(&self, key_code: u16) -> Option<usize> {
        for (idx, combo_key_code) in self.combo_key_codes.iter().enumerate() {
            if key_code == *combo_key_code {
                return Some(idx);
            }
        }
        return None;
    }
}
