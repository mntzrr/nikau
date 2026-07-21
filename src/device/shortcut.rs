use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use evdev::{EventType, KeyCode};

use crate::device::Event;

pub struct KeyCombos {
    pub combos: Vec<KeyCombo>,
    pub all_keys: HashSet<KeyCode>,
}

/// A combination of keys paired with an action to be emitted when the combination is entered by the user
pub struct KeyCombo {
    pub keys: Vec<KeyCode>,
    pub action: Event,
}

/// Parses user-provided key shortcuts into a list of combinations paired with actions to be performed
pub fn parse_key_combos(
    keys_next: &str,
    keys_prev: Option<&str>,
    keys_goto: Vec<String>,
    keys_pause: Option<&str>,
) -> Result<KeyCombos> {
    let mut combos = vec![];
    combos.push(parse_action(keys_next, Event::SwitchNext)?);
    if let Some(kp) = keys_prev {
        combos.push(parse_action(kp, Event::SwitchPrev)?);
    }
    for kg in keys_goto.into_iter() {
        combos.push(parse_goto(&kg)?);
    }
    if let Some(kp) = keys_pause {
        combos.push(parse_action(kp, Event::PauseToggle)?);
    }
    let all_keys = combos.iter().flat_map(|combo| combo.keys.clone()).collect();
    Ok(KeyCombos { combos, all_keys })
}

/// Parses a user-provided goto shortcut of the form "key1,key2,key3=fingerprint-prefix"
fn parse_goto(keys_goto: &str) -> Result<KeyCombo> {
    let split: Vec<&str> = keys_goto.split('=').collect();
    if split.len() != 2 {
        bail!(
            "Invalid --shortcut-goto: Expected 'key1,key2,key3=[fingerprint-prefix]', but was '{}'",
            keys_goto
        );
    }
    let keys = split.get(0).expect("entry_split has len=2");
    let fingerprint_prefix = split.get(1).expect("entry_split has len=2").to_string();
    parse_action(keys, Event::SwitchTo(fingerprint_prefix))
}

/// Parses a user-provided key combination of the form "key1,key2,..." or "key1+key2+...",
/// paired with some action to be performed
fn parse_action(keys: &str, action: Event) -> Result<KeyCombo> {
    // Allow key string to be either 'x,y,z' or 'x+y+z' but not a mix of both
    let keys_iter = if keys.contains(",") {
        keys.split(',')
    } else {
        keys.split('+')
    };
    let mut keys = vec![];
    for keyname_orig in keys_iter {
        // First try 'KEY_<X>'
        let keyname = keyname_orig.trim().to_uppercase();
        if let Ok(key) = KeyCode::from_str(format!("KEY_{}", keyname).as_str()) {
            keys.push(key);
        } else {
            // Didn't find 'KEY_<X>', try just '<X>' for things like 'BTN_0'
            keys.push(
                KeyCode::from_str(format!("{}", keyname).as_str())
                    .map_err(|e| anyhow!("Unsupported key '{}': Tried KEY_{} and {}, see list of available keys at https://docs.rs/evdev/latest/evdev/struct.KeyCode.html (error: {:?})", keyname_orig, keyname, keyname, e))?,
            );
        }
    }
    // Sort the keys to detect duplicates across e.g. "shift+alt+n" and "alt+shift+n".
    // The key combo handling waits for all keys to be held simultaneously in any order and
    // so doesn't check for keypress ordering. So sorting the keys here shouldn't affect that.
    keys.sort();

    Ok(KeyCombo { keys, action })
}

/// Result of checking an input event for matching key combination shortcuts
pub(crate) enum ComboAction {
    /// The caller should not send the input event.
    ConsumeEvent,
    /// The caller should emit the input event as-is.
    PassEvent,
    /// The caller should emit the provided switch event, without sending the original input event.
    ConsumeEventAndEmitAction(Event),
}

/// Checks input events for a specified key combination.
///
/// Keys may be pressed in any order, as long as there's a point where they're all being pressed at the same time.
///
/// The combination fires the moment the last missing combo key is *pressed* (fire-on-prime):
/// stuck-key protection doesn't depend on fire-on-release because both sides run release_all
/// on every switch. Combo key presses forwarded before the combo primed (e.g. the initial
/// Shift/Alt presses) are cleaned up by that release_all, so they're passed through untouched.
/// After firing, all combo key events are consumed until every combo key has been released,
/// so the new target (which never saw those keys pressed) doesn't get stray repeats/releases.
/// A fresh press (after an actual release) that completes the combo again while the other
/// combo keys are still held fires again — e.g. holding Shift+Alt and tapping R cycles
/// through clients. Autorepeat (value 2) never fires.
#[derive(Clone)]
pub(crate) struct ComboState {
    /// The action to take
    action: Event,
    /// The combo keys that we're looking for. Indexes are mapped to pressed_keys.
    combo_key_codes: Vec<u16>,
    pressed_keys: bit_vec::BitVec,
    /// Whether the combo has fired and we're now consuming combo key events
    /// until they've all been released.
    fired: bool,
}

impl ComboState {
    pub(crate) fn new(combo_keys: Vec<KeyCode>, action: Event) -> ComboState {
        let len = combo_keys.len();
        ComboState {
            action,
            combo_key_codes: combo_keys.into_iter().map(|k| k.code()).collect(),
            pressed_keys: bit_vec::BitVec::from_elem(len, false),
            fired: false,
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
            // Matching is value-aware: a release (value 0) never completes the combo,
            // and autorepeat (value 2) never fires it.
            let was_pressed = self.pressed_keys.get(idx).expect("idx is in bounds");
            self.pressed_keys.set(idx, event.value() >= 1);
            if self.fired {
                // The combo fired already; consume every combo key event until all the
                // combo keys have been released, so the new target (which never saw these
                // keys pressed) doesn't get stray repeats or releases.
                if event.value() == 1 && !was_pressed && self.pressed_keys.all() {
                    // A fresh press completing the combo again while the other combo keys
                    // are still held: fire again. Holding Shift+Alt and tapping R cycles
                    // through clients. Autorepeat (value 2) never re-fires.
                    return ComboAction::ConsumeEventAndEmitAction(self.action.clone());
                }
                if self.pressed_keys.none() {
                    // All the combo keys were released: ready for the next attempt.
                    self.fired = false;
                }
                ComboAction::ConsumeEvent
            } else {
                // Waiting for the full combo to be pressed. Events pass through untouched
                // until then: we can't speculatively consume presses, since the user might
                // be using the keys for something else (e.g. the N in ALT+N).
                if event.value() == 1 && self.pressed_keys.all() {
                    // The final missing combo key was just pressed: the combo is complete.
                    // Fire right away and consume this press. The earlier combo key presses
                    // were already forwarded to the current target, which is fine: the
                    // release_all both sides run on a switch cleans them up.
                    self.fired = true;
                    ComboAction::ConsumeEventAndEmitAction(self.action.clone())
                } else {
                    // Not all of the keys are pressed (or this is a release/repeat).
                    // A partial combo abandoned mid-way (a combo key released before the
                    // combo completed) leaves the state clean for the next attempt.
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

    /// Number of keys in this combo. Used to pick the most specific chord when
    /// several combos complete on the same event (see handle_input_event).
    pub(crate) fn num_keys(&self) -> usize {
        self.combo_key_codes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHIFT: KeyCode = KeyCode::KEY_LEFTSHIFT;
    const ALT: KeyCode = KeyCode::KEY_LEFTALT;
    const R: KeyCode = KeyCode::KEY_R;
    const G: KeyCode = KeyCode::KEY_G;
    const X: KeyCode = KeyCode::KEY_X;

    fn rotate_combo() -> ComboState {
        ComboState::new(vec![SHIFT, ALT, R], Event::SwitchNext)
    }

    fn key(code: KeyCode, value: i32) -> evdev::InputEvent {
        evdev::InputEvent::new(EventType::KEY.0, code.code(), value)
    }

    fn press(code: KeyCode) -> evdev::InputEvent {
        key(code, 1)
    }

    fn repeat(code: KeyCode) -> evdev::InputEvent {
        key(code, 2)
    }

    fn release(code: KeyCode) -> evdev::InputEvent {
        key(code, 0)
    }

    fn assert_pass(action: ComboAction) {
        assert!(
            matches!(action, ComboAction::PassEvent),
            "expected PassEvent, got {}",
            action_name(&action)
        );
    }

    fn assert_consume(action: ComboAction) {
        assert!(
            matches!(action, ComboAction::ConsumeEvent),
            "expected ConsumeEvent, got {}",
            action_name(&action)
        );
    }

    fn assert_fired(action: ComboAction, expected: &Event) {
        match &action {
            ComboAction::ConsumeEventAndEmitAction(action) => {
                assert_eq!(format!("{:?}", action), format!("{:?}", expected))
            }
            _ => panic!(
                "expected ConsumeEventAndEmitAction, got {}",
                action_name(&action)
            ),
        }
    }

    fn action_name(action: &ComboAction) -> &'static str {
        match action {
            ComboAction::ConsumeEvent => "ConsumeEvent",
            ComboAction::PassEvent => "PassEvent",
            ComboAction::ConsumeEventAndEmitAction(_) => "ConsumeEventAndEmitAction",
        }
    }

    /// Releases every combo key, asserting each is consumed, and returns the
    /// combo to its initial state.
    fn drain_all(cs: &mut ComboState) {
        for code in [SHIFT, ALT, R] {
            assert_consume(cs.check_combo(&release(code)));
        }
    }

    #[test]
    fn primes_and_fires_on_final_press() {
        let mut cs = rotate_combo();
        // Combo keys may be pressed in any order; each passes through until the
        // combo is complete (they might be meant for the current target).
        assert_pass(cs.check_combo(&press(ALT)));
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
    }

    #[test]
    fn consumes_chord_events_until_all_released() {
        let mut cs = rotate_combo();
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        // Until every combo key is released, all combo key events (releases,
        // autorepeats) are consumed so no stray events reach the new target,
        // which never saw these keys pressed.
        assert_consume(cs.check_combo(&repeat(R)));
        assert_consume(cs.check_combo(&release(R)));
        assert_consume(cs.check_combo(&repeat(SHIFT)));
        // Non-combo keys pass through untouched meanwhile.
        assert_pass(cs.check_combo(&press(X)));
        assert_pass(cs.check_combo(&release(X)));
        assert_consume(cs.check_combo(&release(SHIFT)));
        // Not all released yet (ALT still held): still consuming.
        assert_consume(cs.check_combo(&press(ALT)));
        assert_consume(cs.check_combo(&release(ALT)));
        // All released now: the state machine is clean, a fresh combo fires.
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
    }

    #[test]
    fn repress_of_final_key_refires_while_others_held() {
        let mut cs = rotate_combo();
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        // Holding Shift+Alt and tapping R cycles through clients.
        assert_consume(cs.check_combo(&release(R)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        assert_consume(cs.check_combo(&release(R)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        drain_all(&mut cs);
    }

    #[test]
    fn autorepeat_never_fires() {
        let mut cs = rotate_combo();
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        // An autorepeat of the last combo key doesn't complete the combo.
        assert_pass(cs.check_combo(&repeat(R)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        // Autorepeats of the held combo keys after firing don't re-fire either
        // (no machine-gunning while the chord is held).
        assert_consume(cs.check_combo(&repeat(R)));
        assert_consume(cs.check_combo(&repeat(SHIFT)));
        assert_consume(cs.check_combo(&repeat(ALT)));
        drain_all(&mut cs);
    }

    #[test]
    fn abandoned_partial_chord_does_not_fire() {
        let mut cs = rotate_combo();
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        // The user gives up on the chord: no fire, and the releases pass
        // through (their presses were forwarded earlier).
        assert_pass(cs.check_combo(&release(SHIFT)));
        assert_pass(cs.check_combo(&release(ALT)));
        // The state machine is clean: a fresh attempt fires normally.
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        drain_all(&mut cs);
    }

    #[test]
    fn release_never_completes_chord() {
        let mut cs = rotate_combo();
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        // A release event for the missing combo key must not count as a match.
        assert_pass(cs.check_combo(&release(R)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        drain_all(&mut cs);
    }

    #[test]
    fn non_chord_events_pass_through() {
        let mut cs = rotate_combo();
        // Non-key events and non-combo keys are ignored, before and after firing.
        assert_pass(cs.check_combo(&evdev::InputEvent::new(EventType::RELATIVE.0, 0, 5)));
        assert_pass(cs.check_combo(&press(X)));
        assert_pass(cs.check_combo(&press(SHIFT)));
        assert_pass(cs.check_combo(&press(ALT)));
        assert_fired(cs.check_combo(&press(R)), &Event::SwitchNext);
        assert_pass(cs.check_combo(&press(X)));
        assert_pass(cs.check_combo(&repeat(X)));
        assert_pass(cs.check_combo(&release(X)));
        assert_pass(cs.check_combo(&evdev::InputEvent::new(EventType::RELATIVE.0, 0, -3)));
        drain_all(&mut cs);
    }

    #[test]
    fn rotate_and_goto_chords_interact() {
        let mut rotate = rotate_combo();
        let mut goto = ComboState::new(vec![SHIFT, ALT, G], Event::SwitchTo("abcd".to_string()));
        // Shared modifier presses pass through and prime both state machines.
        for cs in [&mut rotate, &mut goto] {
            assert_pass(cs.check_combo(&press(SHIFT)));
            assert_pass(cs.check_combo(&press(ALT)));
        }
        // Completing the rotate chord fires only the rotate action.
        assert_fired(rotate.check_combo(&press(R)), &Event::SwitchNext);
        assert_pass(goto.check_combo(&press(R)));
        // Tapping the goto chord's last key while the modifiers are still held
        // fires the goto action; the primed rotate combo ignores the other key.
        assert_consume(rotate.check_combo(&release(R)));
        assert_pass(goto.check_combo(&release(R)));
        assert_pass(rotate.check_combo(&press(G)));
        assert_fired(
            goto.check_combo(&press(G)),
            &Event::SwitchTo("abcd".to_string()),
        );
        // Releasing everything leaves both state machines clean.
        for code in [SHIFT, ALT] {
            assert_consume(rotate.check_combo(&release(code)));
        }
        for code in [SHIFT, ALT, G] {
            assert_consume(goto.check_combo(&release(code)));
        }
        assert_pass(rotate.check_combo(&press(SHIFT)));
        assert_pass(goto.check_combo(&press(SHIFT)));
    }
}
