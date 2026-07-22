# monux improvement plan (2026-07 review)

Full-codebase review against `a0fdfa7`, optimized for Local/LAN KVM.
Four review passes: input path, network transport, clipboard, build/ops.
Findings below are line-referenced and were verified by reading code (not by
running). Each phase is independently committable; build zero-warning +
`cargo test` green at the end of every phase.

Execution order (agreed with user): Phases 1–3 first (freeze/switch/WiFi
fixes), then Phase 8, then Phases 4–7, then Phase 9 (ON HOLD per user
request — parked until all other phases are done).
Review checkpoints (agreed with user): one full review after Phase 3
(covering Phases 1–3), then a review after every subsequent phase.

## Headlines

- **Dead-key/freeze bug — two prime suspects found**, both stalls of the
  rotation loop (all keyboards are grabbed and route through it, so a stalled
  loop = dead typing everywhere, self-healing on timeout — exactly the
  reported symptom):
  1. `rotation.rs:640-643` — clipboard cache `invalidate()` awaits the
     serve mutex inline in the loop; that mutex is held across whole serves
     (up to 5s wayland read + unbounded zip). Same on the client input loop
     (`clipboard/client.rs:132`).
  2. `client.rs:216,586-587` — client holds `bulk_send` across whole
     clipboard payload writes, and the fetch branch awaits that lock inside
     the `select!` arm, suspending input application + datagram reads.
     The safety comment at `client.rs:214-215` is wrong.
- **"Switch takes several tries" + silently eaten keys**: stale primed-combo
  state in `shortcut.rs:133-170` — combo matching ignores `event.value()`,
  re-presses of a chord key are silently consumed until ALL chord keys are
  released, and the action fires late on the final release.
- **`udevadm settle --timeout=2000` unit bug** (`main.rs:429`): the flag is
  seconds, not ms — a wedged udev queue = ~33 min silent hang on every
  auto-update restart / single-instance takeover.
- **WiFi drops**: LAN idle timeout (10s) is shorter than real 2.4GHz
  black-holes; recovery costs ~15-16s (10s detect + 5s fixed sleep) while
  the re-activation deadline closes at ~20s.

Verified sound (no action): v8 datagram motion + loss healing, TOFU approval
flow, hot-path framing/serialization, switch critical path (clipboard fetch
is not on it), dep freshness otherwise, no polling loops.

## Phase 1 — Unblock the input loops (freeze fixes)

STATUS: DONE — committed 529d8e0 (2026-07-21), hardened by checkpoint fix
9c8a0fd (cancellation-safe wayland reader slot).

1. Lock-free clipboard cache invalidation: replace the mutex-guarded
   `invalidate()` with an epoch `AtomicU64` bumped on invalidate and checked
   in `read()` (`clipboard/serve.rs:50-59`, call sites `rotation.rs:640-643`,
   `clipboard/client.rs:132`).
2. Move the invalidation above the debounce early-return
   (`rotation.rs:632-643`) — today a debounced copy leaves the cache serving
   stale content.
3. Client: stop holding `bulk_send` across the payload serve; route fetch
   replies through a dedicated writer task (mirrors the server's
   `rotation.rs:386-406`) or `try_lock`+defer (`client.rs:216,586-587`).
4. `spawn_blocking` the wayland reader's synchronous `queue.roundtrip()`s
   (`clipboard/wayland/reader.rs:54,72`) — a wedged compositor currently
   parks a tokio worker under the serve mutex and fails all fetches until
   restart.
5. Move heartbeat counters / SIGHUP dump state out of the rotation loop into
   shared state the signal thread can read (`rotation.rs:1179,1232`) — today
   the dump goes silent exactly when the loop is stalled.

## Phase 2 — Shortcut & switching reliability

STATUS: DONE — committed 5b4a47e (2026-07-21), hardened by checkpoint fix
9c8a0fd (debounced re-fire still releases the current target's keys).

1. Fix stale primed-combo state (`device/shortcut.rs:133-170`): make
   `matching` value-aware (ignore release events), stop silently consuming
   re-presses, and re-evaluate fire-on-prime vs fire-on-release now that
   `release_all` exists on both sides (`rotation.rs:1664-1670`,
   `client.rs:347-351`). This is the "switch needs several tries" bug and
   the eaten-capital-R bug.
2. Unit tests for `ComboState` press/release ordering (currently zero tests
   for the most subtle state machine in the input path).
3. Drop value-2 (autorepeat) events for codes not in `pressed_keys`
   (`device/output/uinput.rs:337-338`) — key held across a switch currently
   repeats into the new target with no press.
4. uinput write errors: log-and-continue (or recreate devices) instead of
   tearing down the whole client connection (`client.rs:339-341,389,405-406,
   448`).

## Phase 3 — Connection resilience (WiFi drops)

STATUS: DONE — committed ccf8f88 (2026-07-21), hardened by checkpoint fix
9c8a0fd (clipboard types push reordered before Switch(true); stale
RemoveClient ignored via connection tokens).

1. LAN idle timeout 10s → 25s (`network/transport.rs:25`); keepalive stays
   2s. Converts most "Lost bulk connection: timed out" drops into invisible
   stalls. (WWW profile unchanged.)
2. Reconnect backoff: first retry immediate, then capped backoff (0,1,2,5s…)
   instead of the fixed 5s sleep (`main.rs:725`).
3. `REMOVED_CLIENT_RECOVERY_DEADLINE` 10s → 45s (`rotation.rs:21`) so a
   reconnecting client reliably re-activates its session.
4. Reset `consecutive_failures` after a healthy session (e.g. connection
   survived > 60s) — today every 3rd lifetime disconnect triggers a 10s
   mDNS re-discovery (`main.rs:696-722`).
5. Register SIGUSR1/SIGUSR2/SIGHUP handlers on the client too
   (`main.rs:523`) — they currently kill the process with no key-release
   cleanup.
6. Motion loss-healing history 8 → 32 frames (`rotation.rs:140`) — ~300B,
   heals ~128ms bursts instead of ~32ms.
7. Re-announce/restore clipboard ownership after reconnect when the dropped
   client owned the clipboard (rotation.rs:493-496,1618-1625,
   `client.rs:231-233,347`).
8. Fix duplicate-endpoint insert on reconnect (`rotation.rs:382-385`).

→ **REVIEW CHECKPOINT: DONE (2026-07-21).** Verification review of
a0fdfa7..ccf8f88 found 5 issues (2 user-visible); all fixed and committed
as 9c8a0fd. Build zero-warning, 47 tests green, e2e reconnect/resume green.

## Phase 8 — Always-on & visibility (user-requested features)

STATUS: DONE (2026-07-21/22) — 8A scaling + pause hotkey (9dcaaa5, v1.2.0),
8B notifications + link monitor + --autostart (16d7044, v1.3.0), 8C1 control
socket + `monux system status` (81868d7, v1.4.0), 8C2 tray indicator +
diagnostics command (0c9c0eb, v1.5.0). Review checkpoint passed (all claims
verified); nits fixed in 7eeb38f (v1.5.1). Live tray-host verification is on
the user's waybar.

1. Autostart: `monux system setup` generates/enables a systemd **user** service for
   server/client (must be a user service — the Wayland clipboard needs the
   session).
2. Desktop notifications on client drop/reconnect, plus a degraded-link
   warning when datagram loss/RTT crosses a threshold (gives real data on
   the WiFi issue).
3. Active-machine indicator **and control panel**: tray icon
   (StatusNotifierItem via the `ksni` crate — works in waybar, no GTK)
   showing which machine owns input; doubles as the degraded-connection UI
   (red in the dead-but-not-yet-timed-out window — covers the deferred
   silent-input-loss UX below). Runs as `monux system indicator`, a thin
   client of a local control IPC (a unix socket the daemon exposes at
   `$XDG_RUNTIME_DIR/monux/<role>.sock`) — the same socket also serves the
   notifications (item 2), pause (item 4), and a future `monux system
   status` CLI. Menu content:
   - State: current target; per-client live connection health (RTT,
     packet/datagram loss, connected-since); clipboard owner (machine,
     type, size); update-available badge when the auto-updater sees a
     newer commit.
   - Actions: switch to machine, pause/resume, update now (wakes the
     auto-updater), restart, exit, "copy diagnostics" (version, protocol,
     SIGHUP-style state dump, recent log lines — onto the clipboard for
     bug reports).
4. Pause/resume hotkey: suspend forwarding + release grabs, keep
   connections alive (games, apps that want raw input).
5. Client-side `--mouse-scale` / `--scroll-scale` flags for
   DPI/sensitivity differences between machines (motion deltas are
   forwarded raw today).

→ Review checkpoint after this phase (and every phase from here on).

## Phase 4 — Clipboard correctness tail

STATUS: DONE — committed 948c210 (v1.5.3); review checkpoint passed (all
claims verified); follow-ups in 14434bb (v1.5.4).

1. Trailing-edge, per-source debounce (`rotation.rs:632-637`) — leading-edge
   global debounce drops real copies (<300ms double Ctrl+C, deactivate
   announcements) and the dropped state is never re-sent.
2. Empty-string split bug: `"".split(' ')` yields `[""]` — a clipboard_clear
   makes the client advertise a phantom `""` mime type
   (`client.rs:395-396`, same unguarded split at `server.rs:348`).
3. Announce client-side revocation (empty types) instead of swallowing it
   (`client.rs:354`); extend the empty-means-clear rule to remote sources
   (`rotation.rs:622`).
4. Drain server-originated pending fetches on `clipboard_clear`
   (`rotation.rs:1712-1737`) — they currently wait out the 5s timeout.
5. Overall serve timeout on the server side (mirror the client's 4s at
   `client.rs:553`) covering convert/zip (`rotation.rs:852-894`).
6. Close the pipe read end on wayland read timeout so the blocked
   `spawn_blocking` worker wakes with EOF instead of leaking thread+fd
   (`clipboard/wayland/reader.rs:96-111`).
7. Bounded per-client bulk channel (e.g. 4) with drop-on-full
   (`rotation.rs:390`) — explicit memory bound on a weeks-long daemon.

→ Review checkpoint after this phase.

## Phase 5 — Touchpad discrete axes (protocol v9)

STATUS: DONE — committed 7cba960 (v2.0.0, protocol 9); review checkpoint
passed (all claims verified); min==max normalization guard in 186e092
(v2.0.1). UPDATE ORDER: server first, then clients (gate opens on reconnect).

Discrete axes (ABS_MT_SLOT, TRACKING_ID, BLOB_ID, TOOL_TYPE, ABS_MISC) are
normalized to 0..1 (`device/util.rs:83`, `device/input.rs:359-366`) then
expanded to 0..65535 on injection (`uinput.rs:285`), while the virtual
touchpad advertises slot 0..32 / tracking-id −1..1048576
(`uinput.rs:615-642`). Result: multitouch/gestures broken, tracking-id −1
(liftoff) maps to 0 = stuck touches. Fix: forward discrete axes as integers
(hardcoded kernel-constant list on both sides, or a range advertisement —
decide at implementation; bump PROTOCOL_VERSION if the wire format changes).
Single-touch pointer/buttons are unaffected today.

→ Review checkpoint after this phase.

## Phase 6 — Build & ops

STATUS: DONE — committed 903e651/224c061 (v2.0.2, incl. mdns-sd 0.20);
review checkpoint passed (all claims verified).

1. `udevadm settle --timeout=2000` → `--timeout=2` (`main.rs:429`) —
   seconds, not ms. One line, kills a 33-min hang risk on every auto-update
   restart.
2. `[profile.release]`: `lto = "thin"`, `codegen-units = 1`. Keep
   panic=unwind (daemon crash semantics already safe; abort changes
   silent-task-death behavior) and keep symbols (field debugging).
3. `.worker_threads(2)` (`main.rs:225-230`) — one-thread-per-CPU is waste
   for this daemon; keep ≥2 because of blocking prompts/roundtrips.
4. Auto-update: `spawn_blocking` + low-speed timeout for `git ls-remote`
   (`autoupdate.rs:118`, `update.rs:26-29`) — a dead route currently parks
   a tokio worker for minutes.
5. `cargo install --locked` in `install.sh` and the self-update build
   (`update.rs:137-145`) — updates build exactly what was tested.
6. `build.rs` rerun gating: `-dirty` suffix goes stale
   (`build.rs:16-20`); add a cheap rerun trigger.
7. Drop unmaintained `atty` for `std::io::IsTerminal`
   (`network/approval.rs:377`).
8. Clone events buffer only on the deserialize-error path
   (`server.rs:326`).
9. Optional: `mdns-sd` 0.11 → 0.20 (TTL=255, down-iface exclusion,
   multi-homed source-IP selection; API changes contained to
   `discovery.rs`).

→ Review checkpoint after this phase.

## Phase 7 — Test coverage

STATUS: DONE — committed 83623f6 (v2.0.3); review checkpoint passed
(extraction verified verbatim, goldens proven serializer-accurate).

1. Postcard/COBS round-trip suite for every wire message
   (`ServerEvent`/`ClientEvent`/`ServerBulk`/`ClientBulk`/
   `VersionBootstrapMessage`) + a guard that wire-format edits bump
   `PROTOCOL_VERSION` (what the update gate keys on).
2. `clipboard/convert.rs` round-trips: zstd, gnome/uri file paths, zip
   build/unpack incl. path sanitization.
3. Rotation navigation tests (`next_client`/`prev_client`/`set_client`
   prefix matching, `rotation.rs:517-604`).

→ Review checkpoint after this phase.

## Phase 9 — Screen-edge switching (user-requested feature)

STATUS: Phase 9A (server-side) implemented 2026-07-22 (v3.1.0) — `--edge-map
<direction>=<target>` switches input when the cursor dwells on an exposed
screen edge. Hyprland-only (IPC monitor layout), cursorpos polling every
40 ms for edge detection (layer-shell enter/leave at screen edges proved
undeliverable on Hyprland), 2-poll debounce, multi-monitor exposed-edge
computation with corner dead zones (~8%/end),
250 ms dwell (`--edge-dwell-ms`) plus 1s re-arm cooldown, targets resolve to
a fingerprint at switch time from the live client list (prefix / `auto` /
hostname→IP). No protocol change: the switch reuses `set_client`.
Phase 9B (client-initiated return) implemented 2026-07-22 (v4.0.0, protocol
v11) — the client runs the same edge detection on its own machine
(`monux client --edge-map left=auto`, `auto` only); a completed dwell sends
the new `ClientEvent::SwitchRequest { y_fraction }` (the crossing fraction
along the edge, reserved for future cursor warping), which the server honors
only from the current client, switching back to local through the normal
rotation path. Later phases may add other compositors.
Phase 9C (server-driven edge inference / EdgeInfo) implemented 2026-07-22
(v5.0.0, protocol v12) — the server tells each mapped client which server
edge it sits beyond (new appended `ServerEvent::EdgeInfo { direction }`,
sent before the first `Switch(true)`; `Direction` moved to `msgs::event` as
the shared wire enum), and the client infers the OPPOSITE edge for the
return trip, so no `--edge-map` is needed on the client. An explicit client
`--edge-map` still wins over the inference; the inferred map is rebuilt per
connection.

Move cursor off a screen edge to switch machines. Hyprland-only initially
(compositor IPC for cursor position/monitor layout); needs edge resistance,
corner handling, and a multi-monitor layout config. Largest single item in
this plan. (Lock synchronization was considered here and dropped per user
request.)

→ Review checkpoint after this phase.

## Considered and rejected (recorded at user request)

- Audio forwarding — PipeWire already does network audio natively; use
  that instead.
- Lock synchronization via logind — dropped per user request.
- Pairing wizard / short codes — TOFU approval suffices for two machines.
- Dedicated file-send command — clipboard file copy already covers it.

## Deferred / needs a decision (not scheduled)

Reviewed and pruned 2026-07-22: one item kept, the rest dropped (rationale
below each). 2026-07-22 update: the kept item is now DONE (below).

- DONE — Silent input loss during the dead-but-undetected window: the
  liveness ping landed as protocol v10 (2f508d6, v3.0.0; hardened in
  1349fac, v3.0.1). The server ungrabs and goes local ~6s after the current
  client goes silent, without tearing the session down.
- DROPPED — Repeat-burst coalescing: implemented in 880507a (v2.0.5).
- DROPPED — Cert approval blocking a tokio worker: mitigated by
  worker_threads(2) (903e651); the daemon no longer freezes.
- DROPPED — First-pairing timeout mismatch: rare and self-healing.
- DROPPED — Clipboard payload copy reduction / skip file-type pre-fetch:
  invisible on LAN.
- DROPPED — LED/lock-state sync (caps/numlock): cosmetic wontfix.
- DROPPED — Combos spanning multiple devices: wontfix, documented here.

## Future foundations (designed, awaiting a real requirement)

**Peer capabilities/status channel (protocol v11).** One EXTENSIBLE
handshake — version + capabilities bitmap + lifecycle status (updating,
paused, exiting) + key-value room for future fields — exchanged on every
connection, so later features ride it without more protocol bumps. Do NOT
add single-purpose messages as needs arise. Recorded at user request
(2026-07-22); the PeerVersions tracker (d17ddc1) is its seed. Use cases it
unlocks, in rough priority order:
- Update-aware UX: suppress "client lost" notifications for update-driven
  drops, a "client updating" tray state, deliberate clipboard survival.
- Rollout coordination: the server knows every client's version ("all
  machines on vX"), staggered updates, server-nudged updates.
- Feature negotiation: degrade to the common capability subset instead of
  hard exact-match refusal (a bigger protocol philosophy shift — this is
  the only door to it).
- First-class lifecycle states: updating / paused / shutting-down-for-real
  as explicit signals rather than inferred from timeouts.
Build it when one of these decisions actually needs it, so a real
requirement pressure-tests the extension points.
