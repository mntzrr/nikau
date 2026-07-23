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

## Optional polish (user-approved, unscheduled)

Recorded 2026-07-22; no plan order — take any when wanted.

1. **Cursor warp on arrival.** When the server switches input to a client,
   the client's cursor warps to the matching edge at the Y fraction the
   server crossed at, completing the spatial illusion (the return trip
   needs nothing: the server's cursor is frozen at its edge). The fraction
   already rides the wire in SwitchRequest (y_fraction); the server→client
   direction needs the fraction passed with the switch, and the client
   warps via absolute-axis injection (the touchpad ABS mechanism from
   Phase 5). Fullscreen/multi-monitor edge cases: clamp to the mapped
   output's segment.
2. **Fullscreen-game edge-switch suppression.** Edge switching should not
   fire while a fullscreen window is focused (a game slamming the pointer
   into an edge must not yank input). Hyprland IPC reports fullscreen
   state; the poller already talks to it. Interim workaround documented in
   README: pause monux before gaming (pause is opt-in since 4712899).
3. **Stale-current-client navigation skip.** Pre-existing: with
   current_client no longer in the sorted clients list, next_client steps
   idx+1 past the insertion point and can skip one entry (pinned by
   next_prev_targets_stale_current_uses_its_sort_position in
   src/rotation.rs tests). Decide whether insert-position or skip is the
   intended semantic, then align code and test.

## Considered and rejected — addendum (2026-07-22)

- `--no-encryption` flag (for events or clipboard) — QUIC has no plaintext
  mode; the traffic is the most sensitive on the machine (keystrokes,
  clipboard), and the CPU cost on LAN is unmeasurable.
- QoS/DSCP marking — single QUIC connection can't split input from bulk
  per-packet via quinn; the bulk throttle already solves the real delay
  (bufferbloat); consumer-AP WMM honoring is a coin flip.
- Two-connection events/bulk split — QUIC already provides stream
  priority + per-stream flow control; a second connection adds
  self-competition and duplicated handshake state for no measurable gain.
4. **Adaptive motion coalescing.** Today motion coalesces at a fixed 250Hz
   (office mode). Scale the rate with link health: full 250Hz when RTT is
   clean, back off (~60Hz) when the link monitor reports degradation
   (RTT/loss over the warn thresholds), restore when it recovers. Serves
   the recurring WiFi pain directly: fewer datagrams fighting a bad link
   means smoother cursor at exactly the moments the link is worst.
5. **Property-based testing of the wire protocol (proptest).** Three
   properties, in value order: (a) every decoder (events, bulk, version
   bootstrap, COBS framing) errors cleanly — never panics — on arbitrary
   byte sequences; (b) random instances of all wire message variants
   round-trip serialize/deserialize equal; (c) motion-datagram healing
   under random reorder/loss/duplication always converges to the newest
   cursor position. ~2-3 days incl. Arbitrary impls; dev-dependency only,
   runs in cargo test + CI.

## Battery optimizations (laptop client, unscheduled)

Ranked by real power cost:
1. **Idle backoff for the edge poller.** 40ms cursorpos poll → ~200ms after
   a few idle seconds with no cursor movement, ramp back on motion. Same
   responsiveness when it matters, ~5x fewer wakeups when idle. (25 polls/s
   is the main monux battery cost on both machines today.)
2. **Keepalive relaxation on battery.** 2s QUIC keepalives wake the WiFi
   radio (DTIM). 5-8s still sits far inside the 25s idle timeout; make it a
   flag or a battery profile (server+client agree on the profile).
3. **Pre-coalesce high-rate mice at capture.** An 8000Hz mouse costs the
   server ~8000 evdev events/sec of context switches, serialization and
   sends; the wire is already coalesced to 250Hz but capture is not.
   Coalesce redundant REL motion in the evdev read path before it reaches
   rotation.

## Full review findings (2026-07-23, four-pass review of the whole tree)

STATUS: All six P0 items fixed in 1f059ff (v5.4.0), hardened by 273bafb
(v5.4.1, zombie reaper + quiet close logs). The X11 clipboard backend the
P1/P2 sections reference was dropped in the same commit — monux is
Wayland-only for clipboard — so every X11-specific finding below is MOOT.
P1/P2 items not otherwise marked remain open.

### P0 — fix first (user-visible or high impact) — DONE (1f059ff)
1. **notify.rs panics in the indicator process** — DONE (1f059ff): switched to
   std::process::Command (fire-and-forget needs no tokio reactor).
2. **Hostname --edge-map targets stall the rotation loop on DNS** — DONE
   (1f059ff): resolved edge directions cached per client and refreshed only on
   client-list/map change; update_diagnostics rate-limited to ~10Hz
   (DIAGNOSTICS_REFRESH_INTERVAL).
3. **Wayland writer pre-fetches the full payload on every advertisement** —
   DONE (1f059ff): file-list types no longer pre-fetched (huge files aren't
   zipped/transferred/unpacked unless pasted).
4. **Double input when a mouse grab fails with an active client** — DONE
   (1f059ff): ungrabbed batches dropped while a client is active.
5. **Wayland writer: unbounded thread+runtime pileup on paste storms** — DONE
   (1f059ff): concurrent serves bounded to 4 (drop-newest), one shared tokio
   runtime per advertisement.
6. **Server shutdown never closes QUIC gracefully** — DONE (1f059ff): shutdown
   now Endpoint::close + bounded wait_idle (ENDPOINT_DRAIN_TIMEOUT) before
   aborting loop tasks.

### P1 — correctness worth fixing — DONE (42a062c..127c216), one sub-item open
STATUS: All 19 non-MOOT P1 items fixed across four commits (device/ops, connection/lifecycle, edge correctness, clipboard/wayland). The EdgeInfo "revoke" sub-item is partially addressed (dedup landed in d08e92b; an explicit revoke message to a client whose edge target disconnected is not yet implemented). Build zero-warning, 271/271 tests pass.
- PeerVersions keyed by addr:port: ephemeral-port churn breaks the refusal
  rate limit, the upgrade note, and grows unbounded. Key by IP. [S]
  — DONE (fc59261): keyed by IpAddr; added ephemeral_port_reconnect test.
- Silence -> drop -> reconnect loses auto-reactivation (no defunct window
  when removed while not current). Seed removed_current_client on
  silence-armed removal. [S-M]
  — DONE (fc59261): seeds DefunctClientInfo on silence-armed removal of a
  non-current client so its reconnect re-activates.
- Rotated outputs: Hyprland `transform` never read; portrait monitors get
  wrong rects/zones. Swap w/h for odd transforms. [S]
  — DONE (d08e92b): read the transform field; swap w/h for odd transforms.
- Fractional scales: abutment equality off by 1px can manufacture mid-desktop
  trigger zones. Tolerate +/-1px (or match Hyprland's own rounding). [S-M]
  — DONE (d08e92b): ±1px tolerance in the shares_boundary check.
- Edge manager runs blocking syscalls (layout query, hostname DNS) on the
  async executor (2 workers; one already blockable by cert prompts). Move to
  spawn_blocking. [S]
  — DONE (d08e92b): layout queries (startup + 30s re-query) and the hostname
  DNS resolution at fire time run on spawn_blocking.
- Duplicate device Created events leak the old reader task (double-forward +
  grab fight). Abort an existing handle for the path at insert. [trivial]
  — DONE (42a062c): aborts the displaced reader task at insert.
- live_holder trusts a stale pid file with substring cmdline matching (pid
  reuse; `monux client <host>` matches "client"). Verify with a flock probe.
  [S]
  — DONE (42a062c): flock probe rejects stale pid files; exact argv-token
  match (split_whitespace().any) replaces substring contains.
- Takeover vs pending auto-update restart: old instance re-execs and kills
  the just-started manual instance (ping-pong). Loud log / skip re-exec when
  the lock is contended. [S]
  — DONE (42a062c): a MONUX_RESTARTED instance that finds the lock held
  yields instead of killing the contender.
- Update staging cleanup deletes ALL staging dirs incl. a concurrent
  updater's. Skip dirs whose pid suffix is alive. [S]
  — DONE (42a062c): skips staging dirs whose /proc/<pid> exists.
- Wayland writer dispatcher deadlock on wedged compositor + unbounded ad
  queue. Roundtrip deadline + keep-latest queue semantics. [M]
  — DONE (127c216): drains stale advertisements to latest; 10s timeout on
  store_types so a wedged compositor can't deadlock the dispatcher.
- Wayland type_watcher dies permanently on compositor error (no reconnect —
  the former X11 watcher had backoff, but that backend is gone). Add
  reconnect. [M-L]
  — DONE (127c216): reconnect loop with exponential backoff (1s → 10s cap).
- ~~X11 backend: requestors hang probing unadvertised targets (bail before
  SelectionNotify); watcher drops revocations (empty types); non-INCR
  transfers registered for chunking; PROPERTY_CHANGE mask never removed. [S]~~
  — MOOT: X11 backend dropped in 1f059ff (monux is Wayland-only for clipboard).
- Supervisor spawn races (show vs respawn vs concurrent shows) leak an
  unreaped child -> permanent zombie indicator. Re-check the slot under the
  lock. [S]
  — DONE (42a062c): takes the lock before spawning; reaps any displaced child.
- pipe_into can block the tray thread forever on a >64K bundle written to a
  wedged wl-copy. Bound the stdin write. [S]
  — DONE (42a062c): writes on a worker thread with a timeout; kills the child
  on timeout to break the pipe.
- Armed dwell can fire after the cursor already left (leave needs 2 stable
  polls; poller stall leaves deadlines firing blind). Require fresh contact
  at fire time. [M]
  — DONE (d08e92b): re-checks zone contact at fire time using last_pos.
- EdgeInfo has no revoke; each re-advertise resets the client's in-progress
  dwell (detector respawn). Send revoke on un-resolution; dedup unchanged
  maps. [M]
  — PARTLY DONE (d08e92b): dedup cache skips re-advertising unchanged
  directions. An explicit revoke message (telling the client to stop watching
  an edge whose target disconnected) is still OPEN.
- Silence recovery not tied to the silenced client: A silences, user picks B,
  A recovers first -> input jumps to A. Store the silenced endpoint. [S]
  — DONE (fc59261): replaced the bool went_local_via_silence with an
  Option<SocketAddr> silenced_endpoint; recovery only re-activates that peer.
- No-op/debounced-chord Switch(false)+Switch(true) pair flaps clipboard
  ownership (client re-announces on fake deactivation). Dedicated
  release-keys signal or suppress announce on immediate re-activate. [S-M]
  — DONE (fc59261): deferred-reannounce pattern — Switch(false) defers the
  clipboard re-announce; an immediately-following Switch(true) suppresses it.
- Unbounded partial-frame buffers from an authenticated peer (no COBS
  terminator -> memory growth). Cap retained bytes, reset connection. [S]
  — DONE (fc59261): MAX_FRAME_BUFFER_BYTES (1MB) cap on both event and bulk
  COBS buffers, server and client.
- mDNS own-advertisement skip matches hostname only (cloned images). Also
  compare advertised IPs against local addresses. [S]
  — DONE (42a062c): compares resolved service IPs against local_ipv4_addrs.

### P2 — hygiene and nits — DONE (2024c5e, ad73fb2)
STATUS: All P2 items addressed across two commits (trivial fixes + documentation, and medium correctness fixes). Build zero-warning, 271/271 tests pass.
- Chord with a duplicated key ("shift,shift,p") never fires, silently: dedup
  or bail at parse. [trivial]
  — DONE (2024c5e): dedup keys after sort in parse_action.
- Heartbeat logs every 10s whenever the (ungrabbed) mouse moves while local:
  treat ungrabbed-only activity as idle. [trivial]
  — DONE (2024c5e): idle guard now keys off physical_grabbed instead of physical.
- Chords don't work across keyboards: document (wontfix). [trivial]
  — DONE (2024c5e): documented the per-device ComboState constraint.
- Motion-before-click ordering comment overpromises (datagram races stream);
  document, or fall back to stream when a non-motion batch is pending. [S/M]
  — DONE (2024c5e): softened the comment — datagrams race the stream, so the
  ordering is best-effort, not guaranteed.
- datagrams_ok fallback is effectively dead (peers must match exactly):
  document as defense-only. ensure_compatible_version after PeerVersions is
  unreachable. MAX_REQUEST_LINE off-by-one (exactly-8192+newline rejected).
  reader work_active stuck on caller-cancel+worker-panic. convert.rs temp-dir
  generations: 3rd unpack can delete one in use; pid-namespaced dirs never
  cleaned. Empty-entry URL parse fails whole serve (trailing newline). (The
  "x11 discovery drops shutdown-response receiver" sub-item is MOOT — X11
  backend dropped in 1f059ff.) Backoff sleep delays client
  shutdown up to 5s (select with shutdown). prompt_active can wedge true on
  lock poisoning (scope guard). install_root doesn't trim " (deleted)".
  recv_version reads exactly one chunk (loop on has_complete_cobs_frame).
  — DONE (2024c5e + ad73fb2): datagrams_ok documented as defense-only;
  unreachable ensure_compatible_version dropped; MAX_REQUEST_LINE off-by-one
  fixed (+1 for newline, also reject truncated lines); work_active fix uses
  catch_unwind in the worker so mark_dead runs even on cancel+panic;
  convert.rs keeps 5 generations + sweeps all clipboard-* dirs regardless of
  PID; empty-entry URL stripped in read_gnome_file_paths + skipped in
  build_zip_payload; backoff sleep raced against shutdown; prompt_active
  force-reset on lock poisoning; install_root trims " (deleted)";
  recv_version loops until has_complete_cobs_frame.
- Log typos + comment drift — DONE (4eb6358): "Comfirm"→"Confirm"
  (approval.rs), "adverstised"→"advertised" (bulk.rs), stray ", :" (input.rs),
  layer-shell→cursorpos-poller (main.rs), "nikau"→"monux" (watch.rs), control.rs
  "future tray indicator"→"tray indicator" + query_first doc. The "X11" comments
  in rotation.rs were made backend-neutral in 273bafb; no X11 comments remain in
  the clipboard/client path. (edge.rs's "layer-shell probes" is a legitimate
  design-rationale note, not drift.)
- pub->pub(crate) + ClipboardType collapse — DONE: edge.rs visibility tightened
  in 9a0ae4e, rotation helpers in 4eb6358, ClipboardType enum collapsed in
  2ff4bb8.

### Optimizations (ordered by value) — 9 of 10 DONE (b724a7f..4eb6358)
1. DONE (b724a7f): rustls ring-only features — drops aws-lc-rs + aws-lc-sys
   (heavy C build) and prefer-post-quantum; we only use the ring provider. [big]
2. DONE (b724a7f): quinn feature trim — dropped default platform-verifier
   (unused) -> rustls-platform-verifier + webpki-root-certs out. [S]
3. DONE (b724a7f): tokio `time` + `sync` features declared explicitly (were
   only transitive). [trivial]
4. DONE (1f059ff, with P0.2): update_diagnostics rate-limited to ~10Hz
   (DIAGNOSTICS_REFRESH_INTERVAL); ServerState/edge rebuild cached and
   invalidated on client-list/map change. [S]
5. PARTLY DONE: shared runtime + per-type serve cache landed (1f059ff + 2ff4bb8);
   the Arc<[u8]> payload-instead-of-clone sub-item is still open (writer still
   clones Vec<u8> on cache insert). [S-M]
6. DONE (06e4f86): client.rs step() — the two select blocks merged via the
   Option-pending pattern. [M — biggest duplication]
7. DONE (27358be): bulk-writer task shared between server and client
   (throttle::spawn_bulk_writer, failure handling as a closure). [S]
8. DONE (9a0ae4e): Hyprland IPC collapsed into one hyprland_query() helper;
   hyprland_socket_path hoisted out of the 40ms poll (resolved once at start).
   [trivial]
9. DONE (0da5e92): postcard scratch buffers reused on send paths (serialize +
   datagram scratch on Rotation); uinput write/route/release Vecs pre-sized.
   [S-M]
10. OPEN: routine `cargo update` for semver-compatible bumps. [routine]

### Decisions to make
- DECIDED (1f059ff): X11 clipboard backend dropped entirely (~1000 lines +
  x11rb-async dep) — monux is Wayland-only for clipboard; a disabled warning
  fires when wayland is unreachable. The user is Wayland-only.
- Expose silenced-client state in the tray (new color/tooltip) — currently a
  silenced current client looks healthy.

### Checked out clean (no action)
Chord state machine; grab-state single-source broadcast; capture->rotation->
client ordering; MotionDatagram apply/heal; discrete-axis classification;
RepeatCoalescer; exposed-segments zone math (negative coords/offsets/steps);
serve-cache epoch + negative cache + trailing-edge interplay; control socket
protocol/pipelining/oversized/PostAction/queue-full/stale-reclaim; bulk
channel cap-4 policy both peers; liveness state machine + stall guard;
approval TOFU flow; reconnect backoff; update gate semantics; atomic install;
close_loops ordering + EADDRINUSE retry; single-instance verification;
no fully-dead functions or unused dependencies found.
