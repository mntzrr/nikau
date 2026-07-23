# monux

```
\\ //
 \V/
  U
  |
  | monux
```

TLS-encrypted server-client KVM software for sharing input devices and clipboards across Linux machines.

Monux relies on the Linux uinput API, and supports keyboards, mice, and touchpads across Wayland, X11, and even bare Linux consoles. Clipboards can be seamlessly copied between machines. OSX and Windows are not currently supported.

This fork adds low-latency tuning for local networks and a `--www` mode for use over the public internet.

## Installation

### Prerequisites

- Linux with `uinput` and `evdev` kernel modules enabled (`/dev/uinput` and `/dev/input/` should exist).
- A Rust toolchain (`rustup` recommended).
- Access to input devices: your user in the `input` group with `/dev/uinput` group-writable. `monux system setup` persists both for you (it re-executes with sudo and prompts for your password; log out and back in after the group change). Running the server as root with `sudo -E monux server` also works as a fallback, `-E` preserving your session so clipboard sharing works.

### From this repository

```bash
git clone https://github.com/mntzrr/monux.git
cd monux
./install.sh
```

Or install directly with cargo:

```bash
cargo install --path . --root ~/.local
```

The repository includes `.cargo/config.toml` with `target-cpu=native`, so the binary is automatically optimized for the machine you build it on.

To uninstall later: `monux system uninstall` stops any running server/client, removes the binary (and stale copies), the `/usr/local/bin` link, and the system settings persisted by `monux system setup` (udev rules, uinput module load, WiFi powersave and UDP buffer configs). It asks before removing `~/.config/monux` (identity keypair and peer approvals) — non-interactively the config is kept — and prints a hint for undoing the `input` group membership (`sudo gpasswd -d $USER input`), which is deliberately left alone since it may predate monux. If the binary is already gone, `./uninstall.sh` from the repo is a fallback wrapper that prints the remaining manual steps.

After installation, the binary is available as `monux` in `~/.local/bin`, which is in `PATH` by default on systemd-based distros and in most shell profiles (unlike `~/.cargo/bin`). If your shell doesn't find it, add `export PATH="$HOME/.local/bin:$PATH"` to your shell's rc file.

### Autostart on login (optional)

`monux system setup` can also install a per-user systemd service that starts monux with your graphical session:

```bash
monux system setup --autostart server   # or: --autostart client
```

This writes `~/.config/systemd/user/monux-<role>.service` and enables+starts it via `systemctl --user` (`Restart=on-failure`, 3s delay). The client service runs plain `monux client` with no address argument, so it finds the server via mDNS auto-discovery — nothing machine-specific is baked into the unit. `--autostart off` disables both services and removes the unit files; omitting the flag leaves autostart untouched. The tray indicator comes for free: the daemon auto-spawns it (see the tray-indicator section below), subject to the same `DBUS_SESSION_BUS_ADDRESS` caveat as clipboard sharing.

Check status and logs with:

```bash
systemctl --user status monux-server
journalctl --user -u monux-server
```

**Clipboard sharing caveat:** the service inherits the systemd user manager's environment, not your compositor's session. Clipboard sharing needs `WAYLAND_DISPLAY`, `XDG_RUNTIME_DIR`, and `DBUS_SESSION_BUS_ADDRESS` imported into the user manager. Hyprland handles this when launched via UWSM, or with its systemd integration (`exec-once = dbus-update-activation-environment --systemd WAYLAND_DISPLAY XDG_CURRENT_DESKTOP`). Without it the service still works for input, but clipboard sharing stays disabled — exactly like running monux with `WAYLAND_DISPLAY` unset.

### If you want a portable binary

Remove or edit `.cargo/config.toml` and change `target-cpu=native` to `target-cpu=x86-64`, then rebuild. This produces a binary that runs on any x86-64 CPU.

## Updating

```bash
monux system update
```

This pulls the latest source from GitHub into `~/.cache/monux/src`, rebuilds it on this machine (with `target-cpu=native`), and installs over the existing binary. Run it on each machine (server and clients), then restart any running `monux server` / `monux client` to pick up the new version. `monux --version` prints the commit the binary was built from, so you can check that all machines match.

Updating never disrupts a running session: the processes keep their in-memory binary while the file on disk is replaced, so you can update mid-session.

To pick up the new version, restart the processes — the session then heals itself:

- **Server:** start `monux server` again however you normally run it (the new instance asks the old one to shut down and takes over). Clients reconnect within a few seconds, and the machine that was active is re-activated automatically — no client-side steps needed.
- **Client:** run `monux system update` on the client machine and restart the client there (e.g. over SSH). It reconnects and resumes by itself. With auto-update (below, on by default) it does both by itself — no remote access needed.

Active-session resumption survives server restarts for up to an hour (see `active_client` in `~/.config/monux`).

**Protocol-compatibility gate:** a client never installs a build whose protocol version differs from its server's — such a build would be unable to reconnect. The client records the server's protocol version at every connection, including handshakes the server refused, and `monux system update`` (manual or automatic) checks the new source against it before building. Servers also advertise their protocol version via mDNS, so a manual `monux system update`` first refreshes the record from the LAN (gating on the lowest version when several servers answer) and only falls back to the last recorded version when no server answers. If they differ, the update is skipped with a log message telling you to update the server first; once the server is updated, the client learns the new version via mDNS or on its next (refused) connection attempt and the gate opens by itself. `monux system update` --force` bypasses the gate. Touchpad multitouch (gestures) requires protocol v9 or newer on both ends — earlier versions only forward single-touch pointer and button events — and mixed versions refuse to connect at the handshake anyway.

### Automatic updates

`monux server` and `monux client` automatically check the GitHub repo once shortly after startup and then daily (opt out with `--no-auto-update`). When a newer commit appears, it is rebuilt and installed in the background at low CPU priority; a few seconds later (after a desktop notification) the process restarts itself into the new binary. The restart drops the session for a few seconds, which then heals itself: clients reconnect automatically and whichever machine was active is re-activated (see above). This is handy for machines you can't easily reach — e.g. keeping a client up to date without SSH access. Clients are additionally protected by the protocol-compatibility gate above: a client only auto-updates to builds its server can talk to, so a version split can't happen. And if the client ever connects to a server running a newer protocol version (the server upgraded ahead of it), that refused handshake immediately wakes the client's auto-updater instead of waiting for the daily tick — the pair converges on its own. Auto-update trusts the configured GitHub repo and this machine's git setup implicitly; pass `--no-auto-update` if you prefer to review changes first.

## Usage

Run the server on the machine with the physical input devices:

```bash
monux server
```

Run the client on each machine you want to control:

```bash
monux client <server-ip-or-hostname>
```

On a local network you can omit the host and let the client discover the server via mDNS:

```bash
monux client
```

The first time a client connects, verify the fingerprint shown on both sides matches, then approve it. Approved certificates are stored in `~/.config/monux/known_certs/`.

### Server: sudo vs non-sudo

The server runs as your normal user (in the `input` group, with `/dev/uinput` accessible — see `monux system setup`). This is the recommended setup.

`sudo -E monux server` remains available as a fallback (e.g. if device permissions aren't set up); `-E` preserves your session environment so clipboard sharing keeps working. Note that running as root did **not** prove to prevent intermittent input freezes: with aggressive clipboard managers (`wl-clip-persist`, `wl-paste --watch`) a stall is still possible on some compositors. If you hit freezes, see *Troubleshooting* — `WAYLAND_DISPLAY= monux server` (clipboard sharing disabled) is the isolation test.

Switch between the server and connected clients using `LeftShift+LeftAlt+R` (next) and `LeftAlt+P` (previous), or send `SIGUSR1` / `SIGUSR2` to the server process. Shortcuts are configurable via `--shortcut` / `--shortcut-prev`. The switch fires the moment the full combo is pressed; keep holding the modifier keys and tap the last key again to cycle through further clients.

Pause input handling entirely with a pause chord (opt-in via `--pause-shortcut <keys>`, e.g. `leftshift,leftalt,p`; disabled by default). While paused, monux ungrabs **all** input devices — keyboards included — so the local machine gets raw evdev input with monux's re-emit completely out of the way (useful for games and raw-input apps). monux keeps listening ungrabbed, so the pause chord still works: press it again to resume, which re-grabs per the current rotation state (keyboards always, mice only while a client is active). While paused nothing is forwarded to clients and switch chords are not acted on — since devices are ungrabbed, those keystrokes also pass through to the local system. Clipboard sharing continues untouched while paused.

Every switch also shows a desktop notification (via `notify-send`), so an unexpected switch is visible immediately. The same goes for connection lifecycle events: the server notifies when a client joins or is dropped, the client notifies when the connection is lost and when it (re)connects, and a client on a degraded link (RTT over 50ms or packet loss over 2% — a WiFi/link problem, not monux) warns at most once per 5 minutes, plus once when the link recovers.

> **Pick a shortcut that doesn't collide with your compositor/WM/application binds.** monux consumes only the *last* key of the combo, so if the same combo is bound elsewhere (e.g. `Alt+Shift+R` toggling your clipboard manager), pressing it fires *both* actions — and a switch you didn't mean to make looks exactly like dead keys: your input silently goes to the other machine. The notification exists to make such accidents obvious.

### Screen-edge switching (Hyprland)

As an alternative to shortcuts, the server can switch input when you push the cursor against a screen edge and hold it there briefly — the classic "screen-edge KVM" behavior. It's opt-in: map an edge to a client with `--edge-map` (repeatable, and values may be comma-separated):

```bash
monux server --edge-map right=auto
monux server --edge-map right=aa11bb --edge-map left=laptop
monux server '--edge-map right=auto,left=laptop'
```

The target is a client fingerprint prefix (see the `Added client ...` log line), a hostname (resolved via the system resolver, including `<name>.local` mDNS records, and matched to a connected client by IP), or `auto` for "exactly one connected client" (an error while zero or several clients are connected). Targets are re-resolved against the live client list on every connect and at switch time, so reconnects and IP changes are tolerated; the server logs the resolution at startup and on every client (dis)connect. Switching fires through the same path as the goto shortcuts, so pause mode and no-op handling behave identically.

Detection polls the cursor position from Hyprland's IPC every 40 ms and checks it against the mapped edges (Hyprland delivers no usable pointer enter/leave at screen edges, so an event-driven design is not viable there). With multiple monitors, only the *exposed* parts of an edge count: where two outputs abut, the cursor crosses over instead of switching (two side-by-side monitors expose the right edge only on the rightmost one; differing heights and vertical offsets produce the expected step segments). Each end of an exposed segment has a corner dead zone (~8%), so flinging the cursor into a screen corner never triggers a switch. The switch fires after the cursor dwells on the edge for 250 ms (tune with `--edge-dwell-ms`), and a short re-arm cooldown prevents accidental repeat switches while parked on the edge.

Caveats: this requires a Hyprland session on the server (the layout comes from Hyprland's IPC, re-queried when it changes) — on other compositors the feature disables itself with a warning. Fullscreen games (and anything else that pins or rapidly slams the pointer into an edge) can trigger a switch mid-game; pause monux with the `--pause-shortcut` chord before gaming, or raise `--edge-dwell-ms`.

**Switching back by edge:** the client can run the same detection on its own machine, so pushing the cursor against the opposite edge returns input to the server. Usually there is nothing to configure on the client: the server tells each mapped client which server edge it sits beyond (a `Telling client <fp> it is our <dir>-hand neighbor` log line), and the client infers the return trip from that — sitting beyond the server's right edge means watching its own *left* edge (`Server says we're its right-hand client: watching the left edge (inferred)`). The inference is re-applied on every (re)connect. An explicit `--edge-map` on the client always wins over the inference — configure it only to override what the server advertises (the client's only valid target is `auto`, meaning "the server" — a client has exactly one peer):

```bash
monux server --edge-map right=auto    # push right: input goes to the client
                                      # (the client infers: push left to come back)
monux client --edge-map left=auto     # explicit override of the inferred edge
```

While the client has input, dwelling on a mapped edge of the client machine sends a switch request to the server, which honors it only from the client that currently owns input (stale or foreign requests are ignored). The request carries the fraction along the edge where the cursor crossed (0.0–1.0); the server ignores it for now — it's reserved for future cursor warping — and the server's cursor is already parked at the edge the switch-out left from, so the pointer doesn't jump on the round trip. Detection on the client is quiet while disconnected and, like on the server, needs a Hyprland session (otherwise it disables itself with a warning); `--edge-dwell-ms` applies there too.

### Client silence: the liveness check

While a client owns the input, the server pings it every 2 seconds and the client answers immediately (any data received from the client counts, not just pongs). If nothing arrives for ~6 seconds (~12 with `--www`, matching its relaxed QUIC timers) — the classic symptom of a WiFi link that black-holed — the server switches back to the local machine and ungrabs, so keystrokes stop flowing into the void: `No sign of life from current client <addr> ... switching to the local machine and ungrabbing`. The client is **not** disconnected or removed from the rotation; the 25s QUIC idle timeout still owns that, and pinging continues meanwhile. When the client answers again, the server requires 3 consecutive heard-events (each received chunk counts once, so pongs buffered during a freeze can complete this in a single burst on thaw) **and** at least 5 seconds spent in the silenced state — whichever finishes later (hysteresis against a flapping link) — then re-activates it automatically: `Client <addr> is answering again ... re-activating it`. Switching by hand in the meantime — to another client, or deliberately to the local machine — always wins: the client is then just marked healthy again, without yanking input. Manually switching to a silenced client is allowed; the same silence check applies and ungrabs again if the silence continues.

### Local network vs. internet

By default Monux is tuned for low-latency local networks (LAN, wired links, direct WiFi). Use `--www` on both server and client when connecting over the public internet:

```bash
monux server --www
monux client --www <server-host-or-ip>
```

`--www` uses conservative QUIC settings (default congestion control and RTT estimation) and skips socket QoS flags.

### Pointer motion rate (office vs gaming)

By default the server coalesces pointer motion to **250 updates per second**: high-polling-rate mice (1000-8000 Hz) otherwise produce thousands of tiny packets per second for no visible benefit at a desk. Motion deltas are summed losslessly — the cursor ends up in exactly the same place, just updated less often, with far less network traffic and CPU use on both machines. All motion travels as unreliable QUIC datagrams: they are never retransmitted, so a WiFi blip can't stall later input or replay a stale backlog (the "cursor crawls for a second" effect); each coalesced datagram repeats the last few deltas so the client heals lost frames and the cursor position stays exact. At full rate (`--motion-hz 0`, gaming) no history is repeated — skipping a superseded frame beats healing it. Tune with `--motion-hz`, e.g. `--motion-hz 60` for maximum savings, `--motion-hz 500` for extra smoothness.

### Pointer and scroll sensitivity (client)

When the server's mouse and the client's machine disagree on DPI/sensitivity, scale the deltas on the client: `--mouse-scale 0.5` halves pointer motion, `--scroll-scale 2` doubles scroll steps (including hi-res wheels). Both default to `1.0` and accept values from 0.05 to 20. Fractional remainders are carried between events per axis, so small scales lose no motion over time — 0.5x emits exactly one tick per two input ticks. The scaling applies only where the client injects into its own virtual devices; the server machine's local input always stays 1:1.

### Control socket and `monux system status`

Both daemons publish their live state and accept a small command set over a per-user unix socket: `$XDG_RUNTIME_DIR/monux/server.sock` and `$XDG_RUNTIME_DIR/monux/client.sock` (under `/tmp/monux-<uid>/` when XDG_RUNTIME_DIR is unset). The socket is same-user only — the directory is 0700, there is no further authentication — and the file is removed again on shutdown.

The quickest way to use it is the built-in CLI, which pretty-prints the daemon's state (rotation target, connected clients with RTT, clipboard owner, update availability) or the raw JSON with `--json`:

```bash
monux system status            # server socket first, then the client's
monux system status --client   # restrict to one role
monux system status --json     # machine-readable response
```

The wire protocol is newline-delimited JSON, one request and one response per line, so any language can drive it (this is the backend of the tray indicator below). Requests: `{"cmd":"status"}`, `{"cmd":"diagnostics"}` (a troubleshooting bundle: state dump plus the daemon's recent log lines), `{"cmd":"switch","target":"next"|"prev"|"local"|<fingerprint-prefix>}`, `{"cmd":"pause"}` / `{"cmd":"resume"}` (idempotent: pausing a paused server is a no-op), `{"cmd":"update_now"}` (wakes the background update check), `{"cmd":"indicator","action":"hide"|"show"}` (hides the auto-spawned tray indicator without stopping the daemon, or restores it), `{"cmd":"restart"}` (graceful shutdown + re-exec, like after an update), `{"cmd":"exit"}`. Responses: `{"ok":true,"state":{...}}` for status, `{"ok":true,"diagnostics":{...}}` for diagnostics, `{"ok":true}` for accepted commands, `{"ok":false,"error":"..."}` on failure. The server socket serves the full set; the client socket only status/diagnostics/update_now/indicator/restart/exit — rotation and pause are server concepts. Example with socat:

```bash
echo '{"cmd":"pause"}' | socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/monux/server.sock
```

### Managing the daemon (`monux daemon`)

The same socket backs the `monux daemon` management verbs — drive the running daemon without touching its terminal or signals:

```bash
monux daemon status              # live state (same as 'monux system status')
monux daemon switch next         # or prev / local / a client fingerprint prefix
monux daemon pause               # ungrab everything (raw local input)
monux daemon resume
monux daemon restart             # graceful restart into the installed binary
monux daemon exit                # graceful stop
monux daemon update              # wake the background update check now
```

Commands try the server socket first, then the client's (`--socket <path>` overrides where offered); server-only actions (switch/pause/resume) return the daemon's error when pointed at a client. Acknowledgement is immediate — `switch` is queued to the rotation, `exit`/`restart` ack before the daemon begins shutting down.

### Tray indicator (`monux system indicator`)

`monux system indicator` puts a StatusNotifierItem (SNI) tray icon in your panel — any SNI host works: waybar (with a `tray` module), KDE Plasma, xfce4-panel, and so on. It is a thin client of the control socket: it polls `{"cmd":"status"}` every 2 seconds (server socket first, then the client's) and never talks to the daemon's event loops, so it can neither stall nor be stalled by monux.

The icon is a colored dot whose tooltip carries the details ("monux: input on 192.168.1.102", per-client RTT and uptime, clipboard owner):

- **green** — input is local (client role: connected, not owning input)
- **blue** — input is on a client (client role: this machine owns the input)
- **grey** — the server is paused
- **red** — the link is degraded: any client with RTT over 50 ms (server role), or not connected to the server (client role)
- hollow grey **?** — no monux daemon is running

The menu follows the current state: switch to local / to a specific client and pause/resume (server only), per-client connection facts and clipboard owner, "Check for update now" (or "Update available: `<sha>` — update now" when the auto-updater has seen a newer commit), "Copy diagnostics" (puts a bug-report bundle — version, state dump, recent logs — on the clipboard via `wl-copy`/`xclip`/`xsel`), and "Restart monux" / "Exit monux".

The indicator starts automatically with the daemon: whenever `monux server` or `monux client` runs with a desktop session bus available, it spawns `monux system indicator` as a child process and stops it again on shutdown (opt out with `--no-indicator` or `MONUX_NO_INDICATOR=1`). If the indicator dies on its own (e.g. its tray host restarted), the daemon respawns it — a bounded few times, after which it logs how to start it manually. Only one indicator runs at a time: a manually started `monux system indicator` takes over from the auto-spawned one (and vice versa), never a duplicate icon.

You can hide the icon without stopping the daemon — the menu's **Hide tray icon**, or `monux system tray hide` — and bring it back with `monux system tray show` (or a manually started `monux system indicator`); the daemon suppresses (re)spawns only until then, and a daemon restart always starts the indicator fresh. `show` refuses to override a daemon started with `--no-indicator`.

Headless sessions are detected and skipped silently by the daemon; a manually started indicator there exits with a "no D-Bus session / no tray host" error. The systemd units installed by `monux system setup --autostart` get the indicator for free, since the daemon spawns it — with the same caveat as clipboard sharing: the service needs `DBUS_SESSION_BUS_ADDRESS` in the systemd user manager's environment (see the autostart caveat above), otherwise the auto-spawn is skipped.

## Troubleshooting

If input (e.g. the Enter key) stops registering on the server machine while `monux server` runs, the server log tells you what monux sees. The first log line records the exact build (`monux v1.0.0+<sha> starting`) — always include it when reporting.

**While the freeze is happening** (switch to a TTY or SSH in):

1. `pgrep -f 'monux server' | xargs -r sudo kill -HUP` — dumps the server's full internal state (switch state, grab state, clients, clipboard owner, counters) to its log. SIGHUP is safe; it only logs.
2. Check the 10-second heartbeat lines in the log: `Input status: local (Ungrab): N events in, M emitted locally`. They show whether monux sees your keystrokes at all, and where they went.
3. `hyprctl devices | grep -i 'monux virtual'` — are the virtual keyboard/mouse still there? (The startup log lists their `/dev/input/eventN` nodes.)
4. `sudo libinput debug-events` — does the kernel see the physical key presses?
5. For a recurring dead key, restart the server with tracing: `MONUX_TRACE_KEYS=28 monux server` (28 = Enter; comma-separate more codes). Every pipeline stage then logs `KEYTRACE` lines: `capture` (the physical device delivered it, and whether a combo consumed it), `route` (forward to client / emit local / passthrough drop), `uinput` (emitted to the virtual device, or a repeat dropped). Where the trail stops is where the bug lives.

**Reading the evidence:**

- `INPUT SWALLOWED: ...` — monux sees your keys but they have nowhere to go (grab state vs switch state mismatch). Report the log.
- `KEYTRACE capture` appears but no `KEYTRACE route`/`uinput` follows → the rotation loop stalled before routing (pair with the SIGHUP dump). `route: emit local` + `uinput: emit` appear but apps see nothing → the virtual device/compositor side (`hyprctl` checks above). `capture: consumed=true` for a key that isn't in your shortcut → report your `--shortcut`/`--shortcut-goto` config.
- `Synthetic (resync-injected) key event: ...` — the evdev buffer overflowed (SYN_DROPPED, typically an 8K device during a busy startup) and the crate injected a state-diff event. A synthetic key press whose release never arrives is a stuck/phantom key — if phantom input correlates with these lines, report it.
- Phantom keypresses (e.g. a flood of newlines) right after starting the server, stopping at your next real keypress, are the compositor's key-repeat: monux grabbed the keyboard between a press and its release, so the compositor kept repeating the key it never saw released. Since v1.0.5 monux waits for all keys to be released before grabbing, so this can't happen; a `Grabbing ... with keys still held` warning means the 3s fallback fired (a key stuck held in the kernel — press and release it once).
- **Repeated characters on the client** — the client log distinguishes the mechanisms: `Duplicate press for key N` means the same press was delivered twice (event duplication), `Input burst: N key events delivered after a gap` means what you typed during a stall arrived at once when it cleared, and `Key N was held Ns before its release arrived` (debug level) marks delayed releases. Since v2.0.5, auto-repeats arriving faster than the physical repeat rate (the stall-backlog signature) are coalesced before injection, so a WiFi blip no longer flushes as a burst of repeats; real-time key holds repeat normally. Repeats that coincide with freeze warnings share the same root cause.
- Keys visible in `libinput debug-events` and the heartbeat's *emitted* counter rises, but apps see nothing, and `hyprctl devices` lacks the virtual keyboard → the compositor dropped the virtual device. `hyprctl reload` recovers it; report it.
- `Clipboard paste storm` or `Serving paste request ... took Ns` warnings coinciding with freezes → a clipboard manager (`wl-clip-persist`, `wl-paste --watch`) is hammering monux's clipboard serving. Tame or remove it.
- `Our own virtual device node ... vanished` → the virtual devices were destroyed mid-session; restart monux.
- Freeze windows that self-heal after seconds-to-a-minute point at a blocking wait that timed out — check whether they line up with clipboard warnings above.
- On connection loss, both sides log `Connection stats on drop: rtt=... lost_packets=N/M congestion_events=... black_holes=...`. High loss/congestion/black-holes means a lossy link (WiFi interference, weak signal); near-zero loss with a normal RTT means the *peer* went silent (CPU stall on that machine, or WiFi buffering/power saving there despite setup — recheck `iw dev` on the client).

### RTT spikes and degraded links (WiFi)

Latency-sensitive input shares the link with bulk clipboard traffic, and QUIC's stream priorities only order data *inside* the connection — the kernel/WiFi driver queue below is FIFO, so an unthrottled multi-MB clipboard transfer fills it and input packets behind it wait for the whole backlog to drain (bufferbloat, seen as RTT spikes for the duration of the transfer). monux therefore paces bulk transfers to **40 Mbps by default** on both server and client (`--bulk-throttle-mbps`), keeping that queue short so input stays responsive; large clipboard transfers take slightly longer (5 MB ≈ 1 s at 40 Mbps). Set `--bulk-throttle-mbps 0` to disable, or tune the rate to your link.

When the link is degraded, monux says so in several places: a desktop notification (at most once per 5 minutes, plus once on recovery), the client's `Link stats:` / `Link degraded:` log lines (every 15s sample), and the server's 10-second input-status heartbeat (`Link to <client> is degraded: rtt=...`, only while above the threshold). If you see sporadic RTT spikes on WiFi, the checklist:

1. **Power saving off on BOTH machines** — check with `iw dev <iface> get power_save` (`monux system setup` disables it, but only on the machine where you ran it).
2. **2.4 GHz congestion** — wireless peripherals, Bluetooth, USB3 ports, and the neighbors' networks all share the band; sporadic spikes that correlate with nothing on either machine are usually this.
3. **Move the AP and clients to 5 GHz** — the single biggest fix when the hardware allows it.
4. Read the trend around a spike in the client's debug-level `Link stats:` lines (rtt and window loss every 15s).

What monux already marks for you: in local mode both endpoints run with `SO_PRIORITY=6` on the QUIC socket, which the WiFi driver maps to 802.11 UP 6 — the voice access category (AC_VO) — so monux packets cut ahead of best-effort traffic in each machine's own wireless uplink queue, no router cooperation needed. A DSCP mark on the wire is not possible from inside the process (quinn-udp overwrites the TOS byte per packet with its ECN codepoint), so the AP/router hop (which picks its downlink queue from each packet's DSCP) is covered by netfilter rules instead: `monux system setup` installs them automatically on both server and client machines (a dedicated `inet monux-qos` nftables table, or two iptables mangle OUTPUT rules as fallback), and `monux system uninstall` removes them again. The rules don't persist across reboots — re-run `monux system setup` after a reboot (or wrap the manual equivalent below in a systemd unit):

```bash
# nftables
sudo nft 'add table inet monux-qos'
sudo nft 'add chain inet monux-qos output { type filter hook output priority mangle; policy accept; }'
sudo nft 'add rule inet monux-qos output udp sport 1213 ip dscp set cs6'
sudo nft 'add rule inet monux-qos output udp dport 1213 ip dscp set cs6'
# undo: sudo nft delete table inet monux-qos

# or iptables
sudo iptables -t mangle -A OUTPUT -p udp --sport 1213 -j DSCP --set-dscp-class CS6
sudo iptables -t mangle -A OUTPUT -p udp --dport 1213 -j DSCP --set-dscp-class CS6
```

## License

This project is licensed under the AGPLv3 (or later versions) and is copyright Nicholas Parker.
