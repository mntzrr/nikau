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

This writes `~/.config/systemd/user/monux-<role>.service` and enables+starts it via `systemctl --user` (`Restart=on-failure`, 3s delay). The client service runs plain `monux client` with no address argument, so it finds the server via mDNS auto-discovery — nothing machine-specific is baked into the unit. `--autostart off` disables both services and removes the unit files; omitting the flag leaves autostart untouched.

Check status and logs with:

```bash
systemctl --user status monux-server
journalctl --user -u monux-server
```

**Clipboard sharing caveat:** the service inherits the systemd user manager's environment, not your compositor's session. Clipboard sharing needs `WAYLAND_DISPLAY`/`DISPLAY`, `XDG_RUNTIME_DIR`, and `DBUS_SESSION_BUS_ADDRESS` imported into the user manager. Hyprland handles this when launched via UWSM, or with its systemd integration (`exec-once = dbus-update-activation-environment --systemd WAYLAND_DISPLAY XDG_CURRENT_DESKTOP`). Without it the service still works for input, but clipboard sharing stays disabled — exactly like running monux with `WAYLAND_DISPLAY` unset.

### If you want a portable binary

Remove or edit `.cargo/config.toml` and change `target-cpu=native` to `target-cpu=x86-64`, then rebuild. This produces a binary that runs on any x86-64 CPU.

## Updating

```bash
monux update
```

This pulls the latest source from GitHub into `~/.cache/monux/src`, rebuilds it on this machine (with `target-cpu=native`), and installs over the existing binary. Run it on each machine (server and clients), then restart any running `monux server` / `monux client` to pick up the new version. `monux --version` prints the commit the binary was built from, so you can check that all machines match.

Updating never disrupts a running session: the processes keep their in-memory binary while the file on disk is replaced, so you can update mid-session.

To pick up the new version, restart the processes — the session then heals itself:

- **Server:** start `monux server` again however you normally run it (the new instance asks the old one to shut down and takes over). Clients reconnect within a few seconds, and the machine that was active is re-activated automatically — no client-side steps needed.
- **Client:** run `monux update` on the client machine and restart the client there (e.g. over SSH). It reconnects and resumes by itself. With auto-update (below, on by default) it does both by itself — no remote access needed.

Active-session resumption survives server restarts for up to an hour (see `active_client` in `~/.config/monux`).

**Protocol-compatibility gate:** a client never installs a build whose protocol version differs from its server's — such a build would be unable to reconnect. The client records the server's protocol version at every connection, including handshakes the server refused, and `monux update` (manual or automatic) checks the new source against it before building. Servers also advertise their protocol version via mDNS, so a manual `monux update` first refreshes the record from the LAN (gating on the lowest version when several servers answer) and only falls back to the last recorded version when no server answers. If they differ, the update is skipped with a log message telling you to update the server first; once the server is updated, the client learns the new version via mDNS or on its next (refused) connection attempt and the gate opens by itself. `monux update --force` bypasses the gate.

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

Pause input handling entirely with `LeftShift+LeftAlt+P` (configurable via `--pause-shortcut`, empty string disables). While paused, monux ungrabs **all** input devices — keyboards included — so the local machine gets raw evdev input with monux's re-emit completely out of the way (useful for games and raw-input apps). monux keeps listening ungrabbed, so the pause chord still works: press it again to resume, which re-grabs per the current rotation state (keyboards always, mice only while a client is active). While paused nothing is forwarded to clients and switch chords are not acted on — since devices are ungrabbed, those keystrokes also pass through to the local system. Clipboard sharing continues untouched while paused.

Every switch also shows a desktop notification (via `notify-send`), so an unexpected switch is visible immediately. The same goes for connection lifecycle events: the server notifies when a client joins or is dropped, the client notifies when the connection is lost and when it (re)connects, and a client on a degraded link (RTT over 50ms or packet loss over 2% — a WiFi/link problem, not monux) warns at most once per 5 minutes, plus once when the link recovers.

> **Pick a shortcut that doesn't collide with your compositor/WM/application binds.** monux consumes only the *last* key of the combo, so if the same combo is bound elsewhere (e.g. `Alt+Shift+R` toggling your clipboard manager), pressing it fires *both* actions — and a switch you didn't mean to make looks exactly like dead keys: your input silently goes to the other machine. The notification exists to make such accidents obvious.

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

The wire protocol is newline-delimited JSON, one request and one response per line, so any language can drive it (this is the backend of the tray indicator below). Requests: `{"cmd":"status"}`, `{"cmd":"diagnostics"}` (a troubleshooting bundle: state dump plus the daemon's recent log lines), `{"cmd":"switch","target":"next"|"prev"|"local"|<fingerprint-prefix>}`, `{"cmd":"pause"}` / `{"cmd":"resume"}` (idempotent: pausing a paused server is a no-op), `{"cmd":"update_now"}` (wakes the background update check), `{"cmd":"restart"}` (graceful shutdown + re-exec, like after an update), `{"cmd":"exit"}`. Responses: `{"ok":true,"state":{...}}` for status, `{"ok":true,"diagnostics":{...}}` for diagnostics, `{"ok":true}` for accepted commands, `{"ok":false,"error":"..."}` on failure. The server socket serves the full set; the client socket only status/diagnostics/update_now/restart/exit — rotation and pause are server concepts. Example with socat:

```bash
echo '{"cmd":"pause"}' | socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/monux/server.sock
```

### Tray indicator (`monux system indicator`)

`monux system indicator` puts a StatusNotifierItem (SNI) tray icon in your panel — any SNI host works: waybar (with a `tray` module), KDE Plasma, xfce4-panel, and so on. It is a thin client of the control socket: it polls `{"cmd":"status"}` every 2 seconds (server socket first, then the client's) and never talks to the daemon's event loops, so it can neither stall nor be stalled by monux.

The icon is a colored dot whose tooltip carries the details ("monux: input on 192.168.1.102", per-client RTT and uptime, clipboard owner):

- **green** — input is local (client role: connected, not owning input)
- **blue** — input is on a client (client role: this machine owns the input)
- **grey** — the server is paused
- **red** — the link is degraded: any client with RTT over 50 ms (server role), or not connected to the server (client role)
- hollow grey **?** — no monux daemon is running

The menu follows the current state: switch to local / to a specific client and pause/resume (server only), per-client connection facts and clipboard owner, "Check for update now" (or "Update available: `<sha>` — update now" when the auto-updater has seen a newer commit), "Copy diagnostics" (puts a bug-report bundle — version, state dump, recent logs — on the clipboard via `wl-copy`/`xclip`/`xsel`), and "Restart monux" / "Exit monux".

The indicator needs a desktop session: on a headless TTY it exits with a "no D-Bus session / no tray host" error. To autostart it, add `monux system indicator` to your compositor's autostart (e.g. `exec-once = monux system indicator` in Hyprland). It is deliberately **not** part of the systemd units installed by `monux system setup --autostart` — those inherit the systemd user manager's environment, which usually lacks `DBUS_SESSION_BUS_ADDRESS` unless your compositor imports it (see the autostart caveat above); the compositor's own autostart always has the session bus.

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
- **Repeated characters on the client** — the client log distinguishes the mechanisms: `Duplicate press for key N` means the same press was delivered twice (event duplication), `Input burst: N key events delivered after a gap` means what you typed during a stall arrived at once when it cleared, and `Key N was held Ns before its release arrived` (debug level) marks delayed releases. Repeats that coincide with freeze warnings share the same root cause.
- Keys visible in `libinput debug-events` and the heartbeat's *emitted* counter rises, but apps see nothing, and `hyprctl devices` lacks the virtual keyboard → the compositor dropped the virtual device. `hyprctl reload` recovers it; report it.
- `Clipboard paste storm` or `Serving paste request ... took Ns` warnings coinciding with freezes → a clipboard manager (`wl-clip-persist`, `wl-paste --watch`) is hammering monux's clipboard serving. Tame or remove it.
- `Our own virtual device node ... vanished` → the virtual devices were destroyed mid-session; restart monux.
- Freeze windows that self-heal after seconds-to-a-minute point at a blocking wait that timed out — check whether they line up with clipboard warnings above.
- On connection loss, both sides log `Connection stats on drop: rtt=... lost_packets=N/M congestion_events=... black_holes=...`. High loss/congestion/black-holes means a lossy link (WiFi interference, weak signal); near-zero loss with a normal RTT means the *peer* went silent (CPU stall on that machine, or WiFi buffering/power saving there despite setup — recheck `iw dev` on the client).

## License

This project is licensed under the AGPLv3 (or later versions) and is copyright Nicholas Parker.
