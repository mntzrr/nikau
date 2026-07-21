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
- Access to input devices: your user in the `input` group with `/dev/uinput` group-writable. `monux setup` persists both for you (it re-executes with sudo and prompts for your password; log out and back in after the group change). Running the server as root with `sudo -E monux server` also works as a fallback, `-E` preserving your session so clipboard sharing works.

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

After installation, the binary is available as `monux` in `~/.local/bin`, which is in `PATH` by default on systemd-based distros and in most shell profiles (unlike `~/.cargo/bin`). If your shell doesn't find it, add `export PATH="$HOME/.local/bin:$PATH"` to your shell's rc file.

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
- **Client:** run `monux update` on the client machine and restart the client there (e.g. over SSH). It reconnects and resumes by itself. Or run the client with `--auto-update` (below) so it updates and restarts itself — no remote access needed.

Active-session resumption survives server restarts for up to an hour (see `active_client` in `~/.config/monux`).

### Automatic updates

Pass `--auto-update` to `monux server` and/or `monux client` to have them check the GitHub repo once shortly after startup and then daily. When a newer commit appears, it is rebuilt and installed in the background at low CPU priority; a few seconds later (after a desktop notification) the process restarts itself into the new binary. The restart drops the session for a few seconds, which then heals itself: clients reconnect automatically and whichever machine was active is re-activated (see above). This is handy for machines you can't easily reach — e.g. keeping a client up to date without SSH access. Auto-update trusts the configured GitHub repo and this machine's git setup implicitly; leave it off if you prefer to review changes first.

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

The server runs as your normal user (in the `input` group, with `/dev/uinput` accessible — see `monux setup`). This is the recommended setup.

`sudo -E monux server` remains available as a fallback (e.g. if device permissions aren't set up); `-E` preserves your session environment so clipboard sharing keeps working. Note that running as root did **not** prove to prevent intermittent input freezes: with aggressive clipboard managers (`wl-clip-persist`, `wl-paste --watch`) a stall is still possible on some compositors. If you hit freezes, see *Troubleshooting* — `WAYLAND_DISPLAY= monux server` (clipboard sharing disabled) is the isolation test.

Switch between the server and connected clients using `LeftShift+LeftAlt+R` (next) and `LeftAlt+P` (previous), or send `SIGUSR1` / `SIGUSR2` to the server process. Shortcuts are configurable via `--shortcut` / `--shortcut-prev`.

Every switch also shows a desktop notification (via `notify-send`), so an unexpected switch is visible immediately.

> **Pick a shortcut that doesn't collide with your compositor/WM/application binds.** monux consumes only the *last* key of the combo, so if the same combo is bound elsewhere (e.g. `Alt+Shift+R` toggling your clipboard manager), pressing it fires *both* actions — and a switch you didn't mean to make looks exactly like dead keys: your input silently goes to the other machine. The notification exists to make such accidents obvious.

### Local network vs. internet

By default Monux is tuned for low-latency local networks (LAN, wired links, direct WiFi). Use `--www` on both server and client when connecting over the public internet:

```bash
monux server --www
monux client --www <server-host-or-ip>
```

`--www` uses conservative QUIC settings (default congestion control and RTT estimation) and skips socket QoS flags.

### Pointer motion rate (office vs gaming)

By default the server coalesces pointer motion to **250 updates per second**: high-polling-rate mice (1000-8000 Hz) otherwise produce thousands of tiny packets per second for no visible benefit at a desk. Motion deltas are summed losslessly — the cursor ends up in exactly the same place, just updated less often, with far less network traffic and CPU use on both machines. Coalesced motion travels the reliable ordered stream (at this rate it's cheap, and losing an accumulated delta would show as a cursor jump on a lossy link). At full rate (`--motion-hz 0`, gaming) motion instead goes as loss-tolerant QUIC datagrams, where skipping a superseded frame beats retransmitting it. Tune with `--motion-hz`, e.g. `--motion-hz 60` for maximum savings, `--motion-hz 500` for extra smoothness.

## Troubleshooting

If input (e.g. the Enter key) stops registering on the server machine while `monux server` runs, the server log tells you what monux sees. The first log line records the exact build (`monux v0.3.3+<sha> starting`) — always include it when reporting.

**While the freeze is happening** (switch to a TTY or SSH in):

1. `pgrep -f 'monux server' | xargs -r sudo kill -HUP` — dumps the server's full internal state (switch state, grab state, clients, clipboard owner, counters) to its log. SIGHUP is safe; it only logs.
2. Check the 10-second heartbeat lines in the log: `Input status: local (Ungrab): N events in, M emitted locally`. They show whether monux sees your keystrokes at all, and where they went.
3. `hyprctl devices | grep -i 'monux virtual'` — are the virtual keyboard/mouse still there? (The startup log lists their `/dev/input/eventN` nodes.)
4. `sudo libinput debug-events` — does the kernel see the physical key presses?

**Reading the evidence:**

- `INPUT SWALLOWED: ...` — monux sees your keys but they have nowhere to go (grab state vs switch state mismatch). Report the log.
- **Repeated characters on the client** — the client log distinguishes the mechanisms: `Duplicate press for key N` means the same press was delivered twice (event duplication), `Input burst: N key events delivered after a gap` means what you typed during a stall arrived at once when it cleared, and `Key N was held Ns before its release arrived` (debug level) marks delayed releases. Repeats that coincide with freeze warnings share the same root cause.
- Keys visible in `libinput debug-events` and the heartbeat's *emitted* counter rises, but apps see nothing, and `hyprctl devices` lacks the virtual keyboard → the compositor dropped the virtual device. `hyprctl reload` recovers it; report it.
- `Clipboard paste storm` or `Serving paste request ... took Ns` warnings coinciding with freezes → a clipboard manager (`wl-clip-persist`, `wl-paste --watch`) is hammering monux's clipboard serving. Tame or remove it.
- `Our own virtual device node ... vanished` → the virtual devices were destroyed mid-session; restart monux.
- Freeze windows that self-heal after seconds-to-a-minute point at a blocking wait that timed out — check whether they line up with clipboard warnings above.
- On connection loss, both sides log `Connection stats on drop: rtt=... lost_packets=N/M congestion_events=... black_holes=...`. High loss/congestion/black-holes means a lossy link (WiFi interference, weak signal); near-zero loss with a normal RTT means the *peer* went silent (CPU stall on that machine, or WiFi buffering/power saving there despite setup — recheck `iw dev` on the client).

## License

This project is licensed under the AGPLv3 (or later versions) and is copyright Nicholas Parker.
