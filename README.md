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
- sudo privileges at runtime. `monux server` **re-executes itself with `sudo -E` automatically** (prompting for your password once), which preserves your session environment (`WAYLAND_DISPLAY`, `XDG_RUNTIME_DIR`) so clipboard sharing works. Opt out with `MONUX_NO_ELEVATE=1` if you want to run non-root (see *Server: sudo vs non-sudo* below).

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

## Usage

Run the server on the machine with the physical input devices (it re-executes with `sudo -E` for you and prompts for your password once):

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

`monux server` elevates itself with `sudo -E` automatically (preserving your session, so clipboard sharing works). This is the recommended way: it's the most reliable setup for both input and clipboard.

To run without elevation (`MONUX_NO_ELEVATE=1 monux server`): keyboard/mouse work, but **only with clipboard sharing disabled** (`WAYLAND_DISPLAY= monux server`). With the Wayland clipboard active, clipboard managers that aggressively re-own/poll the clipboard (e.g. `wl-clip-persist`, `wl-paste --watch`) can, on some compositors, backpressure the compositor's connection to monux and freeze keyboard input. If you hit this, let the server elevate (the default) or disable the clipboard.

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

## Troubleshooting

If input (e.g. the Enter key) stops registering on the server machine while `monux server` runs, the server log tells you what monux sees. The first log line records the exact build (`monux v0.3.3+<sha> starting`) — always include it when reporting.

**While the freeze is happening** (switch to a TTY or SSH in):

1. `pgrep -f 'monux server' | xargs -r sudo kill -HUP` — dumps the server's full internal state (switch state, grab state, clients, clipboard owner, counters) to its log. SIGHUP is safe; it only logs.
2. Check the 10-second heartbeat lines in the log: `Input status: local (Ungrab): N events in, M emitted locally`. They show whether monux sees your keystrokes at all, and where they went.
3. `hyprctl devices | grep -i 'monux virtual'` — are the virtual keyboard/mouse still there? (The startup log lists their `/dev/input/eventN` nodes.)
4. `sudo libinput debug-events` — does the kernel see the physical key presses?

**Reading the evidence:**

- `INPUT SWALLOWED: ...` — monux sees your keys but they have nowhere to go (grab state vs switch state mismatch). Report the log.
- Keys visible in `libinput debug-events` and the heartbeat's *emitted* counter rises, but apps see nothing, and `hyprctl devices` lacks the virtual keyboard → the compositor dropped the virtual device. `hyprctl reload` recovers it; report it.
- `Clipboard paste storm` or `Serving paste request ... took Ns` warnings coinciding with freezes → a clipboard manager (`wl-clip-persist`, `wl-paste --watch`) is hammering monux's clipboard serving. Tame or remove it.
- `Our own virtual device node ... vanished` → the virtual devices were destroyed mid-session; restart monux.
- Freeze windows that self-heal after seconds-to-a-minute point at a blocking wait that timed out — check whether they line up with clipboard warnings above.

## License

This project is licensed under the AGPLv3 (or later versions) and is copyright Nicholas Parker.
