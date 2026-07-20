# nikau

```
\\ //
 \V/
  U
  |
  | nikau
```

TLS-encrypted server-client KVM software for sharing input devices and clipboards across Linux machines.

Nikau relies on the Linux uinput API, and supports keyboards, mice, and touchpads across Wayland, X11, and even bare Linux consoles. Clipboards can be seamlessly copied between machines. OSX and Windows are not currently supported.

This fork adds low-latency tuning for local networks and a `--www` mode for use over the public internet.

## Installation

### Prerequisites

- Linux with `uinput` and `evdev` kernel modules enabled (`/dev/uinput` and `/dev/input/` should exist).
- A Rust toolchain (`rustup` recommended).
- Read/write access to `/dev/uinput` and `/dev/input/event*`. On most distributions this means your user must be in the `input` group:

  ```bash
  sudo usermod -aG input $USER
  # log out and back in for the change to take effect
  ```

  If `/dev/uinput` is not group-writable on your distribution, add a udev rule such as `SUBSYSTEM=="misc", KERNEL=="uinput", GROUP="input", MODE="0660"` under `/etc/udev/rules.d/`.

  No root privileges are needed at runtime. Running as your regular user also gives nikau access to your Wayland/X11 session for clipboard sharing. (Running as root is possible but not recommended; if you do, use `sudo -E` or the clipboard will be silently disabled.)

### From this repository

```bash
git clone https://github.com/mntzrr/nikau.git
cd nikau
./install.sh
```

Or install directly with cargo:

```bash
cargo install --path . --root ~/.local
```

The repository includes `.cargo/config.toml` with `target-cpu=native`, so the binary is automatically optimized for the machine you build it on.

After installation, the binary is available as `nikau` in `~/.local/bin`, which is in `PATH` by default on systemd-based distros and in most shell profiles (unlike `~/.cargo/bin`). If your shell doesn't find it, add `export PATH="$HOME/.local/bin:$PATH"` to your shell's rc file.

### If you want a portable binary

Remove or edit `.cargo/config.toml` and change `target-cpu=native` to `target-cpu=x86-64`, then rebuild. This produces a binary that runs on any x86-64 CPU.

## Usage

Run the server on the machine with the physical input devices:

```bash
nikau server
```

Run the client on each machine you want to control:

```bash
nikau client <server-ip-or-hostname>
```

On a local network you can omit the host and let the client discover the server via mDNS:

```bash
nikau client
```

The first time a client connects, verify the fingerprint shown on both sides matches, then approve it. Approved certificates are stored in `~/.config/nikau/known_certs/`.

Switch between the server and connected clients using `LeftShift+LeftAlt+R` (next) and `LeftAlt+P` (previous), or send `SIGUSR1` / `SIGUSR2` to the server process. Shortcuts are configurable via `--shortcut` / `--shortcut-prev`.

### Local network vs. internet

By default Nikau is tuned for low-latency local networks (LAN, wired links, direct WiFi). Use `--www` on both server and client when connecting over the public internet:

```bash
nikau server --www
nikau client --www <server-host-or-ip>
```

`--www` uses conservative QUIC settings (default congestion control and RTT estimation) and skips socket QoS flags.

## License

This project is licensed under the AGPLv3 (or later versions) and is copyright Nicholas Parker.
