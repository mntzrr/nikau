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
- Root privileges at runtime (for direct access to input devices).

### From this repository

```bash
git clone https://github.com/mntzrr/nikau.git
cd nikau
cargo install --path .
```

The repository includes `.cargo/config.toml` with `target-cpu=native`, so the binary is automatically optimized for the machine you build it on.

After installation, the binary is available as `nikau` in your cargo bin directory (usually `~/.cargo/bin/nikau`).

### If you want a portable binary

Remove or edit `.cargo/config.toml` and change `target-cpu=native` to `target-cpu=x86-64`, then rebuild. This produces a binary that runs on any x86-64 CPU.

## Usage

Run the server on the machine with the physical input devices:

```bash
sudo nikau server
```

Run the client on each machine you want to control:

```bash
sudo nikau client <server-ip-or-hostname>
```

On a local network you can omit the host and let the client discover the server via mDNS:

```bash
sudo nikau client
```

The first time a client connects, verify the fingerprint shown on both sides matches, then approve it. Approved certificates are stored in `~/.config/nikau/known_certs/`.

Switch between the server and connected clients using `LeftAlt+N` (next) and `LeftAlt+P` (previous), or send `SIGUSR1` / `SIGUSR2` to the server process.

### Local network vs. internet

By default Nikau is tuned for low-latency local networks (LAN, wired links, direct WiFi). Use `--www` on both server and client when connecting over the public internet:

```bash
sudo nikau server --www
sudo nikau client --www <server-host-or-ip>
```

`--www` uses conservative QUIC settings (default congestion control and RTT estimation) and skips socket QoS flags.

## License

This project is licensed under the AGPLv3 (or later versions) and is copyright Nicholas Parker.
