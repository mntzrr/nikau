#!/usr/bin/env bash
set -euo pipefail

# Quick installer for nikau
# Usage: ./install.sh

if ! command -v cargo >/dev/null 2>&1; then
    echo "error: cargo not found. Install Rust via https://rustup.rs/" >&2
    exit 1
fi

if [ ! -e /dev/uinput ]; then
    echo "warning: /dev/uinput not found. nikau requires uinput and evdev kernel modules." >&2
fi

if [ ! -d /dev/input ]; then
    echo "warning: /dev/input not found. nikau requires uinput and evdev kernel modules." >&2
fi

# nikau runs as a regular user; it needs read/write access to the input devices.
if [ -e /dev/uinput ] && [ ! -r /dev/uinput -o ! -w /dev/uinput ]; then
    cat >&2 <<'EOF'
warning: /dev/uinput is not accessible by your user. Fix it with:
    sudo usermod -aG input $USER
then log out and back in. If /dev/uinput is not group-writable on your
distribution, also add a udev rule, e.g.:
    echo 'SUBSYSTEM=="misc", KERNEL=="uinput", GROUP="input", MODE="0660"' | sudo tee /etc/udev/rules.d/99-nikau-uinput.rules
    sudo udevadm control --reload && sudo udevadm trigger
EOF
elif ! id -nG "$USER" | grep -qw input; then
    cat >&2 <<'EOF'
note: your user is not in the 'input' group. If nikau fails to open input
devices, run: sudo usermod -aG input $USER  (then log out and back in)
EOF
fi

# Install into ~/.local/bin: present in PATH by default on systemd-based
# distros and in most shell profiles, unlike ~/.cargo/bin.
echo "Installing nikau..."
cargo install --path . --root "$HOME/.local" --force

# Remove a stale copy from the previous cargo-bin install location, so it
# can't shadow the new one depending on PATH order.
if [ -f "$HOME/.cargo/bin/nikau" ]; then
    echo "Removing previous install at $HOME/.cargo/bin/nikau"
    rm -f "$HOME/.cargo/bin/nikau"
fi

echo "Installed nikau to $(which nikau 2>/dev/null || echo "$HOME/.local/bin/nikau")"
case ":$PATH:" in
    *":$HOME/.local/bin:"*) ;;
    *)
        cat >&2 <<'EOF'
warning: ~/.local/bin is not in your PATH. Add it with:
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc   # or ~/.bashrc
then restart your shell.
EOF
        ;;
esac

echo
echo "Run server: nikau server"
echo "Run client: nikau client [host]"
echo
echo "No sudo needed: nikau uses your 'input' group membership for device"
echo "access, and your session for clipboard sharing."
