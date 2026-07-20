#!/usr/bin/env bash
set -euo pipefail

# Quick installer for monux
# Usage: ./install.sh

if ! command -v cargo >/dev/null 2>&1; then
    echo "error: cargo not found. Install Rust via https://rustup.rs/" >&2
    exit 1
fi

if [ ! -e /dev/uinput ]; then
    echo "warning: /dev/uinput not found. monux requires uinput and evdev kernel modules." >&2
fi

if [ ! -d /dev/input ]; then
    echo "warning: /dev/input not found. monux requires uinput and evdev kernel modules." >&2
fi

# monux runs as a regular user; it needs read/write access to the input devices.
if [ -e /dev/uinput ] && [ ! -r /dev/uinput -o ! -w /dev/uinput ]; then
    cat >&2 <<'EOF'
warning: /dev/uinput is not accessible by your user. Fix it with:
    sudo usermod -aG input $USER
then log out and back in. If /dev/uinput is not group-writable on your
distribution, also add a udev rule, e.g.:
    echo 'SUBSYSTEM=="misc", KERNEL=="uinput", GROUP="input", MODE="0660"' | sudo tee /etc/udev/rules.d/99-monux-uinput.rules
    sudo udevadm control --reload && sudo udevadm trigger
EOF
elif ! id -nG "$USER" | grep -qw input; then
    cat >&2 <<'EOF'
note: your user is not in the 'input' group. If monux fails to open input
devices, run: sudo usermod -aG input $USER  (then log out and back in)
EOF
fi

# Install into ~/.local/bin: present in PATH by default on systemd-based
# distros and in most shell profiles, unlike ~/.cargo/bin.
echo "Installing monux..."
cargo install --path . --root "$HOME/.local" --force

# Remove stale copies from previous install locations/names, so they can't
# shadow the new one depending on PATH order.
for stale in "$HOME/.cargo/bin/nikau" "$HOME/.cargo/bin/monux" "$HOME/.local/bin/nikau"; do
    if [ -f "$stale" ]; then
        echo "Removing previous install at $stale"
        rm -f "$stale"
    fi
done

echo "Installed monux to $(which monux 2>/dev/null || echo "$HOME/.local/bin/monux")"
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
echo "Run server: monux server"
echo "Run client: monux client [host]"
echo
echo "No sudo needed: monux uses your 'input' group membership for device"
echo "access, and your session for clipboard sharing."
