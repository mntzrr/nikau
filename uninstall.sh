#!/usr/bin/env bash
set -euo pipefail

# Uninstaller for monux: thin wrapper around 'monux system uninstall', which
# stops running instances, removes the binary, the /usr/local/bin link, and
# the system settings persisted by 'monux system setup' (udev rules, uinput
# module load, WiFi powersave and UDP buffer configs), and prompts before
# removing ~/.config/monux (identity keypair + peer approvals).
# Usage: ./uninstall.sh

if command -v monux >/dev/null 2>&1; then
    exec monux system uninstall "$@"
elif [ -x "$HOME/.local/bin/monux" ]; then
    exec "$HOME/.local/bin/monux" system uninstall "$@"
fi

# No binary left: monux was already uninstalled (or never installed here).
# Only the per-user config and any system settings can remain.
echo "No monux binary found on PATH or at ~/.local/bin/monux."
if [ -d "$HOME/.config/monux" ]; then
    echo "The config dir remains; remove it manually with:"
    echo "  rm -rf ~/.config/monux"
fi
echo "System settings persisted by 'monux system setup', if any, need sudo:"
echo "  sudo rm -f /etc/udev/rules.d/99-monux-uinput.rules /etc/modules-load.d/monux-uinput.conf /etc/NetworkManager/conf.d/99-monux-disable-wifi-powersave.conf /etc/sysctl.d/90-monux-udp-buffers.conf /usr/local/bin/monux"
