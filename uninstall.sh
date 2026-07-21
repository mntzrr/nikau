#!/usr/bin/env bash
set -euo pipefail

# Uninstaller for monux: removes the binary, the /usr/local/bin link, and the
# system settings persisted by 'monux setup' (udev rules, uinput module load,
# WiFi powersave and UDP buffer configs).
#
# Prompts before removing ~/.config/monux (identity keypair + peer approvals);
# when not running interactively the config is kept.
# Usage: ./uninstall.sh

# Stop running instances (the server may hold input devices grabbed).
if pgrep -x monux >/dev/null 2>&1; then
    echo "Stopping running monux processes..."
    pkill -x monux || true
    sleep 1
fi

echo "Removing monux binaries..."
rm -f "$HOME/.local/bin/monux"
# Stale copies from previous install locations/names.
for stale in "$HOME/.cargo/bin/monux" "$HOME/.cargo/bin/nikau" "$HOME/.local/bin/nikau"; do
    [ -f "$stale" ] && rm -f "$stale" && echo "Removed stale $stale"
done

# System-level changes need sudo (skip them with a note if it isn't available).
SYSTEM_FILES=(
    /usr/local/bin/monux
    /etc/udev/rules.d/99-monux-uinput.rules
    /etc/modules-load.d/monux-uinput.conf
    /etc/NetworkManager/conf.d/99-monux-disable-wifi-powersave.conf
    /etc/sysctl.d/90-monux-udp-buffers.conf
)
needs_sudo=0
for f in "${SYSTEM_FILES[@]}"; do
    [ -e "$f" ] && needs_sudo=1 && break
done

if [ "$needs_sudo" -eq 1 ]; then
    if sudo -n true 2>/dev/null || sudo -v; then
        echo "Removing system settings persisted by 'monux setup'..."
        sudo rm -f "${SYSTEM_FILES[@]}" 2>/dev/null || true
        sudo udevadm control --reload 2>/dev/null || true
        # Restore kernel-default UDP buffer limits (the persisted config is
        # gone, this also reverts the live values without waiting for reboot).
        sudo sysctl -w net.core.rmem_max=212992 net.core.wmem_max=212992 >/dev/null 2>&1 || true
        echo "Removed udev rules, uinput module load, WiFi powersave and UDP buffer configs."
        echo "note: WiFi powersave re-enables on next NetworkManager restart/reboot."
    else
        echo "note: skipped system files (no sudo): remove manually if desired:"
        printf '  %s\n' "${SYSTEM_FILES[@]}"
    fi
fi

if [ -d "$HOME/.config/monux" ]; then
    purge=""
    # Read from /dev/tty so the prompt works even when stdin is a pipe;
    # probe first since /dev/tty may exist but be unopenable (cron, CI).
    if (exec 3< /dev/tty) 2>/dev/null; then
        read -r -p "Also remove ~/.config/monux (identity keypair and peer approvals)? [y/N] " purge < /dev/tty || true
    fi
    case "$purge" in
        y | Y | yes | YES)
            echo "Removing ~/.config/monux..."
            rm -rf "$HOME/.config/monux"
            ;;
        *)
            echo "Kept ~/.config/monux (identity + approvals); a reinstall will pick up where it left off."
            ;;
    esac
fi

# Group membership is deliberately left alone: it may predate monux or be
# used by other software.
if id -nG "$USER" | grep -qw input; then
    echo "note: your user is still in the 'input' group. If you added it only"
    echo "for monux, remove it with: sudo gpasswd -d $USER input"
fi

echo "monux uninstalled."
