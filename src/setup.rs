//! `monux setup`: persists machine-local settings that optimize the host for
//! local KVM use. Everything here is idempotent and reported step by step.
//!
//! Currently applied:
//! - `input` group membership for the invoking user (input device access
//!   without running monux as root; takes effect on next login)
//! - a udev rule making /dev/uinput accessible to the `input` group (only if
//!   the current permissions are insufficient)
//! - the uinput kernel module loaded and persisted, if /dev/uinput is missing
//! - WiFi power saving disabled persistently via NetworkManager, and applied
//!   immediately to current wireless interfaces (power saving buffers packets
//!   and causes 60-300ms latency spikes, felt as stutter)

use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};

const NM_POWERSAVE_CONF_PATH: &str = "/etc/NetworkManager/conf.d/99-monux-disable-wifi-powersave.conf";
const UDEV_RULE_PATH: &str = "/etc/udev/rules.d/99-monux-uinput.rules";
const MODULES_LOAD_PATH: &str = "/etc/modules-load.d/monux-uinput.conf";

fn powersave_conf_content() -> &'static str {
    "[connection]\nwifi.powersave = 2\n"
}

fn udev_rule_content() -> &'static str {
    "SUBSYSTEM==\"misc\", KERNEL==\"uinput\", GROUP=\"input\", MODE=\"0660\"\n"
}

pub fn run() -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        // sudo resets PATH, so 'sudo monux setup' often fails with
        // "command not found": print the full invocation that works.
        let exe = std::env::current_exe()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "monux".to_string());
        bail!("monux setup persists system settings and needs root. Run it with: sudo {} setup", exe);
    }

    let mut failures = 0;
    setup_input_group(&mut failures);
    setup_uinput_access(&mut failures);
    setup_wifi_powersave(&mut failures);

    println!();
    if failures == 0 {
        println!("All done. Undo any of these by removing the files listed above and/or removing the user from the 'input' group.");
    } else {
        println!("Done with {} failed step(s); see messages above.", failures);
    }
    Ok(())
}

/// Runs a command, returning its stdout on success.
fn run_cmd(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("Failed to run {}: is it installed?", program))?;
    if !output.status.success() {
        bail!(
            "{} {} failed: {}",
            program,
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Checks `id -nG` output for group membership.
fn groups_contain(id_ng_output: &str, group: &str) -> bool {
    id_ng_output
        .split_whitespace()
        .any(|g| g == group)
}

fn setup_input_group(failures: &mut u32) {
    // The user to grant device access: the one who invoked sudo.
    let user = std::env::var("SUDO_USER").unwrap_or_default();
    if user.is_empty() || user == "root" {
        println!("[skip] input group: no invoking user (running as root directly; root needs no group)");
        return;
    }
    match run_cmd("id", &["-nG", &user]) {
        Ok(groups) if groups_contain(&groups, "input") => {
            println!("[ok]   input group: user '{}' is already a member", user);
        }
        Ok(_) => match run_cmd("usermod", &["-aG", "input", &user]) {
            Ok(_) => println!(
                "[done] input group: added user '{}' (takes effect on next login)",
                user
            ),
            Err(e) => {
                *failures += 1;
                println!("[fail] input group: {}", e);
            }
        },
        Err(e) => {
            *failures += 1;
            println!("[fail] input group: could not query groups for '{}': {}", user, e);
        }
    }
}

/// Checks whether a path's permissions grant the group read+write.
fn group_has_rw(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o060 == 0o060
}

fn setup_uinput_access(failures: &mut u32) {
    let uinput = Path::new("/dev/uinput");
    if !uinput.exists() {
        // Try to load the kernel module right now, and persist it across boots.
        match run_cmd("modprobe", &["uinput"]) {
            Ok(_) if uinput.exists() => {
                println!("[done] uinput: loaded the kernel module");
            }
            Ok(_) => {
                *failures += 1;
                println!("[fail] uinput: modprobe succeeded but /dev/uinput still doesn't exist");
                return;
            }
            Err(e) => {
                *failures += 1;
                println!("[fail] uinput: could not load the kernel module: {}", e);
                return;
            }
        }
        match std::fs::write(MODULES_LOAD_PATH, "uinput\n") {
            Ok(_) => println!("[done] uinput: module persisted in {}", MODULES_LOAD_PATH),
            Err(e) => {
                *failures += 1;
                println!("[fail] uinput: could not write {}: {}", MODULES_LOAD_PATH, e);
            }
        }
    }

    let meta = match std::fs::metadata(uinput) {
        Ok(m) => m,
        Err(e) => {
            *failures += 1;
            println!("[fail] uinput: could not stat /dev/uinput: {}", e);
            return;
        }
    };
    if group_has_rw(&meta) {
        println!("[ok]   uinput: /dev/uinput is already group-accessible");
        return;
    }
    match std::fs::write(UDEV_RULE_PATH, udev_rule_content()) {
        Ok(_) => println!("[done] uinput: wrote group-access rule to {}", UDEV_RULE_PATH),
        Err(e) => {
            *failures += 1;
            println!("[fail] uinput: could not write {}: {}", UDEV_RULE_PATH, e);
            return;
        }
    }
    if let Err(e) = run_cmd("udevadm", &["control", "--reload"]) {
        *failures += 1;
        println!("[fail] uinput: udevadm reload failed: {}", e);
        return;
    }
    match run_cmd("udevadm", &["trigger"]) {
        Ok(_) => println!("[done] uinput: udev rules reloaded and triggered"),
        Err(e) => {
            *failures += 1;
            println!("[fail] uinput: udevadm trigger failed: {}", e);
        }
    }
}

/// Parses `iw dev` output into a list of interface names.
fn parse_iw_interfaces(iw_dev_output: &str) -> Vec<String> {
    iw_dev_output
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            line.strip_prefix("Interface ").map(|s| s.to_string())
        })
        .collect()
}

fn setup_wifi_powersave(failures: &mut u32) {
    // Persistent setting, via NetworkManager when it's in use.
    if Path::new("/etc/NetworkManager").exists() {
        let already = std::fs::read_to_string(NM_POWERSAVE_CONF_PATH)
            .map(|c| c.contains("wifi.powersave = 2"))
            .unwrap_or(false);
        if already {
            println!("[ok]   wifi powersave: already disabled in {}", NM_POWERSAVE_CONF_PATH);
        } else {
            match std::fs::write(NM_POWERSAVE_CONF_PATH, powersave_conf_content()) {
                Ok(_) => println!(
                    "[done] wifi powersave: wrote {} (applies to new connections; reconnect WiFi or reboot to activate)",
                    NM_POWERSAVE_CONF_PATH
                ),
                Err(e) => {
                    *failures += 1;
                    println!("[fail] wifi powersave: could not write {}: {}", NM_POWERSAVE_CONF_PATH, e);
                }
            }
        }
    } else {
        println!("[skip] wifi powersave: NetworkManager not found; disable power saving via your network stack if latency spikes appear");
    }

    // Immediate setting for currently-present wireless interfaces.
    let iw_dev = match run_cmd("iw", &["dev"]) {
        Ok(out) => out,
        Err(e) => {
            println!("[skip] wifi powersave: 'iw dev' unavailable ({}); skipping immediate apply", e);
            return;
        }
    };
    let ifaces = parse_iw_interfaces(&iw_dev);
    if ifaces.is_empty() {
        println!("[skip] wifi powersave: no wireless interfaces found");
        return;
    }
    for iface in ifaces {
        match run_cmd("iw", &["dev", &iface, "set", "power_save", "off"]) {
            Ok(_) => println!("[done] wifi powersave: disabled on {} (immediate)", iface),
            Err(e) => {
                *failures += 1;
                println!("[fail] wifi powersave: could not disable on {}: {}", iface, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_membership_parsing() {
        assert!(groups_contain("mntzr sys network docker input users", "input"));
        assert!(!groups_contain("mntzr sys network docker users", "input"));
        assert!(!groups_contain("", "input"));
        // Exact matching: 'input2' must not count as 'input'
        assert!(!groups_contain("mntzr input2", "input"));
    }

    #[test]
    fn iw_dev_interface_parsing() {
        let sample = "phy#0\n\tUnnamed/non-netdev interface\n\t\twdev 0x2\n\t\taddr aa:bb:cc:dd:ee:ff\n\t\ttype P2P-device\n\tInterface wlan0\n\t\tifindex 3\n\t\twdev 0x1\n\t\ttype managed\n\tchannel 7 (2442 MHz)\nphy#1\n\tInterface wlan1\n\t\tifindex 4\n\t\ttype managed\n";
        assert_eq!(parse_iw_interfaces(sample), vec!["wlan0", "wlan1"]);
        assert!(parse_iw_interfaces("phy#0\n").is_empty());
    }

    #[test]
    fn powersave_conf_disables() {
        assert!(powersave_conf_content().contains("wifi.powersave = 2"));
    }

    #[test]
    fn group_rw_mode_check() {
        // rw for group means at least 0o060 in the group bits
        assert_eq!(0o660 & 0o060, 0o060);
        assert_ne!(0o600 & 0o060, 0o060);
    }
}
