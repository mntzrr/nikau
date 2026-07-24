//! `monux system uninstall`: removes monux from this machine. The binary is always
//! at hand even when the repo clone (and its uninstall.sh) is gone, so the
//! uninstaller lives in the binary itself; uninstall.sh is a thin wrapper.
//!
//! Order matters:
//! 1. running server/client instances are asked to shut down first (a running
//!    server may hold input devices grabbed);
//! 2. the user is asked about ~/.config/monux (identity keypair + peer
//!    approvals) — only on a terminal, otherwise it is kept;
//! 3. root-owned system settings persisted by `monux system setup` — the
//!    files, plus the netfilter DSCP marks — and the /usr/local/bin link are
//!    removed via sudo subprocesses (unlike setup, no sudo re-exec: uninstall
//!    must not swap its own process image mid-flight);
//! 4. the running binary itself plus stale copies are removed (self-delete is
//!    fine on Linux: the file unlinks while the process keeps running);
//! 5. ~/.config/monux is removed, only if the user said yes.
//!
//! The `input` group membership is deliberately left alone (it may predate
//! monux or be used by other software); a hint with the undo command is printed.

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use crate::{setup, single_instance};

/// Runs the uninstall. Best-effort throughout: individual failures downgrade
/// to notes with manual-removal hints instead of aborting the remaining steps.
pub fn run() -> Result<()> {
    // First: a running server may hold input devices grabbed.
    stop_running_instances();

    let home =
        home::home_dir().context("No home dir found: unable to locate binaries and config")?;
    let exe = current_exe_path()?;
    let mut plan = plan(&home, &exe);

    if plan.config_dir.is_some() {
        plan.remove_config = prompt_remove_config();
    }

    execute(&plan);
    Ok(())
}

/// What exists and what will be removed, computed up front so the destructive
/// stage is a simple replay (and so this logic is unit-testable against
/// temporary directories).
struct Plan {
    /// Existing root-owned paths to remove via sudo: the system settings
    /// persisted by `monux system setup`, plus /usr/local/bin/monux when it is
    /// clearly ours.
    root_owned: Vec<PathBuf>,
    /// User-owned binaries to remove directly: the running executable and
    /// stale copies from previous install locations/names.
    user_binaries: Vec<PathBuf>,
    /// ~/.config/monux, when it exists.
    config_dir: Option<PathBuf>,
    /// Whether to remove the config dir; set by the interactive prompt.
    remove_config: bool,
}

fn plan(home: &Path, current_exe: &Path) -> Plan {
    let system_paths = [
        PathBuf::from(setup::UDEV_RULE_PATH),
        PathBuf::from(setup::MODULES_LOAD_PATH),
        PathBuf::from(setup::NM_POWERSAVE_CONF_PATH),
        PathBuf::from(setup::SYSCTL_BUF_CONF_PATH),
        PathBuf::from(setup::IP_FORWARD_SYSCTL_PATH),
    ];
    plan_impl(
        home,
        current_exe,
        &system_paths,
        Path::new("/usr/local/bin/monux"),
    )
}

fn plan_impl(home: &Path, current_exe: &Path, system_paths: &[PathBuf], usr_local: &Path) -> Plan {
    let mut root_owned: Vec<PathBuf> = system_paths
        .iter()
        .filter(|p| p.exists())
        .cloned()
        .collect();
    if let Some(path) = removable_usr_local(usr_local, current_exe) {
        root_owned.push(path);
    }

    let mut user_binaries = vec![current_exe.to_path_buf()];
    // Stale copies from previous install locations/names (see install.sh).
    for stale in [
        ".cargo/bin/monux",
        ".cargo/bin/nikau",
        ".local/bin/nikau",
        ".local/bin/monux",
    ] {
        let candidate = home.join(stale);
        if candidate.exists() && !same_file(&candidate, current_exe) {
            user_binaries.push(candidate);
        }
    }
    // Root-owned paths go via sudo; don't also try (and fail) them as the user.
    user_binaries.retain(|p| !root_owned.contains(p));

    let config_dir = home.join(".config").join("monux");
    Plan {
        root_owned,
        user_binaries,
        config_dir: config_dir.is_dir().then_some(config_dir),
        remove_config: false,
    }
}

/// /usr/local/bin/monux qualifies for removal only when it is clearly ours:
/// a symlink (install.sh links it to ~/.local/bin/monux) or a file identical
/// to the running binary. Anything else there is left alone.
fn removable_usr_local(usr_local: &Path, current_exe: &Path) -> Option<PathBuf> {
    let meta = fs::symlink_metadata(usr_local).ok()?;
    if meta.file_type().is_symlink() || files_identical(usr_local, current_exe) {
        return Some(usr_local.to_path_buf());
    }
    None
}

fn files_identical(a: &Path, b: &Path) -> bool {
    match (fs::read(a), fs::read(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// True when both paths resolve to the same file (or are the same path).
fn same_file(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

fn execute(plan: &Plan) {
    remove_root_owned(&plan.root_owned);
    remove_qos_marking();
    remove_hotspot_profile();

    for path in &plan.user_binaries {
        match fs::remove_file(path) {
            Ok(()) => println!("Removed {}", path.display()),
            Err(e) => println!("note: couldn't remove {}: {}", path.display(), e),
        }
    }

    match (&plan.config_dir, plan.remove_config) {
        (Some(dir), true) => match fs::remove_dir_all(dir) {
            Ok(()) => println!("Removed {}", dir.display()),
            Err(e) => println!("note: couldn't remove {}: {}", dir.display(), e),
        },
        (Some(_), false) => println!(
            "Kept ~/.config/monux (identity + approvals); a reinstall will pick up where it left off."
        ),
        (None, _) => {}
    }

    print_group_hint();
    println!("monux uninstalled.");
}

/// Removes root-owned paths via sudo subprocesses (sudo prompts inline).
/// A failure downgrades to a manual-removal hint; the rest of the uninstall
/// continues regardless.
fn remove_root_owned(paths: &[PathBuf]) {
    if paths.is_empty() {
        return;
    }
    println!("Removing system settings persisted by 'monux system setup'...");
    let removed = Command::new("sudo")
        .arg("rm")
        .arg("-f")
        .args(paths)
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if !removed {
        println!("note: couldn't remove root-owned files (sudo failed); remove them manually:");
        for path in paths {
            println!("  sudo rm -f {}", path.display());
        }
        return;
    }
    // Reload udev so the removed rule stops applying, and restore the
    // kernel-default UDP buffer limits live: the persisted sysctl config is
    // gone, this also reverts the running values without waiting for reboot.
    let _ = Command::new("sudo")
        .args(["udevadm", "control", "--reload"])
        .status();
    let _ = Command::new("sudo")
        .args([
            "sysctl",
            "-w",
            "net.core.rmem_max=212992",
            "net.core.wmem_max=212992",
        ])
        .status();
    println!("Removed udev rules, uinput module load, WiFi powersave and UDP buffer configs.");
    if paths
        .iter()
        .any(|p| p == Path::new(setup::NM_POWERSAVE_CONF_PATH))
    {
        println!("note: WiFi powersave re-enables on next NetworkManager restart/reboot.");
    }
}

/// Removes the netfilter DSCP marks 'monux system setup' installs. The rules
/// are self-describing (our own nftables table; two exact iptables rules), so
/// removal is idempotent and needs no state. Non-interactive: when sudo would
/// have to ask for a password (nothing earlier in the uninstall warmed the
/// credential cache), the rules are left alone with a manual hint instead —
/// an unexpected prompt mid-uninstall is worse than leftover QoS marks, which
/// are inert without monux traffic.
fn remove_qos_marking() {
    let sudo_ready = Command::new("sudo")
        .args(["-n", "true"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if !sudo_ready {
        println!("note: DSCP QoS rules (if any were installed) were left in place; remove them with:");
        println!(
            "  sudo nft delete table inet {}  # or:",
            setup::NFT_QOS_TABLE
        );
        for spec in setup::iptables_qos_rule_specs() {
            println!("  sudo iptables -t mangle -D OUTPUT {}", spec.join(" "));
        }
        return;
    }
    // nftables variant: one delete undoes the whole feature. iptables variant:
    // delete both exact rules. Absence of either backend or rule is fine.
    let _ = Command::new("sudo")
        .args(["nft", "delete", "table", "inet", setup::NFT_QOS_TABLE])
        .status();
    for spec in setup::iptables_qos_rule_specs() {
        let _ = Command::new("sudo")
            .arg("iptables")
            .args(["-t", "mangle", "-D", "OUTPUT"])
            .args(&spec)
            .status();
    }
    println!("Removed DSCP QoS marking rules (if any were installed).");
}

/// Removes the 'monux-direct' NetworkManager hotspot profile 'monux system
/// setup --hotspot/--hotspot-join' installs. Self-describing and idempotent
/// (deleting a missing profile just errors, ignored). Same non-interactive
/// rule as the QoS cleanup: only attempted when sudo won't prompt.
fn remove_hotspot_profile() {
    let sudo_ready = Command::new("sudo")
        .args(["-n", "true"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if !sudo_ready {
        println!(
            "note: the '{}' NetworkManager profile (if installed) was left in place; remove it with:",
            setup::HOTSPOT_CON_NAME
        );
        println!("  sudo nmcli connection delete {}", setup::HOTSPOT_CON_NAME);
        return;
    }
    let _ = Command::new("sudo")
        .args(["nmcli", "connection", "delete", setup::HOTSPOT_CON_NAME])
        .status();
    println!("Removed the '{}' NetworkManager profile (if it was installed).", setup::HOTSPOT_CON_NAME);
    // The VPN workaround rules setup may have installed: the tagged rule in
    // Mullvad's table, and the policy rule by its priority. Best-effort —
    // anything already gone is skipped silently.
    let priority = setup::VPN_WORKAROUND_RULE_PRIORITY.to_string();
    let _ = Command::new("sudo")
        .args(["ip", "rule", "del", "priority", &priority])
        .status();
    if let Some(list) = sudo_output(&["nft", "-a", "list", "table", "inet", "mullvad"]) {
        for handle in setup::monux_rule_handles(&list) {
            let rule = format!("delete rule inet mullvad forward handle {}", handle);
            let _ = Command::new("sudo").args(["nft", &rule]).status();
        }
    }
}

/// Runs sudo non-interactively with output captured; None when sudo needs a
/// password or the command fails (best-effort cleanup treats it as "skip").
fn sudo_output(args: &[&str]) -> Option<String> {
    let out = Command::new("sudo").arg("-n").args(args).output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).to_string())
    } else {
        None
    }
}

/// Asks any running monux server and client to shut down gracefully, waiting
/// for them to exit. Reuses the single-instance machinery: acquiring the lock
/// SIGTERMs a live holder and waits for it to release; the lock is dropped
/// right away since uninstall itself needs no instance protection. Honors
/// MONUX_LOCK_DIR. Best-effort: a holder we can't signal (e.g. one running as
/// root) only warrants a note — the files are removed regardless.
fn stop_running_instances() {
    for kind in ["server", "client"] {
        match single_instance::acquire(kind) {
            Ok(lock) => {
                if lock.took_over {
                    println!("Stopped the running monux {}", kind);
                }
                drop(lock);
            }
            Err(e) => println!("note: couldn't stop the running monux {}: {}", kind, e),
        }
    }
}

/// Asks whether to also remove the config dir, reading from /dev/tty so the
/// prompt works even when stdin is a pipe. /dev/tty may exist but be
/// unopenable (cron, CI, no controlling terminal), so probe by opening it
/// first; without a usable terminal the config is kept.
fn prompt_remove_config() -> bool {
    let tty = match fs::File::open("/dev/tty") {
        Ok(tty) => tty,
        Err(_) => return false,
    };
    print!("Also remove ~/.config/monux (identity keypair and peer approvals)? [y/N] ");
    let _ = io::stdout().flush();
    let mut answer = String::new();
    match io::BufReader::new(tty).read_line(&mut answer) {
        Ok(_) => answered_yes(&answer),
        Err(_) => false,
    }
}

/// Interprets the config-removal answer; anything but an explicit yes keeps
/// the config (the prompt defaults to no).
fn answered_yes(answer: &str) -> bool {
    matches!(answer.trim_start().chars().next(), Some('y' | 'Y'))
}

/// The `input` group membership is left alone (it may predate monux); print
/// the undo command instead, mirroring uninstall.sh.
fn print_group_hint() {
    let in_input_group = Command::new("id")
        .arg("-nG")
        .output()
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .split_whitespace()
                .any(|g| g == "input")
        })
        .unwrap_or(false);
    if in_input_group {
        let user = std::env::var("USER").unwrap_or_else(|_| "$USER".to_string());
        println!("note: your user is still in the 'input' group. If you added it only");
        println!("for monux, remove it with: sudo gpasswd -d {} input", user);
    }
}

/// Own executable path, with the " (deleted)" suffix Linux appends when the
/// file was replaced while running (auto-update) trimmed off — the plain
/// path is what to remove.
fn current_exe_path() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("Failed to find our own executable")?;
    Ok(PathBuf::from(
        exe.to_string_lossy().trim_end_matches(" (deleted)"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_file(path: &Path, contents: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn plan_collects_only_existing_system_files() {
        let tmp = tempfile::tempdir().unwrap();
        let system_paths = [
            tmp.path().join("udev.rules"),
            tmp.path().join("modules-load.conf"),
            tmp.path().join("nm-powersave.conf"),
            tmp.path().join("sysctl.conf"),
        ];
        write_file(&system_paths[0], b"rule");
        write_file(&system_paths[2], b"conf");
        let home = tmp.path().join("home");
        let exe = tmp.path().join("monux");
        write_file(&exe, b"binary");
        let plan = plan_impl(
            &home,
            &exe,
            &system_paths,
            &tmp.path().join("usr-local-monux"),
        );
        assert_eq!(
            plan.root_owned,
            vec![system_paths[0].clone(), system_paths[2].clone()]
        );
        assert_eq!(plan.user_binaries, vec![exe]);
    }

    #[test]
    fn usr_local_symlink_is_removable() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("monux");
        write_file(&exe, b"binary");
        let link = tmp.path().join("usr-local-monux");
        std::os::unix::fs::symlink(&exe, &link).unwrap();
        let plan = plan_impl(&tmp.path().join("home"), &exe, &[], &link);
        assert_eq!(plan.root_owned, vec![link]);
    }

    #[test]
    fn usr_local_identical_file_is_removable() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("monux");
        write_file(&exe, b"binary");
        let copy = tmp.path().join("usr-local-monux");
        write_file(&copy, b"binary");
        let plan = plan_impl(&tmp.path().join("home"), &exe, &[], &copy);
        assert_eq!(plan.root_owned, vec![copy]);
    }

    #[test]
    fn usr_local_unrelated_file_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("monux");
        write_file(&exe, b"binary");
        let unrelated = tmp.path().join("usr-local-monux");
        write_file(&unrelated, b"some other tool");
        let plan = plan_impl(&tmp.path().join("home"), &exe, &[], &unrelated);
        assert!(plan.root_owned.is_empty());
    }

    #[test]
    fn usr_local_missing_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("monux");
        write_file(&exe, b"binary");
        let plan = plan_impl(
            &tmp.path().join("home"),
            &exe,
            &[],
            &tmp.path().join("usr-local-monux"),
        );
        assert!(plan.root_owned.is_empty());
    }

    #[test]
    fn stale_binaries_collected_and_deduped_against_current_exe() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        // The current exe is the canonical install location: listed once.
        let exe = home.join(".local/bin/monux");
        write_file(&exe, b"binary");
        let cargo_monux = home.join(".cargo/bin/monux");
        let local_nikau = home.join(".local/bin/nikau");
        write_file(&cargo_monux, b"old");
        write_file(&local_nikau, b"old");
        // ~/.cargo/bin/nikau deliberately absent.
        let plan = plan_impl(&home, &exe, &[], &tmp.path().join("usr-local-monux"));
        assert_eq!(plan.user_binaries, vec![exe, cargo_monux, local_nikau]);
    }

    #[test]
    fn root_owned_current_exe_is_removed_via_sudo_only() {
        // Running from /usr/local/bin/monux directly: the user can't unlink a
        // root-owned path, so it must only appear in the sudo list.
        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("usr-local-monux");
        write_file(&exe, b"binary");
        let plan = plan_impl(&tmp.path().join("home"), &exe, &[], &exe);
        assert_eq!(plan.root_owned, vec![exe]);
        assert!(plan.user_binaries.is_empty());
    }

    #[test]
    fn config_dir_detected_only_when_present() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let exe = tmp.path().join("monux");
        write_file(&exe, b"binary");
        let missing_usr_local = tmp.path().join("usr-local-monux");
        let plan = plan_impl(&home, &exe, &[], &missing_usr_local);
        assert_eq!(plan.config_dir, None);
        assert!(!plan.remove_config);

        let config_dir = home.join(".config").join("monux");
        fs::create_dir_all(&config_dir).unwrap();
        let plan = plan_impl(&home, &exe, &[], &missing_usr_local);
        assert_eq!(plan.config_dir, Some(config_dir));
    }

    #[test]
    fn answered_yes_parsing() {
        assert!(answered_yes("y"));
        assert!(answered_yes("Y\n"));
        assert!(answered_yes("yes"));
        assert!(answered_yes(" y \n"));
        assert!(!answered_yes(""));
        assert!(!answered_yes("\n"));
        assert!(!answered_yes("n"));
        assert!(!answered_yes("no\n"));
    }
}
