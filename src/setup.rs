//! `monux system setup`: persists machine-local settings that optimize the host for
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
//! - raised net.core.rmem_max/wmem_max so the QUIC UDP socket buffers aren't
//!   silently clamped to the stock ~208 KiB (clamped buffers drop packets
//!   during clipboard bursts)
//! - with `--autostart`, a per-user systemd service starting monux with the
//!   graphical session (the only step that is NOT machine tuning; off by
//!   default)

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

pub(crate) const NM_POWERSAVE_CONF_PATH: &str =
    "/etc/NetworkManager/conf.d/99-monux-disable-wifi-powersave.conf";
pub(crate) const UDEV_RULE_PATH: &str = "/etc/udev/rules.d/99-monux-uinput.rules";
pub(crate) const MODULES_LOAD_PATH: &str = "/etc/modules-load.d/monux-uinput.conf";
pub(crate) const SYSCTL_BUF_CONF_PATH: &str = "/etc/sysctl.d/90-monux-udp-buffers.conf";

/// Where per-user systemd units live, relative to the target user's home.
const SYSTEMD_USER_UNIT_DIR: &str = ".config/systemd/user";

/// `--autostart` for `monux system setup`: manage a per-user systemd service
/// that starts monux with the graphical session. When the flag is omitted, no
/// autostart changes are made.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Autostart {
    /// Write and enable+start monux-server.service.
    Server,
    /// Write and enable+start monux-client.service (mDNS auto-discovery).
    Client,
    /// Disable and remove both services.
    Off,
}

/// The roles a service unit can run (`off` maps to no role).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Server => "server",
            Role::Client => "client",
        }
    }

    fn unit_name(self) -> String {
        format!("monux-{}.service", self.as_str())
    }
}

/// Content of the per-user systemd unit for a role. `%h` expands to the
/// user's home at unit load time. `monux client` without an address argument
/// uses mDNS auto-discovery, so no server IP is baked into the unit.
fn unit_content(role: Role) -> String {
    let role = role.as_str();
    format!(
        "[Unit]\nDescription=monux KVM {role}\nAfter=graphical-session.target\n\n[Service]\nExecStart=%h/.local/bin/monux {role}\nRestart=on-failure\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n"
    )
}

/// A command to spawn, in test-inspectable form.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CmdSpec {
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
}

impl CmdSpec {
    fn run(&self) -> Result<()> {
        let status = Command::new(&self.program)
            .args(&self.args)
            .envs(self.env.iter().cloned())
            .stdin(std::process::Stdio::null())
            .status()
            .with_context(|| format!("Failed to run {}: is it installed?", self.program))?;
        if !status.success() {
            bail!(
                "{} {} exited with {}",
                self.program,
                self.args.join(" "),
                status
            );
        }
        Ok(())
    }

    /// The equivalent command for the user to run in their own session: for a
    /// runuser-wrapped invocation that's the inner command (the session
    /// environment is already right there).
    fn manual_line(&self) -> String {
        match self.args.iter().position(|a| a == "--") {
            Some(pos) if self.program == "runuser" => self.args[pos + 1..].join(" "),
            _ => std::iter::once(self.program.as_str())
                .chain(self.args.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

/// A user to run `systemctl --user` as, when setup runs as root via sudo.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UserCtx {
    name: String,
    uid: u32,
}

/// Builds `systemctl --user` invocations for the autostart target user: plain
/// when running as that user, or wrapped in `runuser` with the session
/// environment pointed at the user's runtime dir when running as root.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Systemctl {
    user: Option<UserCtx>,
}

impl Systemctl {
    fn spec(&self, args: &[&str]) -> CmdSpec {
        let mut full: Vec<String> = std::iter::once("--user".to_string())
            .chain(args.iter().map(|s| s.to_string()))
            .collect();
        match &self.user {
            None => CmdSpec {
                program: "systemctl".to_string(),
                args: full,
                env: vec![],
            },
            Some(user) => {
                let mut wrapped = vec![
                    "-u".to_string(),
                    user.name.clone(),
                    "--".to_string(),
                    "systemctl".to_string(),
                ];
                wrapped.append(&mut full);
                let runtime_dir = format!("/run/user/{}", user.uid);
                CmdSpec {
                    program: "runuser".to_string(),
                    args: wrapped,
                    env: vec![
                        ("XDG_RUNTIME_DIR".to_string(), runtime_dir.clone()),
                        (
                            "DBUS_SESSION_BUS_ADDRESS".to_string(),
                            format!("unix:path={}/bus", runtime_dir),
                        ),
                    ],
                }
            }
        }
    }
}

/// Who the autostart service belongs to. Setup normally runs as root via
/// `sudo -E`: the unit must land in the INVOKING user's home and be managed
/// through their user manager, not root's.
struct AutostartTarget {
    unit_dir: PathBuf,
    systemctl: Systemctl,
    /// uid/gid the unit file (and directories we create) is chowned to, so a
    /// root-written file stays user-manageable.
    owner: Option<(u32, u32)>,
}

/// Looks up a user's home directory and uid/gid (getpwnam(3)).
fn passwd_entry(name: &str) -> Result<(PathBuf, u32, u32)> {
    use std::os::unix::ffi::OsStrExt;
    let cname = std::ffi::CString::new(name).context("Invalid user name")?;
    let pw = unsafe { libc::getpwnam(cname.as_ptr()) };
    if pw.is_null() {
        bail!("User '{}' not found", name);
    }
    let (dir, uid, gid) = unsafe {
        let pw = &*pw;
        (std::ffi::CStr::from_ptr(pw.pw_dir), pw.pw_uid, pw.pw_gid)
    };
    Ok((
        PathBuf::from(std::ffi::OsStr::from_bytes(dir.to_bytes())),
        uid,
        gid,
    ))
}

/// Resolves the autostart target user. Returns Ok(None) when there is no
/// sensible target: running as root directly (root's user manager is not the
/// one a desktop session uses, and autostart is per-user).
fn resolve_autostart_target() -> Result<Option<AutostartTarget>> {
    let sudo_user = std::env::var("SUDO_USER").unwrap_or_default();
    if unsafe { libc::geteuid() } == 0 {
        if sudo_user.is_empty() || sudo_user == "root" {
            return Ok(None);
        }
        let (home, uid, gid) = passwd_entry(&sudo_user)?;
        Ok(Some(AutostartTarget {
            unit_dir: home.join(SYSTEMD_USER_UNIT_DIR),
            systemctl: Systemctl {
                user: Some(UserCtx {
                    name: sudo_user,
                    uid,
                }),
            },
            owner: Some((uid, gid)),
        }))
    } else {
        // Unprivileged (e.g. experiments): manage the current user's own
        // service directly.
        let home = home::home_dir().context("No home dir found")?;
        Ok(Some(AutostartTarget {
            unit_dir: home.join(SYSTEMD_USER_UNIT_DIR),
            systemctl: Systemctl { user: None },
            owner: None,
        }))
    }
}

/// Best-effort chown (used so root-written files stay user-manageable).
fn chown_best_effort(path: &Path, uid: u32, gid: u32) {
    use std::os::unix::ffi::OsStrExt;
    let cpath = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return,
    };
    unsafe {
        libc::chown(cpath.as_ptr(), uid, gid);
    }
}

fn write_unit_file(path: &Path, role: Role, owner: Option<(u32, u32)>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
        // Directories we may have just created must stay user-manageable when
        // running as root: chown the two levels this feature owns
        // (.config/systemd and .config/systemd/user).
        if let Some((uid, gid)) = owner {
            chown_best_effort(parent, uid, gid);
            if let Some(grandparent) = parent.parent() {
                chown_best_effort(grandparent, uid, gid);
            }
        }
    }
    std::fs::write(path, unit_content(role))
        .with_context(|| format!("could not write {}", path.display()))?;
    if let Some((uid, gid)) = owner {
        chown_best_effort(path, uid, gid);
    }
    Ok(())
}

/// Applies the `--autostart` choice: writes/removes the unit files under the
/// target's unit dir and runs the systemctl steps via `run` (the seam that
/// keeps tests off the real systemd). No flag: no autostart changes at all.
fn apply_autostart(
    choice: Option<Autostart>,
    target: &AutostartTarget,
    failures: &mut u32,
    run: &mut dyn FnMut(&CmdSpec) -> Result<()>,
) {
    let choice = match choice {
        Some(c) => c,
        None => return,
    };
    match choice {
        Autostart::Server => enable_role(Role::Server, target, failures, run),
        Autostart::Client => enable_role(Role::Client, target, failures, run),
        Autostart::Off => disable_all_roles(target, failures, run),
    }
}

fn enable_role(
    role: Role,
    target: &AutostartTarget,
    failures: &mut u32,
    run: &mut dyn FnMut(&CmdSpec) -> Result<()>,
) {
    let unit_path = target.unit_dir.join(role.unit_name());
    if let Err(e) = write_unit_file(&unit_path, role, target.owner) {
        *failures += 1;
        println!("[fail] autostart: {}", e);
        return;
    }
    println!("[done] autostart: wrote {}", unit_path.display());
    let daemon_reload = target.systemctl.spec(&["daemon-reload"]);
    let enable = target.systemctl.spec(&["enable", "--now", &role.unit_name()]);
    for spec in [&daemon_reload, &enable] {
        if let Err(e) = run(spec) {
            *failures += 1;
            println!("[fail] autostart: {}", e);
            println!("       Run these yourself in your session:");
            println!("       $ {}", daemon_reload.manual_line());
            println!("       $ {}", enable.manual_line());
            return;
        }
    }
    println!(
        "[done] autostart: {} enabled and started (systemd user service)",
        role.unit_name()
    );
    println!(
        "[note] autostart: clipboard sharing in the service needs the session environment (WAYLAND_DISPLAY/DISPLAY/XDG_RUNTIME_DIR/DBUS) imported into the systemd user manager — Hyprland handles this when launched via UWSM or with its systemd integration. See README.md for details."
    );
}

fn disable_all_roles(
    target: &AutostartTarget,
    failures: &mut u32,
    run: &mut dyn FnMut(&CmdSpec) -> Result<()>,
) {
    for role in [Role::Server, Role::Client] {
        // Best-effort: the service may not exist or be enabled; the unit file
        // is removed regardless.
        let disable = target.systemctl.spec(&["disable", "--now", &role.unit_name()]);
        if let Err(e) = run(&disable) {
            println!(
                "[skip] autostart: could not disable {} ({}); removing the unit file anyway",
                role.unit_name(),
                e
            );
        }
        let unit_path = target.unit_dir.join(role.unit_name());
        match std::fs::remove_file(&unit_path) {
            Ok(()) => println!("[done] autostart: removed {}", unit_path.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                *failures += 1;
                println!(
                    "[fail] autostart: could not remove {}: {}",
                    unit_path.display(),
                    e
                );
            }
        }
    }
    // Forget the removed units; a failure here is harmless.
    let _ = run(&target.systemctl.spec(&["daemon-reload"]));
    println!("[done] autostart: monux services disabled and unit files removed");
}

fn setup_autostart(choice: Option<Autostart>, failures: &mut u32) {
    if choice.is_none() {
        // No flag: leave autostart untouched.
        return;
    }
    let target = match resolve_autostart_target() {
        Ok(Some(t)) => t,
        Ok(None) => {
            *failures += 1;
            println!("[fail] autostart: no invoking user found (run setup via sudo from your user session, not from a root shell)");
            return;
        }
        Err(e) => {
            *failures += 1;
            println!("[fail] autostart: {}", e);
            return;
        }
    };
    apply_autostart(choice, &target, failures, &mut |spec| spec.run());
}

/// Target for net.core.{r,w}mem_max: comfortably above the 2 MiB that monux
/// requests for its QUIC UDP socket buffers (the kernel clamps SO_SNDBUF/
/// SO_RCVBUF to these sysctls).
const SOCK_MEM_MAX: u64 = 2_621_440;

fn powersave_conf_content() -> &'static str {
    "[connection]\nwifi.powersave = 2\n"
}

fn udev_rule_content() -> &'static str {
    "SUBSYSTEM==\"misc\", KERNEL==\"uinput\", GROUP=\"input\", MODE=\"0660\"\n"
}

fn sysctl_buf_conf_content() -> String {
    format!(
        "net.core.rmem_max = {}\nnet.core.wmem_max = {}\n",
        SOCK_MEM_MAX, SOCK_MEM_MAX
    )
}

pub fn run(autostart: Option<Autostart>) -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        // Reaching here non-root means auto-elevation was opted out of
        // (MONUX_NO_ELEVATE). sudo resets PATH, so 'sudo monux system setup' often
        // fails with "command not found": print the full invocation that works.
        let exe = std::env::current_exe()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "monux".to_string());
        bail!("monux system setup persists system settings and needs root. Run it with: sudo {} system setup (or re-run without MONUX_NO_ELEVATE to elevate automatically)", exe);
    }

    let mut failures = 0;
    setup_input_group(&mut failures);
    setup_uinput_access(&mut failures);
    setup_wifi_powersave(&mut failures);
    setup_socket_buffers(&mut failures);
    setup_autostart(autostart, &mut failures);

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

/// Reads a numeric /proc sysctl value, e.g. /proc/sys/net/core/rmem_max.
fn read_proc_sysctl(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn setup_socket_buffers(failures: &mut u32) {
    const RMEM_PROC: &str = "/proc/sys/net/core/rmem_max";
    const WMEM_PROC: &str = "/proc/sys/net/core/wmem_max";
    let rmem = read_proc_sysctl(RMEM_PROC);
    let wmem = read_proc_sysctl(WMEM_PROC);
    if rmem.is_some_and(|v| v >= SOCK_MEM_MAX) && wmem.is_some_and(|v| v >= SOCK_MEM_MAX) {
        println!("[ok]   udp buffers: net.core.rmem_max/wmem_max already >= {}", SOCK_MEM_MAX);
        return;
    }

    // Persist for future boots, then apply immediately (don't require a reboot).
    if let Err(e) = std::fs::write(SYSCTL_BUF_CONF_PATH, sysctl_buf_conf_content()) {
        *failures += 1;
        println!("[fail] udp buffers: could not write {}: {}", SYSCTL_BUF_CONF_PATH, e);
        return;
    }
    println!("[done] udp buffers: wrote {}", SYSCTL_BUF_CONF_PATH);
    let rmem = format!("net.core.rmem_max={}", SOCK_MEM_MAX);
    let wmem = format!("net.core.wmem_max={}", SOCK_MEM_MAX);
    match run_cmd("sysctl", &["-w", &rmem, &wmem]) {
        Ok(_) => println!("[done] udp buffers: applied immediately (net.core.rmem_max=wmem_max={})", SOCK_MEM_MAX),
        Err(e) => {
            *failures += 1;
            println!("[fail] udp buffers: persisted but immediate apply failed (takes effect on reboot): {}", e);
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
    fn sysctl_conf_covers_both_buffers() {
        let content = sysctl_buf_conf_content();
        assert!(content.contains("net.core.rmem_max = 2621440"));
        assert!(content.contains("net.core.wmem_max = 2621440"));
    }

    #[test]
    fn group_rw_mode_check() {
        // rw for group means at least 0o060 in the group bits
        assert_eq!(0o660 & 0o060, 0o060);
        assert_ne!(0o600 & 0o060, 0o060);
    }

    /// A target rooted at a tempdir, managing the current user directly.
    fn test_target(dir: &Path) -> AutostartTarget {
        AutostartTarget {
            unit_dir: dir.to_path_buf(),
            systemctl: Systemctl { user: None },
            owner: None,
        }
    }

    /// An executor that records every command instead of running it.
    fn recording_executor() -> (
        std::rc::Rc<std::cell::RefCell<Vec<CmdSpec>>>,
        impl FnMut(&CmdSpec) -> Result<()>,
    ) {
        let recorded = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let rec = recorded.clone();
        let run = move |spec: &CmdSpec| -> Result<()> {
            rec.borrow_mut().push(spec.clone());
            Ok(())
        };
        (recorded, run)
    }

    #[test]
    fn unit_file_contents() {
        assert_eq!(
            unit_content(Role::Server),
            "[Unit]\nDescription=monux KVM server\nAfter=graphical-session.target\n\n[Service]\nExecStart=%h/.local/bin/monux server\nRestart=on-failure\nRestartSec=3\n\n[Install]\nWantedBy=default.target\n"
        );
        let client = unit_content(Role::Client);
        assert!(client.contains("Description=monux KVM client\n"));
        assert!(client.contains("After=graphical-session.target\n"));
        assert!(client.contains("Restart=on-failure\n"));
        // Client with no address argument = mDNS auto-discovery: nothing
        // machine-specific (like a hardcoded server IP) may be baked in.
        assert!(client.contains("ExecStart=%h/.local/bin/monux client\n"));
        assert!(!client.contains("monux client "));
    }

    #[test]
    fn systemctl_specs_plain_and_runuser() {
        // Without a sudo target, systemctl runs directly.
        let plain = Systemctl { user: None }.spec(&["daemon-reload"]);
        assert_eq!(plain.program, "systemctl");
        assert_eq!(plain.args, vec!["--user", "daemon-reload"]);
        assert!(plain.env.is_empty());
        assert_eq!(plain.manual_line(), "systemctl --user daemon-reload");

        // As root via sudo: wrapped in runuser with the user's runtime dir.
        let sys = Systemctl {
            user: Some(UserCtx {
                name: "alice".to_string(),
                uid: 1001,
            }),
        };
        let spec = sys.spec(&["enable", "--now", "monux-server.service"]);
        assert_eq!(spec.program, "runuser");
        assert_eq!(
            spec.args,
            vec![
                "-u",
                "alice",
                "--",
                "systemctl",
                "--user",
                "enable",
                "--now",
                "monux-server.service"
            ]
        );
        assert_eq!(
            spec.env,
            vec![
                ("XDG_RUNTIME_DIR".to_string(), "/run/user/1001".to_string()),
                (
                    "DBUS_SESSION_BUS_ADDRESS".to_string(),
                    "unix:path=/run/user/1001/bus".to_string()
                ),
            ]
        );
        // The manual hint is the plain command the user runs in their session.
        assert_eq!(
            spec.manual_line(),
            "systemctl --user enable --now monux-server.service"
        );
    }

    #[test]
    fn autostart_server_writes_unit_and_enables() {
        let tmp = tempfile::tempdir().unwrap();
        let target = test_target(tmp.path());
        let (recorded, mut run) = recording_executor();
        let mut failures = 0;
        apply_autostart(Some(Autostart::Server), &target, &mut failures, &mut run);
        assert_eq!(failures, 0);
        // Unit file written with the expected content.
        let content =
            std::fs::read_to_string(tmp.path().join("monux-server.service")).unwrap();
        assert_eq!(content, unit_content(Role::Server));
        // daemon-reload, then enable --now, in order.
        let cmds = recorded.borrow();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], Systemctl { user: None }.spec(&["daemon-reload"]));
        assert_eq!(
            cmds[1],
            Systemctl { user: None }.spec(&["enable", "--now", "monux-server.service"])
        );
    }

    #[test]
    fn autostart_client_writes_unit_and_enables() {
        let tmp = tempfile::tempdir().unwrap();
        let target = test_target(tmp.path());
        let (recorded, mut run) = recording_executor();
        let mut failures = 0;
        apply_autostart(Some(Autostart::Client), &target, &mut failures, &mut run);
        assert_eq!(failures, 0);
        let content =
            std::fs::read_to_string(tmp.path().join("monux-client.service")).unwrap();
        assert_eq!(content, unit_content(Role::Client));
        let cmds = recorded.borrow();
        assert_eq!(cmds.len(), 2);
        assert_eq!(
            cmds[1],
            Systemctl { user: None }.spec(&["enable", "--now", "monux-client.service"])
        );
    }

    #[test]
    fn autostart_off_disables_and_removes() {
        let tmp = tempfile::tempdir().unwrap();
        let target = test_target(tmp.path());
        std::fs::write(
            tmp.path().join("monux-server.service"),
            unit_content(Role::Server),
        )
        .unwrap();
        std::fs::write(
            tmp.path().join("monux-client.service"),
            unit_content(Role::Client),
        )
        .unwrap();
        let (recorded, mut run) = recording_executor();
        let mut failures = 0;
        apply_autostart(Some(Autostart::Off), &target, &mut failures, &mut run);
        assert_eq!(failures, 0);
        // Both unit files removed...
        assert!(!tmp.path().join("monux-server.service").exists());
        assert!(!tmp.path().join("monux-client.service").exists());
        // ...after disabling both, then a daemon-reload.
        let cmds = recorded.borrow();
        assert_eq!(cmds.len(), 3);
        assert_eq!(
            cmds[0],
            Systemctl { user: None }.spec(&["disable", "--now", "monux-server.service"])
        );
        assert_eq!(
            cmds[1],
            Systemctl { user: None }.spec(&["disable", "--now", "monux-client.service"])
        );
        assert_eq!(cmds[2], Systemctl { user: None }.spec(&["daemon-reload"]));
    }

    #[test]
    fn autostart_none_changes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let target = test_target(tmp.path());
        std::fs::write(tmp.path().join("monux-server.service"), "keep me").unwrap();
        let mut failures = 0;
        let mut run = |_: &CmdSpec| -> Result<()> {
            panic!("no systemctl commands may run without --autostart")
        };
        apply_autostart(None, &target, &mut failures, &mut run);
        assert_eq!(failures, 0);
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("monux-server.service")).unwrap(),
            "keep me"
        );
    }

    #[test]
    fn autostart_enable_failure_counts_and_stops() {
        let tmp = tempfile::tempdir().unwrap();
        let target = test_target(tmp.path());
        let mut failures = 0;
        let mut run = |_: &CmdSpec| -> Result<()> { bail!("no systemd here") };
        apply_autostart(Some(Autostart::Client), &target, &mut failures, &mut run);
        assert_eq!(failures, 1);
        // The unit file was still written, so the printed manual commands work.
        assert!(tmp.path().join("monux-client.service").exists());
    }
}
