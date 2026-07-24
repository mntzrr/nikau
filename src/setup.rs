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
//! - DSCP CS6 netfilter marking of monux's UDP traffic (the AP/router hop
//!   picks its downlink queue from each packet's DSCP; quinn overwrites the
//!   TOS byte per packet, so only netfilter can set it)
//! - with `--hotspot`, a 'monux-direct' WiFi hotspot hosted by this machine
//!   (the KVM link then bypasses the router; the peer is NATed through this
//!   machine so its internet keeps working); with `--hotspot-join`, this
//!   machine joins the other machine's hotspot
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

/// Name of the NetworkManager connection profile for the direct, routerless
/// KVM link (see setup_hotspot / setup_hotspot_join). One name for both roles
/// keeps uninstall trivial: delete the profile.
pub(crate) const HOTSPOT_CON_NAME: &str = "monux-direct";

/// Priority of the policy rule that routes the hotspot subnet around a VPN
/// (one above Mullvad's 32764 suppress rule, so it wins).
pub(crate) const VPN_WORKAROUND_RULE_PRIORITY: u32 = 32763;

/// The comment tag on the nftables rule we insert into Mullvad's table, so
/// teardown finds it even after Mullvad regenerates everything around it.
pub(crate) const VPN_WORKAROUND_COMMENT: &str = "monux-hotspot";

/// Masks the host bits out of an "A.B.C.D/plen" address, yielding the subnet
/// in the same form ("10.42.0.1/24" -> "10.42.0.0/24").
fn subnet_of(addr: &str) -> Option<String> {
    let (ip, plen) = addr.split_once('/')?;
    let plen: u32 = plen.parse().ok()?;
    if plen > 32 {
        return None;
    }
    let octets: Vec<u8> = ip.split('.').map(|o| o.parse().ok()).collect::<Option<Vec<_>>>()?;
    if octets.len() != 4 {
        return None;
    }
    let addr_u32 = u32::from_be_bytes([octets[0], octets[1], octets[2], octets[3]]);
    let mask = if plen == 0 { 0 } else { u32::MAX << (32 - plen) };
    let net = addr_u32 & mask;
    Some(format!(
        "{}.{}.{}.{}/{}",
        (net >> 24) & 0xff,
        (net >> 16) & 0xff,
        (net >> 8) & 0xff,
        net & 0xff,
        plen
    ))
}

/// Handles of our comment-tagged rules in `nft -a list table inet mullvad`
/// output (the tag lets teardown find the rules after Mullvad rewrites the
/// rest of its table).
pub(crate) fn monux_rule_handles(nft_list_output: &str) -> Vec<u64> {
    nft_list_output
        .lines()
        .filter(|line| line.contains(VPN_WORKAROUND_COMMENT))
        .filter_map(|line| {
            line.rsplit_once("handle ")
                .and_then(|(_, handle)| handle.trim().parse().ok())
        })
        .collect()
}

/// The hotspot's live subnet: NetworkManager's shared mode assigns a 10.42.x
/// address to the AP interface on activation. Polled briefly — the address
/// can lag 'connection up' by a moment.
fn hotspot_live_subnet() -> Option<String> {
    for attempt in 0..4 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        let out = run_cmd("ip", &["-o", "-4", "addr", "show", "scope", "global"]).ok()?;
        for line in out.lines() {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            let Some(pos) = tokens.iter().position(|t| *t == "inet") else {
                continue;
            };
            let Some(addr) = tokens.get(pos + 1) else {
                continue;
            };
            if addr.starts_with("10.42.") {
                return subnet_of(addr);
            }
        }
    }
    None
}

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

/// `--hotspot` for `monux system setup`: host ('on', the default when the
/// flag is given bare) or remove ('off') the 'monux-direct' hotspot profile.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hotspot {
    /// Create and activate the hotspot profile (server side).
    On,
    /// Delete the profile (either role) without uninstalling monux.
    Off,
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
        // ~/.config may not exist yet; note it BEFORE create_dir_all creates
        // it, so we chown only a ~/.config we created ourselves — never a
        // pre-existing one (that directory isn't ours to touch).
        let config_dir = parent.parent().and_then(|p| p.parent());
        let config_dir_existed = config_dir.map(|dir| dir.exists());
        std::fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
        // Directories we may have just created must stay user-manageable when
        // running as root: chown the levels this feature owns
        // (.config/systemd and .config/systemd/user, plus ~/.config itself
        // when we just created it).
        if let Some((uid, gid)) = owner {
            chown_best_effort(parent, uid, gid);
            if let Some(grandparent) = parent.parent() {
                chown_best_effort(grandparent, uid, gid);
            }
            if let (Some(dir), Some(false)) = (config_dir, config_dir_existed) {
                chown_best_effort(dir, uid, gid);
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

/// The nftables table monux's DSCP marks live in. A dedicated table makes the
/// feature atomic to install and to remove — `nft delete table` undoes
/// everything without touching anyone else's rules.
pub(crate) const NFT_QOS_TABLE: &str = "monux-qos";

/// The UDP port the QoS marks match: the default monux listen port. A server
/// on a custom --port needs matching custom rules.
const QOS_MARK_PORT: u16 = 1213;

/// The nftables commands installing monux's DSCP marks, in order (one `nft`
/// invocation per entry). nft takes a whole command as a single argument, so
/// no shell quoting is involved.
pub(crate) fn nft_qos_install_cmds() -> Vec<String> {
    vec![
        format!("add table inet {}", NFT_QOS_TABLE),
        format!(
            "add chain inet {} output {{ type filter hook output priority mangle; policy accept; }}",
            NFT_QOS_TABLE
        ),
        format!(
            "add rule inet {} output udp sport {} ip dscp set cs6",
            NFT_QOS_TABLE, QOS_MARK_PORT
        ),
        format!(
            "add rule inet {} output udp dport {} ip dscp set cs6",
            NFT_QOS_TABLE, QOS_MARK_PORT
        ),
    ]
}

/// Whether `nft list table inet <table>` output already carries both marks.
fn nft_ruleset_has_marks(ruleset: &str) -> bool {
    ruleset.contains("udp sport 1213")
        && ruleset.contains("udp dport 1213")
        && ruleset.contains("cs6")
}

/// The two iptables rules (everything after `iptables -t mangle <verb>
/// OUTPUT`) matching monux's DSCP marks.
pub(crate) fn iptables_qos_rule_specs() -> [Vec<String>; 2] {
    ["--sport", "--dport"].map(|side| {
        [
            "-p".to_string(),
            "udp".to_string(),
            side.to_string(),
            QOS_MARK_PORT.to_string(),
            "-j".to_string(),
            "DSCP".to_string(),
            "--set-dscp-class".to_string(),
            "CS6".to_string(),
        ]
        .to_vec()
    })
}

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

pub fn run(autostart: Option<Autostart>, hotspot: Option<Hotspot>, hotspot_join: Option<(String, String)>) -> Result<()> {
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
    setup_qos_marking(&mut failures);
    match (hotspot, hotspot_join) {
        (Some(_), Some(_)) => {
            failures += 1;
            println!("[fail] hotspot: --hotspot and --hotspot-join are opposite roles; pass only one");
        }
        (Some(Hotspot::On), None) => setup_hotspot(&mut failures),
        (Some(Hotspot::Off), None) => remove_hotspot(&mut failures),
        (None, Some((ssid, psk))) => setup_hotspot_join(&ssid, &psk, &mut failures),
        (None, None) => {}
    }
    setup_autostart(autostart, &mut failures);

    println!();
    if failures == 0 {
        println!("All done. Undo any of these by removing the files listed above, removing the user from the 'input' group, and/or deleting the QoS rules ('sudo nft delete table inet {}' or the iptables -D equivalents).", NFT_QOS_TABLE);
    } else {
        println!("Done with {} failed step(s); see messages above.", failures);
    }
    Ok(())
}

/// Runs a command, returning its stdout on success.
pub(crate) fn run_cmd(program: &str, args: &[&str]) -> Result<String> {
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

/// Runs a command with a hard timeout (via coreutils `timeout`). A busy
/// NetworkManager can park nmcli on D-Bus for many seconds (WiFi churn,
/// profile activation), and callers near critical paths must not inherit
/// that stall; expiry (exit 124) is just a command failure to them.
pub(crate) fn run_cmd_timeout(program: &str, args: &[&str], timeout_secs: u32) -> Result<String> {
    let secs = timeout_secs.to_string();
    let full_args: Vec<&str> = std::iter::once(secs.as_str())
        .chain(std::iter::once(program))
        .chain(args.iter().copied())
        .collect();
    run_cmd("timeout", &full_args)
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

/// Installs DSCP CS6 netfilter marking for monux's UDP traffic. SO_PRIORITY
/// (set by monux itself in local mode) already covers this machine's own
/// wireless egress queue, but the AP/router hop picks its downlink queue from
/// each packet's DSCP — and quinn overwrites the TOS byte per packet with its
/// ECN codepoint, so only netfilter can set a wire-level mark. nftables is
/// preferred (a dedicated table is atomic to install and remove); iptables is
/// the fallback. Idempotent; the rules don't persist across reboots.
fn setup_qos_marking(failures: &mut u32) {
    if run_cmd("nft", &["--version"]).is_ok() {
        // A complete existing install is left alone; a partial one (older
        // version, manual edits) is replaced wholesale.
        match run_cmd("nft", &[&format!("list table inet {}", NFT_QOS_TABLE)]) {
            Ok(ruleset) if nft_ruleset_has_marks(&ruleset) => {
                println!(
                    "[ok]   qos marking: DSCP CS6 rules already installed (nftables table inet {})",
                    NFT_QOS_TABLE
                );
                return;
            }
            Ok(_) => {
                let _ = run_cmd("nft", &[&format!("delete table inet {}", NFT_QOS_TABLE)]);
            }
            Err(_) => {}
        }
        for cmd in nft_qos_install_cmds() {
            if let Err(e) = run_cmd("nft", &[&cmd]) {
                *failures += 1;
                println!("[fail] qos marking: nft {}: {}", cmd, e);
                // Don't leave a half-installed table behind.
                let _ = run_cmd("nft", &[&format!("delete table inet {}", NFT_QOS_TABLE)]);
                return;
            }
        }
        println!(
            "[done] qos marking: monux UDP marked CS6 via nftables (table inet {}; covers both server and client roles; does not persist across reboots)",
            NFT_QOS_TABLE
        );
        return;
    }
    if run_cmd("iptables", &["--version"]).is_ok() {
        let mut added = 0;
        for spec in iptables_qos_rule_specs() {
            let check: Vec<&str> = ["-t", "mangle", "-C", "OUTPUT"]
                .into_iter()
                .chain(spec.iter().map(String::as_str))
                .collect();
            if run_cmd("iptables", &check).is_ok() {
                continue;
            }
            let add: Vec<&str> = ["-t", "mangle", "-A", "OUTPUT"]
                .into_iter()
                .chain(spec.iter().map(String::as_str))
                .collect();
            match run_cmd("iptables", &add) {
                Ok(_) => added += 1,
                Err(e) => {
                    *failures += 1;
                    println!("[fail] qos marking: iptables {}: {}", add.join(" "), e);
                    return;
                }
            }
        }
        if added == 0 {
            println!("[ok]   qos marking: DSCP CS6 rules already installed (iptables mangle OUTPUT)");
        } else {
            println!(
                "[done] qos marking: monux UDP marked CS6 via iptables ({} rule(s) added to mangle OUTPUT; does not persist across reboots)",
                added
            );
        }
        return;
    }
    println!("[skip] qos marking: neither nft nor iptables found; monux's SO_PRIORITY WMM marking still covers this machine's own wireless egress");
}

/// Reads a numeric /proc sysctl value, e.g. /proc/sys/net/core/rmem_max.
fn read_proc_sysctl(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Whether `iw list` reports AP mode in the card's valid interface
/// combinations (the group is spelled exactly '#{ AP }' to avoid matching
/// '#{ AP/VLAN }' — every card with AP also lists plain AP).
fn iw_supports_ap(iw_list: &str) -> bool {
    iw_list.contains("#{ AP }")
}

/// Whether a NetworkManager connection profile with this name exists.
pub(crate) fn nmcli_con_exists(name: &str) -> bool {
    run_cmd("nmcli", &["-t", "-f", "NAME", "connection", "show"])
        .map(|out| out.lines().any(|line| line == name))
        .unwrap_or(false)
}

/// Alphabet for the generated hotspot passphrase: no easily-confused
/// characters (0/O, 1/l) since it's typed once on the other machine.
const PSK_ALPHABET: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789";

/// Maps bytes onto the PSK alphabet (pure, so the generator is testable).
fn psk_from_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(16)
        .map(|b| PSK_ALPHABET[(*b % PSK_ALPHABET.len() as u8) as usize] as char)
        .collect()
}

/// A random 16-character WPA passphrase for the hotspot.
fn gen_psk() -> String {
    use std::io::Read;
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .expect("/dev/urandom is always available on Linux");
    psk_from_bytes(&bytes)
}

/// Detects active VPN tunnel interfaces from `ip -o link show up` output: a
/// VPN's policy routing and forward-dropping firewall typically break the
/// NAT that gives hotspot clients internet (the KVM link itself is fine).
fn tunnel_iface_names(ip_link_output: &str) -> Vec<String> {
    ip_link_output
        .lines()
        .filter_map(|line| line.split(':').nth(1).map(str::trim))
        .filter(|name| {
            ["wg", "tun", "tap", "zt", "tailscale", "mullvad"]
                .iter()
                .any(|prefix| name.starts_with(prefix))
        })
        .map(str::to_string)
        .collect()
}

/// Installs the workaround for Mullvad's VPN breaking the hotspot client's
/// internet: accept forwards from the hotspot subnet in Mullvad's own forward
/// chain (its drop hooks see all forwarded traffic, so the accept must live
/// in ITS table), and policy-route the subnet via the main table so the
/// client's traffic exits through the router like un-VPN'd traffic instead of
/// the tunnel. Best-effort and idempotent; a tunnel bounce wipes Mullvad's
/// table, so re-running setup re-installs (noted in the output).
fn install_vpn_workaround(failures: &mut u32) {
    let subnet = match hotspot_live_subnet() {
        Some(subnet) => subnet,
        None => {
            println!("[warn] hotspot: couldn't read the hotspot subnet yet; re-run 'monux system setup --hotspot' once the AP is up to install the VPN workaround");
            return;
        }
    };
    // nftables: accept forwards from the hotspot subnet, tagged for teardown.
    let list = run_cmd("nft", &["-a", "list", "table", "inet", "mullvad"]).unwrap_or_default();
    if list.contains(&subnet) {
        println!("[ok]   hotspot: VPN forward rule already installed");
    } else {
        let rule = format!(
            "insert rule inet mullvad forward ip saddr {} accept comment \"{}\"",
            subnet, VPN_WORKAROUND_COMMENT
        );
        match run_cmd("nft", &[&rule]) {
            Ok(_) => println!(
                "[done] hotspot: Mullvad VPN workaround installed (forward-accept for {} in its table)",
                subnet
            ),
            Err(e) => {
                *failures += 1;
                println!("[fail] hotspot: could not install the VPN forward rule: {}", e);
            }
        }
    }
    // Policy routing: the hotspot subnet uses the main table (router), ahead
    // of Mullvad's rules — the client's internet no longer depends on the
    // tunnel at all.
    let rules = run_cmd("ip", &["rule", "show"]).unwrap_or_default();
    if rules.contains(&subnet) {
        println!("[ok]   hotspot: hotspot subnet already routed around the VPN");
    } else {
        let priority = VPN_WORKAROUND_RULE_PRIORITY.to_string();
        let from = format!("from {}", subnet);
        match run_cmd("ip", &["rule", "add", &from, "lookup", "main", "priority", &priority]) {
            Ok(_) => println!(
                "[done] hotspot: hotspot subnet routed around the VPN (ip rule priority {})",
                priority
            ),
            Err(e) => {
                *failures += 1;
                println!("[fail] hotspot: could not add the ip rule: {}", e);
            }
        }
    }
    println!("[note] hotspot: Mullvad regenerates its firewall when the tunnel reconnects; re-run 'monux system setup --hotspot' then to re-install the workaround");
}

/// Removes the VPN workaround pieces (tagged nft rule + the policy rule).
/// Best-effort: anything already gone (e.g. Mullvad rewrote its table) is
/// skipped silently.
pub(crate) fn remove_vpn_workaround() {
    if let Ok(list) = run_cmd("nft", &["-a", "list", "table", "inet", "mullvad"]) {
        for handle in monux_rule_handles(&list) {
            let rule = format!("delete rule inet mullvad forward handle {}", handle);
            let _ = run_cmd("nft", &[&rule]);
        }
    }
    let priority = VPN_WORKAROUND_RULE_PRIORITY.to_string();
    let rules = run_cmd("ip", &["rule", "show"]).unwrap_or_default();
    if rules.contains(&format!("{}:", priority)) {
        let _ = run_cmd("ip", &["rule", "del", "priority", &priority]);
    }
}
/// NATs the peer through this machine, so its internet keeps working over the
/// Hosts the 'monux-direct' WiFi hotspot (--hotspot, server side): the KVM
/// link then bypasses the router entirely. NetworkManager's shared IPv4 mode
/// NATs the peer through this machine, so its internet keeps working over the
/// single radio it has; the KVM connection prefers the direct link via the
/// ordinary same-subnet match in mDNS discovery (no path code needed).
/// Idempotent; the profile is removed by 'monux system uninstall'.
fn setup_hotspot(failures: &mut u32) {
    if run_cmd("nmcli", &["--version"]).is_err() {
        println!("[skip] hotspot: NetworkManager (nmcli) not found; host a hotspot via your network stack instead");
        return;
    }
    let iw_list = match run_cmd("iw", &["list"]) {
        Ok(out) => out,
        Err(e) => {
            println!("[skip] hotspot: 'iw list' unavailable ({}); cannot check for AP support", e);
            return;
        }
    };
    if !iw_supports_ap(&iw_list) {
        println!("[skip] hotspot: the WiFi card does not support AP mode (no '#{{ AP }}' in its valid interface combinations)");
        return;
    }
    // A VPN tunnel on this machine (Mullvad/WireGuard/OpenVPN) hijacks
    // routing and drops forwarded packets: the KVM link would work, but the
    // NAT that gives hotspot clients internet would not. Mullvad's layout is
    // auto-fixed after the profile is up (install_vpn_workaround); anything
    // else gets a loud warning.
    let tunnels = run_cmd("ip", &["-o", "link", "show", "up"])
        .map(|out| tunnel_iface_names(&out))
        .unwrap_or_default();
    if nmcli_con_exists(HOTSPOT_CON_NAME) {
        println!("[ok]   hotspot: profile '{}' already exists", HOTSPOT_CON_NAME);
        handle_vpn_after_up(&tunnels, failures);
        return;
    }
    let ifname = match run_cmd("iw", &["dev"]) {
        Ok(out) => match parse_iw_interfaces(&out).into_iter().next() {
            Some(ifname) => ifname,
            None => {
                println!("[skip] hotspot: no wireless interface found");
                return;
            }
        },
        Err(e) => {
            println!("[skip] hotspot: 'iw dev' unavailable ({}); no wireless interface", e);
            return;
        }
    };
    let hostname = run_cmd("hostname", &[])
        .map(|h| h.trim().to_string())
        .unwrap_or_else(|_| "server".to_string());
    let ssid = format!("monux-direct-{}", hostname);
    let psk = gen_psk();
    if let Err(e) = run_cmd(
        "nmcli",
        &[
            "connection", "add", "type", "wifi", "ifname", &ifname, "con-name", HOTSPOT_CON_NAME,
            "autoconnect", "yes", "ssid", &ssid,
        ],
    )
    .and_then(|_| {
        run_cmd(
            "nmcli",
            &[
                "connection", "modify", HOTSPOT_CON_NAME, "wifi.mode", "ap",
                "wifi-sec.key-mgmt", "wpa-psk", "wifi-sec.psk", &psk, "ipv4.method", "shared",
            ],
        )
    })
    .and_then(|_| run_cmd("nmcli", &["connection", "up", HOTSPOT_CON_NAME]))
    {
        *failures += 1;
        println!("[fail] hotspot: {}", e);
        // Don't leave a half-configured profile behind.
        let _ = run_cmd("nmcli", &["connection", "delete", HOTSPOT_CON_NAME]);
        return;
    }
    println!("[done] hotspot: hosting '{}' (WPA2) on {} — the KVM link now bypasses the router", ssid, ifname);
    handle_vpn_after_up(&tunnels, failures);
    println!("       Join the other machine with: sudo monux system setup --hotspot-join '{}' '{}'", ssid, psk);
    println!("       Its internet keeps working through this machine (NAT); the WiFi may hiccup for a second while the AP starts. Revert with: sudo nmcli connection delete {}", HOTSPOT_CON_NAME);
}

/// After the AP is up: install the Mullvad workaround when its table is
/// present (the only layout we can auto-fix), otherwise warn loudly — the KVM
/// link works either way, but the hotspot client's internet rides on it.
fn handle_vpn_after_up(tunnels: &[String], failures: &mut u32) {
    if tunnels.is_empty() {
        return;
    }
    if run_cmd("nft", &["list", "table", "inet", "mullvad"]).is_ok() {
        install_vpn_workaround(failures);
    } else {
        println!(
            "[warn] hotspot: VPN tunnel(s) up ({}): a VPN's policy routing and forward-dropping firewall typically break the NAT that gives hotspot clients internet — the KVM link will work, the client's internet may not. Only Mullvad's layout is auto-fixed (no 'inet mullvad' table found); disconnect the VPN while using the hotspot (see README).",
            tunnels.join(", ")
        );
    }
}

/// Joins this machine to the other machine's 'monux-direct' hotspot
/// (--hotspot-join, client side). NOTE the topology change: this machine's
/// WiFi association moves to the hotspot (its previous profile reconnects
/// automatically when the hotspot is off), and its internet then flows
/// through the hosting machine.
fn setup_hotspot_join(ssid: &str, psk: &str, failures: &mut u32) {
    if run_cmd("nmcli", &["--version"]).is_err() {
        println!("[skip] hotspot: NetworkManager (nmcli) not found; join the hotspot via your network stack instead");
        return;
    }
    if nmcli_con_exists(HOTSPOT_CON_NAME) {
        println!("[ok]   hotspot: profile '{}' already exists", HOTSPOT_CON_NAME);
        return;
    }
    if let Err(e) = run_cmd(
        "nmcli",
        &[
            "connection", "add", "type", "wifi", "con-name", HOTSPOT_CON_NAME, "autoconnect",
            "yes", "connection.autoconnect-priority", "10", "ssid", ssid,
        ],
    )
    .and_then(|_| {
        run_cmd(
            "nmcli",
            &[
                "connection", "modify", HOTSPOT_CON_NAME, "wifi-sec.key-mgmt", "wpa-psk",
                "wifi-sec.psk", psk,
            ],
        )
    })
    .and_then(|_| run_cmd("nmcli", &["connection", "up", HOTSPOT_CON_NAME]))
    {
        *failures += 1;
        println!("[fail] hotspot: {}", e);
        let _ = run_cmd("nmcli", &["connection", "delete", HOTSPOT_CON_NAME]);
        return;
    }
    println!("[done] hotspot: joined '{}' — this machine's WiFi association moved to the hotspot", ssid);
    println!("       Its internet now flows through the hosting machine (NAT), and the KVM link is direct.");
    println!("       The previous WiFi profile reconnects automatically when the hotspot is off. Revert with: sudo nmcli connection delete {}", HOTSPOT_CON_NAME);
}

/// Removes the hotspot profile (either role) without touching anything else
/// (`--hotspot off`): the targeted undo for the hotspot steps, as opposed to
/// 'monux system uninstall', which removes the whole installation. Also
/// removes the VPN workaround rules, which linger independently of the
/// profile.
fn remove_hotspot(failures: &mut u32) {
    remove_vpn_workaround();
    if !nmcli_con_exists(HOTSPOT_CON_NAME) {
        println!("[ok]   hotspot: no '{}' profile installed", HOTSPOT_CON_NAME);
        return;
    }
    match run_cmd("nmcli", &["connection", "delete", HOTSPOT_CON_NAME]) {
        Ok(_) => println!("[done] hotspot: removed the '{}' profile", HOTSPOT_CON_NAME),
        Err(e) => {
            *failures += 1;
            println!("[fail] hotspot: could not remove the profile: {}", e);
        }
    }
}

/// The active hotspot's (ssid, psk), for the server to advertise to approved
/// clients (ServerEvent::HotspotInfo). Some only when the profile exists AND
/// the AP is currently up: advertising a down hotspot would flap clients
/// between profiles. Reading the passphrase needs root (the server has it).
pub fn active_hotspot_credentials() -> Option<(String, String)> {
    let active = run_cmd_timeout("nmcli", &["-t", "-f", "NAME", "connection", "show", "--active"], 5)
        .map(|out| out.lines().any(|line| line == HOTSPOT_CON_NAME))
        .unwrap_or(false);
    if !active {
        return None;
    }
    let out = run_cmd_timeout(
        "nmcli",
        &[
            "--show-secrets",
            "-t",
            "-f",
            "802-11-wireless.ssid,802-11-wireless-security.psk",
            "connection",
            "show",
            HOTSPOT_CON_NAME,
        ],
        5,
    )
    .ok()?;
    parse_hotspot_credentials(&out)
}

/// Parses nmcli -t property output into (ssid, psk); None when either is
/// missing or empty (e.g. an open/unconfigured profile).
fn parse_hotspot_credentials(output: &str) -> Option<(String, String)> {
    let mut ssid = None;
    let mut psk = None;
    for line in output.lines() {
        if let Some(value) = line.strip_prefix("802-11-wireless.ssid:") {
            ssid = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("802-11-wireless-security.psk:") {
            psk = Some(value.to_string());
        }
    }
    match (ssid, psk) {
        (Some(ssid), Some(psk)) if !ssid.is_empty() && !psk.is_empty() => Some((ssid, psk)),
        _ => None,
    }
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
    fn ap_support_detection_matches_only_plain_ap() {
        // A real combo dump from an Intel card that supports managed+AP.
        let capable = "valid interface combinations:\n\t\t * #{ managed, P2P-client } <= 2, #{ P2P-GO } <= 1, #{ P2P-device } <= 1,\n\t\t   total <= 3, #channels <= 2,\n\t\t * #{ managed, P2P-client } <= 2, #{ AP } <= 1, #{ P2P-device } <= 1,\n\t\t   total <= 3, #channels <= 1";
        assert!(iw_supports_ap(capable));
        // AP/VLAN alone must not count as AP support.
        let vlan_only = " * #{ managed } <= 1, #{ AP/VLAN } <= 1, total <= 2";
        assert!(!iw_supports_ap(vlan_only));
        let incapable = " * #{ managed } <= 1, #{ P2P-device } <= 1, total <= 2";
        assert!(!iw_supports_ap(incapable));
    }

    #[test]
    fn tunnel_iface_detection() {
        let out = "1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536\n3: wlan0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500\n4: wg0-mullvad: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1380\n5: tun0: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1500\n";
        assert_eq!(
            tunnel_iface_names(out),
            vec!["wg0-mullvad".to_string(), "tun0".to_string()]
        );
        let plain = "1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536\n3: wlan0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500\n";
        assert!(tunnel_iface_names(plain).is_empty());
        // 'wgp0' starts with 'wg' too (prefix match, like VIRTUAL_IFACE_PREFIXES).
        assert_eq!(tunnel_iface_names("2: wgpia0: <POINTOPOINT,UP>\n"), vec!["wgpia0".to_string()]);
    }

    #[test]
    fn subnet_masks_host_bits() {
        assert_eq!(subnet_of("10.42.0.1/24"), Some("10.42.0.0/24".to_string()));
        assert_eq!(subnet_of("192.168.1.187/24"), Some("192.168.1.0/24".to_string()));
        assert_eq!(subnet_of("10.64.1.5/16"), Some("10.64.0.0/16".to_string()));
        assert_eq!(subnet_of("10.42.0.1/32"), Some("10.42.0.1/32".to_string()));
        assert_eq!(subnet_of("10.42.0.1/33"), None);
        assert_eq!(subnet_of("not-an-address"), None);
        assert_eq!(subnet_of("10.42.0.300/24"), None);
    }

    #[test]
    fn tagged_rule_handles_are_found() {
        let list = "table inet mullvad {\n\tchain forward {\n\t\ttype filter hook forward priority filter; policy drop;\n\t\tip saddr 10.42.0.0/24 accept comment \"monux-hotspot\" # handle 7\n\t\tct state established,related accept # handle 2\n\t}\n}\n";
        assert_eq!(monux_rule_handles(list), vec![7]);
        let untagged = "table inet mullvad {\n\tchain forward {\n\t\tct state established,related accept # handle 2\n\t}\n}\n";
        assert!(monux_rule_handles(untagged).is_empty());
        assert!(monux_rule_handles("").is_empty());
    }

    #[test]
    fn psk_is_sixteen_safe_chars() {
        let psk = psk_from_bytes(&[0, 1, 61, 255, 42, 7, 99, 128, 3, 250, 17, 200, 30, 90, 111, 64]);
        assert_eq!(psk.len(), 16);
        assert!(psk.chars().all(|c| PSK_ALPHABET.contains(&(c as u8))));
        // No easily-confused characters ever appear.
        assert!(!psk.contains('0') && !psk.contains('1') && !psk.contains('l') && !psk.contains('o'));
        assert_eq!(psk_from_bytes(&[9; 16]), psk_from_bytes(&[9; 16]));
    }

    #[test]
    fn run_cmd_timeout_bounds_execution() {
        assert!(run_cmd_timeout("true", &[], 1).is_ok());
        assert!(run_cmd_timeout("false", &[], 1).is_err());
        // A command outliving the timeout fails (exit 124) in ~the timeout,
        // not in the command's own sweet time.
        let start = std::time::Instant::now();
        assert!(run_cmd_timeout("sleep", &["10"], 1).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn hotspot_credentials_parsing() {
        let out = "802-11-wireless.ssid:monux-direct-box\n802-11-wireless-security.psk:abc123xyz4567890\n";
        assert_eq!(
            parse_hotspot_credentials(out),
            Some(("monux-direct-box".to_string(), "abc123xyz4567890".to_string()))
        );
        // A missing or empty psk (open/unconfigured profile) advertises nothing.
        assert_eq!(parse_hotspot_credentials("802-11-wireless.ssid:monux-direct-box\n"), None);
        assert_eq!(
            parse_hotspot_credentials("802-11-wireless.ssid:monux-direct-box\n802-11-wireless-security.psk:\n"),
            None
        );
        assert_eq!(parse_hotspot_credentials(""), None);
    }

    #[test]
    fn sysctl_conf_covers_both_buffers() {
        let content = sysctl_buf_conf_content();
        assert!(content.contains("net.core.rmem_max = 2621440"));
        assert!(content.contains("net.core.wmem_max = 2621440"));
    }

    #[test]
    fn nft_install_cmds_build_table_chain_and_both_marks() {
        let cmds = nft_qos_install_cmds();
        assert_eq!(cmds.len(), 4);
        assert!(cmds[0].contains(NFT_QOS_TABLE));
        assert!(cmds[1].contains("type filter hook output priority mangle"));
        assert!(cmds.iter().any(|c| c.contains("udp sport 1213")));
        assert!(cmds.iter().any(|c| c.contains("udp dport 1213")));
        assert!(cmds.iter().filter(|c| c.contains("cs6")).count() == 2);
    }

    #[test]
    fn nft_ruleset_mark_detection() {
        let installed = "table inet monux-qos {\n\tchain output {\n\t\ttype filter hook output priority mangle; policy accept;\n\t\tmeta l4proto udp udp sport 1213 ip dscp set cs6\n\t\tmeta l4proto udp udp dport 1213 ip dscp set cs6\n\t}\n}";
        assert!(nft_ruleset_has_marks(installed));
        // Only one direction marked: incomplete, gets replaced.
        let partial = "table inet monux-qos {\n\tchain output {\n\t\tmeta l4proto udp udp sport 1213 ip dscp set cs6\n\t}\n}";
        assert!(!nft_ruleset_has_marks(partial));
        assert!(!nft_ruleset_has_marks(""));
    }

    #[test]
    fn iptables_rule_specs_cover_both_directions() {
        let specs = iptables_qos_rule_specs();
        assert_eq!(specs.len(), 2);
        for spec in &specs {
            let joined = spec.join(" ");
            assert!(joined.contains("-p udp"));
            assert!(joined.contains("1213"));
            assert!(joined.contains("-j DSCP --set-dscp-class CS6"));
        }
        assert!(specs[0].join(" ").contains("--sport 1213"));
        assert!(specs[1].join(" ").contains("--dport 1213"));
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
