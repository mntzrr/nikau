use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use regex::Regex;
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{mpsc, watch as watchchan};
use tokio::{runtime, task, time};
use tracing::{debug, error, info, warn};

use monux::device::output::OutputHandler;
use monux::device::{handles, input, output, shortcut, watch, Event};
use monux::network::{approval, transport::NetworkMode};
use monux::{client, clipboard, discovery, logging, rotation, server, single_instance};

/// Version string including the git revision (see build.rs).
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("MONUX_GIT_SHA"));

#[derive(Parser)]
#[command(
    author,
    version = format!("{} (protocol {})", VERSION, monux::msgs::shared::PROTOCOL_VERSION),
    about,
    long_about = format!(
        "{}\n\nWire protocol version: {}",
        env!("CARGO_PKG_DESCRIPTION"),
        monux::msgs::shared::PROTOCOL_VERSION
    )
)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Runs a Monux server
    Server(ServerArgs),

    /// Runs a Monux client
    Client(ClientArgs),

    /// Manages this machine's system integration for monux: persisting
    /// machine-local settings (setup) and removing monux again (uninstall)
    System(SystemArgs),

    /// Updates monux to the latest version from GitHub, rebuilding from
    /// source. The server protocol-compatibility gate is first refreshed
    /// from the mDNS advertisements of servers on the LAN.
    Update(UpdateArgs),
}

#[derive(Args)]
struct SystemArgs {
    #[command(subcommand)]
    command: SystemCommands,
}

#[derive(Subcommand)]
enum SystemCommands {
    /// Persists machine-local settings that optimize this machine for local KVM
    /// (input device access, /dev/uinput permissions, WiFi power saving,
    /// UDP socket buffers). Re-executes with sudo automatically.
    Setup,

    /// Removes monux from this machine: stops any running server/client, then
    /// removes the binary (and stale copies), the /usr/local/bin link, and the
    /// system settings persisted by 'monux system setup'. Asks before also
    /// removing ~/.config/monux (identity keypair and peer approvals).
    Uninstall,
}

#[derive(Args)]
struct ServerArgs {
    /// Keyboard shortcut for switching to the next client in the rotation
    #[arg(
        long,
        alias = "shortcut-next",
        default_value = "leftshift,leftalt,r",
        value_name = "key1,key2,key3"
    )]
    shortcut: String,

    /// Keyboard shortcut for switching to the previous client in the rotation
    #[arg(long, default_value = "leftalt,p", value_name = "key1,key2,key3")]
    shortcut_prev: Option<String>,

    /// Keyboard shortcut for switching directly to a client by its fingerprint prefix,
    /// or to the server for an empty fingerprint
    #[arg(long, value_name = "key1,key2,key3=[fingerprint-prefix]")]
    shortcut_goto: Option<Vec<String>>,

    /// Keyboard shortcut for pausing/resuming input handling. While paused,
    /// ALL input devices (keyboards included) are ungrabbed so the local
    /// machine gets raw input with monux's re-emit out of the way (games,
    /// raw-input apps); press the chord again to resume. Empty string disables.
    #[arg(long, default_value = "leftshift,leftalt,p", value_name = "key1,key2,key3")]
    pause_shortcut: String,

    /// Substring or regular expression for selecting specific devices to monitor,
    /// argument can be repeated for multiple filters
    #[arg(long, value_name = "device-name-pattern")]
    device: Option<Vec<Regex>>,

    /// Server listen IP
    #[arg(short = 'l', long, default_value = "0.0.0.0", value_name = "ip")]
    listen: IpAddr,

    /// Server port
    #[arg(short = 'p', long, default_value_t = 1213, value_name = "port")]
    port: u16,

    /// Client certificate fingerprint to automatically accept without prompting (repeat for multiple fingerprints)
    #[arg(long, alias = "fingerprints", value_name = "fingerprint")]
    fingerprint: Option<Vec<String>>,

    /// Number of seconds to wait before automatically exiting the server, to safely test configuration
    #[arg(long, value_name = "seconds")]
    exit_secs: Option<u32>,

    /// Maximum size in KB for transferring clipboard data (default: 5MB)
    #[arg(long, default_value_t = 5120, value_name = "kb")]
    max_clipboard_size_kb: u64,

    /// Use conservative tuning suitable for traversing the public internet (WWW).
    /// The default is low-latency tuning for local networks.
    #[arg(long)]
    www: bool,

    /// Target rate for forwarding pointer motion, in updates per second. Motion
    /// deltas are coalesced (summed losslessly) between updates and sent as
    /// unreliable datagrams with recent deltas repeated, so WiFi loss neither
    /// stalls nor misplaces the cursor. The default of 250 is plenty for
    /// office work; set 0 to forward every event as it comes (e.g. for gaming
    /// with a high-polling-rate mouse).
    #[arg(long, default_value_t = 250, value_name = "hz")]
    motion_hz: u32,

    /// Disable the automatic background update (on by default): a daily check
    /// at low CPU priority, then an automatic restart into the new binary.
    /// The session resumes automatically on reconnect.
    #[arg(long)]
    no_auto_update: bool,
}

#[derive(Args)]
struct ClientArgs {
    /// Server hostname or IP. If omitted, the server is discovered on the local network via mDNS.
    host: Option<String>,

    /// Server port
    #[arg(short = 'p', long, value_name = "port")]
    port: Option<u16>,

    /// Server certificate fingerprint to automatically accept without prompting (repeat for multiple fingerprints)
    #[arg(long, alias = "fingerprints", value_name = "fingerprint")]
    fingerprint: Option<Vec<String>>,

    /// Maximum size in KB for transferring clipboard data (default: 5MB)
    #[arg(long, default_value_t = 5120, value_name = "kb")]
    max_clipboard_size_kb: u64,

    /// Use conservative tuning suitable for traversing the public internet (WWW).
    /// The default is low-latency tuning for local networks.
    #[arg(long)]
    www: bool,

    /// Multiplier applied to pointer motion deltas before injecting them on
    /// this machine, for compensating DPI/sensitivity differences with the
    /// server's mouse. Sub-tick fractions are carried between events, so small
    /// scales lose no motion over time.
    #[arg(long, default_value = "1.0", value_name = "scale", value_parser = parse_input_scale)]
    mouse_scale: f64,

    /// Multiplier applied to scroll wheel deltas (including the hi-res wheel
    /// axes) before injecting them on this machine.
    #[arg(long, default_value = "1.0", value_name = "scale", value_parser = parse_input_scale)]
    scroll_scale: f64,

    /// Disable the automatic background update (on by default): a daily check
    /// at low CPU priority, then an automatic restart into the new binary.
    /// The session resumes automatically on reconnect.
    #[arg(long)]
    no_auto_update: bool,
}

#[derive(Args)]
struct UpdateArgs {
    /// Rebuild and reinstall even if already up to date, and bypass the
    /// server protocol-compatibility gate
    #[arg(long)]
    force: bool,
}

/// Accepted range for --mouse-scale/--scroll-scale: wide enough for genuine
/// DPI/sensitivity mismatches, narrow enough to catch typos.
const MIN_INPUT_SCALE: f64 = 0.05;
const MAX_INPUT_SCALE: f64 = 20.0;

/// clap value parser for the client's --mouse-scale/--scroll-scale flags.
fn parse_input_scale(s: &str) -> std::result::Result<f64, String> {
    match s.parse::<f64>() {
        Ok(v) if v.is_finite() && (MIN_INPUT_SCALE..=MAX_INPUT_SCALE).contains(&v) => Ok(v),
        _ => Err(format!(
            "scale must be a number between {} and {}",
            MIN_INPUT_SCALE, MAX_INPUT_SCALE
        )),
    }
}

/// Listens for SIGUSR1 and SIGUSR2, treating them as "switch to next client" and "switch to prev client" respectively.
/// SIGHUP dumps the server's mirrored diagnostics state to the log for troubleshooting.
/// The dump reads the mirror directly instead of going through the server event
/// loop, so it still prints when the loop itself is stalled — the exact scenario
/// the dump exists to debug.
fn handle_signals(mut signals: Signals, out: mpsc::Sender<Event>, diagnostics: Arc<rotation::DiagnosticsMirror>) {
    let mut iter = signals.into_iter();
    loop {
        match iter.next() {
            Some(signal::SIGUSR1) => {
                if let Err(e) = out.blocking_send(Event::SwitchNext) {
                    error!("Failed to submit SwitchNext event for SIGUSR1: {:?}", e);
                }
            }
            Some(signal::SIGUSR2) => {
                if let Err(e) = out.blocking_send(Event::SwitchPrev) {
                    error!("Failed to submit SwitchPrev event for SIGUSR2: {:?}", e);
                }
            }
            Some(signal::SIGHUP) => {
                diagnostics.dump();
            }
            other => {
                // None means the signal stream closed; exit instead of spinning on it.
                warn!(
                    "Unexpected signal iterator state: {:?}, exiting signal handler",
                    other
                );
                return;
            }
        }
    }
}

/// Resolves when the process receives SIGINT (ctrl-c) or SIGTERM.
async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

/// Client variant of shutdown_signal: additionally resolves on SIGUSR1, SIGUSR2
/// and SIGHUP. Those switch clients or dump diagnostics on the server (see
/// handle_signals), but have no such meaning on a client — where their default
/// action kills the process outright, skipping the cleanup that releases held
/// keys on the virtual devices (they'd stay pressed until kernel teardown).
/// Dying cleanly beats dying dirty.
async fn client_shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Failed to install SIGTERM handler");
    let mut sigusr1 = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
        .expect("Failed to install SIGUSR1 handler");
    let mut sigusr2 = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())
        .expect("Failed to install SIGUSR2 handler");
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .expect("Failed to install SIGHUP handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
        _ = sigusr1.recv() => {}
        _ = sigusr2.recv() => {}
        _ = sighup.recv() => {}
    }
}

/// The server protocol version to gate an update on: the minimum of the
/// versions Monux servers currently advertise via mDNS (also recorded, healing
/// a stale gate file), falling back to the version this client recorded at its
/// last handshake when no server answers (offline, another subnet, or a build
/// predating the advertisement). Never fails: discovery is best-effort.
fn refresh_protocol_constraint(config_dir: Option<&Path>) -> Option<u64> {
    let recorded = config_dir.and_then(monux::update::server_protocol_constraint);
    let discovered = match monux::discovery::discover_server_protocol_versions() {
        Ok(versions) => versions,
        Err(e) => {
            debug!(
                "Server protocol version discovery failed ({}); using the recorded gate value",
                e
            );
            return recorded;
        }
    };
    let constraint = match monux::discovery::protocol_version_constraint(&discovered) {
        Some(constraint) => constraint,
        // No server answered: fall back to the last recorded version.
        None => return recorded,
    };
    if discovered.len() > 1 {
        info!(
            "Monux servers advertise different protocol versions ({}); gating on the oldest, v{}",
            discovered
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            constraint
        );
    }
    if recorded != Some(constraint) {
        info!(
            "Refreshed the server protocol version gate via mDNS: v{} -> v{}",
            recorded
                .map(|v| v.to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            constraint
        );
    }
    if let Some(dir) = config_dir {
        let _ = fs::create_dir_all(dir);
        monux::update::record_server_protocol_version(dir, constraint);
    }
    Some(constraint)
}

fn main() -> Result<()> {
    logging::init_logging();
    let cli = Cli::parse();
    // Record the exact build in the log: invaluable when diagnosing bug reports.
    info!("monux v{} starting", VERSION);

    // System commands and update don't need the config dir, devices, or the
    // async runtime.
    match &cli.command {
        Commands::System(args) => match &args.command {
            SystemCommands::Setup => {
                maybe_elevate("to persist system settings")?;
                return monux::setup::run();
            }
            SystemCommands::Uninstall => {
                return monux::uninstall::run();
            }
        },
        Commands::Update(args) => {
            // Gate on the server's protocol version when this machine acts as
            // a client, so an update can't break the connection. The version
            // recorded at the last handshake can be stale (the server upgraded
            // while this client was away), so refresh it from the servers'
            // mDNS advertisements first; the config dir may not exist yet, the
            // constraint is simply absent then.
            let constraint = if args.force {
                // --force bypasses the gate; skip the discovery delay.
                None
            } else {
                let config_dir = home::home_dir().map(|h| h.join(".config").join("monux"));
                refresh_protocol_constraint(config_dir.as_deref())
            };
            return monux::update::run(args.force, false, constraint).map(|_| ());
        }
        _ => {}
    }

    let config_dir = init_config_dir()?;

    let rt = Arc::new(
        runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime"),
    );

    match cli.command {
        Commands::System(_) | Commands::Update(_) => {
            unreachable!("system commands and update are handled before runtime initialization")
        }
        Commands::Server(args) => {
            if args.port == 0 {
                bail!("--port 0 (ephemeral port) is not supported: the mDNS advertisement must match the actual listen port");
            }
            let server_lock = single_instance::acquire("server")?;
            settle_after_takeover(&server_lock);
            if !args.no_auto_update {
                // The server leads protocol upgrades: no compatibility gate.
                rt.spawn(monux::autoupdate::run(None));
            }
            let fingerprint = Arc::new(Mutex::new(None));
            let verifier = approval::MonuxCertVerification::new(
                "server",
                args.fingerprint.unwrap_or(vec![]),
                &config_dir,
                fingerprint.clone(),
                // No interactive approval prompts when facing the public internet:
                // unknown peers must be pre-approved via --fingerprints instead.
                !args.www,
            )?;
            info!(
                "Our certificate fingerprint: {} (pre-approve this server on clients with '--fingerprints {}')",
                verifier.our_fingerprint(),
                verifier.our_fingerprint()
            );
            let mode = if args.www {
                NetworkMode::Www
            } else {
                NetworkMode::Local
            };
            let max_clipboard_size_bytes = args
                .max_clipboard_size_kb
                .checked_mul(1024)
                .context("--max-clipboard-size-kb is too large")?;
            let motion_flush_interval =
                (args.motion_hz > 0).then(|| Duration::from_secs_f64(1.0 / args.motion_hz as f64));
            if motion_flush_interval.is_some() {
                info!(
                    "Coalescing pointer motion to {} updates/s (--motion-hz 0 to disable)",
                    args.motion_hz
                );
            }
            rt.block_on(async {
                server(
                    config_dir,
                    SocketAddr::new(args.listen, args.port),
                    &args.shortcut,
                    args.shortcut_prev.as_deref(),
                    args.shortcut_goto.unwrap_or(vec![]),
                    // An empty --pause-shortcut disables pause/resume.
                    if args.pause_shortcut.trim().is_empty() {
                        None
                    } else {
                        Some(args.pause_shortcut.as_str())
                    },
                    args.device.unwrap_or(vec![]),
                    args.exit_secs,
                    verifier,
                    fingerprint,
                    max_clipboard_size_bytes,
                    mode,
                    motion_flush_interval,
                )
                .await
            })?;
        }
        Commands::Client(args) => {
            let client_lock = single_instance::acquire("client")?;
            settle_after_takeover(&client_lock);
            if !args.no_auto_update {
                rt.spawn(monux::autoupdate::run(Some(config_dir.clone())));
            }
            // When no host is given, the server address comes from mDNS discovery,
            // which allows re-discovering it after repeated connection failures.
            let from_discovery = args.host.is_none();
            let port = args.port.unwrap_or(1213);
            // Server instance name from mDNS discovery, for the approval prompt.
            let mut discovered_server_name: Option<String> = None;
            let connect_addr: SocketAddr = match &args.host {
                Some(host) => {
                    if let Ok(host_ip) = host.parse::<IpAddr>() {
                        // It's an IP.
                        SocketAddr::new(host_ip, port)
                    } else {
                        // Its a hostname? Try resolving it.
                        let mut socket_addrs = format!("{}:{}", host, port)
                            .to_socket_addrs()
                            .map_err(|e| anyhow!("Failed to resolve --host={}: {:?}", host, e))?;
                        if let Some(first) = socket_addrs.next() {
                            first
                        } else {
                            bail!("Provided --host={} didn't resolve to an IP", host);
                        }
                    }
                }
                None => {
                    if args.port.is_some() {
                        warn!("--port is ignored when the server is auto-discovered via mDNS");
                    }
                    // Discover the server on the local network via mDNS.
                    info!("No server host provided, discovering via mDNS...");
                    let (addr, name) =
                        rt.block_on(async { discovery::discover_server(None).await })?;
                    discovered_server_name = Some(name);
                    addr
                }
            };
            let verifier = approval::MonuxCertVerification::new(
                "client",
                args.fingerprint.unwrap_or(vec![]),
                &config_dir,
                Arc::new(Mutex::new(None)),
                // The client connects outbound to a server it chose, so interactive
                // approval prompts stay enabled even in --www mode (unlike the server).
                true,
            )?;
            if let Some(name) = discovered_server_name {
                verifier.set_discovered_server_name(name);
            }
            info!(
                "Our certificate fingerprint: {} (pre-approve this client on the server with '--fingerprints {}')",
                verifier.our_fingerprint(),
                verifier.our_fingerprint()
            );
            let mode = if args.www {
                NetworkMode::Www
            } else {
                NetworkMode::Local
            };
            let max_clipboard_size_bytes = args
                .max_clipboard_size_kb
                .checked_mul(1024)
                .context("--max-clipboard-size-kb is too large")?;
            if args.mouse_scale != 1.0 || args.scroll_scale != 1.0 {
                info!(
                    "Scaling injected input: pointer motion x{}, scroll x{}",
                    args.mouse_scale, args.scroll_scale
                );
            }
            rt.block_on(async {
                client(
                    config_dir,
                    connect_addr,
                    verifier,
                    max_clipboard_size_bytes,
                    mode,
                    from_discovery,
                    args.mouse_scale,
                    args.scroll_scale,
                )
                .await
            })?;
        }
    }
    // A background auto-update may have scheduled a restart (autoupdate.rs):
    // the graceful shutdown above has completed, so replace this process with
    // the freshly installed binary.
    if monux::autoupdate::restart_scheduled() {
        reexec_after_update()?;
    }
    Ok(())
}

/// Replaces this process image with the freshly installed monux binary after
/// a background auto-update. execve preserves our pid, args and environment
/// and closes our (CLOEXEC) fds, releasing the single-instance lock, keyboard
/// grabs and virtual devices for the new image in one atomic step.
/// MONUX_RESTARTED tells the new image to let udev settle before creating its
/// virtual devices (the same teardown/create race as a take-over restart).
fn reexec_after_update() -> Result<()> {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe()
        .context("Failed to find our own executable for the post-update restart")?;
    // The update replaced the binary on disk while we were running, so Linux
    // reports our exe as "<path> (deleted)"; the plain path is the new binary.
    let exe = exe.to_string_lossy().trim_end_matches(" (deleted)").to_string();
    info!("Restarting into the updated monux ({})...", exe);
    let err = std::process::Command::new(&exe)
        .args(std::env::args_os().skip(1))
        .env("MONUX_RESTARTED", "1")
        .exec();
    Err(anyhow!(
        "Failed to restart into the updated monux ({}): {}",
        exe,
        err
    ))
}

/// After taking over from a previous instance (or re-exec'ing ourselves after
/// an auto-update), wait for udev to finish processing the previous instance's
/// virtual-device teardown before we create ours. Without this, rapid restarts
/// race: the old devices' evdev remove events can reach the compositor after
/// the new devices' add events for the same devpath, making the compositor
/// drop or never register our brand-new virtual keyboard (seen in the wild as
/// all keyboard input going dead after a few restarts; 'hyprctl reload' makes
/// it reappear).
fn settle_after_takeover(lock: &single_instance::InstanceLock) {
    // A re-exec after an auto-update (MONUX_RESTARTED) releases the lock
    // atomically, so took_over is false — but the old image's virtual devices
    // were torn down at the same instant, so the same udev race applies.
    if !lock.took_over && std::env::var_os("MONUX_RESTARTED").is_none() {
        return;
    }
    // udevadm settle waits for udev's event queue to drain, so the old remove
    // events are emitted before our new add events (monitor order is preserved
    // for libinput/the compositor). Fall back to a plain sleep if unavailable.
    let settled = std::process::Command::new("udevadm")
        .args(["settle", "--timeout=2000"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if settled {
        info!("Settled before creating virtual devices (udev queue drained)");
    } else {
        info!("Settling briefly before creating virtual devices");
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

/// 'monux system setup' persists system settings and needs root. Rather than
/// making the user type 'sudo monux system setup' (which also trips over
/// sudo's restricted PATH hiding ~/.local/bin), re-exec with sudo -E,
/// prompting for the password.
/// Opt out with MONUX_NO_ELEVATE=1 to get the manual invocation instead.
fn maybe_elevate(reason: &str) -> Result<()> {
    if unsafe { libc::geteuid() } == 0 || std::env::var_os("MONUX_NO_ELEVATE").is_some() {
        return Ok(());
    }
    let exe = std::env::current_exe()
        .context("Failed to find our own executable for sudo re-exec")?;
    info!("Re-executing with sudo {} (MONUX_NO_ELEVATE=1 to opt out)...", reason);
    let status = std::process::Command::new("sudo")
        .arg("-E")
        .arg(&exe)
        .args(std::env::args().skip(1))
        .status()
        .context("Failed to re-exec with sudo")?;
    std::process::exit(status.code().unwrap_or(1));
}

fn init_config_dir() -> Result<PathBuf> {
    let mut homedir = home::home_dir().context("No home dir found: Unable to store certs")?;
    homedir.push(".config");
    let new_dir = homedir.join("monux");
    // One-time migration from the pre-rename config dir, preserving the
    // keypair (our identity) and known_certs (peer approvals).
    let old_dir = homedir.join("nikau");
    if !new_dir.exists() && old_dir.exists() {
        fs::rename(&old_dir, &new_dir).with_context(|| {
            format!(
                "Failed to migrate config directory from {} to {}",
                old_dir.display(),
                new_dir.display()
            )
        })?;
        info!(
            "Migrated config directory from {} to {}",
            old_dir.display(),
            new_dir.display()
        );
    }
    fs::create_dir_all(&new_dir)
        .with_context(|| format!("Failed to create config directory: {}", new_dir.display()))?;
    Ok(new_dir)
}

async fn server(
    config_dir: PathBuf,
    listen_addr: SocketAddr,
    keys_next: &str,
    keys_prev: Option<&str>,
    keys_goto: Vec<String>,
    keys_pause: Option<&str>,
    device_filters: Vec<Regex>,
    exit_secs: Option<u32>,
    verifier: Arc<approval::MonuxCertVerification<'static>>,
    fingerprint: Arc<Mutex<Option<String>>>,
    max_clipboard_size_bytes: u64,
    mode: NetworkMode,
    motion_flush_interval: Option<Duration>,
) -> Result<()> {
    // Try to set up virtual devices up-front - exit early if we can't access uinput
    let mut output_handler = output::uinput::VirtualUInputDevices::new()
        .context("Failed to create virtual devices for output, possible solutions:
- Add your user to the 'input' group and log back in: 'sudo usermod -aG input $USER'
- Enable uinput and/or evdev in the kernel, check for /dev/uinput and /dev/input/
- As a fallback, run as root with 'sudo -E monux server ...' (-E keeps clipboard support)")?;
    let virtual_nodes = output_handler.device_nodes();
    info!(
        "Virtual device nodes: {}",
        virtual_nodes
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let (event_tx, event_rx): (mpsc::Sender<Event>, mpsc::Receiver<Event>) = mpsc::channel(256);

    // Mirrored diagnostics state: the rotation loop refreshes it as it goes,
    // and the SIGHUP handler dumps it without involving the loop.
    let diagnostics = Arc::new(rotation::DiagnosticsMirror::new());
    let event_tx2 = event_tx.clone();
    let diagnostics2 = diagnostics.clone();
    let signals = Signals::new([signal::SIGUSR1, signal::SIGUSR2, signal::SIGHUP])?;
    std::thread::spawn(move || handle_signals(signals, event_tx2, diagnostics2));

    let (grab_tx, _grab_rx) = watchchan::channel(monux::device::GrabState {
        client_active: false,
        paused: false,
    });
    let grab_tx2 = grab_tx.clone();

    let key_combos = shortcut::parse_key_combos(keys_next, keys_prev, keys_goto, keys_pause)?;
    if let Some(kp) = keys_pause {
        info!("Pause/resume shortcut: {} (ungrabs ALL devices; press again to resume)", kp);
    }
    let input_handler = input::InputHandler::new(&key_combos, event_tx)?;

    let watch_handle = task::spawn(async move {
        let device_handles =
            handles::DeviceHandles::new(input_handler, grab_tx, key_combos.all_keys);
        watch::watch_loop(device_handles, device_filters, virtual_nodes)
            .await
            .context(
                "Failed to listen to any input devices, possible solutions:
- Are any input devices (keyboard, mouse, etc) plugged into the machine?
- If any '--device' filters are specified, they might be filtering out all current devices",
            )
    });

    let (rotation_tx, rotation_rx) = mpsc::channel::<rotation::RotationEvent>(256);
    let rotation_tx2 = rotation_tx.clone();
    let server_events_handle = task::spawn(async move {
        server::run_server_events_loop(
            config_dir,
            event_rx,
            grab_tx2,
            output_handler,
            // Max compressed clipboard size over the wire
            max_clipboard_size_bytes,
            // Max uncompressed clipboard size, just in case
            10 * max_clipboard_size_bytes,
            rotation_tx,
            rotation_rx,
            motion_flush_interval,
            diagnostics,
        )
        .await
    });
    let server_connections_handle = task::spawn(async move {
        server::run_server_connections_loop(
            &listen_addr,
            verifier,
            fingerprint,
            max_clipboard_size_bytes,
            rotation_tx2,
            mode,
        )
        .await
    });

    // Advertise the server on the local network so that clients can discover it.
    let _mdns_registration = match discovery::DiscoveryRegistration::register(listen_addr) {
        Ok(r) => Some(r),
        Err(e) => {
            warn!("Failed to register mDNS service for LAN discovery: {}", e);
            None
        }
    };

    info!("Listening for clients: {}", listen_addr);
    if let Ok(ips) = discovery::advertise_ips(listen_addr.ip()) {
        if !ips.is_empty() {
            info!(
                "Local IP address(es) for clients: {}; connect with 'monux client {}' or omit the address for mDNS auto-discovery",
                ips.iter().map(|ip| ip.to_string()).collect::<Vec<_>>().join(", "),
                ips[0]
            );
        }
    }
    if let Some(exit_secs) = exit_secs {
        info!("Exiting in {} seconds...", exit_secs);
        tokio::select! {
            watch_exit = watch_handle => {
                watch_exit?.context("Failed to watch input events, exiting early")?
            },
            server_events_exit = server_events_handle => {
                server_events_exit?.context("Server events loop failed, exiting early")?
            },
            server_connections_exit = server_connections_handle => {
                server_connections_exit?.context("Server connections loop failed, exiting early")?
            },
            _timeout = time::sleep(Duration::from_secs(exit_secs as u64)) => {
                info!("Exiting automatically as requested (--exit-secs={})", exit_secs);
            },
            _signal = shutdown_signal() => {
                // Dropping _mdns_registration here sends the mDNS goodbye.
                // The active-client state file is deliberately left in place:
                // a restart (e.g. after 'monux update') resumes the session
                // automatically when the client reconnects (bounded by
                // ACTIVE_CLIENT_MAX_AGE).
                info!("Shutting down...");
                return Ok(());
            },
        };
    } else {
        tokio::select! {
            watch_exit = watch_handle => {
                watch_exit?.context("Failed to watch input events, exiting")?
            },
            server_events_exit = server_events_handle => {
                server_events_exit?.context("Server events loop failed, exiting early")?
            },
            server_connections_exit = server_connections_handle => {
                server_connections_exit?.context("Server connections loop failed, exiting early")?
            },
            _signal = shutdown_signal() => {
                // Dropping _mdns_registration here sends the mDNS goodbye.
                // The active-client state file is deliberately left in place:
                // a restart (e.g. after 'monux update') resumes the session
                // automatically when the client reconnects (bounded by
                // ACTIVE_CLIENT_MAX_AGE).
                info!("Shutting down...");
                return Ok(());
            },
        }
    }
    Ok(())
}

/// A failed connection that had survived beyond this was a healthy session: its
/// loss is a fresh network event, not a persistent failure — it neither counts
/// toward mDNS re-discovery nor keeps the reconnect backoff elevated.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);

/// Cap for the reconnect backoff: the first retry after a failure is immediate,
/// then the delay doubles (1s, 2s, ...) up to this.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

async fn client(
    config_dir: PathBuf,
    connect_addr: SocketAddr,
    verifier: Arc<approval::MonuxCertVerification<'static>>,
    max_clipboard_size_bytes: u64,
    mode: NetworkMode,
    from_discovery: bool,
    mouse_scale: f64,
    scroll_scale: f64,
) -> Result<()> {
    // Try to set up virtual devices up-front - exit early if we can't access uinput
    let mut output_handler = output::uinput::VirtualUInputDevices::new()
        .context("Failed to create virtual devices for output, possible solutions:
- Add your user to the 'input' group and log back in: 'sudo usermod -aG input $USER'
- Enable uinput and/or evdev in the kernel, check for /dev/uinput and /dev/input/
- As a fallback, run as root with 'sudo -E monux client ...' (-E keeps clipboard support)")?;
    let max_uncompressed_size_bytes = 10 * max_clipboard_size_bytes;
    let mut local_clipboard = clipboard::client::LocalClipboard::new(
        config_dir.clone(),
        max_uncompressed_size_bytes,
    ).await;

    let mut connect_addr = connect_addr;
    let mut consecutive_failures = 0u32;
    // Delay before the next reconnect attempt: the first retry after a failure
    // is immediate, then the delay doubles per failure (1s, 2s, ...) up to
    // MAX_RECONNECT_BACKOFF. A lost healthy session resets it to immediate.
    let mut reconnect_backoff = Duration::ZERO;
    // Keep one set of signal handlers registered across reconnect attempts.
    let shutdown = client_shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        info!("Connecting to server: {}", connect_addr);
        let connected_at = Instant::now();
        tokio::select! {
            run_result = client::run(
                &connect_addr,
                verifier.clone(),
                max_clipboard_size_bytes,
                &mut local_clipboard,
                &mut output_handler,
                mode,
                &config_dir,
                mouse_scale,
                scroll_scale,
            ) => {
                // client::run only returns on failure (its loop never exits otherwise).
                if let Err(e) = run_result {
                    error!("Client error: {:?}", e);
                }
                // Clear any clipboard status that may have been accumulated while active
                if let Some(lc) = &mut local_clipboard {
                    if let Err(e) = lc.clear_remote_clipboard() {
                        warn!("Failed to clear remote clipboard: {}", e);
                    }
                }
                // Release any keys still held on the virtual devices so they don't
                // stay stuck while we're disconnected.
                if let Err(e) = output_handler.release_all().await {
                    warn!("Failed to release held keys after connection loss: {:?}", e);
                }
                if connected_at.elapsed() > HEALTHY_SESSION {
                    // The lost connection was a healthy session: start over with
                    // a clean failure count and an immediate retry.
                    consecutive_failures = 0;
                    reconnect_backoff = Duration::ZERO;
                } else {
                    consecutive_failures += 1;
                }
                if from_discovery && consecutive_failures >= 3 {
                    // The discovered address may be stale (server restarted elsewhere,
                    // DHCP lease change, ...): try discovering the server again.
                    warn!(
                        "{} consecutive connection failures, re-running mDNS discovery",
                        consecutive_failures
                    );
                    match discovery::discover_server(None).await {
                        Ok((new_addr, new_name)) => {
                            if new_addr != connect_addr {
                                info!(
                                    "Discovered server at new address: {} (was {})",
                                    new_addr, connect_addr
                                );
                            }
                            connect_addr = new_addr;
                            verifier.set_discovered_server_name(new_name);
                        }
                        Err(e) => {
                            warn!(
                                "Re-discovery failed, keeping previous address {}: {:?}",
                                connect_addr, e
                            );
                        }
                    }
                    consecutive_failures = 0;
                }
                // Back off before retrying (immediate on the first failure);
                // the next delay doubles, capped at MAX_RECONNECT_BACKOFF.
                time::sleep(reconnect_backoff).await;
                reconnect_backoff = if reconnect_backoff.is_zero() {
                    Duration::from_secs(1)
                } else {
                    (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF)
                };
            },
            _ = &mut shutdown => {
                // Same cleanup as the connection-loss path, then exit.
                if let Some(lc) = &mut local_clipboard {
                    if let Err(e) = lc.clear_remote_clipboard() {
                        warn!("Failed to clear remote clipboard: {}", e);
                    }
                }
                if let Err(e) = output_handler.release_all().await {
                    warn!("Failed to release held keys after connection loss: {:?}", e);
                }
                info!("Shutting down...");
                return Ok(());
            },
        }
    }
}
