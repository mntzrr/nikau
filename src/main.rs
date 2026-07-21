use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use regex::Regex;
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{mpsc, watch as watchchan};
use tokio::{runtime, task, time};
use tracing::{error, info, warn};

use monux::device::output::OutputHandler;
use monux::device::{handles, input, output, shortcut, watch, Event};
use monux::network::{approval, transport::NetworkMode};
use monux::{client, clipboard, discovery, logging, rotation, server, single_instance};

/// Version string including the git revision (see build.rs).
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("MONUX_GIT_SHA"));

#[derive(Parser)]
#[command(
    author,
    version = VERSION,
    about,
    long_about = None
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

    /// Persists machine-local settings that optimize this machine for local KVM
    /// (input device access, /dev/uinput permissions, WiFi power saving,
    /// UDP socket buffers). Re-executes with sudo automatically.
    Setup,

    /// Updates monux to the latest version from GitHub, rebuilding from source
    Update(UpdateArgs),
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
    /// deltas are coalesced (summed losslessly) between updates: the cursor ends
    /// up in the same place with far less network and CPU load. The default of
    /// 125 is plenty for office work; set 0 to forward every event as it comes
    /// (e.g. for gaming with a high-polling-rate mouse).
    #[arg(long, default_value_t = 125, value_name = "hz")]
    motion_hz: u32,

    /// Automatically check for updates and install them in the background
    /// (daily, at low CPU priority). Never restarts the running session;
    /// you'll get a desktop notification when a restart is due.
    #[arg(long)]
    auto_update: bool,
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

    /// Automatically check for updates and install them in the background
    /// (daily, at low CPU priority). Never restarts the running session;
    /// you'll get a desktop notification when a restart is due.
    #[arg(long)]
    auto_update: bool,
}

#[derive(Args)]
struct UpdateArgs {
    /// Rebuild and reinstall even if already up to date
    #[arg(long)]
    force: bool,
}

/// Listens for SIGUSR1 and SIGUSR2, treating them as "switch to next client" and "switch to prev client" respectively.
/// SIGHUP dumps the server's internal state to the log for troubleshooting.
fn handle_signals(mut signals: Signals, out: mpsc::Sender<Event>) {
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
                if let Err(e) = out.blocking_send(Event::DumpDiagnostics) {
                    error!("Failed to submit DumpDiagnostics event for SIGHUP: {:?}", e);
                }
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

fn main() -> Result<()> {
    logging::init_logging();
    let cli = Cli::parse();
    // Record the exact build in the log: invaluable when diagnosing bug reports.
    info!("monux v{} starting", VERSION);

    // Setup and update don't need the config dir, devices, or the async runtime.
    match &cli.command {
        Commands::Setup => {
            maybe_elevate("to persist system settings")?;
            return monux::setup::run();
        }
        Commands::Update(args) => return monux::update::run(args.force, false),
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
        Commands::Setup | Commands::Update(_) => {
            unreachable!("setup and update are handled before runtime initialization")
        }
        Commands::Server(args) => {
            if args.port == 0 {
                bail!("--port 0 (ephemeral port) is not supported: the mDNS advertisement must match the actual listen port");
            }
            let server_lock = single_instance::acquire("server")?;
            settle_after_takeover(&server_lock);
            if args.auto_update {
                rt.spawn(monux::autoupdate::run());
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
            if args.auto_update {
                rt.spawn(monux::autoupdate::run());
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
            rt.block_on(async {
                client(
                    config_dir,
                    connect_addr,
                    verifier,
                    max_clipboard_size_bytes,
                    mode,
                    from_discovery,
                )
                .await
            })?;
        }
    }
    Ok(())
}

/// After taking over from a previous instance, wait for udev to finish
/// processing the previous instance's virtual-device teardown before we
/// create ours. Without this, rapid restarts race: the old devices' evdev
/// remove events can reach the compositor after the new devices' add events
/// for the same devpath, making the compositor drop or never register our
/// brand-new virtual keyboard (seen in the wild as all keyboard input going
/// dead after a few restarts; 'hyprctl reload' makes it reappear).
fn settle_after_takeover(lock: &single_instance::InstanceLock) {
    if !lock.took_over {
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
        info!("Settled after taking over from the previous instance (udev queue drained)");
    } else {
        info!("Settling briefly after taking over from the previous instance");
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

/// 'monux setup' persists system settings and needs root. Rather than making
/// the user type 'sudo monux setup' (which also trips over sudo's restricted
/// PATH hiding ~/.local/bin), re-exec with sudo -E, prompting for the password.
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

    let event_tx2 = event_tx.clone();
    let signals = Signals::new([signal::SIGUSR1, signal::SIGUSR2, signal::SIGHUP])?;
    std::thread::spawn(|| handle_signals(signals, event_tx2));

    let (grab_tx, _grab_rx) = watchchan::channel(monux::device::GrabEvent::Ungrab);
    let grab_tx2 = grab_tx.clone();

    let key_combos = shortcut::parse_key_combos(keys_next, keys_prev, keys_goto)?;
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

async fn client(
    config_dir: PathBuf,
    connect_addr: SocketAddr,
    verifier: Arc<approval::MonuxCertVerification<'static>>,
    max_clipboard_size_bytes: u64,
    mode: NetworkMode,
    from_discovery: bool,
) -> Result<()> {
    // Try to set up virtual devices up-front - exit early if we can't access uinput
    let mut output_handler = output::uinput::VirtualUInputDevices::new()
        .context("Failed to create virtual devices for output, possible solutions:
- Add your user to the 'input' group and log back in: 'sudo usermod -aG input $USER'
- Enable uinput and/or evdev in the kernel, check for /dev/uinput and /dev/input/
- As a fallback, run as root with 'sudo -E monux client ...' (-E keeps clipboard support)")?;
    let max_uncompressed_size_bytes = 10 * max_clipboard_size_bytes;
    let mut local_clipboard = clipboard::client::LocalClipboard::new(
        config_dir,
        max_uncompressed_size_bytes,
    ).await;

    let mut connect_addr = connect_addr;
    let mut consecutive_failures = 0u32;
    // Keep one set of signal handlers registered across reconnect attempts.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        info!("Connecting to server: {}", connect_addr);
        tokio::select! {
            run_result = client::run(
                &connect_addr,
                verifier.clone(),
                max_clipboard_size_bytes,
                &mut local_clipboard,
                &mut output_handler,
                mode,
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
                consecutive_failures += 1;
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
                // Wait a bit before retrying. Often happens when waiting for server to approve the cert.
                time::sleep(Duration::from_secs(5)).await
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
