use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use regex::Regex;
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{broadcast, mpsc};
use tokio::{runtime, task, time};
use tracing::{error, info, warn};

use nikau::device::{handles, input, output, shortcut, watch, Event};
use nikau::network::approval;
use nikau::{client, clipboard, logging, rotation, server};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Runs a Nikau server
    Server(ServerArgs),

    /// Runs a Nikau client
    Client(ClientArgs),
}

#[derive(Args)]
struct ServerArgs {
    /// Keyboard shortcut for switching to the next client in the rotation
    #[arg(
        long,
        alias = "shortcut-next",
        default_value = "leftalt,n",
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
}

#[derive(Args)]
struct ClientArgs {
    /// Server hostname or IP
    host: String,

    /// Server port
    #[arg(short = 'p', long, default_value_t = 1213, value_name = "port")]
    port: u16,

    /// Server certificate fingerprint to automatically accept without prompting (repeat for multiple fingerprints)
    #[arg(long, alias = "fingerprints", value_name = "fingerprint")]
    fingerprint: Option<Vec<String>>,

    /// Maximum size in KB for transferring clipboard data (default: 5MB)
    #[arg(long, default_value_t = 5120, value_name = "kb")]
    max_clipboard_size_kb: u64,
}

/// Listens for SIGUSR1 and SIGUSR2, treating them as "switch to next client" and "switch to prev client" respectively.
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
            other => {
                info!("no signals here? {:?}", other);
            }
        }
    }
}

fn main() -> Result<()> {
    logging::init_logging();
    let cli = Cli::parse();
    let config_dir = init_config_dir()?;

    let rt = Arc::new(
        runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime"),
    );

    match cli.command {
        Commands::Server(args) => {
            let fingerprint = Arc::new(Mutex::new(None));
            let verifier = approval::NikauCertVerification::new(
                "server",
                args.fingerprint.unwrap_or(vec![]),
                &config_dir,
                fingerprint.clone(),
            )?;
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
                    args.max_clipboard_size_kb * 1024,
                )
                .await
            })?;
        }
        Commands::Client(args) => {
            let connect_addr: SocketAddr = if let Ok(host_ip) = args.host.parse::<IpAddr>() {
                // It's an IP.
                SocketAddr::new(host_ip, args.port)
            } else {
                // Its a hostname? Try resolving it.
                let mut socket_addrs = format!("{}:{}", args.host, args.port)
                    .to_socket_addrs()
                    .map_err(|e| anyhow!("Failed to resolve --host={}: {:?}", args.host, e))?;
                if let Some(first) = socket_addrs.next() {
                    first
                } else {
                    bail!("Provided --host={} didn't resolve to an IP", args.host);
                }
            };
            let verifier = approval::NikauCertVerification::new(
                "client",
                args.fingerprint.unwrap_or(vec![]),
                &config_dir,
                Arc::new(Mutex::new(None)),
            )?;
            rt.block_on(async {
                client(
                    config_dir,
                    connect_addr,
                    verifier,
                    args.max_clipboard_size_kb * 1024,
                )
                .await
            })?;
        }
    }
    Ok(())
}

fn init_config_dir() -> Result<PathBuf> {
    let mut homedir = home::home_dir().context("No home dir found: Unable to store certs")?;
    homedir.push(".config");
    homedir.push("nikau");
    fs::create_dir_all(&homedir)
        .with_context(|| format!("Failed to create config directory: {}", homedir.display()))?;
    Ok(homedir)
}

async fn server(
    config_dir: PathBuf,
    listen_addr: SocketAddr,
    keys_next: &str,
    keys_prev: Option<&str>,
    keys_goto: Vec<String>,
    device_filters: Vec<Regex>,
    exit_secs: Option<u32>,
    verifier: Arc<approval::NikauCertVerification<'static>>,
    fingerprint: Arc<Mutex<Option<String>>>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    // Try to set up virtual devices up-front - exit early if we aren't root
    let output_handler = output::uinput::VirtualUInputDevices::new()
        .context("Failed to create virtual devices for output, possible solutions:
- The server may need to be run as root with 'sudo -E nikau server ...' to allow creating virtual devices.
- Enable uinput and/or evdev in the kernel, check for /dev/uinput and /dev/input/")?;

    let (event_tx, event_rx): (mpsc::Sender<Event>, mpsc::Receiver<Event>) = mpsc::channel(256);

    let event_tx2 = event_tx.clone();
    let signals = Signals::new([signal::SIGUSR1, signal::SIGUSR2])?;
    std::thread::spawn(|| handle_signals(signals, event_tx2));

    let (grab_tx, _grab_rx) = broadcast::channel(1);
    let grab_tx2 = grab_tx.clone();

    let key_combos = shortcut::parse_key_combos(keys_next, keys_prev, keys_goto)?;
    let input_handler = input::InputHandler::new(&key_combos, event_tx)?;

    let watch_handle = task::spawn(async move {
        let device_handles =
            handles::DeviceHandles::new(input_handler, grab_tx, key_combos.all_keys);
        watch::watch_loop(device_handles, device_filters)
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
        )
        .await
    });

    info!("Listening for clients: {}", listen_addr);
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
        }
    }
    Ok(())
}

async fn client(
    config_dir: PathBuf,
    connect_addr: SocketAddr,
    verifier: Arc<approval::NikauCertVerification<'static>>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    // Try to set up virtual devices up-front - exit early if we aren't root
    let mut output_handler = output::uinput::VirtualUInputDevices::new()
        .context("Failed to create virtual devices for output, possible solutions:
- The client may need to be run as root with 'sudo -E nikau client ...' to allow creating virtual devices.
- Enable uinput and/or evdev in the kernel, check for /dev/uinput and /dev/input/")?;
    let max_uncompressed_size_bytes = 10 * max_clipboard_size_bytes;
    let mut local_clipboard = clipboard::client::LocalClipboard::new(
        config_dir,
        max_uncompressed_size_bytes,
    ).await;

    loop {
        info!("Connecting to server: {}", connect_addr);
        if let Err(e) = client::run(
            &connect_addr,
            verifier.clone(),
            max_clipboard_size_bytes,
            &mut local_clipboard,
            &mut output_handler,
        )
        .await
        {
            error!("Client error: {:?}", e);
            // Clear any clipboard status that may have been accumulated while active
            if let Some(lc) = &mut local_clipboard {
                if let Err(e) = lc.clear_remote_clipboard() {
                    warn!("Failed to clear remote clipboard: {}", e);
                }
            }
            // Wait a bit before retrying. Often happens when waiting for server to approve the cert.
            time::sleep(Duration::from_secs(5)).await
        }
    }
}
