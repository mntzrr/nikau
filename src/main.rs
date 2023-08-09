use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use signal_hook::{consts::signal, iterator::Signals};
use tokio::sync::{broadcast, mpsc};
use tokio::{task, time};
use tracing::{error, info, warn};

use nikau::device::{input, output, watch};
use nikau::network::approval;
use nikau::{client, logging, server};

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
    /// Keyboard shortcut for switching to the next client
    #[arg(long, default_value = "leftalt,n")]
    shortcut: String,

    /// Key shortcut for switching to the previous client
    #[arg(long, default_value = "leftalt,p")]
    shortcut_prev: Option<String>,

    /// Server listen IP
    #[arg(short = 'l', long, default_value = "0.0.0.0")]
    listen: IpAddr,

    /// Server port
    #[arg(short = 'p', long, default_value_t = 1213)]
    port: u16,

    /// Client certificate fingerprints to automatically accept without prompting
    #[arg(long)]
    fingerprints: Option<Vec<String>>,

    /// Number of seconds to wait before automatically exiting the server, to safely test configuration
    #[arg(long)]
    exit_secs: Option<u32>,

    /// Maximum size in KB for transferring clipboard data (default: 5MB)
    #[arg(long, default_value_t = 5120)]
    max_clipboard_size_kb: u64,
}

#[derive(Args)]
struct ClientArgs {
    /// Server hostname or IP
    host: String,

    /// Server port
    #[arg(short = 'p', long, default_value_t = 1213)]
    port: u16,

    /// Server certificate fingerprints to automatically accept without prompting
    #[arg(long)]
    fingerprints: Option<Vec<String>>,

    /// Maximum size in KB for transferring clipboard data (default: 5MB)
    #[arg(long, default_value_t = 5120)]
    max_clipboard_size_kb: u64,
}

/// Listens for SIGUSR1 and SIGUSR2, treating them as "switch to next client" and "switch to prev client" respectively.
fn handle_signals(mut signals: Signals, out: mpsc::Sender<input::Event>) {
    let mut iter = signals.into_iter();
    loop {
        match iter.next() {
            Some(signal::SIGUSR1) => {
                if let Err(e) = out.blocking_send(input::Event::SwitchNext) {
                    error!("Failed to submit SwitchNext event for SIGUSR1: {:?}", e);
                }
            }
            Some(signal::SIGUSR2) => {
                if let Err(e) = out.blocking_send(input::Event::SwitchPrev) {
                    error!("Failed to submit SwitchPrev event for SIGUSR2: {:?}", e);
                }
            }
            other => {
                info!("no signals here? {:?}", other);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();
    let cli = Cli::parse();
    let config_dir = init_config_dir()?;

    match cli.command {
        Commands::Server(args) => {
            let listen_addr = SocketAddr::new(args.listen, args.port);
            let verifier = approval::NikauCertVerification::new(
                "server",
                args.fingerprints.unwrap_or(vec![]),
                &config_dir,
            )?;
            server(
                config_dir,
                listen_addr,
                &args.shortcut,
                args.shortcut_prev.as_deref(),
                args.exit_secs,
                verifier,
                args.max_clipboard_size_kb * 1024,
            )
            .await
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
                args.fingerprints.unwrap_or(vec![]),
                &config_dir,
            )?;
            client(config_dir, connect_addr, verifier, args.max_clipboard_size_kb * 1024).await
        }
    }
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
    next_keys: &str,
    prev_keys: Option<&str>,
    exit_secs: Option<u32>,
    verifier: Arc<approval::NikauCertVerification>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    let (event_tx, event_rx): (mpsc::Sender<input::Event>, mpsc::Receiver<input::Event>) =
        mpsc::channel(32);

    let event_tx2 = event_tx.clone();
    let signals = Signals::new([signal::SIGUSR1, signal::SIGUSR2])?;
    std::thread::spawn(|| handle_signals(signals, event_tx2));

    let (grab_tx, _grab_rx) = broadcast::channel(1);
    let grab_tx2 = grab_tx.clone();

    let input_handler = input::InputHandler::new(next_keys, prev_keys, event_tx)?;

    task::spawn(async move {
        if let Err(e) = watch::watch_loop(input_handler, grab_tx).await {
            error!("Input device watch failure: {:?}", e);
        }
    });

    info!("Listening for clients: {}", listen_addr);
    if let Some(exit_secs) = exit_secs {
        info!("Exiting in {} seconds...", exit_secs);
        tokio::select! {
            server_exit = server::run_server(
                &listen_addr,
                verifier,
                config_dir,
                event_rx,
                grab_tx2,
                max_clipboard_size_bytes,
            ) => {
                bail!("Server unexpectedly exited early: {:?}", server_exit);
            },
            _timeout = time::sleep(Duration::from_secs(exit_secs as u64)) => {
                info!("Exiting automatically as requested (--exit-secs={})", exit_secs);
            }
        };
    } else {
        server::run_server(
            &listen_addr,
            verifier,
            config_dir,
            event_rx,
            grab_tx2,
            max_clipboard_size_bytes,
        )
        .await?;
    }
    Ok(())
}

async fn client(
    config_dir: PathBuf,
    connect_addr: SocketAddr,
    verifier: Arc<approval::NikauCertVerification>,
    max_clipboard_size_bytes: u64,
) -> Result<()> {
    // Try to set up virtual devices up-front - exit early if we aren't root
    let mut virtual_devices =
        output::VirtualDevices::new().context("Failed to create virtual devices, are you root?")?;

    let mut local_clipboard = match client::LocalClipboard::new(config_dir).await {
        Ok(c) => Some(c),
        Err(e) => {
            info!("Disabled system clipboard support: {}", e);
            None
        }
    };

    loop {
        info!("Connecting to server: {}", connect_addr);
        if let Err(e) = client::run(
            &connect_addr,
            verifier.clone(),
            max_clipboard_size_bytes,
            &mut local_clipboard,
            &mut virtual_devices,
        )
        .await
        {
            error!("Client error: {:?}", e);
            // Clear any clipboard status that may have been accumulated while active
            if let Some(lc) = &mut local_clipboard {
                if let Err(e) = lc.clear_remote_clipboard().await {
                    warn!("Failed to clear remote clipboard: {}", e);
                }
            }
            // Wait a bit before retrying. Often happens when waiting for server to approve the cert.
            time::sleep(Duration::from_secs(5)).await
        }
    }
}
