use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use async_std::task;
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use signal_hook::consts::signal;
use tracing::{error, info};

use nikau::{approval, client, deviceinput, devicewatch, logging, server};

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
    #[arg(long)]
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

    /// TODO Number of seconds to wait before automatically exiting the server, to safely test configuration
    #[arg(long)]
    exit_secs: Option<u32>,
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
}

/// Listens for SIGUSR1 and SIGUSR2, treating them as "switch to next client" and "switch to prev client" respectively.
async fn handle_signals(mut signals: signal_hook_async_std::Signals, out: async_channel::Sender<deviceinput::Event>) {
    while let Some(signal) = signals.next().await {
        match signal {
            signal::SIGUSR1 => {
                if let Err(e) = out.send(deviceinput::Event::SwitchNext).await {
                    error!("Failed to submit SwitchNext event for SIGUSR1: {}", e);
                }
            },
            signal::SIGUSR2 => {
                if let Err(e) = out.send(deviceinput::Event::SwitchPrev).await {
                    error!("Failed to submit SwitchPrev event for SIGUSR2: {}", e);
                }
            },
            _ => continue,
        }
    }
}

fn main() -> Result<()> {
    logging::init_logging();

    let cli = Cli::parse();
    match cli.command {
        Commands::Server(args) => {
            let listen_addr = SocketAddr::new(args.listen, args.port);
            let verifier = approval::NikauCertVerification::new(args.fingerprints.unwrap_or(vec![]))?;
            server(listen_addr, &args.shortcut, args.shortcut_prev.as_deref(), verifier)
        },
        Commands::Client(args) => {
            let connect_addr: SocketAddr = if let Ok(host_ip) = args.host.parse::<IpAddr>() {
                // It's an IP.
                SocketAddr::new(host_ip, args.port)
            } else {
                // Its a hostname? Try resolving it.
                let mut socket_addrs = format!("{}:{}", args.host, args.port).to_socket_addrs()
                    .map_err(|e| anyhow!("Failed to resolve --host={}: {}", args.host, e))?;
                if let Some(first) = socket_addrs.next() {
                    first
                } else {
                    bail!("Provided --host={} didn't resolve to an IP", args.host);
                }
            };
            let verifier = approval::NikauCertVerification::new(args.fingerprints.unwrap_or(vec![]))?;
            client(connect_addr, verifier)
        },
    }
}

fn server(
    listen_addr: SocketAddr,
    next_keys: &str,
    prev_keys: Option<&str>,
    verifier: Arc<approval::NikauCertVerification>
) -> Result<()> {
    let (event_tx, event_rx): (
        async_channel::Sender<deviceinput::Event>,
        async_channel::Receiver<deviceinput::Event>,
    ) = async_channel::bounded(32);

    let event_tx2 = event_tx.clone();
    let signals = signal_hook_async_std::Signals::new(&[signal::SIGUSR1, signal::SIGUSR2])?;
    task::spawn(async move {
        handle_signals(signals, event_tx2).await;
    });

    task::spawn(async move {
        info!("Listening for clients: {}", listen_addr);
        if let Err(e) = server::run_server(&listen_addr, verifier, event_rx).await {
            error!("server fail: {}", e);
        }
    });

    let input_handler = deviceinput::InputHandler::new(&next_keys, prev_keys, event_tx)?;

    task::block_on(async move {
        if let Err(e) = devicewatch::watch_loop(input_handler).await {
            error!("Input device watch failure: {}", e);
        }
    });

    bail!("Exiting due to server failure")
}

fn client(connect_addr: SocketAddr, verifier: Arc<approval::NikauCertVerification>) -> Result<()> {
    let bind_addr: SocketAddr = "0.0.0.0:0".parse()?;
    task::block_on(async move {
        // TODO connection loop: handle server not up yet, or server restarting
        loop {
            let verifier2 = verifier.clone();
            info!("Connecting to server: {}", connect_addr);
            if let Err(e) = client::run_client(&bind_addr, &connect_addr, verifier2).await {
                error!("Client failure: {}", e);
            }
        }
    });

    bail!("Exiting due to client failure")
}
