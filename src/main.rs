mod approval;
mod certs;
mod client;
mod deviceinput;
mod deviceoutput;
mod deviceutil;
mod devicewatch;
mod logging;
mod messages;
mod server;
mod transport;

use anyhow::{bail, Result};
use async_std::task;
use tracing::{error, info};

fn main() -> Result<()> {
    // TODO args:
    // - server: left/right shortcuts, ip/port to listen on, cert hash(es) to auto-accept
    // - client: server to connect to, cert hash(es) to auto-accept

    logging::init_logging();

    //let _keyboard = deviceoutput::keyboard(true)?;
    //let _mouse = deviceoutput::mouse(true)?;
    //let _touchpad = deviceoutput::touchpad(true)?;
    //deviceoutput::print_virtual_devices();
    //bail!("ok cya");

    let listen_addr: std::net::SocketAddr = "127.0.0.1:5000".parse()?;
    let bind_addr: std::net::SocketAddr = "0.0.0.0:0".parse()?;
    let listen_addr2 = listen_addr.clone();

    // Fetch known certs once up-front.
    // Approvals in this run will be added to this list, as well as written to disk for future runs.
    let known_certs = match certs::load_known_certs() {
        Ok(known_certs) => known_certs,
        Err(e) => {
            error!("failed to load known certs, continuing with no known certs: {}", e);
            vec![]
        }
    };
    let known_certs2 = known_certs.clone();

    let (event_tx, event_rx): (
        async_channel::Sender<server::Event>,
        async_channel::Receiver<server::Event>,
    ) = async_channel::bounded(32);

    task::spawn(async move {
        info!("Listening for clients: {}", listen_addr);
        if let Err(e) = server::run_server(&listen_addr, known_certs, event_rx).await {
            error!("server fail: {}", e);
        }
    });

    task::spawn(async move {
        info!("Connecting to server: {}", listen_addr2);
        if let Err(e) = client::run_client(&bind_addr, &listen_addr2, known_certs2).await {
            error!("client fail: {}", e);
        }
    });

    // TODO args for user-specified key combos. require at least one key
    let input_handler = deviceinput::InputHandler::new("rightctrl,leftshift,f", None, event_tx)?;

    task::block_on(async move {
        if let Err(e) = devicewatch::watch_loop(input_handler).await {
            error!("input device watch failed: {}", e);
        }
    });

    bail!("device watch loop exited, bailing")
}
