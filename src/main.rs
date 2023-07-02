mod approval;
mod certs;
mod client;
mod devicewatch;
mod logging;
mod messages;
mod server;
mod transport;

use anyhow::{anyhow, bail, Result};
use async_std::task;
use devicewatch::DeviceHandler;
use evdev::{EventStream, EventType, InputEvent, Key};
use futures::StreamExt;
use tracing::{error, info, warn};

use std::str::FromStr;

fn check_combo(event: &InputEvent, combo_keys: &Vec<Key>, keys_on: &mut bit_vec::BitVec) -> bool {
    if event.event_type() == EventType::KEY {
        // Check if this key is one of our assigned combo keys
        for (idx, combo_key) in combo_keys.iter().enumerate() {
            if event.code() == combo_key.code() {
                if event.value() == 2 && keys_on.all() {
                    // The key is being repeated (value=2) and we already have a combo.
                    // Avoid repeating the combo event if the keys are being held down for too long.
                    return false;
                }
                // We allow the combo to be reached in any order (e.g. alt+esc or esc+alt).
                // If we wanted strict ordering then we could set keys_on to false for idx+1..combo_keys.len()
                keys_on.set(idx, event.value() >= 1);
                if keys_on.all() {
                    // Combo has been reached.
                    return true;
                }
            }
        }
    }
    return false;
}

async fn read_device_events(mut stream: EventStream, combo_keys: &Vec<Key>) {
    let mut keys_on = bit_vec::BitVec::from_elem(combo_keys.len(), false);
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                if check_combo(&event, &combo_keys, &mut keys_on) {
                    // Combo has been reached on this device
                    // TODO emit signal to switch output target, which may then signal this device task to grab or ungrab the device if it's switching to local or remote
                    info!("COMBO!!!!!");
                } else {
                    info!("event for {:?}: {:?}", stream.device().name(), event);
                }
            },
            Err(e) => {
                // Common when the device has been unplugged.
                // We'll frequently get this error just as inotify is telling us the file is deleted.
                // Exit to avoid an infinite loop on trying to read the missing file.
                warn!("Error event for {:?}, removing device: {:?}", stream.device().name(), e);
                return;
            },
        }
    }
}

struct ComboHandler {
    combo_keys: Vec<Key>,
}

impl DeviceHandler for ComboHandler {
    fn handle_device_stream(&mut self, stream: EventStream) -> Result<task::JoinHandle<()>> {
        let combo_keys = self.combo_keys.clone();
        task::Builder::new().name(format!("device: {:?}", stream.device().name())).spawn(async move {
            read_device_events(stream, &combo_keys).await
        }).map_err(|e| anyhow!(e))
    }
}

fn main() -> Result<()> {
    // TODO args:
    // - server: left/right shortcuts, ip/port to listen on, cert hash(es) to auto-accept
    // - client: server to connect to, cert hash(es) to auto-accept

    logging::init_logging();

    let listen_addr: std::net::SocketAddr = "127.0.0.1:5000".parse()?;
    let bind_addr: std::net::SocketAddr = "0.0.0.0:0".parse()?;
    let listen_addr2 = listen_addr.clone();
    let known_certs = certs::load_known_certs()?;
    let known_certs2 = known_certs.clone();
    task::spawn(async move {
        if let Err(e) = client::run_client(bind_addr, listen_addr2, known_certs2).await {
            error!("client fail: {}", e);
        }
    });
    task::block_on(async move {
        if let Err(e) = server::run_server(listen_addr, known_certs).await {
            error!("server fail: {}", e);
        }
    });

    bail!("ok cya");

    // TODO args for user-specified key combos. require at least one key
    let combo_keys = vec![
        Key::from_str("KEY_RIGHTCTRL").map_err(|e| anyhow!("Unsupported key: {:?}", e))?,
        Key::from_str("KEY_LEFTSHIFT").map_err(|e| anyhow!("Unsupported key: {:?}", e))?,
        Key::from_str("KEY_F").map_err(|e| anyhow!("Unsupported key: {:?}", e))?,
    ];
    let device_handler = ComboHandler{combo_keys};

    task::block_on(async move {
        if let Err(e) = devicewatch::watch_loop(device_handler).await {
            error!("input device watch failed: {}", e);
        }
    });

    bail!("device watch loop exited, bailing")
}
