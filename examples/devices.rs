use nikau::{deviceoutput, deviceutil, devicewatch, logging};

use anyhow::{anyhow, bail, Context, Result};
use async_std::task;
use evdev::{AbsoluteAxisType, EventType, InputEvent, Key};
use futures::StreamExt;
use tracing::{error, info, warn};

use std::thread;
use std::time::Duration;

struct StubHandler {}

impl devicewatch::DeviceHandler for StubHandler {
    fn handle_device_stream(
        &mut self,
        mut stream: evdev::EventStream,
    ) -> Result<task::JoinHandle<()>> {
        task::Builder::new()
            .name(format!("device: {:?}", stream.device().name()))
            .spawn(async move {
                let (_device_target, _device_dims) = deviceutil::device_info(&stream.device());
                let device_name = stream
                    .device()
                    .name()
                    .unwrap_or("(Unnamed device)")
                    .to_string();
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(event) => {
                            info!("Event for {}: {:?}", device_name, event);
                        }
                        Err(e) => {
                            warn!("Error event for {}, removing device: {}", device_name, e);
                        }
                    }
                }
            })
            .map_err(|e| anyhow!(e))
    }
}

fn main() -> Result<()> {
    logging::init_logging();

    let handler = task::spawn(async move {
        if let Err(e) = devicewatch::watch_loop(StubHandler {}).await {
            error!("Input device watch failure: {}", e);
        }
    });

    let pid = std::process::id();
    let mut keyboard =
        deviceoutput::keyboard(pid).context("Failed to init virtual device, are you root?")?;
    let mut mouse = deviceoutput::mouse(pid)?;
    let mut touchpad = deviceoutput::touchpad(pid)?;

    // Sleep for a bit, otherwise events can be missed. Devices need a bit of time to come up.
    thread::sleep(Duration::from_secs(1));

    for _ in 0..50 {
        keyboard
            .emit(&[InputEvent::new(EventType::KEY, Key::KEY_R.code(), 1)])
            .unwrap();
        keyboard
            .emit(&[InputEvent::new(EventType::KEY, Key::KEY_R.code(), 0)])
            .unwrap();
    }

    for _ in 0..50 {
        mouse
            .emit(&[
                InputEvent::new(EventType::RELATIVE, evdev::RelativeAxisType::REL_X.0, 5),
                InputEvent::new(EventType::RELATIVE, evdev::RelativeAxisType::REL_Y.0, 5),
            ])
            .unwrap();
        thread::sleep(Duration::from_micros(5_000));
    }

    for i in 100..200 {
        // Position (i * 100) is scaled to fit within SCALED_DIM_MIN/SCALED_DIM_MAX
        touchpad.emit(&[
            InputEvent::new(EventType::ABSOLUTE, AbsoluteAxisType::ABS_X.0, i * 100),
            InputEvent::new(EventType::ABSOLUTE, AbsoluteAxisType::ABS_Y.0, i * 100),
        ])?;
        thread::sleep(Duration::from_micros(5_000));
    }

    task::block_on(async move {
        handler.await;
    });
    bail!("Exited prematurely");
}
