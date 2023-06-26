mod logging;

use anyhow::{anyhow, bail, Result};
use async_std::stream::Stream;
use async_std::task;
use evdev::{Device, EventType, EventStream, InputEvent};
use futures::StreamExt;
use tracing::{info, warn};

use std::pin::Pin;

fn compatible_device(d: &Device) -> bool {
    let evts = d.supported_events();
    evts.contains(EventType::KEY) || evts.contains(EventType::ABSOLUTE) || evts.contains(EventType::RELATIVE)
}

fn pick_devices() -> Result<Vec<Device>> {
    let devices = evdev::enumerate()
        .map(|t| t.1)
        .filter(|d| compatible_device(d))
        .collect::<Vec<_>>();
    if devices.len() <= 0 {
        bail!("Didn't find any compatible devices");
    }
    for d in &devices {
        info!("- {}, {:?}, {:?}, {:?}", d.name().unwrap_or("(Unnamed device)"), d.properties(), d.supported_events(), d.supported_keys());
    }
    Ok(devices)
}

// TODO this doesn't work - the underling poll function seems to get stuck on the first device
// probably should just have separate async Tasks for each device, with a shared async channel for event output
pub struct DevicesStream {
    pub event_streams: Vec<EventStream>,
}

impl Stream for DevicesStream {
    type Item = Result<InputEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Option<Self::Item>> {
        for stream in &mut self.event_streams {
            match futures_core::ready!(stream.poll_event(cx)) {
                Ok(evt) => return task::Poll::Ready(Some(Ok(evt))),
                //Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return task::Poll::Ready(Some(Err(anyhow!(e)))),
            };
        }
        task::Poll::Pending
    }
}

fn main() -> Result<()> {
    logging::init_logging();

    // TODO listen for new or removed devices, update list
    let devices = pick_devices()?;
    task::block_on(async move {
        let event_streams = devices
            .into_iter()
            .filter_map(|d| {
                match d.into_event_stream() {
                    Ok(s) => {
                        Some(s)
                    },
                    Err(e) => {
                        // Skip this device. Maybe it was just unplugged?
                        warn!("Failed to initialize async fd for device: {}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>();
        let mut ds = DevicesStream{event_streams};
        while let Some(event) = ds.next().await {
            info!("got event: {:?}", event);
        }
    });
    Ok(())
}
