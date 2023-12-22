use anyhow::Result;
use async_trait::async_trait;
use tracing::debug;
use usb_gadget::{
    default_udc,
    Class, Config, Gadget, Id, OsDescriptor, Strings, WebUsb,
};
use usb_gadget::function::hid::Hid;

use crate::device::output::OutputHandler;
use crate::msgs::event;

/*
DOCS: https://www.kernel.org/doc/Documentation/usb/gadget_configfs.txt

modprobe dwc2
modprobe libcomposite

/sys/kernel/config/usb_gadget/nikau/
idVendor: "0x1d6b"
idProduct: "0x0104"
bcdDevice: "0x0100"
bcdUSB: "0x0200"
UDC: $(TODO ls /sys/class/udc?)

/sys/kernel/config/usb_gadget/nikau/configs/c.1/
MaxPower: "250"

/sys/kernel/config/usb_gadget/nikau/configs/c.1/strings/0x409/
configuration: "Config 1: ECM network"

/sys/kernel/config/usb_gadget/nikau/functions/hid.usb0/
protocol: "1"
subclass: "1"
report_length: "8"
report_desc: $(TODO bunch of raw hex codes)

/sys/kernel/config/usb_gadget/nikau/strings/0x409/
serialnumber: "123"
manufacturer: "Nikau"
product: "Virtual Input Gadget"
*/

/// Sends events to the Linux HID Gadget API, acting as a USB HID device to another machine.
pub struct GadgetDevices {
}

impl GadgetDevices {
    pub async fn new() -> Result<GadgetDevices> {
        usb_gadget::remove_all().expect("cannot remove all gadgets");

        let mut hid_builder = Hid::builder();
        hid_builder.sub_class = 1;
        hid_builder.protocol = 1;
        hid_builder.report_desc = [0x0].to_vec(); // TODO some raw hex codes...
        hid_builder.report_len = 8;
        hid_builder.no_out_endpoint = false;
        let (hid, handle) = hid_builder.build();

        let udc = default_udc().expect("cannot get UDC");
        let reg = Gadget::new(
            Class::new(255, 255, 3),
            Id::new(6, 0x11),
            Strings::new("Nikau", "Virtual Input Gadget", "123"),
        )
            .with_config(Config::new("config").with_function(handle))
            .with_os_descriptor(OsDescriptor::microsoft())
            .with_web_usb(WebUsb::new(0xf1, "http://webusb.org"))
            .bind(&udc)
            .expect("cannot bind to UDC");

        println!("hid function at {}", hid.status().path().unwrap().display());
        println!();

        println!("Unregistering");
        reg.remove().unwrap();
        let ret = GadgetDevices {
        };
        Ok(ret)
    }
}

#[async_trait]
impl OutputHandler for GadgetDevices {
    async fn write(&mut self, events: Vec<event::InputEvent>) -> Result<()> {
        debug!("Got events: {:?}", events);
        Ok(())
    }
}
