use std::collections::HashMap;

use tokio::sync::watch;
use tracing::{debug, error, trace};
use wayland_client::globals::GlobalListContents;
use wayland_client::protocol::{wl_registry, wl_seat};
use wayland_client::{event_created_child, Dispatch};

use crate::clipboard::wayland::data_control::{
    self, impl_dispatch_device, impl_dispatch_manager, impl_dispatch_offer,
};

/// A special mime type for clipboards that we're advertising locally.
/// Clipboards containing this mime type are ignored.
/// This avoids the problem of advertising a clipboard then seeing/consuming that advertisement in the update stream.
pub const IGNORED_MIME_TYPE: &str = "application/x.monux.ignore";

struct OfferData {
    /// The offer for clipboard data
    offer: data_control::Offer,

    /// The mime types available for the offer
    mime_types: Vec<String>,
}

pub struct SeatData {
    /// The data device of this seat, if any.
    device: Option<data_control::Device>,

    /// Mime types for offers that are being streamed.
    /// Moved to regular_offer when the stream finishes.
    pending_offer_types: HashMap<data_control::Offer, Vec<String>>,

    /// The regular clipboard's offer, if any.
    regular_offer: Option<OfferData>,
}

impl SeatData {
    pub fn new(device: data_control::Device) -> Self {
        Self{
            device: Some(device),
            pending_offer_types: HashMap::new(),
            regular_offer: None,
        }
    }
}

pub struct State {
    /// Per-seat device/clipboard data. We probably only have one seat in practice?
    seats: HashMap<wl_seat::WlSeat, SeatData>,

    /// Mapping of pending offer to the seat that they're associated with.
    pending_offer_seats: HashMap<data_control::Offer, wl_seat::WlSeat>,

    /// Output for regular clipboard mime type updates
    regular_types_tx: Option<watch::Sender<Vec<String>>>,
}

impl State {
    pub fn new(
        seats: HashMap<wl_seat::WlSeat, SeatData>,
        regular_types_tx: Option<watch::Sender<Vec<String>>>,
    ) -> Self {
        Self {
            seats,
            pending_offer_seats: HashMap::new(),
            regular_types_tx,
        }
    }

    pub fn find_regular_offer(&self, mime_type: &String) -> Option<data_control::Offer> {
        // Just scan the seats for the first match (has the requested mime type).
        // Keep it simple until/unless we know we need multi-seat support.
        let mut found: Option<data_control::Offer> = None;
        for (_seat, data) in self.seats.iter() {
            if let Some(offer_data) = &data.regular_offer {
                if offer_data.mime_types.contains(mime_type) {
                    found = Some(offer_data.offer.clone());
                    break;
                }
            }
        }
        found
    }
}

impl Dispatch<wl_seat::WlSeat, ()> for State {
    fn event(
        _state: &mut Self,
        _seat: &wl_seat::WlSeat,
        _event: <wl_seat::WlSeat as wayland_client::Proxy>::Event,
        _data: &(),
        _conn: &wayland_client::Connection,
        _qh: &wayland_client::QueueHandle<Self>,
    ) {
    }
}

impl Dispatch<wl_registry::WlRegistry, GlobalListContents> for State {
    fn event(
        _state: &mut Self,
        _proxy: &wl_registry::WlRegistry,
        _event: <wl_registry::WlRegistry as wayland_client::Proxy>::Event,
        _data: &GlobalListContents,
        _conn: &wayland_client::Connection,
        _qhandle: &wayland_client::QueueHandle<Self>,
    ) {
    }
}

impl_dispatch_manager!(State);

impl_dispatch_device!(State, wl_seat::WlSeat, |state: &mut Self, event, seat: &wl_seat::WlSeat| {
    trace!("got wayland device event: {:?}", event);
    match event {
        Event::DataOffer { id } => {
            // Start of offer - set up pending map entries
            let offer = data_control::Offer::from(id);
            let seat_data = if let Some(seat) = state.seats.get_mut(seat) {
                seat
            } else {
                error!("Unknown seat in device/Selection event: {:?}", seat);
                return;
            };
            state.pending_offer_seats.insert(offer.clone(), seat.clone());
            seat_data.pending_offer_types.insert(offer, vec![]);
        }
        Event::Selection { id } => {
            // End of regular clipboard's offer - save offer and transfer pending mime types
            let seat_data = if let Some(seat) = state.seats.get_mut(seat) {
                seat
            } else {
                error!("Unknown seat in device/Selection event: {:?}", seat);
                return;
            };
            let offer = if let Some(offer) = id.map(data_control::Offer::from) {
                offer
            } else {
                // Offer revoked: ensure any prior offer is cleaned up
                if let Some(old_offer_data) = seat_data.regular_offer.take() {
                    old_offer_data.offer.destroy();
                }
                return;
            };
            // Clear up offer->seat mapping now that offer is no longer pending
            state.pending_offer_seats.remove(&offer);
            // Move collected mime types
            if let Some(mime_types) = seat_data.pending_offer_types.remove(&offer) {
                if mime_types.contains(&IGNORED_MIME_TYPE.to_string()) {
                    // This is our own advertisement, not from another application
                    debug!("ignoring wayland regular clipboard offer with mime types: {:?}", mime_types);
                    // Ensure any prior offer is cleaned up
                    if let Some(old_offer_data) = seat_data.regular_offer.take() {
                        old_offer_data.offer.destroy();
                    }
                    // This offer is being ignored, destroy it too
                    offer.destroy();
                } else {
                    debug!("storing wayland regular clipboard offer with mime types: {:?}", mime_types);
                    if let Some(tx) = &state.regular_types_tx {
                        // Advertise the local offer to the upstream client or server
                        if let Err(e) = tx.send(mime_types.clone()) {
                            error!("Failed to notify upsteam of changed clipboard mime types: {}", e);
                        }
                    }
                    // Ensure any prior offer is cleaned up
                    if let Some(old_offer_data) = seat_data.regular_offer.replace(OfferData{offer, mime_types}) {
                        old_offer_data.offer.destroy();
                    }
                }
            } else {
                error!("Missing pending mime types for regular clipboard offer");
            }
        }
        Event::PrimarySelection { id } => {
            // We only track the regular clipboard. This arm just consumes the
            // pending state that the DataOffer arm created and destroys the offer,
            // otherwise the pending maps would leak an entry on every
            // primary-selection change (every text highlight).
            let offer = if let Some(offer) = id.map(data_control::Offer::from) {
                offer
            } else {
                return;
            };
            state.pending_offer_seats.remove(&offer);
            let seat_data = if let Some(seat) = state.seats.get_mut(seat) {
                seat
            } else {
                error!("Unknown seat in device/PrimarySelection event: {:?}", seat);
                return;
            };
            seat_data.pending_offer_types.remove(&offer);
            offer.destroy();
        }
        Event::Finished => {
            // Destroy the device stored in the seat as it's no longer valid.
            let seat_data = if let Some(seat) = state.seats.get_mut(seat) {
                seat
            } else {
                error!("Unknown seat in device/Finished event: {:?}", seat);
                return;
            };
            if let Some(old_device) = seat_data.device.take() {
                old_device.destroy();
            }
        }
        _ => {},
    }
});

impl_dispatch_offer!(State, |state: &mut Self, offer: data_control::Offer, event| {
    trace!("got wayland offer event: {:?}", event);
    if let Event::Offer { mime_type } = &event {
        // Find seat where the data is expected to be going
        let seat = if let Some(seat) = state.pending_offer_seats.get(&offer) {
            seat
        } else {
            error!("No offer->seat mapping found for offer/Offer event: {:?}", event);
            return;
        };
        let seat_data = if let Some(seat) = state.seats.get_mut(seat) {
            seat
        } else {
            error!("Unknown seat in offer/Offer event: {:?}", seat);
            return;
        };
        if let Some(mime_types) = seat_data.pending_offer_types.get_mut(&offer) {
            mime_types.push(mime_type.clone());
        } else {
            error!("No offer->types mapping found for offer/Offer event: {:?}", event);
            return;
        };
    }
});
