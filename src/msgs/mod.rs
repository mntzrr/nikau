pub mod bulk;
pub mod event;
pub mod shared;

#[cfg(test)]
mod golden_tests {
    // These goldens pin the wire format. If you changed a wire struct
    // intentionally: bump PROTOCOL_VERSION (src/msgs/shared.rs) AND regenerate
    // the goldens. The client update gate keys on PROTOCOL_VERSION.
    //
    // To regenerate after the bump: run the failing test — the assert prints
    // the hex the current serializer produces; paste it in as the expected
    // value.

    use super::{bulk, event, shared};

    /// Stream messages travel as postcard + COBS (see network/transport.rs).
    fn cobs_hex<T: serde::Serialize>(msg: &T) -> String {
        hex::encode(postcard::to_stdvec_cobs(msg).unwrap())
    }

    /// The update gate keys on the protocol version: bumping it must be a
    /// conscious red test (and per AGENTS.md a MAJOR crate version bump).
    #[test]
    fn protocol_version_is_11() {
        assert_eq!(shared::PROTOCOL_VERSION, 11);
    }

    #[test]
    fn golden_version_bootstrap() {
        let msg = shared::VersionBootstrapMessage { version: 11 };
        assert_eq!(cobs_hex(&msg), "020b00");
    }

    #[test]
    fn golden_server_event_switch() {
        let msg = event::ServerEvent::Switch(event::SwitchEvent { enabled: true });
        assert_eq!(cobs_hex(&msg), "01020100");
    }

    #[test]
    fn golden_server_event_input() {
        let msg = event::ServerEvent::Input(vec![
            event::InputEvent {
                inputi32: Some(event::InputI32 { type_: 2, code: 0, value: 5 }),
                inputf64: None,
            },
            event::InputEvent {
                inputi32: None,
                inputf64: Some(event::InputF64 { type_: 3, code: 53, value: 0.5 }),
            },
        ]);
        assert_eq!(cobs_hex(&msg), "0501020102020a0104010335010101010103e03f00");
    }

    #[test]
    fn golden_server_event_clipboard_types() {
        let msg = event::ServerEvent::ClipboardTypes(event::ClipboardTypes {
            types: "text/plain image/png",
            max_size_bytes: 1048576,
        });
        assert_eq!(
            cobs_hex(&msg),
            "1a0214746578742f706c61696e20696d6167652f706e6780804000"
        );
    }

    #[test]
    fn golden_client_event_clipboard_types() {
        let msg = event::ClientEvent::ClipboardTypes(event::ClipboardTypes {
            types: "text/plain",
            max_size_bytes: 1024,
        });
        assert_eq!(cobs_hex(&msg), "010e0a746578742f706c61696e800800");
    }

    #[test]
    fn golden_server_event_ping() {
        let msg = event::ServerEvent::Ping;
        assert_eq!(cobs_hex(&msg), "020300");
    }

    #[test]
    fn golden_client_event_pong() {
        let msg = event::ClientEvent::Pong;
        assert_eq!(cobs_hex(&msg), "020100");
    }

    #[test]
    fn golden_client_event_switch_request() {
        // Appended variant (protocol v11): variant index 2 followed by the
        // f64 fraction little-endian.
        let msg = event::ClientEvent::SwitchRequest { y_fraction: 0.5 };
        assert_eq!(cobs_hex(&msg), "0202010101010103e03f00");
    }

    #[test]
    fn golden_motion_datagram() {
        // Motion datagrams are plain postcard without COBS framing: QUIC
        // datagrams are already message-framed (see rotation.rs/client.rs).
        let msg = event::MotionDatagram {
            seq: 3,
            history: vec![(1, -2), (3, 4)],
        };
        assert_eq!(hex::encode(postcard::to_stdvec(&msg).unwrap()), "030202030608");
    }

    #[test]
    fn golden_server_bulk_clipboard_request() {
        let msg = bulk::ServerBulk::ClipboardRequest(bulk::ServerClipboardRequest {
            requested_type: "text/plain",
            max_size_bytes: 2048,
            request_client: Some("192.0.2.1:1213".parse().unwrap()),
            request_id: 9,
        });
        assert_eq!(
            cobs_hex(&msg),
            "010f0a746578742f706c61696e80100102c0060201bd090900"
        );
    }

    #[test]
    fn golden_server_bulk_clipboard_header() {
        let msg = bulk::ServerBulk::ClipboardHeader(bulk::ServerClipboardHeader {
            requested_type: "text/plain",
            data_type: Some("application/zstd"),
            content_len_bytes: 100,
            request_id: 10,
        });
        assert_eq!(
            cobs_hex(&msg),
            "21010a746578742f706c61696e01106170706c69636174696f6e2f7a737464640a00"
        );
    }

    #[test]
    fn golden_client_bulk_clipboard_request() {
        let msg = bulk::ClientBulk::ClipboardRequest(bulk::ClientClipboardRequest {
            requested_type: "text/plain",
            max_size_bytes: 1024,
            request_id: 7,
        });
        assert_eq!(cobs_hex(&msg), "010f0a746578742f706c61696e80080700");
    }

    #[test]
    fn golden_client_bulk_clipboard_header() {
        let msg = bulk::ClientBulk::ClipboardHeader(bulk::ClientClipboardHeader {
            requested_type: "text/plain",
            data_type: None,
            content_len_bytes: 50,
            request_client: None,
            request_id: 8,
        });
        assert_eq!(cobs_hex(&msg), "0d010a746578742f706c61696e0232020800");
    }
}
