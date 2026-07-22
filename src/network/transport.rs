use std::net::SocketAddr;
use std::os::fd::FromRawFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use quinn::{
    congestion::BbrConfig, AckFrequencyConfig, ClientConfig, Endpoint, EndpointConfig, IdleTimeout,
    RecvStream, SendStream, ServerConfig, TransportConfig, VarInt,
};
use tracing::{debug, info, trace, warn};

use crate::msgs::shared;
use crate::network::approval;

/// Wireguard for example recommends 25000 for spanning NATs/firewalls, so that'd be optimal.
/// But we need to keep it shorter than TIMEOUT_MILLIS to avoid spurious timeouts.
const KEEPALIVE_MILLIS: u64 = 2000;

/// This is the delay between a client losing connection and the server tearing the
/// connection down (removing the client from the rotation) — NOT the ungrab delay:
/// the app-level Ping/Pong liveness check (see ServerEvent::Ping) switches local
/// and ungrabs after ~6s of silence, while this timeout owns only the teardown.
/// It must be a healthy multiple of KEEPALIVE_MILLIS: with only 3s here, a single dropped or
/// delayed WiFi packet (interference, client power-save, brief CPU stall) severed otherwise
/// healthy connections. 25s tolerates ~12 consecutive lost keepalives, so multi-second WiFi
/// black holes pass as invisible stalls. The tradeoff: a genuinely dead connection now takes
/// up to 25s to detect and remove.
const TIMEOUT_MILLIS: u32 = 25_000;

/// WWW-mode idle timeout: internet paths can stall much longer than LAN ones,
/// so the LAN-grade timeout would sever otherwise-healthy connections.
const WWW_TIMEOUT_MILLIS: u32 = 30_000;

/// WWW-mode keepalive: frequent enough to hold NAT/firewall state open,
/// but kept well shorter than WWW_TIMEOUT_MILLIS to avoid spurious timeouts.
const WWW_KEEPALIVE_MILLIS: u64 = 10_000;

/// Socket buffer size for the QUIC UDP socket. Larger buffers reduce the chance of
/// kernel drops during event bursts.
const SOCKET_BUF_SIZE: libc::c_int = 2 * 1024 * 1024;

/// Linux socket priority for interactive/low-latency traffic.
const SOCKET_PRIORITY: libc::c_int = 6;

/// Network profile for the QUIC transport.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NetworkMode {
    /// Low-latency tuning for local networks (LAN, direct WiFi, wired links).
    Local,
    /// Conservative tuning for traversing the public internet (WWW/WAN).
    Www,
}

pub fn build_client(
    bind_addr: &SocketAddr,
    cert_verifier: Arc<approval::MonuxCertVerification<'static>>,
    mode: NetworkMode,
) -> Result<quinn::Endpoint> {
    let socket = create_socket(*bind_addr, mode)
        .context("Failed to create client UDP socket")?;
    let runtime = quinn::default_runtime()
        .ok_or_else(|| anyhow::anyhow!("no async runtime found"))?;

    let mut client_config = ClientConfig::new(approval::rustls_client_config(cert_verifier)?);
    client_config.transport_config(transport_config(mode));

    let mut client_endpoint =
        Endpoint::new(EndpointConfig::default(), None, socket, runtime).with_context(|| {
            format!("Failed to bind client endpoint to {}", bind_addr)
        })?;
    client_endpoint.set_default_client_config(client_config);
    Ok(client_endpoint)
}

pub fn build_server(
    listen_addr: &SocketAddr,
    cert_verifier: Arc<approval::MonuxCertVerification<'static>>,
    mode: NetworkMode,
) -> Result<quinn::Endpoint> {
    let socket = create_socket(*listen_addr, mode)
        .context("Failed to create server UDP socket")?;
    let runtime = quinn::default_runtime()
        .ok_or_else(|| anyhow::anyhow!("no async runtime found"))?;

    let mut server_config =
        ServerConfig::with_crypto(approval::rustls_server_config(cert_verifier)?);
    server_config.transport_config(transport_config(mode));

    Endpoint::new(
        EndpointConfig::default(),
        Some(server_config),
        socket,
        runtime,
    )
    .with_context(|| format!("Failed to listen on {}", listen_addr))
}

fn create_socket(bind_addr: SocketAddr, mode: NetworkMode) -> Result<std::net::UdpSocket> {
    let domain = if bind_addr.is_ipv6() {
        libc::AF_INET6
    } else {
        libc::AF_INET
    };
    // SOCK_CLOEXEC: the auto-update restart re-execs the binary, and this
    // socket must not leak into the new image — it would keep the listen
    // port bound, failing the new endpoint with EADDRINUSE.
    let fd = unsafe { libc::socket(domain, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        bail!("Failed to create UDP socket: {}", std::io::Error::last_os_error());
    }

    // Helper that closes the fd on error paths before we wrap it in UdpSocket.
    let close_fd = || unsafe { libc::close(fd) };

    let apply_socket_opts = || -> Result<()> {
        setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &SOCKET_BUF_SIZE,
        )?;
        setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &SOCKET_BUF_SIZE,
        )?;
        // The kernel silently clamps these to net.core.{w,r}mem_max (~208 KiB
        // on a stock system), inviting drops during clipboard bursts. Verify
        // what we actually got and point at the fix if clamped.
        verify_socket_buf(fd, libc::SO_SNDBUF, "net.core.wmem_max");
        verify_socket_buf(fd, libc::SO_RCVBUF, "net.core.rmem_max");

        if mode == NetworkMode::Local {
            setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PRIORITY,
                &SOCKET_PRIORITY,
            )?;
            // Note: no DSCP mark here. quinn-udp sets the ECN codepoint via a
            // per-packet cmsg, which overrides any socket-level IP_TOS/IPV6_TCLASS,
            // so a setsockopt DSCP mark would be dead code.
        }
        Ok(())
    };

    if let Err(e) = apply_socket_opts() {
        close_fd();
        return Err(e);
    }

    // Bind using libc so that we keep control of the fd until the UdpSocket takes ownership.
    let bind_ret = match bind_addr {
        SocketAddr::V4(v4) => {
            let sa = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    // s_addr must hold the octets in network (memory) order;
                    // from_ne_bytes preserves the in-memory octet order on any
                    // host endianness (from_be_bytes would byte-swap them).
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            unsafe {
                libc::bind(
                    fd,
                    &sa as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            }
        }
        SocketAddr::V6(v6) => {
            let sa = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };
            unsafe {
                libc::bind(
                    fd,
                    &sa as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            }
        }
    };
    if bind_ret != 0 {
        let e = std::io::Error::last_os_error();
        close_fd();
        // Keep the io error in the chain: callers (and the bind retry in
        // server.rs) match on its raw errno.
        return Err(e).with_context(|| format!("Failed to bind UDP socket to {}", bind_addr));
    }

    // Now hand ownership to std::net::UdpSocket.
    Ok(unsafe { std::net::UdpSocket::from_raw_fd(fd) })
}

fn setsockopt<T>(
    fd: libc::c_int,
    level: libc::c_int,
    opt: libc::c_int,
    value: &T,
) -> Result<()> {
    let ret = unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            value as *const _ as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        warn!("setsockopt(level={}, opt={}) failed: {}", level, opt, err);
    }
    Ok(())
}

/// Warns if the kernel clamped a socket buffer below the requested size.
/// Linux doubles the granted value internally, so a healthy readback is at
/// least the request; anything less means net.core.*mem_max clamped it.
fn verify_socket_buf(fd: libc::c_int, opt: libc::c_int, sysctl: &str) {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            opt,
            &mut value as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if ret != 0 {
        return;
    }
    if value < SOCKET_BUF_SIZE {
        warn!(
            "UDP socket buffer clamped to {} bytes (wanted {}): raise {} (e.g. via 'sudo monux system setup') to avoid drops during clipboard bursts",
            value, SOCKET_BUF_SIZE, sysctl
        );
    }
}

fn transport_config(mode: NetworkMode) -> Arc<TransportConfig> {
    let mut transport_config = TransportConfig::default();

    // Pointer motion rides unreliable QUIC datagrams. Keep the send buffer
    // small: quinn discards the OLDEST queued datagrams to make space when
    // it's full, so a congested link can never accumulate a backlog of stale
    // motion to replay later — the newest position always wins. 2 KiB holds
    // ~15 coalesced frames (~60 ms at 250 Hz), far more than a healthy link
    // ever queues.
    transport_config.datagram_send_buffer_size(2048);

    match mode {
        NetworkMode::Local => {
            // Ask the peer to acknowledge every ack-eliciting packet with minimal delay.
            // This is a small increase in ACK traffic but significantly speeds up loss
            // detection on low-RTT local networks.
            let mut ack_config = AckFrequencyConfig::default();
            ack_config.ack_eliciting_threshold(VarInt::from_u32(0));
            ack_config.max_ack_delay(Some(Duration::from_micros(100)));
            ack_config.reordering_threshold(VarInt::from_u32(0));

            transport_config
                .max_concurrent_bidi_streams(2_u8.into()) // events + bulk
                .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
                .keep_alive_interval(Some(Duration::from_millis(KEEPALIVE_MILLIS)))
                .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(TIMEOUT_MILLIS))))
                // Local-network tuning: assume ~1 ms RTT rather than the default 333 ms.
                .initial_rtt(Duration::from_millis(1))
                // BBR reacts faster and keeps smaller queues than Cubic on low-loss local links.
                .congestion_controller_factory(Arc::new(BbrConfig::default()))
                .ack_frequency_config(Some(ack_config));
        }
        NetworkMode::Www => {
            transport_config
                .max_concurrent_bidi_streams(2_u8.into()) // events + bulk
                .max_concurrent_uni_streams(0_u8.into()) // we only use bidirectional streams
                .keep_alive_interval(Some(Duration::from_millis(WWW_KEEPALIVE_MILLIS)))
                .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(WWW_TIMEOUT_MILLIS))));
        }
    }

    Arc::new(transport_config)
}

/// Logs QUIC path stats when a connection drops, to tell apart a lossy link
/// (high loss/congestion/black holes) from a silent peer (low loss and normal
/// RTT, e.g. a CPU stall or WiFi buffering on the other side).
pub fn log_conn_stats(conn: &quinn::Connection) {
    let path = conn.stats().path;
    info!(
        "Connection stats on drop: rtt={:?} cwnd={} lost_packets={}/{} congestion_events={} black_holes={}",
        path.rtt,
        path.cwnd,
        path.lost_packets,
        path.sent_packets,
        path.congestion_events,
        path.black_holes_detected
    );
}

pub async fn send_version(send: &mut SendStream) -> Result<()> {
    let msg = shared::VersionBootstrapMessage {
        version: shared::PROTOCOL_VERSION,
    };
    let serializedmsg = postcard::to_stdvec_cobs(&msg)
        .map_err(|e| anyhow!("Failed to serialize version message: {:?}", e))?;
    trace!(
        "Sending {} byte version: {:X?}",
        serializedmsg.len(),
        &serializedmsg
    );
    send.write_all(&serializedmsg)
        .await
        .context("Failed to send protocol version")?;
    Ok(())
}

/// Returns the peer's protocol version. Does NOT check it against ours:
/// callers learn the version either way (a client records it for the update
/// gate), then call ensure_compatible_version.
pub async fn recv_version(recv: &mut RecvStream, buf: &mut Vec<u8>) -> Result<u64> {
    debug!("Waiting to receive version");
    let resp = recv
        .read_chunk(1024, true)
        .await
        .context("Failed reading protocol version (possible protocol version mismatch; run 'monux -V' on both ends to compare)")?
        .context("Peer closed connection during version exchange (possible protocol version mismatch; run 'monux -V' on both ends to compare)")?;
    trace!(
        "Received {} byte version: {:X?}",
        resp.bytes.len(),
        &*resp.bytes
    );
    // Copy the immutable response data into a mutable buffer
    buf.extend_from_slice(&resp.bytes);
    let version: u64;
    {
        let (versionmsg, resp_remainder) =
            postcard::take_from_bytes_cobs::<shared::VersionBootstrapMessage>(buf)
                .map_err(|e| anyhow!("Failed to deserialize protocol version message (possible protocol version mismatch; run 'monux -V' on both ends to compare): {:?}", e))?;
        version = versionmsg.version;
        // Remove this message from the front of buf.
        // resp_remainder is relative to buf, which may have had content before this call.
        let remainder_len = resp_remainder.len();
        let buf_len = buf.len();
        let consumed = buf_len - remainder_len;
        buf.copy_within(consumed..buf_len, 0);
        buf.truncate(buf_len - consumed);
    }
    Ok(version)
}

/// Errors when the peer's protocol version doesn't match ours.
pub fn ensure_compatible_version(their_version: u64) -> Result<()> {
    if their_version != shared::PROTOCOL_VERSION {
        bail!(
            "Their protocol version {} doesn't match our expected version {}. You need to update monux across your server and client(s) so that the protocol versions line up. Use 'monux -V' to check the version.",
            their_version,
            shared::PROTOCOL_VERSION
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Binding to a concrete IPv4 address must use that exact address: a
    /// byte-swapped s_addr (e.g. 1.0.0.127 instead of 127.0.0.1) makes the
    /// bind fail with EADDRNOTAVAIL, or worse, bind the wrong address.
    #[test]
    fn binds_to_concrete_ipv4_addr() {
        let socket = create_socket("127.0.0.1:0".parse().unwrap(), NetworkMode::Local)
            .expect("failed to bind loopback");
        let addr = socket.local_addr().expect("no local addr");
        assert_eq!(
            addr.ip(),
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
    }
}
