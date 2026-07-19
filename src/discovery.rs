use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tracing::{debug, info, warn};

/// mDNS service type used to advertise and discover Nikau servers on the local network.
const SERVICE_TYPE: &str = "_nikau._udp.local.";

/// Default time to wait for a server to be discovered on the LAN.
const DEFAULT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Registers a Nikau server on the local network via mDNS.
pub struct DiscoveryRegistration {
    daemon: ServiceDaemon,
    fullname: String,
}

impl DiscoveryRegistration {
    /// Advertises a Nikau server listening on the given port.
    pub fn register(port: u16) -> Result<Self> {
        let hostname = get_hostname().context("Failed to get hostname")?;
        let instance_name = if hostname.is_empty() {
            "nikau".to_string()
        } else {
            hostname
        };
        let host_name = format!("{}.local.", instance_name);
        let ip = get_local_ip().context("Failed to determine local IP address")?;

        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            ip,
            port,
            None,
        )
        .context("Failed to create mDNS service info")?;

        let fullname = service_info.get_fullname().to_string();
        let daemon = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
        daemon
            .register(service_info)
            .context("Failed to register mDNS service")?;

        info!(
            "Registered mDNS service: {} at {}:{}",
            fullname, ip, port
        );

        Ok(Self {
            daemon,
            fullname,
        })
    }
}

impl Drop for DiscoveryRegistration {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.unregister(&self.fullname) {
            warn!("Failed to unregister mDNS service: {}", e);
        }
        if let Err(e) = self.daemon.shutdown() {
            warn!("Failed to shutdown mDNS daemon: {}", e);
        }
    }
}

/// Discovers a Nikau server on the local network via mDNS.
/// Returns the first server found.
pub async fn discover_server(timeout: Option<Duration>) -> Result<SocketAddr> {
    let timeout = timeout.unwrap_or(DEFAULT_DISCOVERY_TIMEOUT);
    let daemon = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
    let receiver = daemon
        .browse(SERVICE_TYPE)
        .context("Failed to browse for Nikau servers")?;

    let deadline = Instant::now() + timeout;

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("Discovery timeout"))?;

        let event = match tokio::time::timeout(remaining, receiver.recv_async()).await {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => {
                let _ = daemon.shutdown();
                bail!("mDNS browse error: {}", e);
            }
            Err(_) => {
                let _ = daemon.shutdown();
                bail!("Discovery timeout");
            }
        };

        match event {
            ServiceEvent::ServiceResolved(resolved) => {
                let port = resolved.get_port();
                let addresses = resolved.get_addresses();
                // Prefer IPv4 for compatibility, fall back to any address.
                let ip = addresses
                    .iter()
                    .find(|ip| ip.is_ipv4())
                    .or_else(|| addresses.iter().next())
                    .ok_or_else(|| anyhow!("Resolved service has no addresses"))?;

                let addr = SocketAddr::new(*ip, port);
                info!("Discovered Nikau server: {}", addr);
                let _ = daemon.shutdown();
                return Ok(addr);
            }
            other => {
                debug!("mDNS event: {:?}", other);
            }
        }
    }
}

/// Returns the machine hostname.
fn get_hostname() -> Result<String> {
    let mut buf = [0i8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr(), buf.len()) };
    if ret != 0 {
        bail!("gethostname failed: {}", std::io::Error::last_os_error());
    }
    let c_str = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
    Ok(c_str.to_string_lossy().to_string())
}

/// Determines the primary local IP address by connecting a UDP socket to a public IP.
fn get_local_ip() -> Result<IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    Ok(socket.local_addr()?.ip())
}
