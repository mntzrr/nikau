use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tracing::{debug, info, warn};

/// mDNS service type used to advertise and discover Nikau servers on the local network.
const SERVICE_TYPE: &str = "_nikau._udp.local.";

/// Default time to wait for a server to be discovered on the LAN.
const DEFAULT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// After the first server resolves, keep listening this long for additional servers
/// so that "first wins" doesn't silently hide them.
const EXTRA_RESOLVE_GRACE: Duration = Duration::from_millis(500);

/// Error message when no server is discovered within the timeout.
const DISCOVERY_TIMEOUT_HINT: &str = "Discovery timeout: no Nikau server found on the local network. Check that: the server is running, both machines are on the same subnet, and no firewall is blocking UDP port 5353 (mDNS). Alternatively, connect directly with 'nikau client <ip>'";

/// Registers a Nikau server on the local network via mDNS.
pub struct DiscoveryRegistration {
    daemon: ServiceDaemon,
    fullname: String,
}

impl DiscoveryRegistration {
    /// Advertises a Nikau server listening on the given address.
    pub fn register(listen_addr: SocketAddr) -> Result<Self> {
        let hostname = get_hostname().context("Failed to get hostname")?;
        let instance_name = if hostname.is_empty() {
            "nikau".to_string()
        } else {
            hostname
        };
        let host_name = format!("{}.local.", instance_name);
        let port = listen_addr.port();
        let ips = advertise_ips(listen_addr.ip())?;

        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            &ips[..],
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
            "Registered mDNS service: {} at {:?}:{}",
            fullname, ips, port
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
            .ok_or_else(|| anyhow!("{}", DISCOVERY_TIMEOUT_HINT))?;

        let event = match tokio::time::timeout(remaining, receiver.recv_async()).await {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => {
                let _ = daemon.shutdown();
                bail!("mDNS browse error: {}", e);
            }
            Err(_) => {
                let _ = daemon.shutdown();
                bail!("{}", DISCOVERY_TIMEOUT_HINT);
            }
        };

        match event {
            ServiceEvent::ServiceResolved(resolved) => {
                let addr = match resolved_addr(&resolved) {
                    Some(addr) => addr,
                    None => {
                        debug!("Resolved service has no addresses, continuing to browse");
                        continue;
                    }
                };
                info!("Discovered Nikau server: {}", addr);
                // Keep listening briefly so that additional servers on the network
                // don't get silently hidden by first-wins.
                let mut extra: Vec<SocketAddr> = Vec::new();
                while let Ok(Ok(event)) =
                    tokio::time::timeout(EXTRA_RESOLVE_GRACE, receiver.recv_async()).await
                {
                    if let ServiceEvent::ServiceResolved(other) = event {
                        if let Some(other_addr) = resolved_addr(&other) {
                            if other_addr != addr && !extra.contains(&other_addr) {
                                extra.push(other_addr);
                            }
                        }
                    }
                }
                if !extra.is_empty() {
                    let mut all = vec![addr.to_string()];
                    all.extend(extra.iter().map(|a| a.to_string()));
                    info!(
                        "Multiple Nikau servers discovered: {}; connecting to the first: {}",
                        all.join(", "),
                        addr
                    );
                }
                let _ = daemon.shutdown();
                return Ok(addr);
            }
            other => {
                debug!("mDNS event: {:?}", other);
            }
        }
    }
}

/// Picks an address from a resolved service, preferring IPv4 for compatibility.
fn resolved_addr(resolved: &ServiceInfo) -> Option<SocketAddr> {
    let addresses = resolved.get_addresses();
    addresses
        .iter()
        .find(|ip| ip.is_ipv4())
        .or_else(|| addresses.iter().next())
        .map(|ip| SocketAddr::new(*ip, resolved.get_port()))
}

/// Picks the addresses to advertise for a server listening on `listen_ip`.
fn advertise_ips(listen_ip: IpAddr) -> Result<Vec<IpAddr>> {
    if !listen_ip.is_unspecified() {
        // A concrete --listen address was provided: advertise exactly that.
        return Ok(vec![listen_ip]);
    }
    // Listening on the wildcard address: advertise every usable local IPv4 address.
    let ips = local_ipv4_addrs().unwrap_or_else(|e| {
        warn!(
            "Failed to enumerate local IPv4 addresses ({}), falling back to route probe",
            e
        );
        Vec::new()
    });
    if !ips.is_empty() {
        return Ok(ips);
    }
    // Last resort: probe the outbound route for a primary address.
    match get_local_ip() {
        Ok(ip) => Ok(vec![ip]),
        Err(e) => bail!(
            "Failed to determine any local IP address to advertise: {}. Check that the network is up and that no firewall is blocking the route probe, or specify the address to advertise explicitly with '-l <ip>'",
            e
        ),
    }
}

/// Enumerates this host's non-loopback, non-link-local IPv4 addresses.
fn local_ipv4_addrs() -> Result<Vec<IpAddr>> {
    let mut ips: Vec<IpAddr> = Vec::new();
    unsafe {
        let mut ifaddrs: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifaddrs) != 0 {
            bail!("getifaddrs failed: {}", std::io::Error::last_os_error());
        }
        let mut current = ifaddrs;
        while !current.is_null() {
            let ifa = &*current;
            if !ifa.ifa_addr.is_null()
                && (*ifa.ifa_addr).sa_family == libc::AF_INET as libc::sa_family_t
            {
                let sin = &*(ifa.ifa_addr as *const libc::sockaddr_in);
                // s_addr is a u32 in network byte order.
                let ip = std::net::Ipv4Addr::from(sin.sin_addr.s_addr.to_be_bytes());
                let ip = IpAddr::V4(ip);
                if !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !matches!(ip, IpAddr::V4(v4) if v4.is_link_local())
                    && !ips.contains(&ip)
                {
                    ips.push(ip);
                }
            }
            current = ifa.ifa_next;
        }
        libc::freeifaddrs(ifaddrs);
    }
    Ok(ips)
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
