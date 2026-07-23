use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo, TxtProperties};
use tracing::{debug, info, warn};

/// mDNS service type used to advertise and discover Monux servers on the local network.
const SERVICE_TYPE: &str = "_monux._udp.local.";

/// TXT property under which a server advertises its wire protocol version, so
/// clients can refresh their update gate (see update.rs) from the LAN instead
/// of waiting for a handshake.
const PROTOCOL_VERSION_PROPERTY: &str = "pv";

/// How long `monux system update` browses for advertised server protocol versions
/// before falling back to the recorded gate value: long enough for a running
/// server to answer, short enough that the command doesn't appear to hang.
const SERVER_VERSION_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(2);

/// Default time to wait for a server to be discovered on the LAN.
const DEFAULT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// After the first server resolves, keep listening this long for additional servers
/// so that "first wins" doesn't silently hide them.
const EXTRA_RESOLVE_GRACE: Duration = Duration::from_millis(500);

/// Error message when no server is discovered within the timeout.
const DISCOVERY_TIMEOUT_HINT: &str = "Discovery timeout: no Monux server found on the local network. Check that: the server is running, both machines are on the same subnet, and no firewall is blocking UDP port 5353 (mDNS). Alternatively, connect directly with 'monux client <ip>'";

/// Registers a Monux server on the local network via mDNS.
pub struct DiscoveryRegistration {
    daemon: ServiceDaemon,
    fullname: String,
}

impl DiscoveryRegistration {
    /// Advertises a Monux server listening on the given address.
    pub fn register(listen_addr: SocketAddr) -> Result<Self> {
        let hostname = get_hostname().context("Failed to get hostname")?;
        let instance_name = if hostname.is_empty() {
            "monux".to_string()
        } else {
            hostname
        };
        let host_name = format!("{}.local.", instance_name);
        let port = listen_addr.port();
        let ips = advertise_ips(listen_addr.ip())?;

        // Advertise the wire protocol version so clients can refresh their
        // update gate from the LAN (see update.rs). Like all mDNS data this
        // is unauthenticated — acceptable because the gate is only a
        // convenience; real compatibility is enforced at the handshake.
        let properties = HashMap::from([(
            PROTOCOL_VERSION_PROPERTY.to_string(),
            crate::msgs::shared::PROTOCOL_VERSION.to_string(),
        )]);

        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            &ips[..],
            port,
            properties,
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
        // Wait for each response (with a timeout) instead of dropping the
        // receivers: the daemon thread error-logs "failed to send response"
        // when it can't deliver the status to a dropped receiver.
        match self.daemon.unregister(&self.fullname) {
            Ok(resp) => {
                let _ = resp.recv_timeout(std::time::Duration::from_secs(2));
            }
            Err(e) => warn!("Failed to unregister mDNS service: {}", e),
        }
        match self.daemon.shutdown() {
            Ok(resp) => {
                let _ = resp.recv_timeout(std::time::Duration::from_secs(2));
            }
            Err(e) => warn!("Failed to shutdown mDNS daemon: {}", e),
        }
    }
}

/// Discovers a Monux server on the local network via mDNS.
/// Returns the first server found, along with its advertised instance name
/// (normally the server's hostname) for display in e.g. approval prompts.
pub async fn discover_server(timeout: Option<Duration>) -> Result<(SocketAddr, String)> {
    let timeout = timeout.unwrap_or(DEFAULT_DISCOVERY_TIMEOUT);
    let daemon = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
    let receiver = daemon
        .browse(SERVICE_TYPE)
        .context("Failed to browse for Monux servers")?;

    let deadline = Instant::now() + timeout;
    // Addresses of the first-discovered server instance, merged across resolve
    // events: mDNS delivers a service's addresses incrementally, so the first
    // event rarely carries them all. Only after a grace period do we pick one.
    let mut first_instance: Option<String> = None;
    let mut grace_deadline: Option<Instant> = None;
    let mut instance_port = 0u16;
    let mut instance_addrs: Vec<IpAddr> = Vec::new();
    let mut other_servers: Vec<String> = Vec::new();

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("{}", DISCOVERY_TIMEOUT_HINT))?;
        // Once the first instance has resolved, only wait out the grace period
        // for the rest of its addresses (and other servers) to arrive.
        let wait = match grace_deadline {
            Some(grace) => match grace.checked_duration_since(Instant::now()) {
                Some(grace_remaining) => grace_remaining,
                None => break,
            },
            None => remaining,
        };

        let event = match tokio::time::timeout(wait, receiver.recv_async()).await {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => {
                let _ = daemon.shutdown();
                bail!("mDNS browse error: {}", e);
            }
            Err(_) => {
                if grace_deadline.is_some() {
                    // Grace period expired with no more events
                    break;
                }
                let _ = daemon.shutdown();
                bail!("{}", DISCOVERY_TIMEOUT_HINT);
            }
        };

        match event {
            ServiceEvent::ServiceResolved(resolved) => {
                let fullname = resolved.get_fullname().to_string();
                match &first_instance {
                    None => {
                        info!("Discovered Monux server: {}", fullname);
                        first_instance = Some(fullname);
                        grace_deadline = Some(Instant::now() + EXTRA_RESOLVE_GRACE);
                        instance_port = resolved.get_port();
                        for scoped_ip in resolved.get_addresses() {
                            // ScopedIp carries the discovering interface(s);
                            // reduce to a plain address, deduped.
                            let ip = scoped_ip.to_ip_addr();
                            if !instance_addrs.contains(&ip) {
                                instance_addrs.push(ip);
                            }
                        }
                    }
                    Some(current) if *current == fullname => {
                        // More addresses for the same server arrived
                        for scoped_ip in resolved.get_addresses() {
                            let ip = scoped_ip.to_ip_addr();
                            if !instance_addrs.contains(&ip) {
                                instance_addrs.push(ip);
                            }
                        }
                    }
                    Some(_) => {
                        if !other_servers.contains(&fullname) {
                            other_servers.push(fullname);
                        }
                    }
                }
            }
            other => {
                debug!("mDNS event: {:?}", other);
            }
        }
    }

    if !other_servers.is_empty() {
        info!(
            "Multiple Monux servers discovered: {}; connecting to: {}",
            other_servers.join(", "),
            first_instance.as_deref().unwrap_or("<unknown>")
        );
    }
    let fullname = first_instance.ok_or_else(|| anyhow!("Discovered server has no addresses"))?;
    let addr = pick_addr(&instance_addrs, instance_port)
        .ok_or_else(|| anyhow!("Discovered server has no addresses"))?;
    info!(
        "Discovered {} address(es) for server, connecting to: {}",
        instance_addrs.len(),
        addr
    );
    let _ = daemon.shutdown();
    // Strip the service-type suffix, leaving the bare instance (host) name.
    let instance_name = fullname
        .strip_suffix(&format!(".{}", SERVICE_TYPE))
        .unwrap_or(&fullname)
        .to_string();
    Ok((addr, instance_name))
}

/// Extracts a server's advertised protocol version from its mDNS TXT
/// properties: `None` when the property is absent (servers predate the
/// advertisement) or isn't a number — both mean "no information".
fn protocol_version_of(properties: &TxtProperties) -> Option<u64> {
    properties
        .get_property_val_str(PROTOCOL_VERSION_PROPERTY)?
        .parse()
        .ok()
}

/// Picks the update-gate constraint from the protocol versions discovered on
/// the LAN: the minimum, so a client never upgrades beyond any server it
/// might pair with. `None` when nothing was discovered.
pub fn protocol_version_constraint(discovered: &[u64]) -> Option<u64> {
    discovered.iter().min().copied()
}

/// Synchronously collects the distinct protocol versions (sorted) advertised
/// by Monux servers on the LAN, for the update gate in `monux system update` — which
/// runs before the tokio runtime exists, hence the blocking API. Best-effort
/// within a short timeout; servers without the property are skipped.
pub fn discover_server_protocol_versions() -> Result<Vec<u64>> {
    let daemon = ServiceDaemon::new().context("Failed to create mDNS daemon")?;
    let versions = collect_server_protocol_versions(&daemon);
    if let Err(e) = daemon.shutdown() {
        debug!("Failed to shutdown mDNS daemon: {}", e);
    }
    versions
}

fn collect_server_protocol_versions(daemon: &ServiceDaemon) -> Result<Vec<u64>> {
    let receiver = daemon
        .browse(SERVICE_TYPE)
        .context("Failed to browse for Monux servers")?;
    let deadline = Instant::now() + SERVER_VERSION_DISCOVERY_TIMEOUT;
    let own_instance = get_hostname().unwrap_or_default();
    let own_ips: std::collections::HashSet<IpAddr> = local_ipv4_addrs()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut versions = BTreeSet::new();
    loop {
        let remaining = match deadline.checked_duration_since(Instant::now()) {
            Some(remaining) => remaining,
            None => break,
        };
        match receiver.recv_timeout(remaining) {
            Ok(ServiceEvent::ServiceResolved(resolved)) => {
                match protocol_version_of(resolved.get_properties()) {
                    Some(version) => {
                        let instance = instance_name_of(resolved.get_fullname());
                        // Our own advertisement must not gate our own update.
                        // Match on hostname OR advertised IPs: cloned images
                        // share a hostname, so the IP check is essential.
                        let is_own = instance == own_instance
                            || resolved
                                .get_addresses()
                                .iter()
                                .any(|scoped| own_ips.contains(&scoped.to_ip_addr()));
                        if is_own {
                            // Our own advertisement must not gate our own
                            // update: a server leads protocol upgrades — the
                            // gate exists for client machines.
                            debug!(
                                "Ignoring our own mDNS advertisement of protocol v{} for the update gate",
                                version
                            );
                        } else {
                            debug!(
                                "Discovered Monux server {} advertising protocol v{}",
                                resolved.get_fullname(),
                                version
                            );
                            versions.insert(version);
                        }
                    }
                    None => debug!(
                        "Discovered Monux server {} without a protocol version; skipping it",
                        resolved.get_fullname()
                    ),
                }
            }
            Ok(other) => debug!("mDNS event: {:?}", other),
            // Timeout (normal: no more servers answered) or the browse stream
            // ending: return what we have.
            Err(_) => break,
        }
    }
    Ok(versions.into_iter().collect())
}

/// The instance-name part of a service fullname (everything before the
/// service-type suffix), e.g. "myhost" from "myhost._monux._udp.local.".
fn instance_name_of(fullname: &str) -> &str {
    fullname
        .strip_suffix(&format!(".{}", SERVICE_TYPE))
        .unwrap_or(fullname)
}

/// Picks an address to connect to. A server may advertise several addresses
/// (LAN, docker bridges, VPN, ...), so prefer the one sharing the longest bit
/// prefix with one of our own interface addresses (i.e. most likely on our
/// subnet), falling back to any IPv4 address, then any address.
fn pick_addr(addrs: &[IpAddr], port: u16) -> Option<SocketAddr> {
    let local_ips = local_ipv4_addrs().unwrap_or_default();
    addrs
        .iter()
        .filter(|ip| ip.is_ipv4())
        .max_by_key(|ip| {
            local_ips
                .iter()
                .map(|local| common_prefix_len(ip, local))
                .max()
                .unwrap_or(0)
        })
        .or_else(|| addrs.iter().next())
        .map(|ip| SocketAddr::new(*ip, port))
}

/// Length of the common leading bit prefix of two IP addresses (0 across families).
fn common_prefix_len(a: &IpAddr, b: &IpAddr) -> u32 {
    match (a, b) {
        (IpAddr::V4(a), IpAddr::V4(b)) => (a.to_bits() ^ b.to_bits()).leading_zeros(),
        (IpAddr::V6(a), IpAddr::V6(b)) => (a.to_bits() ^ b.to_bits()).leading_zeros(),
        _ => 0,
    }
}

/// Picks the addresses to advertise for a server listening on `listen_ip`.
pub fn advertise_ips(listen_ip: IpAddr) -> Result<Vec<IpAddr>> {
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

/// Interface name prefixes for virtual overlay links (docker/VM bridges, VPN
/// tunnels). LAN peers can't reach these addresses — and docker bridges have
/// the SAME default IPs on every host, which poisons subnet prefix matching.
const VIRTUAL_IFACE_PREFIXES: &[&str] = &[
    "docker", "br-", "veth", "virbr", "vnet", "tun", "tap", "wg", "tailscale", "zt", "mullvad",
];

fn is_virtual_iface(name: &str) -> bool {
    VIRTUAL_IFACE_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Enumerates this host's non-loopback, non-link-local IPv4 addresses,
/// preferring physical/primary interfaces over virtual overlay ones.
fn local_ipv4_addrs() -> Result<Vec<IpAddr>> {
    let mut ips: Vec<(String, IpAddr)> = Vec::new();
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
                let name = if ifa.ifa_name.is_null() {
                    String::new()
                } else {
                    std::ffi::CStr::from_ptr(ifa.ifa_name)
                        .to_string_lossy()
                        .to_string()
                };
                let sin = &*(ifa.ifa_addr as *const libc::sockaddr_in);
                // s_addr is stored in network byte order; to_ne_bytes() preserves
                // the in-memory octet order on any host endianness.
                let ip = std::net::Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
                let ip = IpAddr::V4(ip);
                if !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !matches!(ip, IpAddr::V4(v4) if v4.is_link_local())
                    && !ips.iter().any(|(_, existing)| *existing == ip)
                {
                    ips.push((name, ip));
                }
            }
            current = ifa.ifa_next;
        }
        libc::freeifaddrs(ifaddrs);
    }
    // Drop virtual overlay interfaces; if that would leave nothing (e.g. the
    // machine's only link really is a bridge/VPN), keep the unfiltered list.
    let physical: Vec<IpAddr> = ips
        .iter()
        .filter(|(name, _)| !is_virtual_iface(name))
        .map(|(_, ip)| *ip)
        .collect();
    if !physical.is_empty() {
        return Ok(physical);
    }
    Ok(ips.into_iter().map(|(_, ip)| ip).collect())
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_advertised_protocol_version() {
        use mdns_sd::IntoTxtProperties;
        let props = HashMap::from([("pv".to_string(), "8".to_string())]).into_txt_properties();
        assert_eq!(protocol_version_of(&props), Some(8));
        // No property (a pre-advertisement server) or a malformed one: no
        // information, never an error.
        let empty = HashMap::<String, String>::new().into_txt_properties();
        assert_eq!(protocol_version_of(&empty), None);
        let junk = HashMap::from([("pv".to_string(), "eight".to_string())]).into_txt_properties();
        assert_eq!(protocol_version_of(&junk), None);
    }

    #[test]
    fn instance_name_strips_the_service_type() {
        assert_eq!(instance_name_of("myhost._monux._udp.local."), "myhost");
        // No suffix present: the name is returned unchanged.
        assert_eq!(instance_name_of("myhost"), "myhost");
    }

    #[test]
    fn constraint_is_the_minimum_discovered_version() {
        assert_eq!(protocol_version_constraint(&[]), None);
        assert_eq!(protocol_version_constraint(&[8]), Some(8));
        assert_eq!(protocol_version_constraint(&[8, 7, 9]), Some(7));
    }

    #[test]
    fn prefix_len_prefers_same_subnet() {
        let server: IpAddr = "192.168.1.187".parse().unwrap();
        let same_lan: IpAddr = "192.168.1.23".parse().unwrap();
        let docker: IpAddr = "172.17.0.1".parse().unwrap();
        assert_eq!(common_prefix_len(&server, &same_lan), 24);
        assert!(common_prefix_len(&server, &docker) < 24);
        // Cross-family is always 0
        let v6: IpAddr = "fe80::1".parse().unwrap();
        assert_eq!(common_prefix_len(&server, &v6), 0);
    }

    #[test]
    fn virtual_ifaces_are_detected() {
        for name in ["docker0", "br-9f1c2e", "veth1234", "virbr0", "tun0", "wg0", "tailscale0", "mullvad"] {
            assert!(is_virtual_iface(name), "{} should be treated as virtual", name);
        }
        for name in ["eth0", "enp3s0", "wlan0", "wlp2s0", "eno1"] {
            assert!(!is_virtual_iface(name), "{} should be treated as physical", name);
        }
    }

    #[test]
    fn enumerated_addrs_are_usable_and_not_byte_swapped() {
        let ips = local_ipv4_addrs().expect("failed to enumerate interfaces");
        println!("local ipv4 addrs: {:?}", ips);
        assert!(!ips.is_empty(), "expected at least one usable IPv4 address");
        for ip in &ips {
            assert!(ip.is_ipv4());
            assert!(!ip.is_loopback(), "loopback leaked into advertisement list");
            assert!(!ip.is_unspecified());
            if let IpAddr::V4(v4) = ip {
                assert!(!v4.is_link_local(), "link-local leaked into advertisement list");
                // Byte-reversal guard: 1.0.0.127 is 127.0.0.1 with swapped octets
                assert_ne!(v4.octets()[0], 1, "suspicious byte-swapped address: {}", v4);
                // Docker's default bridges have the same address on every host;
                // advertising them makes discovery picks useless.
                assert!(
                    !v4.octets().starts_with(&[172, 17]) && !v4.octets().starts_with(&[172, 18]),
                    "docker bridge leaked into advertisement list: {}", v4
                );
            }
        }
    }
}
