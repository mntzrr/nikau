use std::time::Duration;

use anyhow::Result;

use monux::discovery;
use monux::logging;

/// Prints the address that a client would get from mDNS discovery right now.
#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();
    let (addr, name) = discovery::discover_server(Some(Duration::from_secs(5))).await?;
    println!("discovered: {} ({})", addr, name);
    Ok(())
}
