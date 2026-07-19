use std::time::Duration;

use anyhow::Result;

use nikau::discovery;
use nikau::logging;

/// Prints the address that a client would get from mDNS discovery right now.
#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();
    let addr = discovery::discover_server(Some(Duration::from_secs(5))).await?;
    println!("discovered: {}", addr);
    Ok(())
}
