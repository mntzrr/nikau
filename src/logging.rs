use tracing;
use tracing_subscriber::EnvFilter;

pub fn init_logging() {
    let filter_layer = EnvFilter::try_from_env("LOG_LEVEL")
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("Failed to initialize filter layer");

    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(filter_layer)
            .finish(),
    )
    .expect("Failed to set default subscriber");
}
