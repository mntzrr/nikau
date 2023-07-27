use tracing;
use tracing_subscriber::EnvFilter;

pub fn init_logging() {
    let filter_layer = EnvFilter::try_from_env("LOG_LEVEL")
        // x11rb_async: Can revert to just 'info' once this is merged: https://github.com/psychon/x11rb/pull/855
        // quinn_udp: Moderately-sized clipboard pastes raise meaningless warnings/errors: https://github.com/quinn-rs/quinn/blob/latest/quinn-udp/src/unix.rs#L264
        .or_else(|_| EnvFilter::try_new("x11rb_async::rust_connection=warn,quinn_udp=off,info"))
        .expect("Failed to initialize filter layer");

    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(filter_layer)
            .finish(),
    )
    .expect("Failed to set default subscriber");
}
