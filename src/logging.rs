use tracing;
use tracing_subscriber::EnvFilter;

pub fn init_logging() {
    let filter_layer = EnvFilter::try_from_env("LOG_LEVEL")
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("Failed to initialize filter layer")
        // quinn_proto: Gets very noisy when LOG_LEVEL=trace
        .add_directive(
            "quinn_proto=info"
                .parse()
                .expect("Failed to parse quinn_proto directive"),
        )
        // x11rb_async: Gets very noisy when copying/pasting things during LOG_LEVEL=trace
        .add_directive(
            "x11rb_async=info"
                .parse()
                .expect("Failed to parse x11rb_async directive"),
        );

    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(filter_layer)
            .finish(),
    )
    .expect("Failed to set default subscriber");
}
