use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use tracing;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::EnvFilter;

/// Total log lines kept in the in-memory ring buffer (see RingBufferLayer).
/// Bounded: older lines are dropped as new ones arrive.
const LOG_RING_CAPACITY: usize = 200;

/// Default number of ring-buffer lines served by the control socket's
/// diagnostics command.
pub const RECENT_LOGS_DEFAULT: usize = 50;

static LOG_RING: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();

fn log_ring() -> &'static Mutex<VecDeque<String>> {
    LOG_RING.get_or_init(|| Mutex::new(VecDeque::with_capacity(LOG_RING_CAPACITY)))
}

/// The last `n` log lines captured by the ring layer, oldest first. Empty
/// when the layer isn't installed (unit tests) or nothing was logged yet.
pub fn recent_logs(n: usize) -> Vec<String> {
    match log_ring().lock() {
        Ok(lines) => {
            let skip = lines.len().saturating_sub(n.min(LOG_RING_CAPACITY));
            lines.iter().skip(skip).cloned().collect()
        }
        Err(_) => Vec::new(),
    }
}

/// Appends one formatted line, evicting the oldest line at capacity.
fn push_line(lines: &mut VecDeque<String>, line: String) {
    if lines.len() >= LOG_RING_CAPACITY {
        lines.pop_front();
    }
    lines.push_back(line);
}

/// Extracts the message plus any extra fields from a tracing event.
#[derive(Default)]
struct EventVisitor {
    message: String,
    fields: String,
}

impl tracing::field::Visit for EventVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else {
            if !self.fields.is_empty() {
                self.fields.push(' ');
            }
            self.fields
                .push_str(&format!("{}={:?}", field.name(), value));
        }
    }
}

/// A tracing layer keeping the daemon's last LOG_RING_CAPACITY log lines in a
/// global ring buffer, served by the control socket's diagnostics command
/// (control.rs). The global EnvFilter short-circuits filtered-out events
/// before they reach this layer, so the hot path (QUIC/input debugging
/// volume) never pays for it; per kept event it's one string format and one
/// short-held mutex push.
struct RingBufferLayer;

impl<S: tracing::Subscriber> Layer<S> for RingBufferLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);
        let metadata = event.metadata();
        let line = if visitor.fields.is_empty() {
            format!(
                "{} {}: {}",
                metadata.level(),
                metadata.target(),
                visitor.message
            )
        } else {
            format!(
                "{} {}: {} {}",
                metadata.level(),
                metadata.target(),
                visitor.message,
                visitor.fields
            )
        };
        if let Ok(mut lines) = log_ring().lock() {
            push_line(&mut lines, line);
        }
    }
}

pub fn init_logging() {
    let filter_layer = EnvFilter::try_from_env("LOG_LEVEL")
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("Failed to initialize filter layer")
        // quinn_proto: Gets very noisy when LOG_LEVEL=trace
        .add_directive(
            "quinn_proto=info"
                .parse()
                .expect("Failed to parse quinn_proto directive"),
        );

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    tracing_subscriber::registry()
        // The filter applies globally, so the ring buffer sees exactly what
        // the stderr log shows.
        .with(filter_layer)
        .with(
            tracing_subscriber::fmt::layer().with_writer(std::io::stderr),
        )
        .with(RingBufferLayer)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_is_bounded_and_drops_oldest() {
        let mut lines = VecDeque::new();
        for i in 0..(LOG_RING_CAPACITY + 50) {
            push_line(&mut lines, format!("line {}", i));
        }
        assert_eq!(lines.len(), LOG_RING_CAPACITY);
        assert_eq!(lines.front().unwrap(), "line 50");
        assert_eq!(lines.back().unwrap(), &format!("line {}", LOG_RING_CAPACITY + 49));
    }

    #[test]
    fn recent_logs_returns_the_tail() {
        use tracing_subscriber::layer::SubscriberExt;
        // The ring is global; a scoped subscriber with the layer installed
        // lets us verify capture without disturbing other tests' output.
        let marker = format!("ring-layer-test-{}", std::process::id());
        let subscriber = tracing_subscriber::registry().with(RingBufferLayer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("{}", marker);
        });
        let logs = recent_logs(RECENT_LOGS_DEFAULT);
        assert!(
            logs.iter().any(|l| l.contains(&marker)),
            "the marker line must be captured: {:?}",
            logs
        );
        // Captured lines carry level and target like the stderr log.
        let line = logs.iter().find(|l| l.contains(&marker)).unwrap();
        assert!(line.contains("INFO"), "{}", line);
        // recent_logs honors the requested tail length.
        assert!(recent_logs(0).is_empty());
        assert!(recent_logs(1).len() <= 1);
    }
}
