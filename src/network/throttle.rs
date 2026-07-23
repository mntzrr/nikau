//! Token-bucket pacing for bulk (clipboard) transfers (--bulk-throttle-mbps).

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::trace;

/// Debts below this are carried, not slept: sub-millisecond sleeps only add
/// scheduler jitter without meaningfully draining the driver queue. The
/// carried debt is amortized into the next frame's pause instead.
const MIN_SLEEP: Duration = Duration::from_millis(1);

/// Token-bucket pacer for a bulk writer task.
///
/// quinn's stream priorities (the bulk stream yields to the events stream)
/// only order data INSIDE the QUIC connection; the queue that actually adds
/// latency on WiFi sits below that — the kernel TX path and the 802.11
/// driver/firmware queue are FIFO. A multi-MB clipboard transfer fills them,
/// and an input packet landing behind it waits for the whole backlog to
/// drain (bufferbloat: hundreds of ms of added RTT for the duration of the
/// transfer). Pacing the bulk stream under the link rate keeps those queues
/// short, so input latency survives a big clipboard transfer — at the cost
/// of the transfer taking longer (5 MB ≈ 1 s at 40 Mbps).
///
/// One pacer per writer task, charged AFTER each frame is written: a frame
/// always goes out whole and immediately (frames are never split, and a lone
/// frame — a header or another small message — is never delayed); pacing
/// only inserts quiet time BETWEEN frames, so the FIFO drains before the
/// next payload arrives.
pub struct BulkThrottle {
    /// Send-time cost of one byte at the configured rate.
    secs_per_byte: f64,
    /// Unpaid send-time: the writer is this far ahead of the configured rate
    /// and owes the link this much quiet time before the next frame.
    debt_secs: f64,
    /// When the debt was last charged/repaid (idle time pays it down).
    last: Instant,
}

impl BulkThrottle {
    /// A pacer for `mbps` megabits (1e6 bits) per second.
    pub fn new(mbps: f64) -> Self {
        BulkThrottle {
            secs_per_byte: 8.0 / (mbps * 1_000_000.0),
            debt_secs: 0.0,
            last: Instant::now(),
        }
    }

    /// Charges a just-written frame of `len` bytes and returns how long to
    /// sleep before writing the next one (ZERO while the carried debt is
    /// below MIN_SLEEP). Idle time since the last charge repays debt first;
    /// it never builds credit, so an idle link doesn't bank a full-speed blast.
    pub fn charge(&mut self, len: usize, now: Instant) -> Duration {
        self.debt_secs = (self.debt_secs - now.duration_since(self.last).as_secs_f64()).max(0.0);
        self.last = now;
        self.debt_secs += len as f64 * self.secs_per_byte;
        if self.debt_secs < MIN_SLEEP.as_secs_f64() {
            Duration::ZERO
        } else {
            let sleep = Duration::from_secs_f64(self.debt_secs);
            self.debt_secs = 0.0;
            sleep
        }
    }
}

/// Spawns the dedicated writer task for a connection's bulk stream, shared by
/// the server (one per client) and the client. Clipboard payloads can be
/// megabytes, and writing them inline would suspend the owner's event loop —
/// including input handling — for the whole transfer, so the event loop
/// queues whole frames (each a header glued to its payload) and the task
/// writes them sequentially, which also keeps overlapping transfers from
/// interleaving on the stream. Large frames are paced (see BulkThrottle) so a
/// transfer can't fill the kernel/WiFi driver FIFO ahead of latency-sensitive
/// input; the wayland pre-fetch — pulling the whole clipboard, up to the max
/// size, on every advertisement — flows through this same writer, so it is
/// paced too. The task exits when the last sender is dropped (connection
/// teardown) or when the stream fails; a write failure runs
/// `on_failure(frame_len, error)` — the server removes the client over it,
/// the client only logs (a broken stream also fails its step loop's read
/// side, which resets the connection).
pub fn spawn_bulk_writer<F, Fut>(
    mut send_stream: quinn::SendStream,
    mut rx: mpsc::Receiver<Vec<u8>>,
    throttle_mbps: Option<f64>,
    peer: std::net::SocketAddr,
    on_failure: F,
) where
    F: FnOnce(usize, quinn::WriteError) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut throttle = throttle_mbps.map(BulkThrottle::new);
    tokio::task::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            trace!("Sending {} byte bulk message to {}", bytes.len(), peer);
            if let Err(e) = send_stream.write_all(&bytes).await {
                on_failure(bytes.len(), e).await;
                return;
            }
            if let Some(throttle) = &mut throttle {
                let wait = throttle.charge(bytes.len(), Instant::now());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_conversion_paces_a_large_frame() {
        // 40 Mbps = 5,000,000 bytes/s: a 5 MB frame costs just over 1 s.
        let mut t = BulkThrottle::new(40.0);
        let t0 = Instant::now();
        let sleep = t.charge(5 * 1024 * 1024, t0);
        let expected = 5.0 * 1024.0 * 1024.0 / 5_000_000.0;
        assert!(
            (sleep.as_secs_f64() - expected).abs() < 1e-9,
            "5 MB at 40 Mbps should sleep ~{expected}s, got {:?}",
            sleep
        );
    }

    #[test]
    fn small_frames_pass_immediately() {
        // A 100-byte header at 40 Mbps costs 20µs: below MIN_SLEEP, carried.
        let mut t = BulkThrottle::new(40.0);
        let t0 = Instant::now();
        assert_eq!(t.charge(100, t0), Duration::ZERO);
    }

    #[test]
    fn deficit_carries_until_one_real_sleep() {
        // Back-to-back small frames accumulate their sub-millisecond costs;
        // once the debt reaches MIN_SLEEP it is paid with a single sleep.
        let mut t = BulkThrottle::new(40.0);
        let t0 = Instant::now();
        let mut slept = Duration::ZERO;
        for i in 0..50 {
            let sleep = t.charge(100, t0);
            if i < 49 {
                assert_eq!(sleep, Duration::ZERO, "frame {i} should still pass");
            } else {
                slept = sleep;
            }
        }
        // 50 x 20µs = 1ms, paid at once on the frame that crosses MIN_SLEEP.
        assert_eq!(slept, Duration::from_millis(1));
        // The debt is cleared by the sleep: the next small frame passes again.
        assert_eq!(t.charge(100, t0), Duration::ZERO);
    }

    #[test]
    fn idle_time_repays_debt_without_banking_credit() {
        let mut t = BulkThrottle::new(40.0);
        let t0 = Instant::now();
        // 1 ms of debt, slept off.
        assert_eq!(t.charge(5000, t0), Duration::from_millis(1));
        // A second of idle fully repays any leftover sub-millisecond debt...
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(t.charge(100, t1), Duration::ZERO);
        // ...but banks no credit: the next large frame still paces in full.
        let sleep = t.charge(5 * 1024 * 1024, t1);
        assert!(sleep > Duration::from_secs(1));
    }

    #[test]
    fn steady_state_matches_the_configured_rate() {
        // Each frame paces by exactly its own cost; real write time between
        // frames only makes the wall-clock rate slightly UNDER the configured
        // one — the right side to err on.
        let mut t = BulkThrottle::new(40.0);
        let t0 = Instant::now();
        let frame = 1024 * 1024;
        let mut now = t0;
        let mut total_sleep = Duration::ZERO;
        for _ in 0..4 {
            let sleep = t.charge(frame, now);
            total_sleep += sleep;
            // Advance past the sleep plus a little write time.
            now += sleep + Duration::from_millis(1);
        }
        // 4 x 1MB frames at 5,000,000 bytes/s = 4 x 0.2097152s of pacing.
        let expected = 4.0 * 1024.0 * 1024.0 / 5_000_000.0;
        assert!(
            (total_sleep.as_secs_f64() - expected).abs() < 1e-9,
            "4 MB at 40 Mbps should pace to {expected}s, got {:?}",
            total_sleep
        );
    }
}
