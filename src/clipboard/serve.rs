use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tracing::debug;

use crate::clipboard::{ClipboardReader, convert};

/// A clipboard reader shared with spawned clipboard-serving tasks.
///
/// Serving a request can be slow and CPU-heavy: when the clipboard holds a
/// copied file, the payload is zipped from disk, and compressing a large
/// clipboard takes noticeable time. Clipboard managers often request every
/// advertised mime type at once and retry on timeout, so without protection
/// the same slow read+convert would run many times concurrently, saturating
/// the CPU and starving the input-forwarding loops (seen in the wild as
/// keyboards freezing on both machines).
///
/// This type serializes serves and caches the last served payload, so a
/// burst of requests for the same clipboard costs only one slow read+convert.
pub struct SharedClipboardReader {
    reader: Box<dyn ClipboardReader>,
    /// (requested_type, content, data_type) of the last successful serve.
    /// Single slot: requests within a burst are for the same clipboard.
    last_served: Option<(String, Vec<u8>, Option<String>)>,
}

impl SharedClipboardReader {
    pub fn new(reader: Box<dyn ClipboardReader>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            reader,
            last_served: None,
        }))
    }

    /// Drops the cached payload. Must be called when the local clipboard
    /// contents change, so stale data is never served.
    pub fn invalidate(&mut self) {
        self.last_served = None;
    }

    /// Reads and converts the clipboard for the specified type, serving from
    /// the cache when a request for the same type was just fulfilled.
    pub async fn read(
        &mut self,
        requested_type: &str,
        max_size_bytes: u64,
        request_source: &str,
    ) -> Result<(Vec<u8>, Option<String>)> {
        if let Some((cached_type, content, data_type)) = &self.last_served {
            if cached_type == requested_type {
                debug!(
                    "Serving clipboard type {} from cache for {}: {} bytes",
                    requested_type,
                    request_source,
                    content.len()
                );
                return Ok((content.clone(), data_type.clone()));
            }
        }
        let original_data = self
            .reader
            .read(requested_type, max_size_bytes, request_source)
            .await?;
        let (content, data_type) = convert::read(original_data, max_size_bytes, requested_type).await?;
        self.last_served = Some((
            requested_type.to_string(),
            content.clone(),
            data_type.clone(),
        ));
        Ok((content, data_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingReader {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ClipboardReader for CountingReader {
        async fn read(
            &mut self,
            requested_type: &str,
            _max_size_bytes: u64,
            _request_source: &str,
        ) -> Result<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(format!("data-for-{}", requested_type).into_bytes())
        }
    }

    #[tokio::test]
    async fn repeated_requests_hit_the_cache() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = SharedClipboardReader::new(Box::new(CountingReader {
            calls: calls.clone(),
        }));
        let (content, _) = reader
            .lock()
            .await
            .read("text/plain", u64::MAX, "test")
            .await
            .unwrap();
        assert_eq!(content, b"data-for-text/plain");
        // Second request for the same type must not hit the system clipboard again.
        let (content, _) = reader
            .lock()
            .await
            .read("text/plain", u64::MAX, "test")
            .await
            .unwrap();
        assert_eq!(content, b"data-for-text/plain");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // A different type misses the single-slot cache and reads again.
        let _ = reader
            .lock()
            .await
            .read("text/html", u64::MAX, "test")
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidation_forces_a_fresh_read() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = SharedClipboardReader::new(Box::new(CountingReader {
            calls: calls.clone(),
        }));
        let _ = reader
            .lock()
            .await
            .read("text/plain", u64::MAX, "test")
            .await
            .unwrap();
        reader.lock().await.invalidate();
        let _ = reader
            .lock()
            .await
            .read("text/plain", u64::MAX, "test")
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
