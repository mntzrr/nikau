use std::io::{Cursor, Error, ErrorKind, Result, Seek, SeekFrom, Write};

/// A wrapper around Cursor that checks the underlying buffer isn't exceeding a max size.
/// This is specifically needed to avoid borrow checker issues around being able to check the buf size.
pub struct LimitedCursor {
    inner: Cursor<Vec<u8>>,
    limit: u64,
}

impl LimitedCursor {
    pub fn new(limit: u64) -> Self {
        Self {
            inner: Cursor::new(vec![]),
            limit
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.inner.into_inner()
    }
}

impl Seek for LimitedCursor {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        // Underlying implementation doesn't seem to alloc on seek - just updates offsets.
        // So let's wait until there's a write() to check limits.
        self.inner.seek(pos)
    }

    fn stream_position(&mut self) -> Result<u64> {
        self.inner.stream_position()
    }
}

impl Write for LimitedCursor {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let length = self.inner.position() + buf.len() as u64;
        if length > self.limit {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Write of {} bytes at position {} would exceed size limit {}", buf.len(), self.inner.position(), self.limit)
            ));
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}
