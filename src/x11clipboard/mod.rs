pub mod reader;
pub mod writer;

mod convert;
mod limited_cursor;
mod shared;

pub struct ClipboardData {
    /// The type that this data is associated with, the format it should be returned as.
    pub requested_type: String,

    /// The type that is actually present in data, if it's different from requested_type.
    /// For example, if the data is compressed text/plain then this is the type of compression.
    pub data_type: Option<String>,

    /// The retrieved data
    pub data: Vec<u8>,

    /// Zero once the data is retrieved
    pub remaining_bytes: usize,
}
