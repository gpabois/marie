use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum RawMessage {
    Text(String),
    Bytes(Vec<u8>)
}

/// Muxed message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Muxed {
    pub channel: String,
    pub payload: Vec<u8>
}
