use libp2p::PeerId;
use serde::{Deserialize, Serialize};

pub mod layer;

#[derive(Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub source: PeerId
}