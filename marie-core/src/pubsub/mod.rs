use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::id::ID;

pub mod layer;

#[derive(Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub source: Option<PeerId>
}