use libp2p::PeerId;
use serde::{Deserialize, Serialize};

pub mod layers;

#[derive(Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub source: Option<PeerId>
}

enum Command {
    Subscribe(String),
    Unsubscribe(String)
}

pub struct PubSub;