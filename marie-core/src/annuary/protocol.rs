use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::events::Event;

#[derive(Clone, Serialize, Deserialize)]
pub enum AnnuaryEvent {
    NodeConnected(PeerId),
    NodeDisconnected(PeerId)
}

impl Event for AnnuaryEvent {
    const TOPIC: &str = "/marie/annuary";

    fn id(&self) -> &str {
        ""
    }

    fn topics(&self) -> Vec<String> {
        vec![Self::TOPIC.to_string()]
    }
}