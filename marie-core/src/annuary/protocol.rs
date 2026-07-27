use serde::{Deserialize, Serialize};

use crate::{events::Event, node::NodeId};

#[derive(Clone, Serialize, Deserialize)]
pub enum AnnuaryEvent {
    NodeConnected(NodeId),
    NodeDisconnected(NodeId)
}

impl Event for AnnuaryEvent {
    const TOPIC: &str = "/marie/annuary";

    fn id(&self) -> String {
        String::default()
    }

    fn topics(&self) -> Vec<String> {
        vec![Self::TOPIC.to_string()]
    }
}