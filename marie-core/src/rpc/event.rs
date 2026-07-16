use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::id::ID;

pub struct RpcEvent {
    pub(crate) id: String,
    pub(crate) source: PeerId,
    pub(crate) kind: RpcEventKind
}

impl RpcEvent {
    pub const TOPIC_PREFIX: &str = "marie/rpc/events";
    
    pub fn topic(&self) -> String {
        use RpcEventKind::*;

        match self.kind {
            Metrics() => String::from("marie/rpc/events/metrics"),
            Joined {..} => String::from("marie/rpc/events/server/joined"),
            CanExecute(_) => String::from("marie/rpc/events/server/can-execute"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum RpcEventKind {
    Metrics(),
    CanExecute(Vec<String>),
    Joined {
        can_execute: Vec<String>
    }
}
