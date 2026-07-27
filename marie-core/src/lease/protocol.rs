use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{id::ID, node::NodeId, session::SessionId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaseMessage {
    Request(LeaseRequest),
    Response(LeaseResponse)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseRequest {
    pub request_id: ID,
    pub session_id: SessionId,
    pub op: LeaseOp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaseOp {
    Acquire { holder: NodeId, ttl: chrono::Duration },
    Renew { holder: NodeId, epoch: u64, ttl: chrono::Duration  },
    Release { holder: NodeId, epoch: u64 },
}
 
 #[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseResponse {
    pub request_id: ID,
    pub result: LeaseResult,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaseResult {
    Granted { epoch: u64, expires_at: chrono::DateTime<Utc> },
    Renewed { expires_at: chrono::DateTime<Utc> },
    Denied { current_holder: NodeId, current_epoch: u64 },
    NotLeader { leader_hint: Option<NodeId> },
}

