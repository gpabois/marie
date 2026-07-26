use chrono::Utc;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::{id::ID, session::{Session, SessionId}};

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
    Acquire { holder: PeerId, ttl: chrono::Duration },
    Renew { holder: PeerId, epoch: u64, ttl: chrono::Duration  },
    Release { holder: PeerId, epoch: u64 },
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
    Denied { current_holder: PeerId, current_epoch: u64 },
    NotLeader { leader_hint: Option<PeerId> },
}

