use serde::{Deserialize, Serialize};

use crate::{hitl::{Hitl, HitlId}, session::SessionId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingHitl {
    pub session_id: SessionId,
    pub id: HitlId,
    pub hitl: Hitl
}
